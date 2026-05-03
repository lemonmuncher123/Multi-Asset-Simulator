"""Tests for sell quantity validation across ledger, trade_preview, holdings, and import."""

import pytest
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.transaction_repo import list_transactions
from src.engines import ledger
from src.engines.holdings import (
    get_asset_quantity, has_sufficient_quantity, find_negative_positions,
    SELLABLE_ASSET_TYPES,
)
from src.engines.trade_preview import (
    TradeDraft, prepare_trade_preview, confirm_trade,
)
from src.engines.import_export import import_transactions_csv, ImportResult
from src.data_sources.price_provider import PriceProvider, PriceRecord, QuoteRecord


# ---------------------------------------------------------------------------
# Mock provider for trade preview tests
# ---------------------------------------------------------------------------

class SimpleQuoteProvider(PriceProvider):
    def source_name(self):
        return "test"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return [PriceRecord(symbol=symbol, date="2025-06-01",
                            close=100, adjusted_close=100, source=self.source_name())]

    def fetch_latest_quote(self, symbol):
        return QuoteRecord(symbol=symbol, bid=99.0, ask=101.0, last=100.0,
                           timestamp="2025-06-01T14:30:00", source=self.source_name())


def _providers():
    p = SimpleQuoteProvider()
    return {"stock": p, "etf": p, "crypto": p}


def _setup(db_conn, symbol="AAPL", asset_type="stock", buy_qty=10, buy_price=100.0):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol=symbol, name=f"Test {symbol}", asset_type=asset_type))
    if buy_qty > 0:
        ledger.buy(db_conn, "2025-01-15", asset.id, buy_qty, buy_price)
    return asset


# ===================================================================
# 1. Holdings helper tests
# ===================================================================

class TestGetAssetQuantity:

    def test_no_transactions_returns_zero(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        assert get_asset_quantity(db_conn, asset.id) == 0.0

    def test_after_buy(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        ledger.buy(db_conn, "2025-01-15", asset.id, 10, 100.0)
        assert get_asset_quantity(db_conn, asset.id) == 10.0

    def test_after_buy_and_partial_sell(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        ledger.sell(db_conn, "2025-02-01", asset.id, 3, 110.0)
        assert get_asset_quantity(db_conn, asset.id) == 7.0

    def test_after_full_sell(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        ledger.sell(db_conn, "2025-02-01", asset.id, 10, 110.0)
        assert get_asset_quantity(db_conn, asset.id) == 0.0

    def test_as_of_date_before_buy(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        assert get_asset_quantity(db_conn, asset.id, as_of_date="2025-01-10") == 0.0

    def test_as_of_date_after_buy(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        assert get_asset_quantity(db_conn, asset.id, as_of_date="2025-01-20") == 10.0

    def test_as_of_date_respects_sell_date(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        ledger.sell(db_conn, "2025-02-01", asset.id, 5, 110.0)
        assert get_asset_quantity(db_conn, asset.id, as_of_date="2025-01-20") == 10.0
        assert get_asset_quantity(db_conn, asset.id, as_of_date="2025-02-01") == 5.0

    def test_crypto_fractional(self, db_conn):
        asset = _setup(db_conn, symbol="BTC", asset_type="crypto", buy_qty=0.5, buy_price=40000.0)
        assert get_asset_quantity(db_conn, asset.id) == pytest.approx(0.5)

    def test_nonexistent_asset_returns_zero(self, db_conn):
        assert get_asset_quantity(db_conn, 9999) == 0.0


class TestHasSufficientQuantity:

    def test_sufficient(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        assert has_sufficient_quantity(db_conn, asset.id, 5) is True

    def test_exact(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        assert has_sufficient_quantity(db_conn, asset.id, 10) is True

    def test_insufficient(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        assert has_sufficient_quantity(db_conn, asset.id, 15) is False

    def test_no_holdings(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="X", name="X", asset_type="stock"))
        assert has_sufficient_quantity(db_conn, asset.id, 1) is False

    def test_with_as_of_date(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        assert has_sufficient_quantity(db_conn, asset.id, 5, as_of_date="2025-01-10") is False
        assert has_sufficient_quantity(db_conn, asset.id, 5, as_of_date="2025-01-20") is True


class TestFindNegativePositions:

    def test_no_negatives(self, db_conn):
        _setup(db_conn, buy_qty=10)
        assert find_negative_positions(db_conn) == []

    def test_empty_db(self, db_conn):
        assert find_negative_positions(db_conn) == []


# ===================================================================
# 2. Ledger sell validation tests
# ===================================================================

class TestLedgerSellValidation:

    def test_sell_without_prior_buy_raises(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        with pytest.raises(ValueError, match="no position"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 5, 100.0)

    def test_sell_more_than_held_raises(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        with pytest.raises(ValueError, match="Insufficient quantity"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 15, 100.0)

    def test_sell_exact_quantity_succeeds(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        txn = ledger.sell(db_conn, "2025-02-01", asset.id, 10, 110.0)
        assert txn.quantity == 10

    def test_sell_partial_quantity_succeeds(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        txn = ledger.sell(db_conn, "2025-02-01", asset.id, 3, 110.0)
        assert txn.quantity == 3

    def test_sell_zero_quantity_raises(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        with pytest.raises(ValueError, match="quantity must be positive"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 0, 100.0)

    def test_sell_negative_quantity_raises(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        with pytest.raises(ValueError, match="quantity must be positive"):
            ledger.sell(db_conn, "2025-02-01", asset.id, -5, 100.0)

    def test_sell_zero_price_raises(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        with pytest.raises(ValueError, match="price must be positive"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 5, 0)

    def test_sell_negative_fees_raises(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        with pytest.raises(ValueError, match="Fees cannot be negative"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 5, 100.0, fees=-1)

    def test_sell_nonexistent_asset_raises(self, db_conn):
        with pytest.raises(ValueError, match="not found"):
            ledger.sell(db_conn, "2025-02-01", 9999, 5, 100.0)

    def test_sell_real_estate_raises(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        asset, _, _ = ledger.add_property(
            db_conn, "2025-01-01", symbol="HOUSE", name="House",
            purchase_price=300000.0,
        )
        with pytest.raises(ValueError, match="Cannot sell asset type"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 1, 300000.0)

    def test_sell_debt_raises(self, db_conn):
        asset, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="CC", name="Card", amount=5000.0,
            payment_per_period=50.0,
        )
        with pytest.raises(ValueError, match="Cannot sell asset type"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 1, 5000.0)

    def test_sell_etf_succeeds(self, db_conn):
        asset = _setup(db_conn, symbol="SPY", asset_type="etf", buy_qty=20)
        txn = ledger.sell(db_conn, "2025-02-01", asset.id, 10, 400.0)
        assert txn.quantity == 10

    def test_sell_crypto_fractional_succeeds(self, db_conn):
        asset = _setup(db_conn, symbol="BTC", asset_type="crypto", buy_qty=1.5, buy_price=40000.0)
        txn = ledger.sell(db_conn, "2025-02-01", asset.id, 0.3, 42000.0)
        assert txn.quantity == pytest.approx(0.3)

    def test_sell_custom_asset_succeeds(self, db_conn):
        asset = _setup(db_conn, symbol="CUSTOM1", asset_type="custom", buy_qty=5)
        txn = ledger.sell(db_conn, "2025-02-01", asset.id, 2, 200.0)
        assert txn.quantity == 2

    def test_sell_respects_as_of_date(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        with pytest.raises(ValueError, match="no position"):
            ledger.sell(db_conn, "2025-01-10", asset.id, 5, 100.0)

    def test_sell_does_not_write_on_failure(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        count_before = len(list_transactions(db_conn))
        with pytest.raises(ValueError):
            ledger.sell(db_conn, "2025-02-01", asset.id, 5, 100.0)
        assert len(list_transactions(db_conn)) == count_before

    def test_multiple_partial_sells(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        ledger.sell(db_conn, "2025-02-01", asset.id, 3, 110.0)
        ledger.sell(db_conn, "2025-02-02", asset.id, 3, 115.0)
        ledger.sell(db_conn, "2025-02-03", asset.id, 4, 120.0)
        assert get_asset_quantity(db_conn, asset.id) == pytest.approx(0.0)

    def test_sell_after_multiple_buys(self, db_conn):
        asset = _setup(db_conn, buy_qty=5)
        ledger.buy(db_conn, "2025-02-01", asset.id, 5, 110.0)
        txn = ledger.sell(db_conn, "2025-03-01", asset.id, 8, 120.0)
        assert txn.quantity == 8
        assert get_asset_quantity(db_conn, asset.id) == pytest.approx(2.0)


# ===================================================================
# 3. Trade preview sell validation tests
# ===================================================================

class TestPreviewSellValidation:

    def test_sell_no_position_blocked(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01", _providers())
        assert preview.can_confirm is False
        assert any("no position" in e.lower() for e in preview.blocking_errors)

    def test_sell_more_than_held_blocked(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=15)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01", _providers())
        assert preview.can_confirm is False
        assert any("insufficient quantity" in e.lower() for e in preview.blocking_errors)

    def test_sell_exact_quantity_allowed(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=10)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01", _providers())
        assert preview.can_confirm is True

    def test_sell_partial_quantity_allowed(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01", _providers())
        assert preview.can_confirm is True

    def test_sell_zero_quantity_blocked(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=0)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01", _providers())
        assert preview.can_confirm is False
        assert any("positive" in e.lower() for e in preview.blocking_errors)

    def test_sell_non_sellable_type_blocked(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        asset = create_asset(db_conn, Asset(symbol="HOME", name="House", asset_type="real_estate"))
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=1)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01", _providers())
        assert preview.can_confirm is False
        assert any("cannot sell" in e.lower() for e in preview.blocking_errors)

    def test_sell_crypto_fractional_allowed(self, db_conn):
        asset = _setup(db_conn, symbol="BTC", asset_type="crypto", buy_qty=2.0, buy_price=40000.0)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=0.5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01", _providers())
        assert preview.can_confirm is True

    def test_confirm_blocked_sell_returns_false(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01", _providers())
        assert preview.can_confirm is False
        result = confirm_trade(db_conn, preview, "2025-06-01")
        assert result is False

    def test_confirm_blocked_sell_no_transaction_saved(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        count_before = len(list_transactions(db_conn))
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01", _providers())
        confirm_trade(db_conn, preview, "2025-06-01")
        assert len(list_transactions(db_conn)) == count_before

    def test_buy_preview_still_works(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01", _providers())
        assert preview.can_confirm is True
        assert preview.price_source == "quote_ask"


# ===================================================================
# 4. Import sell validation tests
# ===================================================================

class TestImportSellValidation:

    def test_import_sell_without_prior_buy_skipped(self, db_conn):
        create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        csv_text = (
            "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
            "2025-02-01,sell,AAPL,5,100,500,USD,0,\n"
        )
        result = import_transactions_csv(db_conn, csv_text)
        assert result.imported == 0
        assert any("no position" in e.lower() for e in result.errors)

    def test_import_sell_more_than_held_skipped(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        ledger.buy(db_conn, "2025-01-15", asset.id, 10, 100.0)
        csv_text = (
            "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
            "2025-02-01,sell,AAPL,15,100,1500,USD,0,\n"
        )
        result = import_transactions_csv(db_conn, csv_text)
        assert result.imported == 0
        assert any("insufficient" in e.lower() for e in result.errors)

    def test_import_buy_then_sell_succeeds(self, db_conn):
        create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        csv_text = (
            "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
            "2025-01-01,deposit_cash,,,,100000,USD,0,\n"
            "2025-01-15,buy,AAPL,10,100,-1000,USD,0,\n"
            "2025-02-01,sell,AAPL,5,110,550,USD,0,\n"
        )
        result = import_transactions_csv(db_conn, csv_text)
        assert result.imported == 3
        assert len(result.errors) == 0

    def test_import_sequential_validation(self, db_conn):
        create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        csv_text = (
            "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
            "2025-01-01,deposit_cash,,,,5000,USD,0,\n"
            "2025-01-15,buy,AAPL,10,100,-1000,USD,0,\n"
            "2025-02-01,sell,AAPL,5,110,550,USD,0,\n"
            "2025-02-15,sell,AAPL,5,120,600,USD,0,\n"
            "2025-03-01,sell,AAPL,1,130,130,USD,0,\n"
        )
        result = import_transactions_csv(db_conn, csv_text)
        # deposit + buy + 2 sells succeed; the third sell hits "no position".
        assert result.imported == 4
        assert any("no position" in e.lower() for e in result.errors)

    def test_import_sell_non_sellable_type_skipped(self, db_conn):
        create_asset(db_conn, Asset(symbol="HOME", name="House", asset_type="real_estate"))
        csv_text = (
            "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
            "2025-02-01,sell,HOME,1,300000,300000,USD,0,\n"
        )
        result = import_transactions_csv(db_conn, csv_text)
        assert result.imported == 0
        assert any("cannot sell" in e.lower() for e in result.errors)

    def test_import_valid_rows_not_affected_by_invalid(self, db_conn):
        create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        csv_text = (
            "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
            "2025-01-01,deposit_cash,,,,50000,USD,0,\n"
            "2025-01-15,buy,AAPL,10,100,-1000,USD,0,\n"
            "2025-02-01,sell,AAPL,20,110,2200,USD,0,\n"
            "2025-02-02,sell,AAPL,5,110,550,USD,0,\n"
        )
        result = import_transactions_csv(db_conn, csv_text)
        assert result.imported == 3
        assert len(result.errors) == 1


# ===================================================================
# 6. Edge cases
# ===================================================================

class TestEdgeCases:

    def test_sell_on_same_date_as_buy(self, db_conn):
        asset = _setup(db_conn, buy_qty=10)
        txn = ledger.sell(db_conn, "2025-01-15", asset.id, 5, 110.0)
        assert txn.quantity == 5

    def test_crypto_fractional_epsilon(self, db_conn):
        asset = _setup(db_conn, symbol="ETH", asset_type="crypto", buy_qty=1.0, buy_price=3000.0)
        ledger.sell(db_conn, "2025-02-01", asset.id, 0.333333333, 3100.0)
        ledger.sell(db_conn, "2025-02-02", asset.id, 0.333333333, 3200.0)
        ledger.sell(db_conn, "2025-02-03", asset.id, 0.333333334, 3300.0)
        remaining = get_asset_quantity(db_conn, asset.id)
        assert remaining == pytest.approx(0.0, abs=1e-8)

    def test_sellable_asset_types_constant(self):
        assert SELLABLE_ASSET_TYPES == {"stock", "etf", "crypto", "custom"}
