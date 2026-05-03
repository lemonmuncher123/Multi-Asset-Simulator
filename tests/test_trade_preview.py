import pytest
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.price_repo import upsert_ohlcv
from src.storage.quote_repo import upsert_quote, get_latest_quote_record
from src.storage.transaction_repo import list_transactions
from src.engines import ledger
from src.engines.ledger import deposit_cash, buy
from src.engines.portfolio import calc_cash_balance
from src.engines.trade_preview import (
    TradeDraft, TradePreview, prepare_trade_preview, confirm_trade,
)
from src.data_sources.price_provider import PriceProvider, PriceRecord, QuoteRecord, ProviderUnavailableError


# --- Mock providers ---

class MockStockProvider(PriceProvider):
    def __init__(self, fail_symbols=None):
        self._fail_symbols = fail_symbols or set()

    def source_name(self):
        return "mock_stock"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        if symbol in self._fail_symbols:
            raise ValueError(f"Mock failure for {symbol}")
        return [
            PriceRecord(symbol=symbol, date="2025-01-15", open=148, high=155,
                        low=147, close=152, adjusted_close=152, volume=1e6,
                        source=self.source_name()),
        ]

    def fetch_latest_quote(self, symbol):
        if symbol in self._fail_symbols:
            return None
        return QuoteRecord(
            symbol=symbol, bid=151.50, ask=152.50, last=152.0,
            timestamp="2025-01-15T10:00:00", source=self.source_name(),
        )


class MockCryptoProvider(PriceProvider):
    def source_name(self):
        return "mock_crypto"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return [
            PriceRecord(symbol=symbol, date="2025-01-15", open=40000, high=41000,
                        low=39500, close=40500, adjusted_close=40500, volume=5000,
                        source=self.source_name()),
        ]

    def fetch_latest_quote(self, symbol):
        return QuoteRecord(
            symbol=symbol, bid=40400, ask=40600, last=40500,
            timestamp="2025-01-15T10:00:00", source=self.source_name(),
        )


def _providers(fail_symbols=None):
    return {
        "stock": MockStockProvider(fail_symbols),
        "etf": MockStockProvider(fail_symbols),
        "crypto": MockCryptoProvider(),
    }


def _setup_portfolio(db_conn, symbol="AAPL", asset_type="stock"):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol=symbol, name=f"Test {symbol}", asset_type=asset_type))
    buy(db_conn, "2025-01-02", asset.id, 10, 150.0)
    return asset


# --- 1. Buy stock uses ask price from quote ---

def test_preview_buy_stock_uses_ask(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.price_source == "quote_ask"
    assert preview.trade_price == 152.50
    assert preview.execution_side == "ask"
    assert preview.bid_price == 151.50
    assert preview.ask_price == 152.50
    assert preview.last_price == 152.0
    assert preview.quote_time == "2025-01-15T10:00:00"
    assert preview.can_confirm is True
    assert len(preview.blocking_errors) == 0


# --- 2. Sell stock uses bid price from quote ---

def test_preview_sell_stock_uses_bid(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.price_source == "quote_bid"
    assert preview.trade_price == 151.50
    assert preview.execution_side == "bid"
    assert preview.bid_price == 151.50
    assert preview.ask_price == 152.50
    assert preview.can_confirm is True


# --- 3. Buy ETF uses ask from quote ---

def test_preview_buy_etf_uses_ask(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="SPY", name="S&P 500 ETF", asset_type="etf"))
    buy(db_conn, "2025-01-02", asset.id, 5, 400.0)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=2)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.price_source == "quote_ask"
    assert preview.execution_side == "ask"
    assert preview.asset_type == "etf"
    assert preview.can_confirm is True


# --- 4. Buy crypto uses ask from quote ---

def test_preview_buy_crypto_uses_ask(db_conn):
    deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.price_source == "quote_ask"
    assert preview.trade_price == 40600.0
    assert preview.execution_side == "ask"
    assert preview.can_confirm is True


# --- 5. Sell crypto uses bid from quote ---

def test_preview_sell_crypto_uses_bid(db_conn):
    deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    buy(db_conn, "2025-01-02", asset.id, 2, 40000.0)
    draft = TradeDraft(action="sell", asset_id=asset.id, quantity=1)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.price_source == "quote_bid"
    assert preview.trade_price == 40400.0
    assert preview.execution_side == "bid"
    assert preview.can_confirm is True


# --- 6. Non-syncable types still use manual price ---

def test_preview_real_estate_uses_manual(db_conn):
    deposit_cash(db_conn, "2025-01-01", 500000.0)
    asset = create_asset(db_conn, Asset(symbol="HOME", name="My House", asset_type="real_estate"))
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1, manual_price=300000)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.price_source == "manual"
    assert preview.trade_price == 300000


def test_preview_debt_uses_manual(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="LOAN", name="Car Loan", asset_type="debt"))
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1, manual_price=20000)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.price_source == "manual"


def test_preview_custom_uses_manual(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="CUST1", name="Custom", asset_type="custom"))
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1, manual_price=100)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
    assert preview.price_source == "manual"


def test_preview_cash_uses_manual(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="CASH1", name="Cash Reserve", asset_type="cash"))
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1, manual_price=1000)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
    assert preview.price_source == "manual"


# --- 7. Quote failure blocks execution for syncable types ---

def test_quote_failure_blocks_even_with_daily_prices(db_conn):
    asset = _setup_portfolio(db_conn)
    upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-01-10",
                 140, 145, 139, 142, 142, 1e6, "yfinance")

    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15",
                                    _providers(fail_symbols={"AAPL"}))

    assert preview.can_confirm is False
    assert any("quote" in e.lower() or "data sync" in e.lower()
               for e in preview.blocking_errors)


# --- 8. Quote failure with no price produces blocking error ---

def test_quote_failure_no_price_blocks(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="NEWSTOCK", name="New Stock", asset_type="stock"))

    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=10)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15",
                                    _providers(fail_symbols={"NEWSTOCK"}))

    assert preview.price_source == "missing"
    assert preview.can_confirm is False
    assert any("price" in e.lower() or "quote" in e.lower() for e in preview.blocking_errors)


# --- 9. Manual price does NOT override quote for syncable types ---

def test_manual_price_does_not_override_quote(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5, manual_price=160.0)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.price_source == "quote_ask"
    assert preview.trade_price == 152.50


# --- 10. Manual price cannot rescue syncable types ---

def test_manual_price_cannot_rescue_syncable(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5, manual_price=150.0)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _unavailable_providers())

    assert preview.can_confirm is False
    assert any("quote" in e.lower() or "data sync" in e.lower()
               for e in preview.blocking_errors)


# --- 11. Cash before and cash after ---

def test_cash_before_and_after_buy(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5, fee=10.0)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    expected_cash_before = 100000.0 - (10 * 150.0)  # 98500
    assert preview.cash_before == pytest.approx(expected_cash_before)
    expected_cost = 5 * 152.50 + 10.0
    assert preview.cash_after == pytest.approx(expected_cash_before - expected_cost)


def test_cash_before_and_after_sell(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5, fee=10.0)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    expected_cash_before = 98500.0
    assert preview.cash_before == pytest.approx(expected_cash_before)
    expected_proceeds = 5 * 151.50 - 10.0
    assert preview.cash_after == pytest.approx(expected_cash_before + expected_proceeds)


# --- 12. Allocation before and after ---

def test_allocation_before_and_after(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert isinstance(preview.allocation_before, dict)
    assert isinstance(preview.allocation_after, dict)
    assert len(preview.allocation_before) > 0
    assert len(preview.allocation_after) > 0


# --- 13. Risk warnings before and after ---

def test_risk_warnings_generated(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert isinstance(preview.risk_warnings_before, list)
    assert isinstance(preview.risk_warnings_after, list)
    assert isinstance(preview.risk_changes_summary, list)


# --- 14. Transaction is NOT saved during preview ---

def test_preview_does_not_save_transaction(db_conn):
    asset = _setup_portfolio(db_conn)
    txns_before = list_transactions(db_conn)
    count_before = len(txns_before)

    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    txns_after = list_transactions(db_conn)
    assert len(txns_after) == count_before


# --- 15. Transaction saved only after confirm_trade ---

def test_confirm_trade_saves_transaction(db_conn):
    asset = _setup_portfolio(db_conn)
    txns_before = list_transactions(db_conn)
    count_before = len(txns_before)

    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
    assert preview.can_confirm is True

    result = confirm_trade(db_conn, preview, "2025-01-15")
    assert result is True

    txns_after = list_transactions(db_conn)
    assert len(txns_after) == count_before + 1
    new_txn = txns_after[-1]
    assert new_txn.txn_type == "buy"
    assert new_txn.quantity == 5
    assert new_txn.price == 152.50


# --- 16. Confirm trade fails if can_confirm is false ---

def test_confirm_trade_fails_if_not_confirmable(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="BAD", name="Bad", asset_type="stock"))

    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=10)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15",
                                    _providers(fail_symbols={"BAD"}))
    assert preview.can_confirm is False

    txns_before = len(list_transactions(db_conn))
    result = confirm_trade(db_conn, preview, "2025-01-15")
    assert result is False
    assert len(list_transactions(db_conn)) == txns_before


# --- 17. Confirm sell saves correctly with bid price ---

def test_confirm_sell_trade(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="sell", asset_id=asset.id, quantity=3, fee=5.0)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
    assert preview.can_confirm is True
    assert preview.trade_price == 151.50

    result = confirm_trade(db_conn, preview, "2025-01-15")
    assert result is True

    cash_after = calc_cash_balance(db_conn)
    expected = 98500.0 + (3 * 151.50 - 5.0)
    assert cash_after == pytest.approx(expected)


# --- Preview does not create duplicate transactions ---

def test_preview_no_duplicate_transactions(db_conn):
    asset = _setup_portfolio(db_conn)
    count_before = len(list_transactions(db_conn))

    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
    prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
    prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert len(list_transactions(db_conn)) == count_before


# --- Confirm creates exactly one transaction ---

def test_confirm_creates_exactly_one(db_conn):
    asset = _setup_portfolio(db_conn)
    count_before = len(list_transactions(db_conn))

    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=2)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
    confirm_trade(db_conn, preview, "2025-01-15")

    assert len(list_transactions(db_conn)) == count_before + 1


# --- Price sync failure does not crash ---

def test_sync_failure_does_not_crash(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15",
                                    _providers(fail_symbols={"AAPL"}))
    assert isinstance(preview, TradePreview)


# --- Estimated trade value uses ask for buy ---

def test_estimated_trade_value(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.estimated_trade_value == pytest.approx(5 * 152.50)


# --- Total assets and net worth ---

def test_total_assets_before(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.total_assets_before > 0


def test_net_worth_before_subtracts_liabilities(db_conn):
    """Net worth must reflect liabilities; total_assets does not."""
    asset = _setup_portfolio(db_conn)
    # Add a debt of 10k to introduce a liability.
    ledger.add_debt(
        db_conn, "2025-01-10", symbol="L", name="Loan",
        amount=10000.0, interest_rate=0.0, cash_received=False,
        payment_per_period=100.0,
    )
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.net_worth_before == pytest.approx(
        preview.total_assets_before - 10000.0
    )


# --- Quote time is set when quote is used ---

def test_quote_time_set_when_quote_used(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.quote_time == "2025-01-15T10:00:00"
    assert preview.quote_source == "mock_stock"


# --- Insufficient cash blocking error ---

def test_insufficient_cash_blocking_error(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))

    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=100)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.can_confirm is False
    assert any("insufficient cash" in e.lower() for e in preview.blocking_errors)


# --- Asset not found ---

def test_asset_not_found(db_conn):
    draft = TradeDraft(action="buy", asset_id=9999, quantity=10)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.can_confirm is False
    assert any("not found" in e.lower() for e in preview.blocking_errors)


# --- Provider unavailable scenarios ---

class UnavailableProvider(PriceProvider):
    def source_name(self):
        return "unavailable"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        raise ProviderUnavailableError("yfinance is not installed")

    def fetch_latest_quote(self, symbol):
        raise ProviderUnavailableError("yfinance is not installed")


def _unavailable_providers():
    return {
        "stock": UnavailableProvider(),
        "etf": UnavailableProvider(),
        "crypto": UnavailableProvider(),
    }


def test_daily_price_fallback_blocked_when_yfinance_missing(db_conn):
    asset = _setup_portfolio(db_conn)
    upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-01-10",
                 140, 145, 139, 142, 142, 1e6, "yfinance")

    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _unavailable_providers())

    assert preview.can_confirm is False
    assert any("quote" in e.lower() or "data sync" in e.lower()
               for e in preview.blocking_errors)


def test_manual_price_blocked_for_syncable_no_yfinance(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5, manual_price=150.0)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _unavailable_providers())

    assert preview.can_confirm is False
    assert any("quote" in e.lower() or "data sync" in e.lower()
               for e in preview.blocking_errors)


def test_preview_crypto_uses_crypto_provider_quote(db_conn):
    deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.price_source == "quote_ask"
    assert preview.trade_price == 40600.0
    assert preview.asset_type == "crypto"


def test_preview_crypto_blocked_without_quote(db_conn):
    deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    upsert_ohlcv(db_conn, asset.id, "BTC", "crypto", "2025-01-10",
                 39000, 41000, 38500, 40000, 40000, 5000, "yfinance_crypto")

    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _unavailable_providers())

    assert preview.can_confirm is False
    assert any("quote" in e.lower() or "data sync" in e.lower()
               for e in preview.blocking_errors)


# --- Note preservation through confirm_trade ---

def test_confirm_trade_preserves_note(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=2, note="Test note for buy")
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.note == "Test note for buy"
    assert preview.can_confirm is True

    confirm_trade(db_conn, preview, "2025-01-15")

    txns = list_transactions(db_conn)
    confirmed = [t for t in txns if t.quantity == 2 and t.txn_type == "buy"]
    assert len(confirmed) == 1
    assert confirmed[0].notes == "Test note for buy"


def test_confirm_sell_preserves_note(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="sell", asset_id=asset.id, quantity=3, note="Selling shares")
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.note == "Selling shares"
    confirm_trade(db_conn, preview, "2025-01-15")

    txns = list_transactions(db_conn)
    sell_txns = [t for t in txns if t.txn_type == "sell"]
    assert len(sell_txns) == 1
    assert sell_txns[0].notes == "Selling shares"


def test_confirm_trade_with_no_note(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1)
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    assert preview.note is None
    confirm_trade(db_conn, preview, "2025-01-15")

    txns = list_transactions(db_conn)
    confirmed = [t for t in txns if t.quantity == 1 and t.txn_type == "buy"]
    assert len(confirmed) == 1
    assert confirmed[0].notes is None


def test_confirm_trade_rejects_non_buy_sell(db_conn):
    preview = TradePreview(action="deposit_cash", can_confirm=True)
    result = confirm_trade(db_conn, preview, "2025-01-15")
    assert result is False


# --- Quote storage ---

def test_quote_stored_after_live_fetch(db_conn):
    asset = _setup_portfolio(db_conn)
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    stored = get_latest_quote_record(db_conn, asset.id)
    assert stored is not None
    assert stored["bid"] == 151.50
    assert stored["ask"] == 152.50
    assert stored["last"] == 152.0


def test_stored_quote_used_when_live_fails(db_conn):
    asset = _setup_portfolio(db_conn)

    # First preview stores a quote
    draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

    # Second preview with failing provider uses stored quote
    preview = prepare_trade_preview(db_conn, draft, "2025-01-15",
                                    _providers(fail_symbols={"AAPL"}))
    assert preview.price_source == "quote_ask"
    assert preview.trade_price == 152.50


# --- Bid/Ask asymmetry ---

def test_buy_and_sell_use_different_prices(db_conn):
    asset = _setup_portfolio(db_conn)

    buy_draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
    buy_preview = prepare_trade_preview(db_conn, buy_draft, "2025-01-15", _providers())

    sell_draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
    sell_preview = prepare_trade_preview(db_conn, sell_draft, "2025-01-15", _providers())

    assert buy_preview.trade_price == 152.50  # ask
    assert sell_preview.trade_price == 151.50  # bid
    assert buy_preview.trade_price > sell_preview.trade_price


# ===================================================================
# Regression: sell-over-position bug — preview layer
# ===================================================================


class TestPreviewBlocksInvalidSells:
    """prepare_trade_preview must set can_confirm=False for invalid sells."""

    def test_sell_no_position_blocked(self, db_conn):
        deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
        assert preview.can_confirm is False
        assert any("no position" in e.lower() for e in preview.blocking_errors)

    def test_sell_more_than_held_blocked(self, db_conn):
        asset = _setup_portfolio(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=15)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
        assert preview.can_confirm is False
        assert any("insufficient" in e.lower() for e in preview.blocking_errors)

    def test_sell_exact_quantity_allowed(self, db_conn):
        asset = _setup_portfolio(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=10)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
        assert preview.can_confirm is True
        assert len(preview.blocking_errors) == 0

    def test_sell_non_sellable_type_blocked(self, db_conn):
        deposit_cash(db_conn, "2025-01-01", 500000.0)
        asset = create_asset(db_conn, Asset(
            symbol="HOME", name="House", asset_type="real_estate",
        ))
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=1)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
        assert preview.can_confirm is False
        assert any("cannot sell" in e.lower() for e in preview.blocking_errors)

    def test_sell_zero_quantity_blocked(self, db_conn):
        asset = _setup_portfolio(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
        assert preview.can_confirm is False

    def test_no_txn_written_for_blocked_sell(self, db_conn):
        deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        count_before = len(list_transactions(db_conn))
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
        confirm_trade(db_conn, preview, "2025-01-15")
        assert len(list_transactions(db_conn)) == count_before


class TestConfirmTradeCannotBypass:
    """confirm_trade must fail if holdings changed between preview and confirm."""

    def test_confirm_sell_after_position_sold_elsewhere(self, db_conn):
        asset = _setup_portfolio(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=10)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
        assert preview.can_confirm is True

        from src.engines.ledger import sell
        sell(db_conn, "2025-01-15", asset.id, quantity=10, price=151.50)

        result = confirm_trade(db_conn, preview, "2025-01-15")
        assert result is False


# ===================================================================
# Trade-by-amount tests
# ===================================================================


class TestBuyByAmount:
    """Buy by target amount derives quantity from ask price."""

    def test_buy_amount_uses_ask_and_floor(self, db_conn):
        asset = _setup_portfolio(db_conn)
        # ask=152.50, target=1000 => floor(1000/152.50) = floor(6.557) = 6
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=0, target_amount=1000.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.can_confirm is True
        assert preview.quantity == 6
        assert preview.trade_price == 152.50
        assert preview.quantity_source == "amount"
        assert preview.target_amount == 1000.0
        assert preview.uninvested_amount == pytest.approx(1000.0 - 6 * 152.50)
        assert preview.estimated_trade_value == pytest.approx(6 * 152.50)

    def test_buy_amount_fee_does_not_affect_quantity(self, db_conn):
        asset = _setup_portfolio(db_conn)
        # ask=152.50, target=1000 => qty=6 regardless of fee
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=0,
                           target_amount=1000.0, fee=50.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.quantity == 6
        assert preview.fee == 50.0
        expected_cash_before = 100000.0 - (10 * 150.0)
        assert preview.cash_after == pytest.approx(
            expected_cash_before - (6 * 152.50 + 50.0)
        )

    def test_buy_amount_too_small_blocks(self, db_conn):
        asset = _setup_portfolio(db_conn)
        # ask=152.50, target=100 => floor(100/152.50)=0 => blocked
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=0, target_amount=100.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.can_confirm is False
        assert any("too small" in e.lower() for e in preview.blocking_errors)

    def test_buy_amount_non_positive_blocks(self, db_conn):
        asset = _setup_portfolio(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=0, target_amount=-500.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.can_confirm is False
        assert any("positive" in e.lower() for e in preview.blocking_errors)

    def test_buy_amount_zero_blocks(self, db_conn):
        asset = _setup_portfolio(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=0, target_amount=0.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.can_confirm is False

    def test_confirm_buy_by_amount(self, db_conn):
        asset = _setup_portfolio(db_conn)
        count_before = len(list_transactions(db_conn))
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=0, target_amount=1000.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
        assert preview.can_confirm is True

        result = confirm_trade(db_conn, preview, "2025-01-15")
        assert result is True

        txns = list_transactions(db_conn)
        assert len(txns) == count_before + 1
        new_txn = txns[-1]
        assert new_txn.txn_type == "buy"
        assert new_txn.quantity == 6
        assert new_txn.price == 152.50


class TestSellByAmount:
    """Sell by target amount derives quantity from bid price."""

    def test_sell_amount_uses_bid_and_floor(self, db_conn):
        asset = _setup_portfolio(db_conn)
        # bid=151.50, target=500 => floor(500/151.50) = floor(3.30) = 3
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=0, target_amount=500.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.can_confirm is True
        assert preview.quantity == 3
        assert preview.trade_price == 151.50
        assert preview.quantity_source == "amount"
        assert preview.target_amount == 500.0
        assert preview.uninvested_amount == pytest.approx(500.0 - 3 * 151.50)

    def test_sell_amount_exceeds_holdings_blocked(self, db_conn):
        asset = _setup_portfolio(db_conn)  # holds 10 shares
        # bid=151.50, target=2000 => floor(2000/151.50) = 13 > 10 => blocked
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=0, target_amount=2000.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.can_confirm is False
        assert any("insufficient" in e.lower() for e in preview.blocking_errors)

    def test_sell_amount_too_small_blocks(self, db_conn):
        asset = _setup_portfolio(db_conn)
        # bid=151.50, target=50 => floor(50/151.50)=0 => blocked
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=0, target_amount=50.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.can_confirm is False
        assert any("too small" in e.lower() for e in preview.blocking_errors)

    def test_sell_amount_non_positive_blocks(self, db_conn):
        asset = _setup_portfolio(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=0, target_amount=-100.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.can_confirm is False

    def test_confirm_sell_by_amount(self, db_conn):
        asset = _setup_portfolio(db_conn)
        count_before = len(list_transactions(db_conn))
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=0, target_amount=500.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())
        assert preview.can_confirm is True

        result = confirm_trade(db_conn, preview, "2025-01-15")
        assert result is True

        txns = list_transactions(db_conn)
        assert len(txns) == count_before + 1
        new_txn = txns[-1]
        assert new_txn.txn_type == "sell"
        assert new_txn.quantity == 3
        assert new_txn.price == 151.50

    def test_sell_amount_fee_does_not_affect_quantity(self, db_conn):
        asset = _setup_portfolio(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=0,
                           target_amount=500.0, fee=25.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.quantity == 3
        assert preview.fee == 25.0
        expected_cash_before = 100000.0 - (10 * 150.0)
        assert preview.cash_after == pytest.approx(
            expected_cash_before + (3 * 151.50 - 25.0)
        )


class TestExplicitQuantityUnchanged:
    """Explicit quantity path must be unchanged by amount-mode addition."""

    def test_quantity_mode_has_correct_source(self, db_conn):
        asset = _setup_portfolio(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.quantity_source == "quantity"
        assert preview.target_amount is None
        assert preview.uninvested_amount == 0.0
        assert preview.quantity == 5

    def test_quantity_mode_sell_still_validates(self, db_conn):
        asset = _setup_portfolio(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=15)
        preview = prepare_trade_preview(db_conn, draft, "2025-01-15", _providers())

        assert preview.can_confirm is False
        assert any("insufficient" in e.lower() for e in preview.blocking_errors)
