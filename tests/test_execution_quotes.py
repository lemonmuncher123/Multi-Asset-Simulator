"""Focused tests for action-specific execution quote behavior.

Proves that Buy uses ask, Sell uses bid, incomplete quotes block,
manual price cannot override live quotes, and quote storage works
independently of market_prices.
"""

import pytest
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.price_repo import (
    upsert_ohlcv, get_latest_price, get_latest_price_record, list_prices,
)
from src.storage.quote_repo import upsert_quote, get_latest_quote_record, list_latest_quotes
from src.storage.transaction_repo import list_transactions
from src.storage.database import init_db, EXPECTED_TABLES
from src.engines.ledger import deposit_cash, buy, sell
from src.engines.portfolio import calc_cash_balance, calc_positions
from src.engines.pricing_engine import sync_asset_price, SYNCABLE_TYPES
from src.engines.trade_preview import (
    TradeDraft, TradePreview, prepare_trade_preview, confirm_trade,
    _pick_execution_price,
)
from src.data_sources.price_provider import (
    PriceProvider, PriceRecord, QuoteRecord, ProviderUnavailableError,
)


# ---------------------------------------------------------------------------
# Mock providers with bid=99, ask=101, last=100
# ---------------------------------------------------------------------------

class ExactQuoteProvider(PriceProvider):
    """Returns bid=99, ask=101, last=100 for every symbol."""

    def source_name(self):
        return "exact_quote"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return [
            PriceRecord(
                symbol=symbol, date="2025-06-01",
                open=98, high=102, low=97, close=100, adjusted_close=100,
                volume=500_000, source=self.source_name(),
            ),
        ]

    def fetch_latest_quote(self, symbol):
        return QuoteRecord(
            symbol=symbol, bid=99.0, ask=101.0, last=100.0,
            timestamp="2025-06-01T14:30:00", source=self.source_name(),
        )


class BidOnlyProvider(PriceProvider):
    """Returns bid=99, ask=None, last=100."""

    def source_name(self):
        return "bid_only"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return [
            PriceRecord(
                symbol=symbol, date="2025-06-01", close=100, adjusted_close=100,
                source=self.source_name(),
            ),
        ]

    def fetch_latest_quote(self, symbol):
        return QuoteRecord(
            symbol=symbol, bid=99.0, ask=None, last=100.0,
            timestamp="2025-06-01T14:30:00", source=self.source_name(),
        )


class AskOnlyProvider(PriceProvider):
    """Returns bid=None, ask=101, last=100."""

    def source_name(self):
        return "ask_only"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return [
            PriceRecord(
                symbol=symbol, date="2025-06-01", close=100, adjusted_close=100,
                source=self.source_name(),
            ),
        ]

    def fetch_latest_quote(self, symbol):
        return QuoteRecord(
            symbol=symbol, bid=None, ask=101.0, last=100.0,
            timestamp="2025-06-01T14:30:00", source=self.source_name(),
        )


class LastOnlyProvider(PriceProvider):
    """Returns bid=None, ask=None, last=100."""

    def source_name(self):
        return "last_only"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return [
            PriceRecord(
                symbol=symbol, date="2025-06-01", close=100, adjusted_close=100,
                source=self.source_name(),
            ),
        ]

    def fetch_latest_quote(self, symbol):
        return QuoteRecord(
            symbol=symbol, bid=None, ask=None, last=100.0,
            timestamp="2025-06-01T14:30:00", source=self.source_name(),
        )


class NoQuoteProvider(PriceProvider):
    """Daily prices succeed but fetch_latest_quote raises."""

    def source_name(self):
        return "no_quote"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return [
            PriceRecord(
                symbol=symbol, date="2025-06-01", close=100, adjusted_close=100,
                source=self.source_name(),
            ),
        ]

    def fetch_latest_quote(self, symbol):
        raise ProviderUnavailableError("Quote service unavailable")


def _exact_providers():
    p = ExactQuoteProvider()
    return {"stock": p, "etf": p, "crypto": p}


def _bid_only_providers():
    p = BidOnlyProvider()
    return {"stock": p, "etf": p, "crypto": p}


def _ask_only_providers():
    p = AskOnlyProvider()
    return {"stock": p, "etf": p, "crypto": p}


def _last_only_providers():
    p = LastOnlyProvider()
    return {"stock": p, "etf": p, "crypto": p}


def _no_quote_providers():
    p = NoQuoteProvider()
    return {"stock": p, "etf": p, "crypto": p}


def _setup(db_conn, cash=50_000.0, symbol="TEST", asset_type="stock",
           initial_qty=20, initial_price=100.0):
    deposit_cash(db_conn, "2025-01-01", cash)
    asset = create_asset(
        db_conn, Asset(symbol=symbol, name=f"Test {symbol}", asset_type=asset_type),
    )
    if initial_qty > 0:
        buy(db_conn, "2025-01-02", asset.id, initial_qty, initial_price)
    return asset


# ===================================================================
# 1. Quote model / provider tests
# ===================================================================

class TestQuoteModelProvider:

    def test_quote_record_has_required_fields(self):
        q = QuoteRecord(symbol="X", bid=99, ask=101, last=100,
                        timestamp="2025-06-01T14:30:00", source="test")
        assert q.bid == 99
        assert q.ask == 101
        assert q.last == 100
        assert q.timestamp == "2025-06-01T14:30:00"
        assert q.source == "test"

    def test_provider_returns_quote_with_bid_ask_last(self):
        provider = ExactQuoteProvider()
        q = provider.fetch_latest_quote("AAPL")
        assert q is not None
        assert q.bid == 99.0
        assert q.ask == 101.0
        assert q.last == 100.0

    def test_buy_execution_resolves_to_ask(self):
        q = QuoteRecord(symbol="X", bid=99, ask=101, last=100)
        assert _pick_execution_price("buy", q) == 101

    def test_sell_execution_resolves_to_bid(self):
        q = QuoteRecord(symbol="X", bid=99, ask=101, last=100)
        assert _pick_execution_price("sell", q) == 99

    def test_quote_source_preserved_in_preview(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.quote_source == "exact_quote"
        assert preview.quote_time == "2025-06-01T14:30:00"

    def test_quote_fields_exposed_in_preview(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.bid_price == 99.0
        assert preview.ask_price == 101.0
        assert preview.last_price == 100.0


# ===================================================================
# 2. Trade preview — Buy uses ask
# ===================================================================

class TestBuyUsesAsk:

    def test_buy_trade_price_is_ask(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=10)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.can_confirm is True
        assert preview.execution_side == "ask"
        assert preview.trade_price == 101.0
        assert preview.estimated_trade_value == pytest.approx(10 * 101.0)

    def test_buy_cash_after_subtracts_ask_plus_fee(self, db_conn):
        asset = _setup(db_conn, cash=50_000.0)
        fee = 7.50
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=10, fee=fee)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        expected_cash_before = 50_000.0 - (20 * 100.0)  # 30000
        assert preview.cash_before == pytest.approx(expected_cash_before)
        expected_cost = 10 * 101.0 + fee
        assert preview.cash_after == pytest.approx(expected_cash_before - expected_cost)

    def test_buy_price_source_is_quote_ask(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.price_source == "quote_ask"


# ===================================================================
# 3. Trade preview — Sell uses bid
# ===================================================================

class TestSellUsesBid:

    def test_sell_trade_price_is_bid(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.can_confirm is True
        assert preview.execution_side == "bid"
        assert preview.trade_price == 99.0

    def test_sell_cash_after_adds_bid_minus_fee(self, db_conn):
        asset = _setup(db_conn, cash=50_000.0)
        fee = 7.50
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5, fee=fee)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        expected_cash_before = 50_000.0 - (20 * 100.0)
        expected_proceeds = 5 * 99.0 - fee
        assert preview.cash_after == pytest.approx(
            expected_cash_before + expected_proceeds
        )

    def test_sell_price_source_is_quote_bid(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.price_source == "quote_bid"


# ===================================================================
# 4. Confirm Buy writes ask to transaction.price
# ===================================================================

class TestConfirmBuyWritesAsk:

    def test_confirmed_buy_txn_price_is_ask(self, db_conn):
        asset = _setup(db_conn)
        fee = 5.0
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=10, fee=fee)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.can_confirm is True
        result = confirm_trade(db_conn, preview, "2025-06-01")
        assert result is True

        txns = list_transactions(db_conn)
        new_buys = [t for t in txns if t.txn_type == "buy" and t.quantity == 10
                    and t.date == "2025-06-01"]
        assert len(new_buys) == 1
        txn = new_buys[0]
        assert txn.price == 101.0
        assert txn.total_amount == pytest.approx(-(10 * 101.0 + fee))

    def test_confirmed_buy_txn_type(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=3)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        confirm_trade(db_conn, preview, "2025-06-01")
        txns = list_transactions(db_conn)
        confirmed = [t for t in txns if t.quantity == 3 and t.date == "2025-06-01"]
        assert confirmed[0].txn_type == "buy"


# ===================================================================
# 5. Confirm Sell writes bid to transaction.price
# ===================================================================

class TestConfirmSellWritesBid:

    def test_confirmed_sell_txn_price_is_bid(self, db_conn):
        asset = _setup(db_conn)
        fee = 5.0
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5, fee=fee)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.can_confirm is True
        result = confirm_trade(db_conn, preview, "2025-06-01")
        assert result is True

        txns = list_transactions(db_conn)
        sells = [t for t in txns if t.txn_type == "sell"]
        assert len(sells) == 1
        txn = sells[0]
        assert txn.price == 99.0
        assert txn.total_amount == pytest.approx(5 * 99.0 - fee)

    def test_confirmed_sell_txn_type(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=2)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        confirm_trade(db_conn, preview, "2025-06-01")
        txns = list_transactions(db_conn)
        sells = [t for t in txns if t.txn_type == "sell"]
        assert sells[0].txn_type == "sell"


# ===================================================================
# 6. Missing bid/ask blocks confirmation
# ===================================================================

class TestMissingBidAskBlocks:

    # -- bid missing, ask present: sell is blocked --

    def test_sell_blocked_when_bid_missing(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _ask_only_providers())
        assert preview.can_confirm is False
        assert any("bid" in e.lower() for e in preview.blocking_errors)

    def test_buy_succeeds_when_bid_missing_ask_present(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _ask_only_providers())
        assert preview.can_confirm is True
        assert preview.trade_price == 101.0

    # -- ask missing, bid present: buy is blocked --

    def test_buy_blocked_when_ask_missing(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _bid_only_providers())
        assert preview.can_confirm is False
        assert any("ask" in e.lower() for e in preview.blocking_errors)

    def test_sell_succeeds_when_ask_missing_bid_present(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _bid_only_providers())
        assert preview.can_confirm is True
        assert preview.trade_price == 99.0

    # -- both missing, last present: both blocked --

    def test_buy_blocked_when_both_bid_ask_missing(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _last_only_providers())
        assert preview.can_confirm is False
        assert any("ask" in e.lower() for e in preview.blocking_errors)

    def test_sell_blocked_when_both_bid_ask_missing(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _last_only_providers())
        assert preview.can_confirm is False
        assert any("bid" in e.lower() for e in preview.blocking_errors)

    def test_last_price_exposed_but_not_used_for_execution(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _last_only_providers())
        assert preview.last_price == 100.0
        assert preview.trade_price == 0.0
        assert preview.can_confirm is False

    # -- confirm_trade rejects blocked previews --

    def test_confirm_returns_false_when_bid_missing(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _ask_only_providers())
        assert confirm_trade(db_conn, preview, "2025-06-01") is False

    def test_confirm_returns_false_when_ask_missing(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _bid_only_providers())
        assert confirm_trade(db_conn, preview, "2025-06-01") is False

    # -- no transaction saved when blocked --

    def test_no_transaction_saved_when_sell_blocked(self, db_conn):
        asset = _setup(db_conn)
        txn_count_before = len(list_transactions(db_conn))
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _ask_only_providers())
        confirm_trade(db_conn, preview, "2025-06-01")
        assert len(list_transactions(db_conn)) == txn_count_before

    def test_no_transaction_saved_when_buy_blocked(self, db_conn):
        asset = _setup(db_conn)
        txn_count_before = len(list_transactions(db_conn))
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _bid_only_providers())
        confirm_trade(db_conn, preview, "2025-06-01")
        assert len(list_transactions(db_conn)) == txn_count_before

    def test_no_transaction_saved_when_both_missing(self, db_conn):
        asset = _setup(db_conn)
        txn_count_before = len(list_transactions(db_conn))
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _last_only_providers())
        confirm_trade(db_conn, preview, "2025-06-01")
        assert len(list_transactions(db_conn)) == txn_count_before

    # -- last is never silently accepted as execution price --

    def test_last_not_used_as_buy_execution_price(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _last_only_providers())
        assert preview.trade_price != 100.0
        assert preview.execution_side != "last"

    def test_last_not_used_as_sell_execution_price(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _last_only_providers())
        assert preview.trade_price != 100.0
        assert preview.execution_side != "last"


# ===================================================================
# 7. Manual price must not override quote
# ===================================================================

class TestManualPriceCannotOverrideQuote:

    def test_manual_price_ignored_for_buy(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5,
                           manual_price=150.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.trade_price == 101.0
        assert preview.price_source == "quote_ask"

    def test_manual_price_ignored_for_sell(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5,
                           manual_price=150.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.trade_price == 99.0
        assert preview.price_source == "quote_bid"

    def test_manual_price_ignored_even_when_higher(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5,
                           manual_price=999.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.trade_price == 101.0

    def test_manual_price_ignored_even_when_lower(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5,
                           manual_price=1.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.trade_price == 99.0


# ===================================================================
# 8. Buy/Sell persistence only through confirm_trade
# ===================================================================

class TestBuySellOnlyThroughConfirm:

    def test_confirm_trade_is_only_persistence_path(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        txn_count_before = len(list_transactions(db_conn))

        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert len(list_transactions(db_conn)) == txn_count_before

        confirm_trade(db_conn, preview, "2025-06-01")
        assert len(list_transactions(db_conn)) == txn_count_before + 1

    def test_confirm_rejects_unconfirmable_preview(self, db_conn):
        preview = TradePreview(
            action="buy", can_confirm=False,
            blocking_errors=["Some error"],
        )
        assert confirm_trade(db_conn, preview, "2025-06-01") is False

    def test_confirm_rejects_non_buy_sell_action(self, db_conn):
        preview = TradePreview(action="deposit_cash", can_confirm=True)
        assert confirm_trade(db_conn, preview, "2025-06-01") is False

    def test_preview_never_persists_even_repeated(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=3)
        txn_count = len(list_transactions(db_conn))

        for _ in range(5):
            prepare_trade_preview(db_conn, draft, "2025-06-01",
                                  _exact_providers())
        assert len(list_transactions(db_conn)) == txn_count


# ===================================================================
# 9. Quote storage tests
# ===================================================================

class TestQuoteStorage:

    def test_market_quotes_table_exists(self, db_conn):
        tables = [r[0] for r in db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "market_quotes" in tables

    def test_market_quotes_in_expected_tables(self):
        assert "market_quotes" in EXPECTED_TABLES

    def test_market_quotes_has_correct_columns(self, db_conn):
        cols = {r[1] for r in db_conn.execute(
            "PRAGMA table_info(market_quotes)"
        ).fetchall()}
        for col in ["asset_id", "symbol", "asset_type", "bid", "ask",
                     "last", "timestamp", "source", "created_at"]:
            assert col in cols, f"Missing column: {col}"

    def test_upsert_quote_insert(self, db_conn):
        asset = create_asset(
            db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        upsert_quote(db_conn, asset.id, "AAPL", "stock",
                     bid=149.5, ask=150.5, last=150.0,
                     timestamp="2025-06-01T10:00:00", source="test")

        rec = get_latest_quote_record(db_conn, asset.id)
        assert rec is not None
        assert rec["bid"] == 149.5
        assert rec["ask"] == 150.5
        assert rec["last"] == 150.0
        assert rec["source"] == "test"
        assert rec["timestamp"] == "2025-06-01T10:00:00"

    def test_upsert_quote_update(self, db_conn):
        asset = create_asset(
            db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        upsert_quote(db_conn, asset.id, "AAPL", "stock",
                     bid=149.0, ask=151.0, last=150.0,
                     timestamp="2025-06-01T10:00:00", source="test")
        upsert_quote(db_conn, asset.id, "AAPL", "stock",
                     bid=148.0, ask=152.0, last=150.5,
                     timestamp="2025-06-01T10:05:00", source="test")

        rec = get_latest_quote_record(db_conn, asset.id)
        assert rec["bid"] == 148.0
        assert rec["ask"] == 152.0
        assert rec["last"] == 150.5
        assert rec["timestamp"] == "2025-06-01T10:05:00"

    def test_get_latest_quote_record_returns_none_for_unknown(self, db_conn):
        assert get_latest_quote_record(db_conn, 9999) is None

    def test_list_latest_quotes_multiple_assets(self, db_conn):
        a1 = create_asset(
            db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        a2 = create_asset(
            db_conn, Asset(symbol="MSFT", name="Microsoft", asset_type="stock"),
        )
        upsert_quote(db_conn, a1.id, "AAPL", "stock",
                     bid=149, ask=151, last=150,
                     timestamp="2025-06-01T10:00:00", source="test")
        upsert_quote(db_conn, a2.id, "MSFT", "stock",
                     bid=399, ask=401, last=400,
                     timestamp="2025-06-01T10:00:00", source="test")

        quotes = list_latest_quotes(db_conn)
        symbols = {q["symbol"] for q in quotes}
        assert "AAPL" in symbols
        assert "MSFT" in symbols
        assert len(quotes) == 2

    def test_quote_storage_does_not_modify_market_prices(self, db_conn):
        asset = create_asset(
            db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-06-01",
                      148, 152, 147, 150, 150, 1e6, "yfinance")
        prices_before = list_prices(db_conn, asset.id)

        upsert_quote(db_conn, asset.id, "AAPL", "stock",
                     bid=149, ask=151, last=150,
                     timestamp="2025-06-01T10:00:00", source="test")

        prices_after = list_prices(db_conn, asset.id)
        assert len(prices_after) == len(prices_before)
        assert prices_after[0]["price"] == prices_before[0]["price"]

    def test_quote_stored_during_preview(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1)
        prepare_trade_preview(db_conn, draft, "2025-06-01",
                              _exact_providers())

        stored = get_latest_quote_record(db_conn, asset.id)
        assert stored is not None
        assert stored["bid"] == 99.0
        assert stored["ask"] == 101.0
        assert stored["last"] == 100.0
        assert stored["source"] == "exact_quote"

    def test_quote_with_null_bid_stored_correctly(self, db_conn):
        asset = create_asset(
            db_conn, Asset(symbol="X", name="Test", asset_type="stock"),
        )
        upsert_quote(db_conn, asset.id, "X", "stock",
                     bid=None, ask=101, last=100,
                     timestamp="2025-06-01T10:00:00", source="test")

        rec = get_latest_quote_record(db_conn, asset.id)
        assert rec["bid"] is None
        assert rec["ask"] == 101


# ===================================================================
# 10. Regression tests
# ===================================================================

class TestRegressions:

    def test_daily_ohlcv_sync_still_writes_market_prices(self, db_conn):
        asset = create_asset(
            db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        count = sync_asset_price(
            db_conn, asset, "2025-01-01", "2025-06-01",
            providers={"stock": ExactQuoteProvider()},
        )
        assert count >= 1
        price = get_latest_price(db_conn, asset.id)
        assert price == 100.0  # adjusted_close from daily sync

    def test_get_latest_price_returns_valuation_price(self, db_conn):
        asset = create_asset(
            db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-06-01",
                      98, 102, 97, 100, 100, 500_000, "yfinance")
        price = get_latest_price(db_conn, asset.id)
        assert price == 100.0

    def test_portfolio_valuation_uses_close_not_quote(self, db_conn):
        deposit_cash(db_conn, "2025-01-01", 50_000.0)
        asset = create_asset(
            db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        buy(db_conn, "2025-01-02", asset.id, 10, 100.0)

        upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-06-01",
                      98, 110, 97, 105, 105, 1e6, "yfinance")
        upsert_quote(db_conn, asset.id, "AAPL", "stock",
                     bid=104, ask=106, last=105,
                     timestamp="2025-06-01T14:00:00", source="test")

        positions = calc_positions(db_conn)
        assert len(positions) == 1
        assert positions[0].current_price == 105.0
        assert positions[0].market_value == pytest.approx(10 * 105.0)

    def test_preview_does_not_save_transaction_regression(self, db_conn):
        asset = _setup(db_conn)
        count_before = len(list_transactions(db_conn))
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        prepare_trade_preview(db_conn, draft, "2025-06-01",
                              _exact_providers())
        assert len(list_transactions(db_conn)) == count_before

    def test_confirm_creates_exactly_one_transaction_regression(self, db_conn):
        asset = _setup(db_conn)
        count_before = len(list_transactions(db_conn))
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=3)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        confirm_trade(db_conn, preview, "2025-06-01")
        assert len(list_transactions(db_conn)) == count_before + 1

    def test_notes_preserved_on_confirm_regression(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=2,
                           note="Buying on dip")
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        confirm_trade(db_conn, preview, "2025-06-01")

        txns = list_transactions(db_conn)
        confirmed = [t for t in txns if t.quantity == 2 and t.date == "2025-06-01"]
        assert len(confirmed) == 1
        assert confirmed[0].notes == "Buying on dip"

    def test_no_quote_blocks_even_with_daily_prices(self, db_conn):
        asset = _setup(db_conn)
        upsert_ohlcv(db_conn, asset.id, "TEST", "stock", "2025-05-30",
                      98, 102, 97, 100, 100, 1e6, "yfinance")

        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _no_quote_providers())
        assert preview.can_confirm is False
        assert any("quote" in e.lower() or "data sync" in e.lower()
                   for e in preview.blocking_errors)

    def test_buy_sell_differ_when_bid_neq_ask_regression(self, db_conn):
        """Regression: would fail if code returned market_prices.price for both."""
        asset = _setup(db_conn)
        upsert_ohlcv(db_conn, asset.id, "TEST", "stock", "2025-05-30",
                      98, 102, 97, 100, 100, 1e6, "yfinance")

        buy_draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        buy_preview = prepare_trade_preview(db_conn, buy_draft, "2025-06-01",
                                             _exact_providers())

        sell_draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5)
        sell_preview = prepare_trade_preview(db_conn, sell_draft, "2025-06-01",
                                              _exact_providers())

        assert buy_preview.trade_price == 101.0   # ask
        assert sell_preview.trade_price == 99.0    # bid
        assert buy_preview.trade_price != sell_preview.trade_price
        assert buy_preview.trade_price != 100.0    # not daily close
        assert sell_preview.trade_price != 100.0   # not daily close

    def test_manual_cannot_rescue_stock(self, db_conn):
        asset = _setup(db_conn, initial_qty=0)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5,
                           manual_price=150.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                         _no_quote_providers())
        assert preview.can_confirm is False
        assert preview.price_source != "manual"

    def test_manual_cannot_rescue_etf(self, db_conn):
        asset = _setup(db_conn, asset_type="etf", initial_qty=0)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5,
                           manual_price=400.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                         _no_quote_providers())
        assert preview.can_confirm is False

    def test_manual_cannot_rescue_crypto(self, db_conn):
        asset = _setup(db_conn, asset_type="crypto", initial_qty=0)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1,
                           manual_price=40000.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                         _no_quote_providers())
        assert preview.can_confirm is False

    def test_manual_valid_for_real_estate(self, db_conn):
        deposit_cash(db_conn, "2025-01-01", 500_000.0)
        asset = create_asset(
            db_conn, Asset(symbol="HOME", name="House", asset_type="real_estate"),
        )
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1,
                           manual_price=300_000.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                         _exact_providers())
        assert preview.can_confirm is True
        assert preview.price_source == "manual"
        assert preview.trade_price == 300_000.0

    def test_manual_valid_for_custom(self, db_conn):
        deposit_cash(db_conn, "2025-01-01", 50_000.0)
        asset = create_asset(
            db_conn, Asset(symbol="PRIV1", name="Private Fund", asset_type="custom"),
        )
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1,
                           manual_price=500.0)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                         _exact_providers())
        assert preview.can_confirm is True
        assert preview.price_source == "manual"


# ===================================================================
# 11. Regression: confirm_trade cannot bypass sell validation
# ===================================================================

class TestConfirmTradeRaceCondition:
    """confirm_trade wraps ledger.sell() which re-validates holdings.
    If position was consumed between preview and confirm, confirm must fail."""

    def test_confirm_fails_after_position_sold_between_preview_and_confirm(self, db_conn):
        asset = _setup(db_conn, cash=50_000.0, initial_qty=10, initial_price=100.0)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=10)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.can_confirm is True

        sell(db_conn, "2025-06-01", asset.id, quantity=10, price=99.0)

        result = confirm_trade(db_conn, preview, "2025-06-01")
        assert result is False

    def test_confirm_fails_after_partial_sell_reduces_holdings(self, db_conn):
        asset = _setup(db_conn, cash=50_000.0, initial_qty=10, initial_price=100.0)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=8)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.can_confirm is True

        sell(db_conn, "2025-06-01", asset.id, quantity=5, price=99.0)

        result = confirm_trade(db_conn, preview, "2025-06-01")
        assert result is False

    def test_no_txn_written_on_failed_confirm(self, db_conn):
        asset = _setup(db_conn, cash=50_000.0, initial_qty=10, initial_price=100.0)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=10)
        preview = prepare_trade_preview(db_conn, draft, "2025-06-01",
                                        _exact_providers())
        assert preview.can_confirm is True

        sell(db_conn, "2025-06-01", asset.id, quantity=10, price=99.0)

        count_before = len(list_transactions(db_conn))
        confirm_trade(db_conn, preview, "2025-06-01")
        assert len(list_transactions(db_conn)) == count_before
