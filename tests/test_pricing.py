import sys
import pytest
from unittest.mock import patch
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.price_repo import (
    upsert_price, upsert_ohlcv, bulk_upsert_ohlcv,
    get_latest_price, get_latest_price_record, list_prices, list_latest_prices,
)
from src.storage.sync_log_repo import (
    create_sync_log, finish_sync_log, list_sync_logs,
    get_latest_sync_log, get_last_successful_sync,
)
from src.engines.ledger import deposit_cash, buy
from src.engines.pricing_engine import (
    sync_asset_price, sync_asset_quote, sync_asset_market_data,
    sync_all_market_assets,
    get_latest_market_price,
    SYNCABLE_TYPES, _is_valid_price,
)
from src.storage.quote_repo import get_latest_quote_record, list_latest_market_data
from src.data_sources.price_provider import PriceProvider, PriceRecord, QuoteRecord, ProviderUnavailableError
from src.utils.deps import is_yfinance_available


# --- Mock provider ---

class MockStockProvider(PriceProvider):
    def __init__(self, prices=None, fail_symbols=None):
        self._prices = prices or {}
        self._fail_symbols = fail_symbols or set()

    def source_name(self) -> str:
        return "mock_stock"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        if symbol in self._fail_symbols:
            raise ValueError(f"Mock failure for {symbol}")
        if symbol in self._prices:
            return self._prices[symbol]
        return [
            PriceRecord(symbol=symbol, date="2025-01-10", open=100, high=105,
                        low=99, close=103, adjusted_close=103, volume=1000000,
                        source=self.source_name()),
            PriceRecord(symbol=symbol, date="2025-01-11", open=103, high=108,
                        low=102, close=107, adjusted_close=107, volume=900000,
                        source=self.source_name()),
        ]

    def fetch_latest_quote(self, symbol):
        if symbol in self._fail_symbols:
            return None
        return QuoteRecord(
            symbol=symbol, bid=106.0, ask=108.0, last=107.0,
            timestamp="2025-01-11T10:00:00", source=self.source_name(),
        )


class MockCryptoProvider(PriceProvider):
    def source_name(self) -> str:
        return "mock_crypto"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return [
            PriceRecord(symbol=symbol, date="2025-01-10", open=40000, high=41000,
                        low=39500, close=40500, adjusted_close=40500, volume=5000,
                        source=self.source_name()),
        ]

    def fetch_latest_quote(self, symbol):
        return QuoteRecord(
            symbol=symbol, bid=40400.0, ask=40600.0, last=40500.0,
            timestamp="2025-01-10T10:00:00", source=self.source_name(),
        )


# --- Table creation ---

def test_market_prices_table_exists(db_conn):
    tables = [r[0] for r in db_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    assert "market_prices" in tables


def test_price_sync_log_table_exists(db_conn):
    tables = [r[0] for r in db_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    assert "price_sync_log" in tables


def test_market_prices_has_ohlcv_columns(db_conn):
    cols = {r[1] for r in db_conn.execute("PRAGMA table_info(market_prices)").fetchall()}
    for col in ["open", "high", "low", "close", "adjusted_close", "volume",
                "symbol", "asset_type", "source"]:
        assert col in cols, f"Missing column: {col}"


# --- Inserting price records ---

def test_insert_price_record(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-01-10",
                 100.0, 105.0, 99.0, 103.0, 103.0, 1000000.0, "yfinance")
    prices = list_prices(db_conn, asset.id)
    assert len(prices) == 1
    assert prices[0]["price"] == 103.0
    assert prices[0]["source"] == "yfinance"


def test_bulk_insert_prices(db_conn):
    asset = create_asset(db_conn, Asset(symbol="MSFT", name="Microsoft", asset_type="stock"))
    rows = [
        {"asset_id": asset.id, "symbol": "MSFT", "asset_type": "stock",
         "date": "2025-01-10", "open": 300, "high": 310, "low": 298,
         "close": 305, "adjusted_close": 305, "volume": 2000000, "source": "yfinance"},
        {"asset_id": asset.id, "symbol": "MSFT", "asset_type": "stock",
         "date": "2025-01-11", "open": 305, "high": 315, "low": 304,
         "close": 312, "adjusted_close": 312, "volume": 1800000, "source": "yfinance"},
    ]
    count = bulk_upsert_ohlcv(db_conn, rows)
    assert count == 2
    prices = list_prices(db_conn, asset.id)
    assert len(prices) == 2


# --- Upsert duplicate ---

def test_upsert_duplicate_price(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-01-10",
                 100, 105, 99, 103, 103, 1000000, "yfinance")
    upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-01-10",
                 100, 106, 98, 104, 104, 1100000, "yfinance")
    prices = list_prices(db_conn, asset.id)
    assert len(prices) == 1
    assert prices[0]["price"] == 104.0


# --- Retrieving latest price ---

def test_get_latest_price(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-01-10",
                 100, 105, 99, 103, 103, 1000000, "yfinance")
    upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-01-11",
                 103, 108, 102, 107, 107, 900000, "yfinance")
    price = get_latest_price(db_conn, asset.id)
    assert price == 107.0


def test_get_latest_price_no_data(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    price = get_latest_price(db_conn, asset.id)
    assert price is None


def test_get_latest_price_record(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-01-10",
                 100, 105, 99, 103, 103, 1000000, "yfinance")
    rec = get_latest_price_record(db_conn, asset.id)
    assert rec is not None
    assert rec["date"] == "2025-01-10"
    assert rec["price"] == 103.0


def test_list_latest_prices(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    a2 = create_asset(db_conn, Asset(symbol="MSFT", name="Microsoft", asset_type="stock"))
    upsert_ohlcv(db_conn, a1.id, "AAPL", "stock", "2025-01-10",
                 100, 105, 99, 103, 103, 1000000, "yfinance")
    upsert_ohlcv(db_conn, a2.id, "MSFT", "stock", "2025-01-10",
                 300, 310, 298, 305, 305, 2000000, "yfinance")
    latest = list_latest_prices(db_conn)
    assert len(latest) == 2
    symbols = {p["symbol"] for p in latest}
    assert "AAPL" in symbols
    assert "MSFT" in symbols


# --- Sync log ---

def test_create_and_finish_sync_log(db_conn):
    log_id = create_sync_log(db_conn, source="yfinance")
    finish_sync_log(db_conn, log_id, "success", 5, 5, 0)
    logs = list_sync_logs(db_conn)
    assert len(logs) == 1
    assert logs[0]["status"] == "success"
    assert logs[0]["assets_attempted"] == 5
    assert logs[0]["assets_succeeded"] == 5


def test_sync_log_with_errors(db_conn):
    log_id = create_sync_log(db_conn, source="yfinance")
    finish_sync_log(db_conn, log_id, "partial", 5, 3, 2, "TSLA: timeout\nGME: not found")
    logs = list_sync_logs(db_conn)
    assert logs[0]["status"] == "partial"
    assert logs[0]["assets_failed"] == 2
    assert "TSLA" in logs[0]["error_message"]


def test_get_latest_sync_log_returns_newest(db_conn):
    log1 = create_sync_log(db_conn, source="yfinance")
    finish_sync_log(db_conn, log1, "success", 3, 3, 0)
    log2 = create_sync_log(db_conn, source="yfinance")
    finish_sync_log(db_conn, log2, "failed", 2, 0, 2, "all failed")
    latest = get_latest_sync_log(db_conn)
    assert latest["id"] == log2
    assert latest["status"] == "failed"


def test_get_latest_sync_log_none(db_conn):
    assert get_latest_sync_log(db_conn) is None


def test_get_last_successful_sync_skips_failed(db_conn):
    log1 = create_sync_log(db_conn, source="yfinance")
    finish_sync_log(db_conn, log1, "success", 3, 3, 0)
    log2 = create_sync_log(db_conn, source="yfinance")
    finish_sync_log(db_conn, log2, "failed", 2, 0, 2, "all failed")
    last = get_last_successful_sync(db_conn)
    assert last["id"] == log1


# --- Sync one stock asset with mock provider ---

def test_sync_stock_asset(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    providers = {"stock": MockStockProvider(), "etf": MockStockProvider()}
    count = sync_asset_price(db_conn, asset, "2025-01-01", "2025-01-15", providers)
    assert count == 2
    price = get_latest_price(db_conn, asset.id)
    assert price == 107.0


def test_sync_etf_asset(db_conn):
    asset = create_asset(db_conn, Asset(symbol="SPY", name="S&P 500 ETF", asset_type="etf"))
    providers = {"stock": MockStockProvider(), "etf": MockStockProvider()}
    count = sync_asset_price(db_conn, asset, "2025-01-01", "2025-01-15", providers)
    assert count == 2


# --- Sync one crypto asset with mock provider ---

def test_sync_crypto_asset(db_conn):
    asset = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    providers = {"crypto": MockCryptoProvider()}
    count = sync_asset_price(db_conn, asset, "2025-01-01", "2025-01-15", providers)
    assert count == 1
    price = get_latest_price(db_conn, asset.id)
    assert price == 40500.0


# --- Skipping unsupported asset types ---

def test_skip_unsupported_asset_types(db_conn):
    asset = create_asset(db_conn, Asset(symbol="HOME", name="My House", asset_type="real_estate"))
    providers = {"stock": MockStockProvider()}
    count = sync_asset_price(db_conn, asset, "2025-01-01", "2025-01-15", providers)
    assert count == 0


def test_skip_custom_asset(db_conn):
    asset = create_asset(db_conn, Asset(symbol="PRIV1", name="Private Fund", asset_type="custom"))
    providers = {"stock": MockStockProvider()}
    count = sync_asset_price(db_conn, asset, "2025-01-01", "2025-01-15", providers)
    assert count == 0


def test_skip_debt_asset(db_conn):
    asset = create_asset(db_conn, Asset(symbol="LOAN1", name="Car Loan", asset_type="debt"))
    providers = {"stock": MockStockProvider()}
    count = sync_asset_price(db_conn, asset, "2025-01-01", "2025-01-15", providers)
    assert count == 0


# --- Logging failed sync ---

def test_sync_logs_failure(db_conn):
    asset = create_asset(db_conn, Asset(symbol="BAD", name="Bad Stock", asset_type="stock"))
    providers = {"stock": MockStockProvider(fail_symbols={"BAD"})}
    result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)
    assert result["failed"] == 1
    assert result["succeeded"] == 0
    assert result["status"] == "failed"
    logs = list_sync_logs(db_conn)
    assert len(logs) == 1
    assert logs[0]["status"] == "failed"
    assert "BAD" in logs[0]["error_message"]


# --- Continuing sync when one asset fails ---

def test_sync_continues_after_failure(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="BAD", name="Bad Stock", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="MSFT", name="Microsoft", asset_type="stock"))
    providers = {"stock": MockStockProvider(fail_symbols={"BAD"})}
    result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)
    assert result["attempted"] == 3
    assert result["succeeded"] == 2
    assert result["failed"] == 1
    assert result["status"] == "partial"
    assert any("BAD" in e for e in result["errors"])
    aapl_price = get_latest_price(db_conn, 1)
    assert aapl_price is not None


# --- Sync all skips non-syncable ---

def test_sync_all_skips_non_syncable(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="HOME", name="My House", asset_type="real_estate"))
    create_asset(db_conn, Asset(symbol="LOAN", name="Loan", asset_type="debt"))
    providers = {"stock": MockStockProvider()}
    result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)
    assert result["attempted"] == 1
    assert result["succeeded"] == 1


def test_position_market_value_uses_latest_price(db_conn):
    from src.engines.portfolio import calc_positions
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    buy(db_conn, "2025-01-02", asset.id, 10, 150.0)

    upsert_ohlcv(db_conn, asset.id, "AAPL", "stock", "2025-01-15",
                 150, 160, 148, 155, 155, 1000000, "yfinance")

    positions = calc_positions(db_conn)
    assert len(positions) == 1
    assert positions[0].current_price == 155.0
    assert positions[0].market_value == 1550.0
    assert positions[0].unrealized_pnl == 50.0


# --- Legacy compat: old upsert_price still works ---

def test_legacy_upsert_price(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    upsert_price(db_conn, asset.id, "2025-01-10", 150.0, "manual")
    price = get_latest_price(db_conn, asset.id)
    assert price == 150.0


# --- Syncable types constant ---

def test_syncable_types(db_conn):
    assert "stock" in SYNCABLE_TYPES
    assert "etf" in SYNCABLE_TYPES
    assert "crypto" in SYNCABLE_TYPES
    assert "real_estate" not in SYNCABLE_TYPES
    assert "custom" not in SYNCABLE_TYPES
    assert "debt" not in SYNCABLE_TYPES
    assert "cash" not in SYNCABLE_TYPES


# --- Provider unavailable (yfinance missing) ---

class UnavailableProvider(PriceProvider):
    def source_name(self):
        return "unavailable"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        raise ProviderUnavailableError("yfinance is not installed")


def test_is_yfinance_available_returns_bool():
    result = is_yfinance_available()
    assert isinstance(result, bool)


def test_provider_raises_provider_unavailable_error():
    provider = UnavailableProvider()
    with pytest.raises(ProviderUnavailableError):
        provider.fetch_daily_prices("AAPL", "2025-01-01", "2025-01-15")


def test_sync_all_missing_yfinance_single_error(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="MSFT", name="Microsoft", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="GLD", name="Gold ETF", asset_type="etf"))

    with patch(
        "src.utils.deps.is_yfinance_available", return_value=False
    ):
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15")

    assert result["status"] == "failed"
    assert result["attempted"] == 3
    assert result["succeeded"] == 0
    assert result["failed"] == 3
    assert len(result["errors"]) == 1
    assert "yfinance" in result["errors"][0]


def test_sync_all_missing_yfinance_creates_one_sync_log(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="NVDA", name="Nvidia", asset_type="stock"))

    with patch(
        "src.utils.deps.is_yfinance_available", return_value=False
    ):
        sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15")

    logs = list_sync_logs(db_conn)
    assert len(logs) == 1
    assert logs[0]["status"] == "failed"
    assert logs[0]["assets_attempted"] == 2
    assert logs[0]["assets_failed"] == 2
    assert "yfinance" in logs[0]["error_message"]


def test_sync_all_missing_yfinance_no_duplicate_per_asset_errors(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="TSLA", name="Tesla", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="QQQ", name="QQQ ETF", asset_type="etf"))

    with patch(
        "src.utils.deps.is_yfinance_available", return_value=False
    ):
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15")

    assert len(result["errors"]) == 1
    assert "AAPL" not in result["errors"][0]
    assert "TSLA" not in result["errors"][0]
    assert "QQQ" not in result["errors"][0]


def test_sync_all_with_providers_skips_yfinance_check(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    providers = {"stock": MockStockProvider()}

    with patch(
        "src.utils.deps.is_yfinance_available", return_value=False
    ):
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)

    assert result["succeeded"] == 1
    assert result["status"] == "success"


def test_sync_single_asset_missing_yfinance_raises(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))

    with pytest.raises(ProviderUnavailableError, match="yfinance"):
        sync_asset_price(
            db_conn, asset, "2025-01-01", "2025-01-15",
            providers={"stock": UnavailableProvider()},
        )


def test_yfinance_missing_blocks_stock(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))

    with patch(
        "src.utils.deps.is_yfinance_available", return_value=False
    ):
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15")

    assert result["failed"] == 1
    assert result["succeeded"] == 0


def test_yfinance_missing_blocks_etf(db_conn):
    create_asset(db_conn, Asset(symbol="SPY", name="S&P 500 ETF", asset_type="etf"))

    with patch(
        "src.utils.deps.is_yfinance_available", return_value=False
    ):
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15")

    assert result["failed"] == 1
    assert result["succeeded"] == 0


def test_yfinance_missing_blocks_crypto_default_provider(db_conn):
    create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))

    with patch(
        "src.utils.deps.is_yfinance_available", return_value=False
    ):
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15")

    assert result["failed"] == 1
    assert result["succeeded"] == 0
    assert len(result["errors"]) == 1


def test_crypto_routed_to_crypto_provider_not_stock(db_conn):
    create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    providers = {"stock": MockStockProvider(), "crypto": MockCryptoProvider()}
    result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)

    assert result["succeeded"] == 1
    price = get_latest_price(db_conn, 1)
    assert price == 40500.0


def test_yfinance_missing_blocks_all_default_providers(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="QQQ", name="QQQ ETF", asset_type="etf"))
    create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))

    with patch(
        "src.utils.deps.is_yfinance_available", return_value=False
    ):
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15")

    assert result["attempted"] == 3
    assert result["succeeded"] == 0
    assert result["failed"] == 3
    assert result["status"] == "failed"
    assert len(result["errors"]) == 1
    assert "yfinance" in result["errors"][0]


def test_error_message_includes_interpreter_path(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))

    with patch(
        "src.utils.deps.is_yfinance_available", return_value=False
    ):
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15")

    assert len(result["errors"]) == 1
    msg = result["errors"][0]
    assert sys.executable in msg
    assert "pip install -r" in msg


def test_install_command_uses_absolute_requirements_path():
    from src.utils.deps import get_install_command, _REQUIREMENTS_TXT
    cmd = get_install_command()
    assert sys.executable in cmd
    assert str(_REQUIREMENTS_TXT) in cmd
    assert _REQUIREMENTS_TXT.is_absolute()
    assert _REQUIREMENTS_TXT.name == "requirements.txt"


def test_dependency_status_returns_dict():
    from src.utils.deps import get_dependency_status
    status = get_dependency_status()
    assert "yfinance" in status
    assert isinstance(status["yfinance"], bool)
    assert status["python"] == sys.executable
    assert status["requirements_txt"].endswith("requirements.txt")
    from pathlib import Path
    assert Path(status["requirements_txt"]).is_absolute()


def test_install_args_uses_sys_executable():
    from src.utils.deps import get_install_args, _REQUIREMENTS_TXT
    args = get_install_args()
    assert args[0] == sys.executable
    assert "-m" in args
    assert "pip" in args
    assert str(_REQUIREMENTS_TXT) in args
    assert _REQUIREMENTS_TXT.is_absolute()


def test_data_sync_disables_buttons_when_yfinance_missing():
    from src.gui.pages.data_sync import DataSyncPage
    from src.storage.database import init_db
    conn = init_db(":memory:")
    page = DataSyncPage(conn)

    with patch("src.gui.pages.data_sync.is_yfinance_available", return_value=False):
        page._update_dep_status()
    assert not page.btn_sync_all.isEnabled()
    assert not page.btn_sync_one.isEnabled()
    assert not page.btn_install.isHidden()

    with patch("src.gui.pages.data_sync.is_yfinance_available", return_value=True):
        page._update_dep_status()
    assert page.btn_sync_all.isEnabled()
    assert page.btn_sync_one.isEnabled()
    assert page.btn_install.isHidden()

    conn.close()


def test_data_sync_hides_install_button_in_frozen_mode():
    """In a packaged/frozen build the user can't pip-install into the
    bundle, so the in-app Install Dependencies button should be hidden
    regardless of whether yfinance happens to be importable."""
    import src.gui.pages.data_sync as ds
    from src.gui.pages.data_sync import DataSyncPage
    from src.storage.database import init_db
    conn = init_db(":memory:")
    page = DataSyncPage(conn)

    # Frozen + yfinance missing: button hidden, message explains bundle.
    with patch.object(ds.sys, "frozen", True, create=True), \
         patch("src.gui.pages.data_sync.is_yfinance_available", return_value=False):
        page._update_dep_status()
    assert page.btn_install.isHidden(), "Install button must be hidden in frozen mode"
    assert "bundled" in page.install_status_label.text().lower()

    # Frozen + yfinance present: still hidden, status still informative.
    with patch.object(ds.sys, "frozen", True, create=True), \
         patch("src.gui.pages.data_sync.is_yfinance_available", return_value=True):
        page._update_dep_status()
    assert page.btn_install.isHidden()

    # Source mode (frozen attr removed): yfinance missing → button visible again.
    # We assert getattr(sys, "frozen", False) is False at this point.
    if hasattr(ds.sys, "frozen"):
        delattr(ds.sys, "frozen")
    with patch("src.gui.pages.data_sync.is_yfinance_available", return_value=False):
        page._update_dep_status()
    assert not page.btn_install.isHidden(), "Install button should be visible in source mode"

    conn.close()


def test_injected_providers_bypass_yfinance_check(db_conn):
    create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    providers = {"crypto": MockCryptoProvider()}

    with patch(
        "src.utils.deps.is_yfinance_available", return_value=False
    ):
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)

    assert result["succeeded"] == 1
    assert result["failed"] == 0
    assert len(result["errors"]) == 0


# ===================================================================
# _is_valid_price validation
# ===================================================================

class TestIsValidPrice:

    def test_positive_float(self):
        assert _is_valid_price(100.0) is True

    def test_positive_int(self):
        assert _is_valid_price(1) is True

    def test_small_positive(self):
        assert _is_valid_price(0.001) is True

    def test_none(self):
        assert _is_valid_price(None) is False

    def test_zero(self):
        assert _is_valid_price(0) is False

    def test_negative(self):
        assert _is_valid_price(-5.0) is False

    def test_nan(self):
        assert _is_valid_price(float("nan")) is False

    def test_inf(self):
        assert _is_valid_price(float("inf")) is False

    def test_neg_inf(self):
        assert _is_valid_price(float("-inf")) is False

    def test_string_invalid(self):
        assert _is_valid_price("abc") is False

    def test_numeric_string(self):
        assert _is_valid_price("100.0") is True


# ===================================================================
# sync_asset_quote unit tests
# ===================================================================

class NanQuoteProvider(PriceProvider):
    def source_name(self):
        return "nan_quote"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return []

    def fetch_latest_quote(self, symbol):
        return QuoteRecord(
            symbol=symbol, bid=float("nan"), ask=float("nan"), last=100.0,
            timestamp="2025-01-11T10:00:00", source=self.source_name(),
        )


class ZeroBidAskProvider(PriceProvider):
    def source_name(self):
        return "zero_quote"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return []

    def fetch_latest_quote(self, symbol):
        return QuoteRecord(
            symbol=symbol, bid=0.0, ask=0.0, last=100.0,
            timestamp="2025-01-11T10:00:00", source=self.source_name(),
        )


class NullQuoteProvider(PriceProvider):
    def source_name(self):
        return "null_quote"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return []

    def fetch_latest_quote(self, symbol):
        return None


class TestSyncAssetQuote:

    def test_stores_bid_ask_last_in_market_quotes(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": MockStockProvider()}
        result = sync_asset_quote(db_conn, asset, providers)
        assert result is True

        rec = get_latest_quote_record(db_conn, asset.id)
        assert rec is not None
        assert rec["bid"] == 106.0
        assert rec["ask"] == 108.0
        assert rec["last"] == 107.0

    def test_stores_timestamp_and_source(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": MockStockProvider()}
        sync_asset_quote(db_conn, asset, providers)

        rec = get_latest_quote_record(db_conn, asset.id)
        assert rec["timestamp"] == "2025-01-11T10:00:00"
        assert rec["source"] == "mock_stock"

    def test_returns_true_when_both_bid_ask_valid(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": MockStockProvider()}
        assert sync_asset_quote(db_conn, asset, providers) is True

    def test_returns_false_when_provider_returns_none(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": NullQuoteProvider()}
        assert sync_asset_quote(db_conn, asset, providers) is False

    def test_nan_bid_ask_stored_as_null_returns_false(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": NanQuoteProvider()}
        result = sync_asset_quote(db_conn, asset, providers)
        assert result is False

        rec = get_latest_quote_record(db_conn, asset.id)
        assert rec is not None
        assert rec["bid"] is None
        assert rec["ask"] is None
        assert rec["last"] == 100.0

    def test_zero_bid_ask_stored_as_null_returns_false(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": ZeroBidAskProvider()}
        result = sync_asset_quote(db_conn, asset, providers)
        assert result is False

        rec = get_latest_quote_record(db_conn, asset.id)
        assert rec["bid"] is None
        assert rec["ask"] is None

    def test_non_syncable_type_returns_false(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="HOME", name="House", asset_type="real_estate"))
        providers = {"stock": MockStockProvider()}
        assert sync_asset_quote(db_conn, asset, providers) is False

    def test_missing_provider_raises(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        with pytest.raises(RuntimeError, match="No provider"):
            sync_asset_quote(db_conn, asset, providers={"crypto": MockCryptoProvider()})

    def test_crypto_quote_stored(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
        providers = {"crypto": MockCryptoProvider()}
        result = sync_asset_quote(db_conn, asset, providers)
        assert result is True

        rec = get_latest_quote_record(db_conn, asset.id)
        assert rec["bid"] == 40400.0
        assert rec["ask"] == 40600.0
        assert rec["last"] == 40500.0

    def test_etf_uses_stock_provider_for_quote(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="SPY", name="S&P ETF", asset_type="etf"))
        providers = {"etf": MockStockProvider()}
        result = sync_asset_quote(db_conn, asset, providers)
        assert result is True

    def test_failed_symbol_returns_false(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="BAD", name="Bad", asset_type="stock"))
        providers = {"stock": MockStockProvider(fail_symbols={"BAD"})}
        assert sync_asset_quote(db_conn, asset, providers) is False


# ===================================================================
# sync_asset_market_data unit tests
# ===================================================================

class TestSyncAssetMarketData:

    def test_both_price_and_quote_succeed(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": MockStockProvider()}
        result = sync_asset_market_data(db_conn, asset, "2025-01-01", "2025-01-15", providers)

        assert result["price_synced"] is True
        assert result["quote_synced"] is True
        assert result["errors"] == []

    def test_writes_both_market_prices_and_quotes(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": MockStockProvider()}
        sync_asset_market_data(db_conn, asset, "2025-01-01", "2025-01-15", providers)

        price = get_latest_price(db_conn, asset.id)
        assert price is not None
        assert price == 107.0

        rec = get_latest_quote_record(db_conn, asset.id)
        assert rec is not None
        assert rec["bid"] == 106.0
        assert rec["ask"] == 108.0

    def test_price_fails_quote_succeeds(self, db_conn):
        class PriceFailProvider(PriceProvider):
            def source_name(self):
                return "price_fail"
            def fetch_daily_prices(self, symbol, start_date, end_date):
                raise ValueError("Daily fetch failed")
            def fetch_latest_quote(self, symbol):
                return QuoteRecord(
                    symbol=symbol, bid=99.0, ask=101.0, last=100.0,
                    timestamp="2025-01-11T10:00:00", source=self.source_name(),
                )

        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": PriceFailProvider()}
        result = sync_asset_market_data(db_conn, asset, "2025-01-01", "2025-01-15", providers)

        assert result["price_synced"] is False
        assert result["quote_synced"] is True
        assert len(result["errors"]) == 1
        assert "Daily fetch failed" in result["errors"][0]

    def test_price_succeeds_quote_fails(self, db_conn):
        class QuoteFailProvider(PriceProvider):
            def source_name(self):
                return "quote_fail"
            def fetch_daily_prices(self, symbol, start_date, end_date):
                return [
                    PriceRecord(symbol=symbol, date="2025-01-10", close=100,
                                adjusted_close=100, source=self.source_name()),
                ]
            def fetch_latest_quote(self, symbol):
                raise ValueError("Quote fetch failed")

        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": QuoteFailProvider()}
        result = sync_asset_market_data(db_conn, asset, "2025-01-01", "2025-01-15", providers)

        assert result["price_synced"] is True
        assert result["quote_synced"] is False
        assert any("Quote" in e for e in result["errors"])

    def test_both_fail(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="BAD", name="Bad", asset_type="stock"))
        providers = {"stock": UnavailableProvider()}
        result = sync_asset_market_data(db_conn, asset, "2025-01-01", "2025-01-15", providers)

        assert result["price_synced"] is False
        assert result["quote_synced"] is False
        assert len(result["errors"]) >= 1

    def test_null_quote_means_quote_not_synced(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": NullQuoteProvider()}
        result = sync_asset_market_data(db_conn, asset, "2025-01-01", "2025-01-15", providers)

        assert result["quote_synced"] is False

    def test_nan_quotes_means_quote_not_synced(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": NanQuoteProvider()}
        result = sync_asset_market_data(db_conn, asset, "2025-01-01", "2025-01-15", providers)

        assert result["quote_synced"] is False


# ===================================================================
# sync_all_market_assets writes market_quotes rows
# ===================================================================

class TestSyncAllWritesQuotes:

    def test_market_quotes_rows_written_for_each_asset(self, db_conn):
        a1 = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        a2 = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
        providers = {"stock": MockStockProvider(), "crypto": MockCryptoProvider()}
        sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)

        rec1 = get_latest_quote_record(db_conn, a1.id)
        rec2 = get_latest_quote_record(db_conn, a2.id)
        assert rec1 is not None
        assert rec1["bid"] == 106.0
        assert rec1["ask"] == 108.0
        assert rec2 is not None
        assert rec2["bid"] == 40400.0
        assert rec2["ask"] == 40600.0

    def test_success_count_reflects_quote_success(self, db_conn):
        create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": MockStockProvider()}
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)
        assert result["succeeded"] == 1
        assert result["status"] == "success"

    def test_missing_bid_ask_counts_as_failure(self, db_conn):
        create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": NanQuoteProvider()}
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)
        assert result["succeeded"] == 0
        assert result["failed"] == 1
        assert any("executable quote" in e.lower() or "bid/ask" in e.lower()
                    for e in result["errors"])

    def test_null_quote_counts_as_failure_with_message(self, db_conn):
        create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": NullQuoteProvider()}
        result = sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)
        assert result["failed"] == 1
        assert result["succeeded"] == 0

    def test_etf_gets_market_quotes_row(self, db_conn):
        a = create_asset(db_conn, Asset(symbol="SPY", name="S&P ETF", asset_type="etf"))
        providers = {"etf": MockStockProvider()}
        sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)

        rec = get_latest_quote_record(db_conn, a.id)
        assert rec is not None
        assert rec["bid"] == 106.0

    def test_non_syncable_assets_get_no_quotes(self, db_conn):
        a = create_asset(db_conn, Asset(symbol="HOME", name="House", asset_type="real_estate"))
        providers = {"stock": MockStockProvider()}
        sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)

        rec = get_latest_quote_record(db_conn, a.id)
        assert rec is None


# ===================================================================
# list_latest_market_data repo output
# ===================================================================

class TestListLatestMarketData:

    def test_returns_bid_ask_last_and_valuation(self, db_conn):
        a = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": MockStockProvider()}
        sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)

        rows = list_latest_market_data(db_conn)
        assert len(rows) == 1
        r = rows[0]
        assert r["symbol"] == "AAPL"
        assert r["bid"] == 106.0
        assert r["ask"] == 108.0
        assert r["last"] == 107.0
        assert r["quote_time"] == "2025-01-11T10:00:00"
        assert r["quote_source"] == "mock_stock"
        assert r["valuation_price"] == 107.0
        assert r["valuation_date"] == "2025-01-11"

    def test_returns_separate_quote_and_valuation_columns(self, db_conn):
        a = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        providers = {"stock": MockStockProvider()}
        sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)

        rows = list_latest_market_data(db_conn)
        r = rows[0]
        assert r["bid"] != r["valuation_price"] or r["ask"] != r["valuation_price"]
        assert "quote_time" in r
        assert "quote_source" in r
        assert "valuation_date" in r

    def test_only_includes_syncable_types(self, db_conn):
        create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        create_asset(db_conn, Asset(symbol="HOME", name="House", asset_type="real_estate"))
        create_asset(db_conn, Asset(symbol="LOAN", name="Loan", asset_type="debt"))
        providers = {"stock": MockStockProvider()}
        sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)

        rows = list_latest_market_data(db_conn)
        symbols = {r["symbol"] for r in rows}
        assert "AAPL" in symbols
        assert "HOME" not in symbols
        assert "LOAN" not in symbols

    def test_multiple_assets_with_mixed_types(self, db_conn):
        create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        create_asset(db_conn, Asset(symbol="SPY", name="S&P ETF", asset_type="etf"))
        create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
        create_asset(db_conn, Asset(symbol="HOME", name="House", asset_type="real_estate"))
        providers = {
            "stock": MockStockProvider(),
            "etf": MockStockProvider(),
            "crypto": MockCryptoProvider(),
        }
        sync_all_market_assets(db_conn, "2025-01-01", "2025-01-15", providers)

        rows = list_latest_market_data(db_conn)
        symbols = {r["symbol"] for r in rows}
        assert symbols == {"AAPL", "SPY", "BTC"}

    def test_no_quote_shows_null_bid_ask(self, db_conn):
        a = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        upsert_ohlcv(db_conn, a.id, "AAPL", "stock", "2025-01-10",
                     100, 105, 99, 103, 103, 1e6, "yfinance")

        rows = list_latest_market_data(db_conn)
        assert len(rows) == 1
        r = rows[0]
        assert r["bid"] is None
        assert r["ask"] is None
        assert r["valuation_price"] == 103.0

    def test_quote_without_daily_prices(self, db_conn):
        from src.storage.quote_repo import upsert_quote
        a = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        upsert_quote(db_conn, a.id, "AAPL", "stock",
                     bid=149, ask=151, last=150,
                     timestamp="2025-01-10T10:00:00", source="test")

        rows = list_latest_market_data(db_conn)
        assert len(rows) == 1
        r = rows[0]
        assert r["bid"] == 149
        assert r["ask"] == 151
        assert r["valuation_price"] is None
        assert r["valuation_date"] is None
