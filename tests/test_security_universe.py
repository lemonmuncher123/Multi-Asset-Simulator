import pytest
from src.models.asset import Asset
from src.models.security_master import SecurityMasterRecord
from src.storage.asset_repo import create_asset, get_asset_by_symbol
from src.storage.security_master_repo import (
    upsert_security, bulk_upsert_securities, search_securities,
    get_security_by_symbol, list_common_etfs, count_securities,
    clear_and_reload,
)
from src.engines.security_universe_engine import (
    initialize_universe, load_full_universe, load_common_etfs_only,
    refresh_universe, search_universe, get_common_etfs, get_universe_count,
    ensure_asset_from_security,
)
from src.data_sources.common_etfs import COMMON_ETFS
from src.data_sources.security_universe_data import COMMON_US_STOCKS


# --- 1. Table exists ---

def test_securities_master_table_exists(db_conn):
    tables = [r[0] for r in db_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    assert "securities_master" in tables


# --- 2. Upsert single security ---

def test_upsert_single_security(db_conn):
    rec = SecurityMasterRecord(symbol="AAPL", name="Apple", asset_type="stock", exchange="NASDAQ")
    upsert_security(db_conn, rec)
    assert count_securities(db_conn) == 1


# --- 3. Upsert updates existing ---

def test_upsert_updates_existing(db_conn):
    rec = SecurityMasterRecord(symbol="AAPL", name="Apple", asset_type="stock")
    upsert_security(db_conn, rec)
    rec2 = SecurityMasterRecord(symbol="AAPL", name="Apple Inc.", asset_type="stock", exchange="NASDAQ")
    upsert_security(db_conn, rec2)
    assert count_securities(db_conn) == 1
    found = get_security_by_symbol(db_conn, "AAPL")
    assert found.name == "Apple Inc."
    assert found.exchange == "NASDAQ"


# --- 4. Bulk upsert ---

def test_bulk_upsert(db_conn):
    records = [
        SecurityMasterRecord(symbol="AAPL", name="Apple", asset_type="stock"),
        SecurityMasterRecord(symbol="MSFT", name="Microsoft", asset_type="stock"),
        SecurityMasterRecord(symbol="SPY", name="S&P 500 ETF", asset_type="etf"),
    ]
    count = bulk_upsert_securities(db_conn, records)
    assert count == 3
    assert count_securities(db_conn) == 3


# --- 5. Search by symbol ---

def test_search_by_symbol(db_conn):
    bulk_upsert_securities(db_conn, [
        SecurityMasterRecord(symbol="AAPL", name="Apple", asset_type="stock"),
        SecurityMasterRecord(symbol="MSFT", name="Microsoft", asset_type="stock"),
    ])
    results = search_securities(db_conn, "AAPL")
    assert len(results) == 1
    assert results[0].symbol == "AAPL"


# --- 6. Search by name ---

def test_search_by_name(db_conn):
    bulk_upsert_securities(db_conn, [
        SecurityMasterRecord(symbol="AAPL", name="Apple Inc.", asset_type="stock"),
        SecurityMasterRecord(symbol="MSFT", name="Microsoft Corporation", asset_type="stock"),
    ])
    results = search_securities(db_conn, "micro")
    assert len(results) == 1
    assert results[0].symbol == "MSFT"


# --- 7. Search with type filter ---

def test_search_with_type_filter(db_conn):
    bulk_upsert_securities(db_conn, [
        SecurityMasterRecord(symbol="SPY", name="S&P 500 ETF", asset_type="etf", is_common_etf=True),
        SecurityMasterRecord(symbol="SPYG", name="SPDR Growth", asset_type="etf"),
        SecurityMasterRecord(symbol="SPGI", name="S&P Global", asset_type="stock"),
    ])
    results = search_securities(db_conn, "SP", asset_type="etf")
    symbols = {r.symbol for r in results}
    assert "SPY" in symbols
    assert "SPYG" in symbols
    assert "SPGI" not in symbols


# --- 8. Get by symbol ---

def test_get_by_symbol(db_conn):
    upsert_security(db_conn, SecurityMasterRecord(symbol="AAPL", name="Apple", asset_type="stock"))
    found = get_security_by_symbol(db_conn, "AAPL")
    assert found is not None
    assert found.name == "Apple"


# --- 9. Get by symbol not found ---

def test_get_by_symbol_not_found(db_conn):
    assert get_security_by_symbol(db_conn, "ZZZZ") is None


# --- 10. List common ETFs ---

def test_list_common_etfs(db_conn):
    bulk_upsert_securities(db_conn, [
        SecurityMasterRecord(symbol="SPY", name="S&P 500 ETF", asset_type="etf", is_common_etf=True),
        SecurityMasterRecord(symbol="QQQ", name="QQQ ETF", asset_type="etf", is_common_etf=True),
        SecurityMasterRecord(symbol="AAPL", name="Apple", asset_type="stock", is_common_etf=False),
    ])
    etfs = list_common_etfs(db_conn)
    assert len(etfs) == 2
    symbols = {e.symbol for e in etfs}
    assert "SPY" in symbols
    assert "QQQ" in symbols


# --- 11. Common ETFs list has 50+ entries ---

def test_common_etfs_data_has_enough(db_conn):
    assert len(COMMON_ETFS) >= 50


# --- 12. Common stocks list is populated ---

def test_common_stocks_data_populated(db_conn):
    assert len(COMMON_US_STOCKS) >= 50


# --- 13. Clear and reload ---

def test_clear_and_reload(db_conn):
    bulk_upsert_securities(db_conn, [
        SecurityMasterRecord(symbol="OLD", name="Old Stock", asset_type="stock"),
    ])
    assert count_securities(db_conn) == 1

    new_records = [
        SecurityMasterRecord(symbol="NEW1", name="New 1", asset_type="stock"),
        SecurityMasterRecord(symbol="NEW2", name="New 2", asset_type="etf"),
    ]
    count = clear_and_reload(db_conn, new_records)
    assert count == 2
    assert count_securities(db_conn) == 2
    assert get_security_by_symbol(db_conn, "OLD") is None


# --- 14. Initialize universe ---

def test_initialize_universe(db_conn):
    count = initialize_universe(db_conn)
    assert count > 0
    assert count_securities(db_conn) == count


# --- 15. Initialize skips if already loaded ---

def test_initialize_skips_if_loaded(db_conn):
    initialize_universe(db_conn)
    count_after_first = count_securities(db_conn)
    result = initialize_universe(db_conn)
    assert result == 0
    assert count_securities(db_conn) == count_after_first


# --- 16. Refresh universe updates ---

def test_refresh_universe(db_conn):
    initialize_universe(db_conn)
    count = refresh_universe(db_conn)
    assert count > 0


# --- 17. Load common ETFs only ---

def test_load_common_etfs_only(db_conn):
    count = load_common_etfs_only(db_conn)
    assert count == len(COMMON_ETFS)
    etfs = list_common_etfs(db_conn)
    assert len(etfs) == len(COMMON_ETFS)


# --- 18. Ensure asset from security creates asset ---

def test_ensure_asset_creates_new(db_conn):
    security = SecurityMasterRecord(symbol="AAPL", name="Apple Inc.", asset_type="stock")
    upsert_security(db_conn, security)

    asset = ensure_asset_from_security(db_conn, security)
    assert asset.id is not None
    assert asset.symbol == "AAPL"
    assert asset.asset_type == "stock"

    found = get_asset_by_symbol(db_conn, "AAPL")
    assert found is not None
    assert found.id == asset.id


# --- 19. Ensure asset returns existing ---

def test_ensure_asset_returns_existing(db_conn):
    existing = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    security = SecurityMasterRecord(symbol="AAPL", name="Apple Inc.", asset_type="stock")

    asset = ensure_asset_from_security(db_conn, security)
    assert asset.id == existing.id


# --- 20. Search universe via engine ---

def test_search_universe_engine(db_conn):
    initialize_universe(db_conn)
    results = search_universe(db_conn, "Apple")
    assert len(results) >= 1
    assert any(r.symbol == "AAPL" for r in results)


# --- 21. ETF categories are diverse ---

def test_etf_categories_diverse(db_conn):
    categories = {e.etf_category for e in COMMON_ETFS}
    assert len(categories) >= 10


# --- 22. UNIQUE constraint prevents duplication ---

def test_unique_constraint(db_conn):
    rec = SecurityMasterRecord(symbol="AAPL", name="Apple", asset_type="stock")
    upsert_security(db_conn, rec)
    upsert_security(db_conn, rec)
    upsert_security(db_conn, rec)
    assert count_securities(db_conn) == 1


# --- 23. Same symbol different asset_type allowed ---

def test_same_symbol_different_type(db_conn):
    bulk_upsert_securities(db_conn, [
        SecurityMasterRecord(symbol="SPY", name="S&P 500 ETF", asset_type="etf"),
        SecurityMasterRecord(symbol="SPY", name="SPY Stock Entry", asset_type="stock"),
    ])
    assert count_securities(db_conn) == 2


# --- 24. Search result limit ---

def test_search_result_limit(db_conn):
    initialize_universe(db_conn)
    results = search_securities(db_conn, "", limit=5)
    assert len(results) <= 5


# --- 25. Common ETFs all flagged ---

def test_common_etfs_all_flagged(db_conn):
    for etf in COMMON_ETFS:
        assert etf.is_common_etf is True
        assert etf.asset_type == "etf"


# --- 26. Securities search works regardless of yfinance ---

def test_search_works_without_yfinance(db_conn):
    from unittest.mock import patch
    initialize_universe(db_conn)
    with patch("src.utils.deps.is_yfinance_available", return_value=False):
        results = search_universe(db_conn, "Apple")
        assert len(results) >= 1
        assert any(r.symbol == "AAPL" for r in results)
