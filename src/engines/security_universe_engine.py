import sqlite3

from src.models.asset import Asset
from src.models.security_master import SecurityMasterRecord
from src.storage.asset_repo import get_asset_by_symbol, create_asset
from src.storage.security_master_repo import (
    bulk_upsert_securities, search_securities, get_security_by_symbol,
    list_common_etfs, count_securities, clear_and_reload,
)
from src.data_sources.common_etfs import COMMON_ETFS
from src.data_sources.security_universe_data import COMMON_US_STOCKS


def initialize_universe(conn: sqlite3.Connection) -> int:
    if count_securities(conn) > 0:
        return 0
    return load_full_universe(conn)


def load_full_universe(conn: sqlite3.Connection) -> int:
    all_records = COMMON_US_STOCKS + COMMON_ETFS
    return clear_and_reload(conn, all_records)


def load_common_etfs_only(conn: sqlite3.Connection) -> int:
    return bulk_upsert_securities(conn, COMMON_ETFS)


def refresh_universe(conn: sqlite3.Connection) -> int:
    all_records = COMMON_US_STOCKS + COMMON_ETFS
    return bulk_upsert_securities(conn, all_records)


def search_universe(
    conn: sqlite3.Connection,
    query: str,
    asset_type: str | None = None,
    limit: int = 50,
) -> list[SecurityMasterRecord]:
    return search_securities(conn, query, asset_type=asset_type, limit=limit)


def get_common_etfs(conn: sqlite3.Connection) -> list[SecurityMasterRecord]:
    return list_common_etfs(conn)


def get_universe_count(conn: sqlite3.Connection) -> int:
    return count_securities(conn)


def ensure_asset_from_security(
    conn: sqlite3.Connection, security: SecurityMasterRecord,
) -> Asset:
    existing = get_asset_by_symbol(conn, security.symbol)
    if existing is not None:
        return existing
    asset = Asset(
        symbol=security.symbol,
        name=security.name,
        asset_type=security.asset_type,
        currency="USD",
        region="US",
        liquidity="liquid",
    )
    return create_asset(conn, asset)
