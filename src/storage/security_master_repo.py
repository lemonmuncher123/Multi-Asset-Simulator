import sqlite3
from src.models.security_master import SecurityMasterRecord


def upsert_security(conn: sqlite3.Connection, rec: SecurityMasterRecord) -> int:
    cursor = conn.execute(
        "INSERT INTO securities_master (symbol, name, asset_type, exchange, sector, industry, etf_category, is_common_etf) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(symbol, asset_type) DO UPDATE SET "
        "name=excluded.name, exchange=excluded.exchange, sector=excluded.sector, "
        "industry=excluded.industry, etf_category=excluded.etf_category, is_common_etf=excluded.is_common_etf",
        (rec.symbol, rec.name, rec.asset_type, rec.exchange, rec.sector,
         rec.industry, rec.etf_category, int(rec.is_common_etf)),
    )
    conn.commit()
    return cursor.lastrowid


def bulk_upsert_securities(conn: sqlite3.Connection, records: list[SecurityMasterRecord]) -> int:
    count = 0
    for rec in records:
        conn.execute(
            "INSERT INTO securities_master (symbol, name, asset_type, exchange, sector, industry, etf_category, is_common_etf) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(symbol, asset_type) DO UPDATE SET "
            "name=excluded.name, exchange=excluded.exchange, sector=excluded.sector, "
            "industry=excluded.industry, etf_category=excluded.etf_category, is_common_etf=excluded.is_common_etf",
            (rec.symbol, rec.name, rec.asset_type, rec.exchange, rec.sector,
             rec.industry, rec.etf_category, int(rec.is_common_etf)),
        )
        count += 1
    conn.commit()
    return count


def search_securities(
    conn: sqlite3.Connection,
    query: str,
    asset_type: str | None = None,
    limit: int = 50,
) -> list[SecurityMasterRecord]:
    q = f"%{query}%"
    if asset_type:
        rows = conn.execute(
            "SELECT * FROM securities_master WHERE (symbol LIKE ? OR name LIKE ?) "
            "AND asset_type = ? ORDER BY is_common_etf DESC, symbol LIMIT ?",
            (q, q, asset_type, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM securities_master WHERE (symbol LIKE ? OR name LIKE ?) "
            "ORDER BY is_common_etf DESC, symbol LIMIT ?",
            (q, q, limit),
        ).fetchall()
    return [_row_to_record(r) for r in rows]


def get_security_by_symbol(
    conn: sqlite3.Connection, symbol: str, asset_type: str | None = None,
) -> SecurityMasterRecord | None:
    if asset_type:
        row = conn.execute(
            "SELECT * FROM securities_master WHERE symbol = ? AND asset_type = ?",
            (symbol, asset_type),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM securities_master WHERE symbol = ? ORDER BY is_common_etf DESC",
            (symbol,),
        ).fetchone()
    if row is None:
        return None
    return _row_to_record(row)


def list_common_etfs(conn: sqlite3.Connection) -> list[SecurityMasterRecord]:
    rows = conn.execute(
        "SELECT * FROM securities_master WHERE is_common_etf = 1 ORDER BY symbol"
    ).fetchall()
    return [_row_to_record(r) for r in rows]


def count_securities(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM securities_master").fetchone()
    return row[0]


def clear_and_reload(conn: sqlite3.Connection, records: list[SecurityMasterRecord]) -> int:
    conn.execute("DELETE FROM securities_master")
    count = 0
    for rec in records:
        conn.execute(
            "INSERT INTO securities_master (symbol, name, asset_type, exchange, sector, industry, etf_category, is_common_etf) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (rec.symbol, rec.name, rec.asset_type, rec.exchange, rec.sector,
             rec.industry, rec.etf_category, int(rec.is_common_etf)),
        )
        count += 1
    conn.commit()
    return count


def _row_to_record(row: sqlite3.Row) -> SecurityMasterRecord:
    return SecurityMasterRecord(
        id=row["id"],
        symbol=row["symbol"],
        name=row["name"],
        asset_type=row["asset_type"],
        exchange=row["exchange"],
        sector=row["sector"],
        industry=row["industry"],
        etf_category=row["etf_category"],
        is_common_etf=bool(row["is_common_etf"]),
        created_at=row["created_at"],
    )
