import sqlite3
from src.models.portfolio_snapshot import PortfolioSnapshot


def create_snapshot(conn: sqlite3.Connection, snap: PortfolioSnapshot) -> PortfolioSnapshot:
    cursor = conn.execute(
        "INSERT OR REPLACE INTO portfolio_snapshots "
        "(date, cash, total_assets, total_liabilities, net_worth, allocation_json) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (snap.date, snap.cash, snap.total_assets, snap.total_liabilities,
         snap.net_worth, snap.allocation_json),
    )
    conn.commit()
    snap.id = cursor.lastrowid
    return snap


def list_snapshots(conn: sqlite3.Connection) -> list[PortfolioSnapshot]:
    rows = conn.execute(
        "SELECT * FROM portfolio_snapshots ORDER BY date"
    ).fetchall()
    return [_row_to_snapshot(r) for r in rows]


def get_latest_snapshot_on_or_before(
    conn: sqlite3.Connection, cutoff_date: str,
) -> PortfolioSnapshot | None:
    """Return the most recent snapshot whose `date` <= `cutoff_date`.

    Used by report generation to embed a period-end snapshot rather than
    today's portfolio state. Returns None if no snapshot is at or before
    the cutoff (e.g., reports generated before any daily snapshot
    has been recorded).
    """
    row = conn.execute(
        "SELECT * FROM portfolio_snapshots WHERE date <= ? "
        "ORDER BY date DESC LIMIT 1",
        (cutoff_date,),
    ).fetchone()
    if row is None:
        return None
    return _row_to_snapshot(row)


def _row_to_snapshot(row: sqlite3.Row) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        id=row["id"],
        date=row["date"],
        cash=row["cash"],
        total_assets=row["total_assets"],
        total_liabilities=row["total_liabilities"],
        net_worth=row["net_worth"],
        allocation_json=row["allocation_json"],
        created_at=row["created_at"],
    )
