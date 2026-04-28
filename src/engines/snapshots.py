import json
import sqlite3
from datetime import date

from src.models.portfolio_snapshot import PortfolioSnapshot
from src.engines.portfolio import get_portfolio_summary
from src.engines.allocation import get_full_allocation
from src.storage.snapshot_repo import create_snapshot


def has_meaningful_portfolio_state(conn: sqlite3.Connection) -> bool:
    for table in ("transactions", "assets", "properties", "debts"):
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        if row[0] > 0:
            return True

    summary = get_portfolio_summary(conn)
    for key in ("cash", "total_assets", "total_liabilities", "net_worth"):
        if summary[key] != 0:
            return True

    return False


def build_portfolio_snapshot(
    conn: sqlite3.Connection, snapshot_date: date | None = None,
) -> PortfolioSnapshot:
    snapshot_date = snapshot_date or date.today()
    summary = get_portfolio_summary(conn)
    allocation = get_full_allocation(conn)

    return PortfolioSnapshot(
        date=snapshot_date.isoformat(),
        cash=summary["cash"],
        total_assets=summary["total_assets"],
        total_liabilities=summary["total_liabilities"],
        net_worth=summary["net_worth"],
        allocation_json=json.dumps(allocation),
    )


def record_daily_portfolio_snapshot(
    conn: sqlite3.Connection, snapshot_date: date | None = None,
) -> PortfolioSnapshot | None:
    if not has_meaningful_portfolio_state(conn):
        return None

    snap = build_portfolio_snapshot(conn, snapshot_date)
    return create_snapshot(conn, snap)
