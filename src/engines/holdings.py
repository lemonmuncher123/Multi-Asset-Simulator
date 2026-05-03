import sqlite3

from src.models.asset_types import SELLABLE_ASSET_TYPES

_EPSILON = 1e-9


def get_asset_quantity(conn: sqlite3.Connection, asset_id: int, as_of_date: str | None = None) -> float:
    if as_of_date:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(
                CASE
                    WHEN txn_type = 'buy' THEN quantity
                    WHEN txn_type = 'sell' THEN -quantity
                    WHEN txn_type = 'manual_adjustment' AND quantity IS NOT NULL
                        THEN quantity
                    ELSE 0
                END
            ), 0) as net_qty
            FROM transactions
            WHERE asset_id = ? AND date <= ?
            """,
            (asset_id, as_of_date),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(
                CASE
                    WHEN txn_type = 'buy' THEN quantity
                    WHEN txn_type = 'sell' THEN -quantity
                    WHEN txn_type = 'manual_adjustment' AND quantity IS NOT NULL
                        THEN quantity
                    ELSE 0
                END
            ), 0) as net_qty
            FROM transactions
            WHERE asset_id = ?
            """,
            (asset_id,),
        ).fetchone()
    return row["net_qty"]


def has_sufficient_quantity(
    conn: sqlite3.Connection, asset_id: int, quantity: float, as_of_date: str | None = None,
) -> bool:
    available = get_asset_quantity(conn, asset_id, as_of_date)
    return available - quantity >= -_EPSILON


def find_negative_positions(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT t.asset_id, a.symbol,
            SUM(CASE
                    WHEN t.txn_type = 'buy' THEN t.quantity
                    WHEN t.txn_type = 'sell' THEN -t.quantity
                    WHEN t.txn_type = 'manual_adjustment' AND t.quantity IS NOT NULL
                        THEN t.quantity
                    ELSE 0
                END) as net_qty
        FROM transactions t
        JOIN assets a ON t.asset_id = a.id
        WHERE t.asset_id IS NOT NULL
          AND a.asset_type IN ('stock', 'etf', 'crypto', 'custom')
        GROUP BY t.asset_id
        HAVING net_qty < -1e-9
        """,
    ).fetchall()
    return [{"asset_id": r["asset_id"], "symbol": r["symbol"], "net_qty": r["net_qty"]} for r in rows]
