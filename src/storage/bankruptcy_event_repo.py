"""Repository for the `bankruptcy_events` table.

A bankruptcy_event is the simulator's terminal failure state: a scheduled
debt or mortgage payment could not be funded even after force-selling all
sellable assets, OR cash went negative with no sellable assets left to
liquidate. It is NOT a recoverable "overdue payment" — it's the moment
the portfolio is declared bankrupt.

Idempotency: while `status='active'`, a (trigger_kind, asset_id, due_date)
triple is unique. Repeated startup auto-settle runs that hit the same
unfunded obligation will not stack duplicate active rows.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


VALID_TRIGGER_KINDS = ("debt", "mortgage", "multiple", "negative_cash")


@dataclass
class BankruptcyEvent:
    id: int | None
    event_date: str
    trigger_kind: str
    asset_id: int | None
    due_date: str | None
    amount_due: float
    cash_balance: float
    shortfall_amount: float
    status: str
    notes: str | None
    created_at: str


def record_bankruptcy_event(
    conn: sqlite3.Connection, *,
    event_date: str,
    trigger_kind: str,
    amount_due: float = 0.0,
    cash_balance: float = 0.0,
    shortfall_amount: float = 0.0,
    asset_id: int | None = None,
    due_date: str | None = None,
    notes: str | None = None,
) -> BankruptcyEvent | None:
    """Persist one bankruptcy event.

    Idempotent: when ``asset_id`` and ``due_date`` are both supplied, an
    active row matching ``(trigger_kind, asset_id, due_date)`` is treated
    as already recorded and we return ``None``. ``negative_cash`` events
    (which carry no asset_id/due_date) dedupe on ``(trigger_kind, status)``
    so repeated startup runs in the same negative-cash state don't stack.
    """
    if trigger_kind not in VALID_TRIGGER_KINDS:
        raise ValueError(f"Invalid bankruptcy trigger_kind: {trigger_kind!r}")

    if asset_id is not None and due_date is not None:
        existing = conn.execute(
            "SELECT id FROM bankruptcy_events "
            "WHERE trigger_kind=? AND asset_id=? AND due_date=? "
            "AND status='active'",
            (trigger_kind, asset_id, due_date),
        ).fetchone()
    else:
        existing = conn.execute(
            "SELECT id FROM bankruptcy_events "
            "WHERE trigger_kind=? AND asset_id IS NULL "
            "AND due_date IS NULL AND status='active'",
            (trigger_kind,),
        ).fetchone()
    if existing is not None:
        return None

    cur = conn.execute(
        "INSERT INTO bankruptcy_events "
        "(event_date, trigger_kind, asset_id, due_date, amount_due, "
        "cash_balance, shortfall_amount, status, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?)",
        (event_date, trigger_kind, asset_id, due_date,
         float(amount_due), float(cash_balance), float(shortfall_amount),
         notes),
    )
    conn.commit()
    return get_bankruptcy_event(conn, cur.lastrowid)


def get_bankruptcy_event(
    conn: sqlite3.Connection, row_id: int,
) -> BankruptcyEvent | None:
    row = conn.execute(
        "SELECT * FROM bankruptcy_events WHERE id=?", (row_id,),
    ).fetchone()
    if row is None:
        return None
    return _row_to_event(row)


def list_active_bankruptcy_events(
    conn: sqlite3.Connection,
) -> list[BankruptcyEvent]:
    """All bankruptcy_events rows that are still ``status='active'``."""
    rows = conn.execute(
        "SELECT * FROM bankruptcy_events WHERE status='active' "
        "ORDER BY event_date, id",
    ).fetchall()
    return [_row_to_event(r) for r in rows]


def has_active_bankruptcy_event(conn: sqlite3.Connection) -> bool:
    """Lightweight existence check for the risk engine — single COUNT query."""
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM bankruptcy_events WHERE status='active'",
    ).fetchone()
    return (row["cnt"] if row else 0) > 0


def clear_bankruptcy_events(conn: sqlite3.Connection) -> int:
    """Remove all bankruptcy_events rows. Used by data-clear flows.

    Returns the number of rows deleted.
    """
    cur = conn.execute("DELETE FROM bankruptcy_events")
    conn.commit()
    return cur.rowcount or 0


def _row_to_event(row: sqlite3.Row) -> BankruptcyEvent:
    return BankruptcyEvent(
        id=row["id"],
        event_date=row["event_date"],
        trigger_kind=row["trigger_kind"],
        asset_id=row["asset_id"],
        due_date=row["due_date"],
        amount_due=row["amount_due"],
        cash_balance=row["cash_balance"],
        shortfall_amount=row["shortfall_amount"],
        status=row["status"],
        notes=row["notes"],
        created_at=row["created_at"],
    )
