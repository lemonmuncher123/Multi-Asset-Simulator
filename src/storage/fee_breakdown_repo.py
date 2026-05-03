import sqlite3
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class FeeBreakdownRow:
    id: int | None = None
    transaction_id: int = 0
    fee_type: str = ""
    amount: float = 0.0
    rate: float | None = None
    notes: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


def create_fee_breakdown(conn: sqlite3.Connection, row: FeeBreakdownRow) -> FeeBreakdownRow:
    cursor = conn.execute(
        "INSERT INTO transaction_fee_breakdown (transaction_id, fee_type, amount, rate, notes) "
        "VALUES (?, ?, ?, ?, ?)",
        (row.transaction_id, row.fee_type, row.amount, row.rate, row.notes),
    )
    conn.commit()
    row.id = cursor.lastrowid
    return row


def list_fee_breakdowns(conn: sqlite3.Connection, transaction_id: int) -> list[FeeBreakdownRow]:
    rows = conn.execute(
        "SELECT * FROM transaction_fee_breakdown WHERE transaction_id = ? ORDER BY id",
        (transaction_id,),
    ).fetchall()
    return [_row_to_obj(r) for r in rows]


def _row_to_obj(row: sqlite3.Row) -> FeeBreakdownRow:
    return FeeBreakdownRow(
        id=row["id"],
        transaction_id=row["transaction_id"],
        fee_type=row["fee_type"],
        amount=row["amount"],
        rate=row["rate"],
        notes=row["notes"],
        created_at=row["created_at"],
    )
