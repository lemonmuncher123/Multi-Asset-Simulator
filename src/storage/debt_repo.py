import sqlite3
from src.models.debt import Debt


def create_debt(conn: sqlite3.Connection, debt: Debt) -> Debt:
    cursor = conn.execute(
        "INSERT INTO debts (asset_id, name, original_amount, current_balance, "
        "interest_rate, minimum_payment, due_date, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (debt.asset_id, debt.name, debt.original_amount, debt.current_balance,
         debt.interest_rate, debt.minimum_payment, debt.due_date, debt.notes),
    )
    conn.commit()
    debt.id = cursor.lastrowid
    return debt


def get_debt(conn: sqlite3.Connection, debt_id: int) -> Debt | None:
    row = conn.execute("SELECT * FROM debts WHERE id = ?", (debt_id,)).fetchone()
    if row is None:
        return None
    return _row_to_debt(row)


def get_debt_by_asset(conn: sqlite3.Connection, asset_id: int) -> Debt | None:
    row = conn.execute("SELECT * FROM debts WHERE asset_id = ?", (asset_id,)).fetchone()
    if row is None:
        return None
    return _row_to_debt(row)


def list_debts(conn: sqlite3.Connection) -> list[Debt]:
    rows = conn.execute("SELECT * FROM debts ORDER BY name").fetchall()
    return [_row_to_debt(r) for r in rows]


def update_debt(conn: sqlite3.Connection, debt: Debt) -> None:
    conn.execute(
        "UPDATE debts SET name=?, current_balance=?, interest_rate=?, "
        "minimum_payment=?, due_date=?, notes=?, updated_at=datetime('now') WHERE id=?",
        (debt.name, debt.current_balance, debt.interest_rate,
         debt.minimum_payment, debt.due_date, debt.notes, debt.id),
    )
    conn.commit()


def _row_to_debt(row: sqlite3.Row) -> Debt:
    return Debt(
        id=row["id"],
        asset_id=row["asset_id"],
        name=row["name"],
        original_amount=row["original_amount"],
        current_balance=row["current_balance"],
        interest_rate=row["interest_rate"],
        minimum_payment=row["minimum_payment"],
        due_date=row["due_date"],
        notes=row["notes"],
        updated_at=row["updated_at"],
    )
