import sqlite3
from src.models.debt import Debt


def create_debt(conn: sqlite3.Connection, debt: Debt) -> Debt:
    cursor = conn.execute(
        "INSERT INTO debts (asset_id, name, original_amount, current_balance, "
        "interest_rate, minimum_payment, due_date, notes, "
        "schedule_frequency, interest_period, monthly_payment_amount, "
        "cashflow_start_date, last_payment_date, "
        "plan_type, original_term_periods, "
        "preview_regular_payment, preview_period_count, "
        "preview_final_payment, preview_total_paid, "
        "preview_total_interest) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        "?, ?, ?, ?, ?)",
        (debt.asset_id, debt.name, debt.original_amount, debt.current_balance,
         debt.interest_rate, debt.minimum_payment, debt.due_date, debt.notes,
         debt.schedule_frequency, debt.interest_period,
         debt.monthly_payment_amount, debt.cashflow_start_date,
         debt.last_payment_date,
         debt.plan_type, debt.original_term_periods,
         debt.preview_regular_payment, debt.preview_period_count,
         debt.preview_final_payment, debt.preview_total_paid,
         debt.preview_total_interest),
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
        "minimum_payment=?, due_date=?, notes=?, "
        "schedule_frequency=?, interest_period=?, monthly_payment_amount=?, "
        "cashflow_start_date=?, last_payment_date=?, "
        "plan_type=?, original_term_periods=?, "
        "preview_regular_payment=?, preview_period_count=?, "
        "preview_final_payment=?, preview_total_paid=?, "
        "preview_total_interest=?, "
        "updated_at=datetime('now') WHERE id=?",
        (debt.name, debt.current_balance, debt.interest_rate,
         debt.minimum_payment, debt.due_date, debt.notes,
         debt.schedule_frequency, debt.interest_period,
         debt.monthly_payment_amount, debt.cashflow_start_date,
         debt.last_payment_date,
         debt.plan_type, debt.original_term_periods,
         debt.preview_regular_payment, debt.preview_period_count,
         debt.preview_final_payment, debt.preview_total_paid,
         debt.preview_total_interest, debt.id),
    )
    conn.commit()


def _row_to_debt(row: sqlite3.Row) -> Debt:
    """Build a Debt dataclass from a sqlite3.Row.

    Schema v10 guarantees every column is present (the migration
    populates them on upgrade), so the defensive
    ``row[col] if col in keys else default`` fallbacks were removed.
    """
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
        schedule_frequency=row["schedule_frequency"],
        interest_period=row["interest_period"],
        monthly_payment_amount=row["monthly_payment_amount"],
        cashflow_start_date=row["cashflow_start_date"],
        last_payment_date=row["last_payment_date"],
        plan_type=row["plan_type"],
        original_term_periods=row["original_term_periods"],
        preview_regular_payment=row["preview_regular_payment"],
        preview_period_count=row["preview_period_count"],
        preview_final_payment=row["preview_final_payment"],
        preview_total_paid=row["preview_total_paid"],
        preview_total_interest=row["preview_total_interest"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
