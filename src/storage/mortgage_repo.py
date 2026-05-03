import sqlite3
from src.models.mortgage import Mortgage


def create_mortgage(conn: sqlite3.Connection, mortgage: Mortgage) -> Mortgage:
    cursor = conn.execute(
        "INSERT INTO mortgages (property_id, name, original_amount, current_balance, "
        "interest_rate, minimum_payment, due_date, notes, "
        "monthly_payment_amount, cashflow_start_date, last_payment_date, "
        "plan_type, original_term_periods, "
        "preview_regular_payment, preview_period_count, "
        "preview_final_payment, preview_total_paid, "
        "preview_total_interest) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (mortgage.property_id, mortgage.name, mortgage.original_amount,
         mortgage.current_balance, mortgage.interest_rate,
         mortgage.minimum_payment, mortgage.due_date, mortgage.notes,
         mortgage.monthly_payment_amount, mortgage.cashflow_start_date,
         mortgage.last_payment_date,
         mortgage.plan_type, mortgage.original_term_periods,
         mortgage.preview_regular_payment, mortgage.preview_period_count,
         mortgage.preview_final_payment, mortgage.preview_total_paid,
         mortgage.preview_total_interest),
    )
    conn.commit()
    mortgage.id = cursor.lastrowid
    return mortgage


def get_mortgage(conn: sqlite3.Connection, mortgage_id: int) -> Mortgage | None:
    row = conn.execute("SELECT * FROM mortgages WHERE id = ?", (mortgage_id,)).fetchone()
    if row is None:
        return None
    return _row_to_mortgage(row)


def get_mortgage_by_property(
    conn: sqlite3.Connection, property_id: int,
) -> Mortgage | None:
    row = conn.execute(
        "SELECT * FROM mortgages WHERE property_id = ?", (property_id,),
    ).fetchone()
    if row is None:
        return None
    return _row_to_mortgage(row)


def list_mortgages(conn: sqlite3.Connection) -> list[Mortgage]:
    rows = conn.execute("SELECT * FROM mortgages ORDER BY name").fetchall()
    return [_row_to_mortgage(r) for r in rows]


def list_active_mortgages(conn: sqlite3.Connection) -> list[Mortgage]:
    """Mortgages with a remaining balance. Mirrors the debt convention:
    `current_balance > 0` IS the active state — there is no separate
    status column."""
    rows = conn.execute(
        "SELECT * FROM mortgages WHERE current_balance > 0 ORDER BY name"
    ).fetchall()
    return [_row_to_mortgage(r) for r in rows]


def update_mortgage(conn: sqlite3.Connection, mortgage: Mortgage) -> None:
    conn.execute(
        "UPDATE mortgages SET name=?, current_balance=?, interest_rate=?, "
        "minimum_payment=?, due_date=?, notes=?, "
        "monthly_payment_amount=?, cashflow_start_date=?, last_payment_date=?, "
        "plan_type=?, original_term_periods=?, "
        "preview_regular_payment=?, preview_period_count=?, "
        "preview_final_payment=?, preview_total_paid=?, "
        "preview_total_interest=?, "
        "updated_at=datetime('now') WHERE id=?",
        (mortgage.name, mortgage.current_balance, mortgage.interest_rate,
         mortgage.minimum_payment, mortgage.due_date, mortgage.notes,
         mortgage.monthly_payment_amount, mortgage.cashflow_start_date,
         mortgage.last_payment_date,
         mortgage.plan_type, mortgage.original_term_periods,
         mortgage.preview_regular_payment, mortgage.preview_period_count,
         mortgage.preview_final_payment, mortgage.preview_total_paid,
         mortgage.preview_total_interest, mortgage.id),
    )
    conn.commit()


def delete_mortgage_by_property(
    conn: sqlite3.Connection, property_id: int,
) -> None:
    """Used by data_management / full_data_io clears. Normal app flow
    leaves the row in place when a mortgage is paid off — current_balance=0
    IS the paid-off state, mirroring debts."""
    conn.execute("DELETE FROM mortgages WHERE property_id = ?", (property_id,))
    conn.commit()


def _row_to_mortgage(row: sqlite3.Row) -> Mortgage:
    return Mortgage(
        id=row["id"],
        property_id=row["property_id"],
        name=row["name"],
        original_amount=row["original_amount"],
        current_balance=row["current_balance"],
        interest_rate=row["interest_rate"],
        minimum_payment=row["minimum_payment"],
        due_date=row["due_date"],
        notes=row["notes"],
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
