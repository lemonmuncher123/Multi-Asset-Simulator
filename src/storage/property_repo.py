import sqlite3
from src.models.property_asset import PropertyAsset


def create_property(conn: sqlite3.Connection, prop: PropertyAsset) -> PropertyAsset:
    cursor = conn.execute(
        "INSERT INTO properties (asset_id, address, purchase_date, purchase_price, "
        "current_value, down_payment, mortgage_balance, mortgage_interest_rate, "
        "monthly_mortgage_payment, monthly_rent, monthly_property_tax, "
        "monthly_insurance, monthly_hoa, monthly_maintenance_reserve, "
        "monthly_property_management, monthly_expense, vacancy_rate, "
        "status, sold_date, sold_price, sale_fees, rent_collection_frequency, "
        "cashflow_start_date, notes, "
        "entry_type, loan_term_years, down_payment_type, down_payment_input_value, "
        "monthly_mortgage_override_enabled, monthly_mortgage_override, "
        "rent_input_amount, rent_input_frequency, "
        "property_tax_input_type, property_tax_input_value, "
        "insurance_input_type, insurance_input_value, "
        "maintenance_input_type, maintenance_input_value, "
        "management_input_type, management_input_value) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (prop.asset_id, prop.address, prop.purchase_date, prop.purchase_price,
         prop.current_value, prop.down_payment, prop.mortgage_balance,
         prop.mortgage_interest_rate, prop.monthly_mortgage_payment,
         prop.monthly_rent, prop.monthly_property_tax, prop.monthly_insurance,
         prop.monthly_hoa, prop.monthly_maintenance_reserve,
         prop.monthly_property_management, prop.monthly_expense,
         prop.vacancy_rate, prop.status, prop.sold_date, prop.sold_price,
         prop.sale_fees, prop.rent_collection_frequency,
         prop.cashflow_start_date, prop.notes,
         prop.entry_type, prop.loan_term_years, prop.down_payment_type,
         prop.down_payment_input_value,
         prop.monthly_mortgage_override_enabled, prop.monthly_mortgage_override,
         prop.rent_input_amount, prop.rent_input_frequency,
         prop.property_tax_input_type, prop.property_tax_input_value,
         prop.insurance_input_type, prop.insurance_input_value,
         prop.maintenance_input_type, prop.maintenance_input_value,
         prop.management_input_type, prop.management_input_value),
    )
    conn.commit()
    prop.id = cursor.lastrowid
    return prop


def get_property(conn: sqlite3.Connection, property_id: int) -> PropertyAsset | None:
    row = conn.execute("SELECT * FROM properties WHERE id = ?", (property_id,)).fetchone()
    if row is None:
        return None
    return _row_to_property(row)


def get_property_by_asset(conn: sqlite3.Connection, asset_id: int) -> PropertyAsset | None:
    row = conn.execute("SELECT * FROM properties WHERE asset_id = ?", (asset_id,)).fetchone()
    if row is None:
        return None
    return _row_to_property(row)


def list_properties(conn: sqlite3.Connection) -> list[PropertyAsset]:
    rows = conn.execute("SELECT * FROM properties ORDER BY id").fetchall()
    return [_row_to_property(r) for r in rows]


def update_property(conn: sqlite3.Connection, prop: PropertyAsset) -> None:
    conn.execute(
        "UPDATE properties SET address=?, purchase_date=?, purchase_price=?, "
        "current_value=?, down_payment=?, mortgage_balance=?, "
        "mortgage_interest_rate=?, monthly_mortgage_payment=?, monthly_rent=?, "
        "monthly_property_tax=?, monthly_insurance=?, monthly_hoa=?, "
        "monthly_maintenance_reserve=?, monthly_property_management=?, "
        "monthly_expense=?, vacancy_rate=?, status=?, sold_date=?, sold_price=?, "
        "sale_fees=?, rent_collection_frequency=?, cashflow_start_date=?, notes=?, "
        "entry_type=?, loan_term_years=?, down_payment_type=?, "
        "down_payment_input_value=?, monthly_mortgage_override_enabled=?, "
        "monthly_mortgage_override=?, rent_input_amount=?, rent_input_frequency=?, "
        "property_tax_input_type=?, property_tax_input_value=?, "
        "insurance_input_type=?, insurance_input_value=?, "
        "maintenance_input_type=?, maintenance_input_value=?, "
        "management_input_type=?, management_input_value=?, "
        "updated_at=datetime('now') WHERE id=?",
        (prop.address, prop.purchase_date, prop.purchase_price, prop.current_value,
         prop.down_payment, prop.mortgage_balance, prop.mortgage_interest_rate,
         prop.monthly_mortgage_payment, prop.monthly_rent,
         prop.monthly_property_tax, prop.monthly_insurance, prop.monthly_hoa,
         prop.monthly_maintenance_reserve, prop.monthly_property_management,
         prop.monthly_expense, prop.vacancy_rate, prop.status, prop.sold_date,
         prop.sold_price, prop.sale_fees, prop.rent_collection_frequency,
         prop.cashflow_start_date, prop.notes,
         prop.entry_type, prop.loan_term_years, prop.down_payment_type,
         prop.down_payment_input_value,
         prop.monthly_mortgage_override_enabled, prop.monthly_mortgage_override,
         prop.rent_input_amount, prop.rent_input_frequency,
         prop.property_tax_input_type, prop.property_tax_input_value,
         prop.insurance_input_type, prop.insurance_input_value,
         prop.maintenance_input_type, prop.maintenance_input_value,
         prop.management_input_type, prop.management_input_value,
         prop.id),
    )
    conn.commit()


def list_active_properties(conn: sqlite3.Connection) -> list[PropertyAsset]:
    rows = conn.execute(
        "SELECT * FROM properties WHERE status = 'active' ORDER BY id"
    ).fetchall()
    return [_row_to_property(r) for r in rows]


def delete_property_by_asset(conn: sqlite3.Connection, asset_id: int) -> None:
    conn.execute("DELETE FROM properties WHERE asset_id = ?", (asset_id,))
    conn.commit()


def _row_to_property(row: sqlite3.Row) -> PropertyAsset:
    keys = row.keys()
    return PropertyAsset(
        id=row["id"],
        asset_id=row["asset_id"],
        address=row["address"],
        purchase_date=row["purchase_date"],
        purchase_price=row["purchase_price"],
        current_value=row["current_value"],
        down_payment=row["down_payment"],
        mortgage_balance=row["mortgage_balance"],
        mortgage_interest_rate=row["mortgage_interest_rate"],
        monthly_mortgage_payment=row["monthly_mortgage_payment"],
        monthly_rent=row["monthly_rent"],
        monthly_property_tax=row["monthly_property_tax"],
        monthly_insurance=row["monthly_insurance"],
        monthly_hoa=row["monthly_hoa"],
        monthly_maintenance_reserve=row["monthly_maintenance_reserve"],
        monthly_property_management=row["monthly_property_management"],
        monthly_expense=row["monthly_expense"],
        vacancy_rate=row["vacancy_rate"],
        status=row["status"],
        sold_date=row["sold_date"],
        sold_price=row["sold_price"],
        sale_fees=row["sale_fees"],
        rent_collection_frequency=row["rent_collection_frequency"],
        cashflow_start_date=row["cashflow_start_date"],
        notes=row["notes"],
        entry_type=row["entry_type"] if "entry_type" in keys else "existing_property",
        loan_term_years=row["loan_term_years"] if "loan_term_years" in keys else None,
        down_payment_type=row["down_payment_type"] if "down_payment_type" in keys else "amount",
        down_payment_input_value=row["down_payment_input_value"] if "down_payment_input_value" in keys else None,
        monthly_mortgage_override_enabled=row["monthly_mortgage_override_enabled"] if "monthly_mortgage_override_enabled" in keys else 0,
        monthly_mortgage_override=row["monthly_mortgage_override"] if "monthly_mortgage_override" in keys else 0.0,
        rent_input_amount=row["rent_input_amount"] if "rent_input_amount" in keys else 0.0,
        rent_input_frequency=row["rent_input_frequency"] if "rent_input_frequency" in keys else "monthly",
        property_tax_input_type=row["property_tax_input_type"] if "property_tax_input_type" in keys else "monthly",
        property_tax_input_value=row["property_tax_input_value"] if "property_tax_input_value" in keys else 0.0,
        insurance_input_type=row["insurance_input_type"] if "insurance_input_type" in keys else "monthly",
        insurance_input_value=row["insurance_input_value"] if "insurance_input_value" in keys else 0.0,
        maintenance_input_type=row["maintenance_input_type"] if "maintenance_input_type" in keys else "monthly",
        maintenance_input_value=row["maintenance_input_value"] if "maintenance_input_value" in keys else 0.0,
        management_input_type=row["management_input_type"] if "management_input_type" in keys else "monthly",
        management_input_value=row["management_input_value"] if "management_input_value" in keys else 0.0,
        updated_at=row["updated_at"],
    )
