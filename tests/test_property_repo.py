from src.models.asset import Asset
from src.models.property_asset import PropertyAsset
from src.storage.asset_repo import create_asset
from src.storage.property_repo import (
    create_property, get_property, get_property_by_asset,
    list_properties, list_active_properties, update_property,
)


def test_create_and_get_property(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="PROP1", name="My House", asset_type="real_estate", liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, address="123 Main St",
        purchase_price=500000.0, current_value=550000.0,
        mortgage_balance=400000.0, monthly_rent=0.0, monthly_expense=2500.0,
    ))
    assert prop.id is not None

    fetched = get_property(db_conn, prop.id)
    assert fetched.address == "123 Main St"
    assert fetched.purchase_price == 500000.0
    assert fetched.current_value == 550000.0
    assert fetched.mortgage_balance == 400000.0


def test_get_property_by_asset(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="PROP2", name="Rental", asset_type="real_estate", liquidity="illiquid",
    ))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, address="456 Oak Ave",
        purchase_price=300000.0, current_value=320000.0,
        monthly_rent=2000.0, monthly_expense=800.0,
    ))
    fetched = get_property_by_asset(db_conn, asset.id)
    assert fetched is not None
    assert fetched.monthly_rent == 2000.0


def test_list_properties(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="P1", name="House 1", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="House 2", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(asset_id=a1.id, purchase_price=500000.0, current_value=500000.0))
    create_property(db_conn, PropertyAsset(asset_id=a2.id, purchase_price=300000.0, current_value=300000.0))

    props = list_properties(db_conn)
    assert len(props) == 2


def test_update_property(db_conn):
    asset = create_asset(db_conn, Asset(symbol="P1", name="House", asset_type="real_estate", liquidity="illiquid"))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0, mortgage_balance=400000.0,
    ))
    prop.current_value = 520000.0
    prop.mortgage_balance = 395000.0
    update_property(db_conn, prop)

    fetched = get_property(db_conn, prop.id)
    assert fetched.current_value == 520000.0
    assert fetched.mortgage_balance == 395000.0


# --- New property lifecycle columns ---

def test_schema_has_new_columns(db_conn):
    cols = {row[1] for row in db_conn.execute("PRAGMA table_info(properties)").fetchall()}
    for col in ("status", "sold_date", "sold_price", "sale_fees", "rent_collection_frequency", "cashflow_start_date"):
        assert col in cols, f"Missing column: {col}"


def test_create_property_persists_lifecycle_fields(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="House", asset_type="real_estate", liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
        status="sold", sold_date="2025-06-01", sold_price=550000.0,
        sale_fees=12000.0, rent_collection_frequency="annual",
    ))
    fetched = get_property(db_conn, prop.id)
    assert fetched.status == "sold"
    assert fetched.sold_date == "2025-06-01"
    assert fetched.sold_price == 550000.0
    assert fetched.sale_fees == 12000.0
    assert fetched.rent_collection_frequency == "annual"


def test_create_property_defaults_lifecycle_fields(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P2", name="Default House", asset_type="real_estate", liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=300000.0, current_value=300000.0,
    ))
    fetched = get_property(db_conn, prop.id)
    assert fetched.status == "active"
    assert fetched.sold_date is None
    assert fetched.sold_price is None
    assert fetched.sale_fees == 0.0
    assert fetched.rent_collection_frequency == "monthly"


def test_update_property_lifecycle_fields(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="House", asset_type="real_estate", liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
    ))
    prop.status = "sold"
    prop.sold_date = "2025-08-01"
    prop.sold_price = 520000.0
    prop.sale_fees = 8000.0
    prop.rent_collection_frequency = "annual"
    update_property(db_conn, prop)

    fetched = get_property(db_conn, prop.id)
    assert fetched.status == "sold"
    assert fetched.sold_date == "2025-08-01"
    assert fetched.sold_price == 520000.0
    assert fetched.sale_fees == 8000.0
    assert fetched.rent_collection_frequency == "annual"


def test_list_active_properties_excludes_sold(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="P1", name="Active", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="Sold", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=300000.0, current_value=300000.0, status="active",
    ))
    create_property(db_conn, PropertyAsset(
        asset_id=a2.id, purchase_price=400000.0, current_value=0.0, status="sold",
        sold_date="2025-06-01", sold_price=420000.0,
    ))

    all_props = list_properties(db_conn)
    assert len(all_props) == 2

    active = list_active_properties(db_conn)
    assert len(active) == 1
    assert active[0].asset_id == a1.id


def test_create_property_with_cashflow_start_date(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="House", asset_type="real_estate", liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
        cashflow_start_date="2026-05-01",
    ))
    fetched = get_property(db_conn, prop.id)
    assert fetched.cashflow_start_date == "2026-05-01"


def test_get_property_by_asset_returns_cashflow_start_date(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="House", asset_type="real_estate", liquidity="illiquid",
    ))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
        cashflow_start_date="2026-07-01",
    ))
    fetched = get_property_by_asset(db_conn, asset.id)
    assert fetched is not None
    assert fetched.cashflow_start_date == "2026-07-01"


def test_create_property_cashflow_start_date_defaults_null(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P2", name="House2", asset_type="real_estate", liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=300000.0, current_value=300000.0,
    ))
    fetched = get_property(db_conn, prop.id)
    assert fetched.cashflow_start_date is None


def test_update_property_cashflow_start_date(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="House", asset_type="real_estate", liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
    ))
    prop.cashflow_start_date = "2026-06-01"
    update_property(db_conn, prop)
    fetched = get_property(db_conn, prop.id)
    assert fetched.cashflow_start_date == "2026-06-01"


def test_list_active_properties_returns_all_when_none_sold(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="P1", name="H1", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="H2", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(asset_id=a1.id, purchase_price=300000.0, current_value=300000.0))
    create_property(db_conn, PropertyAsset(asset_id=a2.id, purchase_price=400000.0, current_value=400000.0))

    active = list_active_properties(db_conn)
    assert len(active) == 2


# --- Raw input field columns ---


def test_schema_has_raw_input_columns(db_conn):
    cols = {row[1] for row in db_conn.execute("PRAGMA table_info(properties)").fetchall()}
    for col in (
        "entry_type", "loan_term_years", "down_payment_type",
        "down_payment_input_value", "monthly_mortgage_override_enabled",
        "monthly_mortgage_override", "rent_input_amount", "rent_input_frequency",
        "property_tax_input_type", "property_tax_input_value",
        "insurance_input_type", "insurance_input_value",
        "maintenance_input_type", "maintenance_input_value",
        "management_input_type", "management_input_value",
    ):
        assert col in cols, f"Missing column: {col}"


def test_create_property_persists_raw_input_fields(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="House", asset_type="real_estate", liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
        entry_type="planned_purchase",
        loan_term_years=30,
        down_payment_type="percent",
        down_payment_input_value=20.0,
        monthly_mortgage_override_enabled=1,
        monthly_mortgage_override=2800.0,
        rent_input_amount=36000.0,
        rent_input_frequency="annual",
        property_tax_input_type="pct_value",
        property_tax_input_value=1.2,
        insurance_input_type="annual",
        insurance_input_value=2400.0,
        maintenance_input_type="pct_value",
        maintenance_input_value=1.0,
        management_input_type="pct_rent",
        management_input_value=10.0,
    ))
    fetched = get_property(db_conn, prop.id)
    assert fetched.entry_type == "planned_purchase"
    assert fetched.loan_term_years == 30
    assert fetched.down_payment_type == "percent"
    assert fetched.down_payment_input_value == 20.0
    assert fetched.monthly_mortgage_override_enabled == 1
    assert fetched.monthly_mortgage_override == 2800.0
    assert fetched.rent_input_amount == 36000.0
    assert fetched.rent_input_frequency == "annual"
    assert fetched.property_tax_input_type == "pct_value"
    assert fetched.property_tax_input_value == 1.2
    assert fetched.insurance_input_type == "annual"
    assert fetched.insurance_input_value == 2400.0
    assert fetched.maintenance_input_type == "pct_value"
    assert fetched.maintenance_input_value == 1.0
    assert fetched.management_input_type == "pct_rent"
    assert fetched.management_input_value == 10.0


def test_raw_input_fields_default_values(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="House", asset_type="real_estate", liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=300000.0, current_value=300000.0,
    ))
    fetched = get_property(db_conn, prop.id)
    assert fetched.entry_type == "existing_property"
    assert fetched.loan_term_years is None
    assert fetched.down_payment_type == "amount"
    assert fetched.down_payment_input_value is None
    assert fetched.monthly_mortgage_override_enabled == 0
    assert fetched.monthly_mortgage_override == 0.0
    assert fetched.rent_input_amount == 0.0
    assert fetched.rent_input_frequency == "monthly"
    assert fetched.property_tax_input_type == "monthly"
    assert fetched.property_tax_input_value == 0.0


def test_update_property_raw_input_fields(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="House", asset_type="real_estate", liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
    ))
    prop.entry_type = "planned_purchase"
    prop.loan_term_years = 15
    prop.down_payment_type = "percent"
    prop.down_payment_input_value = 25.0
    prop.rent_input_amount = 3000.0
    prop.rent_input_frequency = "monthly"
    update_property(db_conn, prop)

    fetched = get_property(db_conn, prop.id)
    assert fetched.entry_type == "planned_purchase"
    assert fetched.loan_term_years == 15
    assert fetched.down_payment_type == "percent"
    assert fetched.down_payment_input_value == 25.0
    assert fetched.rent_input_amount == 3000.0


def test_old_style_property_without_optional_fields_loads(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="Legacy House", asset_type="real_estate", liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=200000.0, current_value=250000.0,
        mortgage_balance=150000.0, monthly_rent=1500.0,
    ))
    fetched = get_property(db_conn, prop.id)
    assert fetched.purchase_price == 200000.0
    assert fetched.entry_type == "existing_property"
    assert fetched.loan_term_years is None
    assert fetched.rent_input_frequency == "monthly"


def test_list_active_properties_excludes_planned(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="P1", name="Active", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="Planned", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=300000.0, current_value=300000.0, status="active",
    ))
    create_property(db_conn, PropertyAsset(
        asset_id=a2.id, purchase_price=500000.0, current_value=500000.0, status="planned",
    ))

    active = list_active_properties(db_conn)
    assert len(active) == 1
    assert active[0].asset_id == a1.id


def test_list_active_excludes_both_sold_and_planned(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="P1", name="Active", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="Sold", asset_type="real_estate", liquidity="illiquid"))
    a3 = create_asset(db_conn, Asset(symbol="P3", name="Planned", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=300000.0, current_value=300000.0, status="active",
    ))
    create_property(db_conn, PropertyAsset(
        asset_id=a2.id, purchase_price=400000.0, current_value=0.0, status="sold",
        sold_date="2025-06-01", sold_price=420000.0,
    ))
    create_property(db_conn, PropertyAsset(
        asset_id=a3.id, purchase_price=600000.0, current_value=600000.0, status="planned",
    ))

    all_props = list_properties(db_conn)
    assert len(all_props) == 3

    active = list_active_properties(db_conn)
    assert len(active) == 1
    assert active[0].asset_id == a1.id


def test_get_property_by_asset_returns_raw_input_fields(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="House", asset_type="real_estate", liquidity="illiquid",
    ))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
        entry_type="planned_purchase", loan_term_years=30,
        property_tax_input_type="annual", property_tax_input_value=6000.0,
    ))
    fetched = get_property_by_asset(db_conn, asset.id)
    assert fetched.entry_type == "planned_purchase"
    assert fetched.loan_term_years == 30
    assert fetched.property_tax_input_type == "annual"
    assert fetched.property_tax_input_value == 6000.0


# --- Legacy planned records in repo ---


def test_legacy_planned_records_still_loadable(db_conn):
    """Legacy records with status='planned' must still load from the database."""
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="Legacy Planned", asset_type="real_estate", liquidity="illiquid",
    ))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=600000.0, current_value=600000.0,
        status="planned", entry_type="planned_purchase",
    ))
    all_props = list_properties(db_conn)
    planned = [p for p in all_props if p.status == "planned"]
    assert len(planned) == 1
    assert planned[0].entry_type == "planned_purchase"


def test_legacy_planned_excluded_from_active(db_conn):
    """Legacy planned records must not appear in list_active_properties."""
    a1 = create_asset(db_conn, Asset(symbol="P1", name="Active", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="Legacy", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=300000.0, current_value=300000.0, status="active",
    ))
    create_property(db_conn, PropertyAsset(
        asset_id=a2.id, purchase_price=500000.0, current_value=500000.0, status="planned",
    ))
    active = list_active_properties(db_conn)
    assert len(active) == 1
    assert active[0].asset_id == a1.id


def test_new_purchase_entry_type_persisted(db_conn):
    """A property with entry_type='new_purchase' roundtrips through repo."""
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="New House", asset_type="real_estate", liquidity="illiquid",
    ))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
        status="active", entry_type="new_purchase",
    ))
    fetched = get_property_by_asset(db_conn, asset.id)
    assert fetched.entry_type == "new_purchase"
    assert fetched.status == "active"


def test_new_purchase_in_active_properties(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="P1", name="New House", asset_type="real_estate", liquidity="illiquid",
    ))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
        status="active", entry_type="new_purchase",
    ))
    active = list_active_properties(db_conn)
    assert len(active) == 1
    assert active[0].entry_type == "new_purchase"
