import pytest
from src.models.asset import Asset
from src.models.mortgage import Mortgage
from src.models.property_asset import PropertyAsset
from src.storage.asset_repo import create_asset
from src.storage.property_repo import create_property
from src.storage.mortgage_repo import create_mortgage
from src.engines.ledger import add_property, deposit_cash
from src.engines.real_estate import (
    calc_equity,
    calc_ltv,
    calc_effective_rent,
    calc_monthly_expenses,
    calc_net_monthly_cash_flow,
    calc_annual_net_cash_flow,
    calc_cap_rate,
    calc_cash_on_cash_return,
    analyze_property,
    analyze_all_properties,
    calc_re_share_of_net_worth,
    calc_illiquid_share,
    get_real_estate_warnings)


# Counter so each call to _make_prop gets a unique symbol.
_PROP_COUNTER = [0]


def _make_prop(
    db_conn=None, *,
    mortgage_balance: float = 400000.0,
    monthly_mortgage_payment: float = 2000.0,
    mortgage_interest_rate: float = 0.0,
    **kwargs,
) -> PropertyAsset:
    """Test fixture: creates a property in the DB. If `db_conn` is None,
    returns a detached PropertyAsset (legacy behavior, used only by
    pure-math tests that don't need mortgage data). When `db_conn` is
    provided, also writes a mortgage row when `mortgage_balance > 0`.
    """
    defaults = dict(
        purchase_price=500000.0,
        current_value=500000.0,
        down_payment=100000.0,
        monthly_rent=3000.0,
        monthly_property_tax=400.0,
        monthly_insurance=150.0,
        monthly_hoa=200.0,
        monthly_maintenance_reserve=100.0,
        monthly_property_management=240.0,
        vacancy_rate=0.05)
    defaults.update(kwargs)

    if db_conn is None:
        # Detached PropertyAsset (no DB write). Skipped tests that need
        # mortgage values must use the db_conn variant.
        return PropertyAsset(asset_id=1, **defaults)

    _PROP_COUNTER[0] += 1
    n = _PROP_COUNTER[0]
    asset = create_asset(db_conn, Asset(
        symbol=f"PROP{n}", name=f"Property {n}", asset_type="real_estate",
        liquidity="illiquid",
    ))
    prop = create_property(db_conn, PropertyAsset(asset_id=asset.id, **defaults))
    if mortgage_balance > 0:
        create_mortgage(db_conn, Mortgage(
            property_id=prop.id, name=f"Mortgage {n}",
            original_amount=mortgage_balance,
            current_balance=mortgage_balance,
            interest_rate=mortgage_interest_rate,
            monthly_payment_amount=monthly_mortgage_payment,
            plan_type="fixed_payment",
        ))
    return prop


# --- Equity ---

def test_equity_basic(db_conn):
    p = _make_prop(db_conn, current_value=500000.0)
    assert calc_equity(p, db_conn) == 100000.0


def test_equity_no_mortgage(db_conn):
    p = _make_prop(db_conn, current_value=300000.0, mortgage_balance=0)
    assert calc_equity(p, db_conn) == 300000.0


def test_equity_no_value(db_conn):
    # current_value=None becomes 0; equity = 0 - mortgage_balance
    p = _make_prop(db_conn, current_value=None)
    assert calc_equity(p, db_conn) == -400000.0


# --- LTV ---

def test_ltv_normal(db_conn):
    p = _make_prop(db_conn, current_value=500000.0)
    assert calc_ltv(p, db_conn) == pytest.approx(0.80)


def test_ltv_zero_value(db_conn):
    p = _make_prop(db_conn, current_value=0.0)
    assert calc_ltv(p, db_conn) is None


def test_ltv_no_mortgage(db_conn):
    p = _make_prop(db_conn, current_value=500000.0, mortgage_balance=0)
    assert calc_ltv(p, db_conn) == 0.0


# --- Effective rent ---

def test_effective_rent():
    p = _make_prop(monthly_rent=3000.0, vacancy_rate=0.05)
    assert calc_effective_rent(p) == pytest.approx(2850.0)


def test_effective_rent_no_vacancy():
    p = _make_prop(monthly_rent=3000.0, vacancy_rate=0.0)
    assert calc_effective_rent(p) == 3000.0


def test_effective_rent_full_vacancy():
    p = _make_prop(monthly_rent=3000.0, vacancy_rate=1.0)
    assert calc_effective_rent(p) == 0.0


# --- Monthly expenses ---

def test_monthly_expenses(db_conn):
    p = _make_prop(
        db_conn, monthly_mortgage_payment=2528.0,
        monthly_property_tax=400.0,
        monthly_insurance=150.0,
        monthly_hoa=200.0,
        monthly_maintenance_reserve=100.0,
        monthly_property_management=240.0)
    assert calc_monthly_expenses(p, db_conn) == pytest.approx(3618.0)


def test_monthly_expenses_no_extras(db_conn):
    p = _make_prop(
        db_conn, monthly_mortgage_payment=2000.0,
        monthly_property_tax=0.0,
        monthly_insurance=0.0,
        monthly_hoa=0.0,
        monthly_maintenance_reserve=0.0,
        monthly_property_management=0.0)
    assert calc_monthly_expenses(p, db_conn) == 2000.0


# --- Net cash flow ---

def test_net_monthly_cash_flow_positive(db_conn):
    p = _make_prop(
        db_conn, monthly_mortgage_payment=2000.0,
        monthly_rent=4000.0, vacancy_rate=0.0,
        monthly_property_tax=200.0, monthly_insurance=100.0,
        monthly_hoa=0.0, monthly_maintenance_reserve=100.0,
        monthly_property_management=0.0)
    assert calc_net_monthly_cash_flow(p, db_conn) == pytest.approx(1600.0)


def test_net_monthly_cash_flow_negative(db_conn):
    p = _make_prop(
        db_conn, monthly_mortgage_payment=2528.0,
        monthly_rent=2000.0, vacancy_rate=0.10,
        monthly_property_tax=400.0, monthly_insurance=150.0,
        monthly_hoa=200.0, monthly_maintenance_reserve=100.0,
        monthly_property_management=240.0)
    expected_rent = 2000.0 * 0.90
    expected_expenses = 2528.0 + 400.0 + 150.0 + 200.0 + 100.0 + 240.0
    assert calc_net_monthly_cash_flow(p, db_conn) == pytest.approx(expected_rent - expected_expenses)


def test_annual_net_cash_flow(db_conn):
    p = _make_prop(
        db_conn, monthly_mortgage_payment=2000.0,
        monthly_rent=4000.0, vacancy_rate=0.0,
        monthly_property_tax=200.0, monthly_insurance=100.0,
        monthly_hoa=0.0, monthly_maintenance_reserve=0.0,
        monthly_property_management=0.0)
    assert calc_annual_net_cash_flow(p, db_conn) == pytest.approx(1700.0 * 12)


# --- Cap rate ---

def test_cap_rate():
    p = _make_prop(
        current_value=500000.0, monthly_rent=3000.0, vacancy_rate=0.05,
        monthly_property_tax=400.0, monthly_insurance=150.0,
        monthly_hoa=200.0, monthly_maintenance_reserve=100.0,
        monthly_property_management=240.0)
    effective_rent = 3000.0 * 0.95
    op_expenses = 400.0 + 150.0 + 200.0 + 100.0 + 240.0
    noi = (effective_rent - op_expenses) * 12
    assert calc_cap_rate(p) == pytest.approx(noi / 500000.0)


def test_cap_rate_zero_value():
    p = _make_prop(current_value=0.0)
    assert calc_cap_rate(p) is None


# --- Cash-on-cash return ---

def test_cash_on_cash_return(db_conn):
    p = _make_prop(
        db_conn, monthly_mortgage_payment=2000.0,
        down_payment=100000.0, monthly_rent=4000.0, vacancy_rate=0.0,
        monthly_property_tax=200.0, monthly_insurance=100.0,
        monthly_hoa=0.0, monthly_maintenance_reserve=0.0,
        monthly_property_management=0.0)
    annual_cf = (4000.0 - 2300.0) * 12
    assert calc_cash_on_cash_return(p, db_conn) == pytest.approx(annual_cf / 100000.0)


def test_cash_on_cash_return_no_down_payment(db_conn):
    p = _make_prop(db_conn, down_payment=None)
    assert calc_cash_on_cash_return(p, db_conn) is None


def test_cash_on_cash_return_zero_down_payment(db_conn):
    p = _make_prop(db_conn, down_payment=0.0)
    assert calc_cash_on_cash_return(p, db_conn) is None


# --- analyze_property (DB) ---

def test_analyze_property(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="PROP1", name="Rental House", asset_type="real_estate", liquidity="illiquid"))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, current_value=500000.0, purchase_price=480000.0,
         down_payment=80000.0,
        monthly_rent=3000.0, vacancy_rate=0.05,
        monthly_property_tax=400.0, monthly_insurance=150.0,
        monthly_hoa=200.0, monthly_maintenance_reserve=100.0,
        monthly_property_management=240.0))
    create_mortgage(db_conn, Mortgage(
        property_id=prop.id, name="Rental Mortgage",
        original_amount=400000.0, current_balance=400000.0,
        interest_rate=0.0,
        monthly_payment_amount=2528.0,
        plan_type="fixed_payment",
    ))
    a = analyze_property(prop, db_conn)
    assert a.name == "Rental House"
    assert a.equity == 100000.0
    assert a.ltv == pytest.approx(0.80)
    assert a.effective_rent == pytest.approx(2850.0)
    assert a.monthly_expenses == pytest.approx(3618.0)
    assert a.net_monthly_cash_flow == pytest.approx(2850.0 - 3618.0)


def test_analyze_all_properties(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="P1", name="House 1", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="House 2", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=300000.0, current_value=300000.0, monthly_rent=2000.0))
    create_property(db_conn, PropertyAsset(
        asset_id=a2.id, purchase_price=400000.0, current_value=400000.0, monthly_rent=2500.0))
    analyses = analyze_all_properties(db_conn)
    assert len(analyses) == 2


# --- RE share / illiquid share (DB) ---

def test_re_share_of_net_worth(db_conn):
    deposit_cash(db_conn, "2025-01-01", 500000.0)
    asset = create_asset(db_conn, Asset(
        symbol="PROP1", name="House", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0))
    share = calc_re_share_of_net_worth(db_conn)
    assert share == pytest.approx(0.50)


def test_illiquid_share(db_conn):
    deposit_cash(db_conn, "2025-01-01", 500000.0)
    asset = create_asset(db_conn, Asset(
        symbol="PROP1", name="House", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0))
    share = calc_illiquid_share(db_conn)
    assert share == pytest.approx(0.50)


# --- Warnings ---

def test_warning_negative_cash_flow(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="PROP1", name="Bad Rental", asset_type="real_estate", liquidity="illiquid"))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
        monthly_rent=1000.0))
    create_mortgage(db_conn, Mortgage(
        property_id=prop.id, name="Mort",
        original_amount=400000.0, current_balance=400000.0,
        interest_rate=0.0, monthly_payment_amount=2500.0,
        plan_type="fixed_payment",
    ))
    warnings = get_real_estate_warnings(db_conn)
    high = [w for w in warnings if w.severity == "high" and "negative" in w.message.lower()]
    assert len(high) >= 1


def test_warning_high_ltv(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="PROP1", name="Over-leveraged", asset_type="real_estate", liquidity="illiquid"))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0))
    create_mortgage(db_conn, Mortgage(
        property_id=prop.id, name="HighLTV",
        original_amount=480000.0, current_balance=480000.0,
        interest_rate=0.0, monthly_payment_amount=2500.0,
        plan_type="fixed_payment",
    ))
    warnings = get_real_estate_warnings(db_conn)
    ltv_warns = [w for w in warnings if "LTV" in w.message]
    assert len(ltv_warns) >= 1


def test_warning_re_over_50_pct_nw(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(
        symbol="PROP1", name="Big House", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0))
    warnings = get_real_estate_warnings(db_conn)
    re_warns = [w for w in warnings if "net worth" in w.message.lower()]
    assert len(re_warns) >= 1


def test_warning_missing_value(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="PROP1", name="Unknown Value", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=None))
    warnings = get_real_estate_warnings(db_conn)
    info = [w for w in warnings if w.severity == "info" and "value" in w.message.lower()]
    assert len(info) >= 1


def test_warning_high_vacancy(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="PROP1", name="High Vacancy", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=500000.0,
        vacancy_rate=0.15, monthly_rent=2000.0))
    warnings = get_real_estate_warnings(db_conn)
    vac_warns = [w for w in warnings if "vacancy" in w.message.lower()]
    assert len(vac_warns) >= 1


def test_no_warnings_healthy_property(db_conn):
    deposit_cash(db_conn, "2025-01-01", 1000000.0)
    asset = create_asset(db_conn, Asset(
        symbol="PROP1", name="Good Rental", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=300000.0, current_value=320000.0,
         down_payment=100000.0,
        monthly_rent=3000.0, vacancy_rate=0.05,
        
        monthly_property_tax=200.0, monthly_insurance=100.0,
        monthly_hoa=0.0, monthly_maintenance_reserve=50.0,
        monthly_property_management=0.0))
    warnings = get_real_estate_warnings(db_conn)
    assert len(warnings) == 0


# --- Sold properties excluded from warnings and analysis ---

def test_warnings_ignore_sold_property(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="PROP1", name="Sold House", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=asset.id, purchase_price=500000.0, current_value=0.0,
         monthly_rent=1000.0, 
        status="sold", sold_date="2025-06-01", sold_price=520000.0))
    warnings = get_real_estate_warnings(db_conn)
    assert len(warnings) == 0


def test_analyze_all_excludes_sold(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="P1", name="Active", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="Sold", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=300000.0, current_value=300000.0, monthly_rent=2000.0))
    create_property(db_conn, PropertyAsset(
        asset_id=a2.id, purchase_price=400000.0, current_value=0.0,
        status="sold", sold_date="2025-06-01", sold_price=420000.0))
    analyses = analyze_all_properties(db_conn)
    assert len(analyses) == 1
    assert analyses[0].name == "Active"


def test_re_share_excludes_sold(db_conn):
    deposit_cash(db_conn, "2025-01-01", 500000.0)
    a1 = create_asset(db_conn, Asset(symbol="P1", name="Active", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="Sold", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=500000.0, current_value=500000.0))
    create_property(db_conn, PropertyAsset(
        asset_id=a2.id, purchase_price=400000.0, current_value=0.0,
        status="sold", sold_date="2025-06-01", sold_price=420000.0))
    share = calc_re_share_of_net_worth(db_conn)
    assert share == pytest.approx(0.50)


def test_illiquid_share_excludes_sold(db_conn):
    deposit_cash(db_conn, "2025-01-01", 500000.0)
    a1 = create_asset(db_conn, Asset(symbol="P1", name="Active", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="Sold", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=500000.0, current_value=500000.0))
    create_property(db_conn, PropertyAsset(
        asset_id=a2.id, purchase_price=400000.0, current_value=0.0,
        status="sold", sold_date="2025-06-01", sold_price=420000.0))
    share = calc_illiquid_share(db_conn)
    assert share == pytest.approx(0.50)


# --- Legacy planned properties excluded from analysis ---


def test_analyze_all_excludes_planned(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="P1", name="Active", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="Planned", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=300000.0, current_value=300000.0, monthly_rent=2000.0,
        status="active"))
    create_property(db_conn, PropertyAsset(
        asset_id=a2.id, purchase_price=600000.0, current_value=600000.0, monthly_rent=3000.0,
        status="planned"))
    analyses = analyze_all_properties(db_conn)
    assert len(analyses) == 1
    assert analyses[0].name == "Active"


def test_warnings_exclude_planned(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="P1", name="Planned Bad", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=500000.0, current_value=500000.0,
        monthly_rent=1000.0, 
        status="planned"))
    warnings = get_real_estate_warnings(db_conn)
    assert len(warnings) == 0


def test_new_purchase_property_included_in_analysis(db_conn):
    """A new_purchase property (status=active) must appear in analysis."""
    a1 = create_asset(db_conn, Asset(symbol="P1", name="New Purchase", asset_type="real_estate", liquidity="illiquid"))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=400000.0, current_value=420000.0,
         monthly_rent=2500.0,
        status="active", entry_type="new_purchase"))
    create_mortgage(db_conn, Mortgage(
        property_id=prop.id, name="m",
        original_amount=320000.0, current_balance=320000.0,
        interest_rate=0.0, monthly_payment_amount=1500.0,
        plan_type="fixed_payment",
    ))
    analyses = analyze_all_properties(db_conn)
    assert len(analyses) == 1
    assert analyses[0].name == "New Purchase"
    assert analyses[0].equity == pytest.approx(100000.0)


def test_new_purchase_property_generates_warnings_if_unhealthy(db_conn):
    """A new_purchase property with negative cash flow should trigger warnings."""
    a1 = create_asset(db_conn, Asset(symbol="P1", name="New Bad Rental", asset_type="real_estate", liquidity="illiquid"))
    prop = create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=500000.0, current_value=500000.0,
        monthly_rent=1000.0,
        status="active", entry_type="new_purchase"))
    create_mortgage(db_conn, Mortgage(
        property_id=prop.id, name="m",
        original_amount=400000.0, current_balance=400000.0,
        interest_rate=0.0, monthly_payment_amount=2500.0,
        plan_type="fixed_payment",
    ))
    warnings = get_real_estate_warnings(db_conn)
    high = [w for w in warnings if w.severity == "high" and "negative" in w.message.lower()]
    assert len(high) >= 1


def test_re_share_excludes_planned(db_conn):
    """Planned properties should not affect RE share of net worth."""
    deposit_cash(db_conn, "2025-01-01", 500000.0)
    a1 = create_asset(db_conn, Asset(symbol="P1", name="Active", asset_type="real_estate", liquidity="illiquid"))
    a2 = create_asset(db_conn, Asset(symbol="P2", name="Planned", asset_type="real_estate", liquidity="illiquid"))
    create_property(db_conn, PropertyAsset(
        asset_id=a1.id, purchase_price=500000.0, current_value=500000.0))
    create_property(db_conn, PropertyAsset(
        asset_id=a2.id, purchase_price=600000.0, current_value=600000.0,
        status="planned"))
    share = calc_re_share_of_net_worth(db_conn)
    assert share == pytest.approx(0.50)
