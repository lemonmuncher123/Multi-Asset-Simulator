import pytest
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.price_repo import upsert_price
from src.engines import ledger
from src.engines.allocation import (
    calc_allocation_by_asset_type,
    calc_allocation_by_asset,
    calc_allocation_by_liquidity,
    calc_allocation_by_currency,
    calc_allocation_by_region,
    calc_cash_pct,
    calc_crypto_pct,
    calc_real_estate_equity_pct,
    calc_debt_ratio,
    calc_liquid_assets,
    calc_illiquid_assets,
    get_full_allocation)


@pytest.fixture
def portfolio(db_conn):
    """Build a mixed portfolio for allocation tests.

    Cash:       50,000
    Deposit:   200,000
    House DP: -100,000  (500k house, 400k mortgage)
    Buy VTI:  -20,000   (100 shares @ 200)
    Buy BTC:  -30,000   (1 BTC @ 30,000)
    Cash left: 50,000
    """
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)

    vti = create_asset(db_conn, Asset(symbol="VTI", name="Vanguard Total", asset_type="etf"))
    ledger.buy(db_conn, "2025-01-15", vti.id, quantity=100, price=200.0)

    btc = create_asset(db_conn, Asset(
        symbol="BTC", name="Bitcoin", asset_type="crypto", currency="USD", region="Global"))
    ledger.buy(db_conn, "2025-01-20", btc.id, quantity=1, price=30000.0)

    asset, prop, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="HOUSE", name="My House",
        purchase_price=500000.0,
        down_payment=100000.0,
        monthly_expense=2500.0)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=400000.0,
        interest_rate=0.06, term_periods=360)

    return db_conn


# --- Allocation by asset type ---

def test_allocation_by_type_keys(portfolio):
    alloc = calc_allocation_by_asset_type(portfolio)
    assert "cash" in alloc
    assert "etf" in alloc
    assert "crypto" in alloc
    assert "real_estate" in alloc


def test_allocation_by_type_values(portfolio):
    alloc = calc_allocation_by_asset_type(portfolio)
    assert alloc["cash"]["value"] == 50000.0
    assert alloc["etf"]["value"] == 20000.0
    assert alloc["crypto"]["value"] == 30000.0
    assert alloc["real_estate"]["value"] == 500000.0


def test_allocation_by_type_pcts_sum_to_one(portfolio):
    alloc = calc_allocation_by_asset_type(portfolio)
    total_pct = sum(v["pct"] for v in alloc.values())
    assert abs(total_pct - 1.0) < 1e-9


# --- Allocation by individual asset ---

def test_allocation_by_asset_count(portfolio):
    items = calc_allocation_by_asset(portfolio)
    assert len(items) == 4  # cash, VTI, BTC, house


def test_allocation_by_asset_sorted_desc(portfolio):
    items = calc_allocation_by_asset(portfolio)
    values = [i["value"] for i in items]
    assert values == sorted(values, reverse=True)


def test_allocation_by_asset_pcts_sum_to_one(portfolio):
    items = calc_allocation_by_asset(portfolio)
    total_pct = sum(i["pct"] for i in items)
    assert abs(total_pct - 1.0) < 1e-9


# --- Liquidity ---

def test_liquid_vs_illiquid(portfolio):
    liq = calc_allocation_by_liquidity(portfolio)
    # liquid: cash(50k) + etf(20k) + crypto(30k) = 100k
    assert liq["liquid"]["value"] == 100000.0
    # illiquid: real_estate(500k)
    assert liq["illiquid"]["value"] == 500000.0


def test_liquid_illiquid_pcts_sum_to_one(portfolio):
    liq = calc_allocation_by_liquidity(portfolio)
    total = liq["liquid"]["pct"] + liq["illiquid"]["pct"]
    assert abs(total - 1.0) < 1e-9


def test_liquid_assets_value(portfolio):
    assert calc_liquid_assets(portfolio) == 100000.0


def test_illiquid_assets_value(portfolio):
    assert calc_illiquid_assets(portfolio) == 500000.0


# --- Currency ---

def test_allocation_by_currency(portfolio):
    alloc = calc_allocation_by_currency(portfolio)
    assert "USD" in alloc
    total_pct = sum(v["pct"] for v in alloc.values())
    assert abs(total_pct - 1.0) < 1e-9


# --- Region ---

def test_allocation_by_region(portfolio):
    alloc = calc_allocation_by_region(portfolio)
    assert "US" in alloc
    assert "Global" in alloc  # BTC
    total_pct = sum(v["pct"] for v in alloc.values())
    assert abs(total_pct - 1.0) < 1e-9


def test_region_values(portfolio):
    alloc = calc_allocation_by_region(portfolio)
    # US: cash(50k) + VTI(20k) + house(500k) = 570k
    assert alloc["US"]["value"] == 570000.0
    # Global: BTC(30k)
    assert alloc["Global"]["value"] == 30000.0


# --- Percentage calculations ---

def test_cash_pct(portfolio):
    # total assets: 50k + 20k + 30k + 500k = 600k
    pct = calc_cash_pct(portfolio)
    assert abs(pct - 50000 / 600000) < 1e-9


def test_crypto_pct(portfolio):
    pct = calc_crypto_pct(portfolio)
    assert abs(pct - 30000 / 600000) < 1e-9


def test_real_estate_equity_pct(portfolio):
    # equity = 500k - 400k = 100k, total assets = 600k
    pct = calc_real_estate_equity_pct(portfolio)
    assert abs(pct - 100000 / 600000) < 1e-9


def test_debt_ratio(portfolio):
    # liabilities = 400k mortgage, total assets = 600k
    ratio = calc_debt_ratio(portfolio)
    assert abs(ratio - 400000 / 600000) < 1e-9


def test_debt_ratio_with_additional_debt(portfolio):
    ledger.add_debt(portfolio, "2025-01-01", symbol="CC", name="Card", amount=10000.0, cash_received=False, payment_per_period=100.0)
    # liabilities = 400k + 10k = 410k, total assets still 600k
    ratio = calc_debt_ratio(portfolio)
    assert abs(ratio - 410000 / 600000) < 1e-9


# --- Empty portfolio ---

def test_empty_portfolio(db_conn):
    alloc = get_full_allocation(db_conn)
    assert alloc["cash_pct"] == 0.0
    assert alloc["crypto_pct"] == 0.0
    assert alloc["debt_ratio"] == 0.0
    assert alloc["liquid_assets"] == 0.0
    assert alloc["illiquid_assets"] == 0.0


# --- Cash-only portfolio ---

def test_cash_only(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    alloc = get_full_allocation(db_conn)
    assert alloc["cash_pct"] == 1.0
    assert alloc["crypto_pct"] == 0.0
    assert alloc["debt_ratio"] == 0.0
    assert alloc["liquid_assets"] == 100000.0
    assert alloc["illiquid_assets"] == 0.0


# --- Full allocation output ---

def test_full_allocation_keys(portfolio):
    alloc = get_full_allocation(portfolio)
    expected_keys = [
        "by_asset_type", "by_asset", "by_liquidity", "by_currency",
        "by_region", "cash_pct", "crypto_pct", "real_estate_equity_pct",
        "debt_ratio", "liquid_assets", "illiquid_assets",
    ]
    for k in expected_keys:
        assert k in alloc, f"Missing key: {k}"


# ===================================================================
# Sold properties excluded from allocation
# ===================================================================


def test_allocation_by_type_excludes_sold_property(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset, _, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0,
        acquisition_mode="existing_property")
    alloc_before = calc_allocation_by_asset_type(db_conn)
    assert "real_estate" in alloc_before

    ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0)
    alloc_after = calc_allocation_by_asset_type(db_conn)
    assert "real_estate" not in alloc_after


def test_allocation_by_asset_excludes_sold_property(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset, _, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0,
        acquisition_mode="existing_property")
    items_before = calc_allocation_by_asset(db_conn)
    re_items = [i for i in items_before if i["asset_type"] == "real_estate"]
    assert len(re_items) == 1

    ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0)
    items_after = calc_allocation_by_asset(db_conn)
    re_items_after = [i for i in items_after if i["asset_type"] == "real_estate"]
    assert len(re_items_after) == 0


def test_allocation_by_liquidity_excludes_sold_property(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset, _, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0,
        acquisition_mode="existing_property")
    liq_before = calc_allocation_by_liquidity(db_conn)
    assert liq_before["illiquid"]["value"] == 500000.0

    ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0)
    liq_after = calc_allocation_by_liquidity(db_conn)
    assert liq_after["illiquid"]["value"] == 0.0


# --- Real estate display name ---

def test_real_estate_shows_name_only(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    ledger.add_property(
        db_conn, "2025-02-01", symbol="RE_HOME", name="My House",
        purchase_price=500000.0,
        acquisition_mode="existing_property")
    items = calc_allocation_by_asset(db_conn)
    re_items = [i for i in items if i["asset_type"] == "real_estate"]
    assert len(re_items) == 1
    assert re_items[0]["name"] == "My House"


def test_real_estate_displays_asset_name(db_conn):
    # Property names are now required (matching the debt contract). The
    # allocation breakdown displays the asset's name; the symbol-fallback
    # path is reserved for other code that bypasses add_property.
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    ledger.add_property(
        db_conn, "2025-02-01", symbol="RE_HOME", name="Family Home",
        purchase_price=500000.0,
        acquisition_mode="existing_property")
    items = calc_allocation_by_asset(db_conn)
    re_items = [i for i in items if i["asset_type"] == "real_estate"]
    assert len(re_items) == 1
    assert re_items[0]["name"] == "Family Home"


def test_stock_keeps_symbol_dash_name(db_conn):
    from src.models.asset import Asset
    from src.storage.asset_repo import create_asset
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="VTI", name="Vanguard Total", asset_type="etf"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=100, price=200.0)
    items = calc_allocation_by_asset(db_conn)
    etf_items = [i for i in items if i["asset_type"] == "etf"]
    assert len(etf_items) == 1
    assert etf_items[0]["name"] == "VTI - Vanguard Total"


def test_pie_breakdown_real_estate_name(db_conn):
    from src.engines.allocation import calc_asset_pie_breakdown
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    ledger.add_property(
        db_conn, "2025-02-01", symbol="RE_HOME", name="Beach Condo",
        purchase_price=500000.0,
        acquisition_mode="existing_property")
    items = calc_asset_pie_breakdown(db_conn)
    re_items = [i for i in items if i["asset_type"] == "real_estate"]
    assert len(re_items) == 1
    assert re_items[0]["name"] == "Beach Condo"
