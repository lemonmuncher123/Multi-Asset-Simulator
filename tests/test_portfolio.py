from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.price_repo import upsert_price
from src.engines import ledger
from src.engines.portfolio import (
    calc_cash_balance,
    calc_positions,
    calc_total_property_value,
    calc_total_mortgage,
    calc_total_debt,
    calc_total_liabilities,
    calc_total_assets,
    calc_net_worth,
    calc_real_estate_equity,
    get_portfolio_summary)


def test_cash_balance_after_deposit(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
    assert calc_cash_balance(db_conn) == 50000.0


def test_cash_balance_after_deposit_and_withdraw(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
    ledger.withdraw_cash(db_conn, "2025-01-02", 10000.0)
    assert calc_cash_balance(db_conn) == 40000.0


def test_cash_balance_after_buy(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    assert calc_cash_balance(db_conn) == 48500.0


def test_cash_balance_after_buy_and_sell(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    ledger.sell(db_conn, "2025-02-15", asset.id, quantity=5, price=170.0)
    assert calc_cash_balance(db_conn) == 49350.0


def test_positions_after_buy(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    positions = calc_positions(db_conn)
    assert len(positions) == 1
    assert positions[0].symbol == "AAPL"
    assert positions[0].quantity == 10
    assert positions[0].average_price == 150.0
    assert positions[0].cost_basis == 1500.0


def test_positions_after_partial_sell(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    ledger.sell(db_conn, "2025-02-15", asset.id, quantity=3, price=170.0)
    positions = calc_positions(db_conn)
    assert len(positions) == 1
    assert positions[0].quantity == 7
    assert positions[0].average_price == 150.0
    assert positions[0].cost_basis == 1050.0


def test_positions_fully_sold(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    ledger.sell(db_conn, "2025-02-15", asset.id, quantity=10, price=170.0)
    positions = calc_positions(db_conn)
    assert len(positions) == 0


def test_positions_multiple_buys(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    ledger.buy(db_conn, "2025-02-15", asset.id, quantity=10, price=170.0)
    positions = calc_positions(db_conn)
    assert len(positions) == 1
    assert positions[0].quantity == 20
    assert positions[0].average_price == 160.0
    assert positions[0].cost_basis == 3200.0


def test_positions_with_market_price(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    upsert_price(db_conn, asset.id, "2025-03-01", 180.0)
    positions = calc_positions(db_conn)
    assert positions[0].current_price == 180.0
    assert positions[0].market_value == 1800.0
    assert positions[0].unrealized_pnl == 300.0


def test_property_value(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    _, prop, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, current_value=550000.0,
        down_payment=100000.0)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=400000.0,
        interest_rate=0.0, payment_per_period=2000.0,
    )
    assert calc_total_property_value(db_conn) == 550000.0
    assert calc_total_mortgage(db_conn) == 400000.0
    assert calc_real_estate_equity(db_conn) == 150000.0


def test_debt_liabilities(db_conn):
    ledger.add_debt(db_conn, "2025-01-01", symbol="SL", name="Student Loan", amount=50000.0, payment_per_period=500.0)
    ledger.add_debt(db_conn, "2025-01-01", symbol="CC", name="Credit Card", amount=5000.0, cash_received=False, payment_per_period=50.0)
    assert calc_total_debt(db_conn) == 55000.0


def test_total_liabilities_combined(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    _, prop, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, down_payment=100000.0)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=400000.0,
        interest_rate=0.0, payment_per_period=2000.0,
    )
    ledger.add_debt(db_conn, "2025-01-01", symbol="CC", name="Card", amount=5000.0, cash_received=False, payment_per_period=50.0)
    assert calc_total_liabilities(db_conn) == 405000.0


def test_net_worth_simple(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    assert calc_net_worth(db_conn) == 100000.0


def test_net_worth_with_stocks(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    # cash: 100000 - 1500 = 98500, positions at cost: 1500 => total 100000
    assert calc_net_worth(db_conn) == 100000.0


def test_net_worth_with_debt(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    ledger.add_debt(db_conn, "2025-01-01", symbol="CC", name="Card", amount=5000.0, cash_received=False, payment_per_period=50.0)
    assert calc_net_worth(db_conn) == 95000.0


def test_net_worth_full_scenario(db_conn):
    # deposit 200k
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    # buy house: 500k, 400k mortgage, 100k down payment
    _, prop, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, down_payment=100000.0)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=400000.0,
        interest_rate=0.0, payment_per_period=2000.0,
    )
    # cash: 200k - 100k = 100k
    # buy stocks
    stock = create_asset(db_conn, Asset(symbol="VTI", name="Vanguard Total", asset_type="etf"))
    ledger.buy(db_conn, "2025-03-01", stock.id, quantity=100, price=200.0)
    # cash: 100k - 20k = 80k
    # add credit card debt
    ledger.add_debt(db_conn, "2025-01-01", symbol="CC", name="Card", amount=3000.0, cash_received=False, payment_per_period=30.0)

    summary = get_portfolio_summary(db_conn)
    assert summary["cash"] == 80000.0
    assert summary["positions_value"] == 20000.0
    assert summary["property_value"] == 500000.0
    assert summary["total_assets"] == 600000.0
    assert summary["mortgage"] == 400000.0
    assert summary["debt"] == 3000.0
    assert summary["total_liabilities"] == 403000.0
    assert summary["net_worth"] == 197000.0
    assert summary["real_estate_equity"] == 100000.0


def test_no_negative_positions_after_sell(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    ledger.sell(db_conn, "2025-02-15", asset.id, quantity=10, price=170.0)
    positions = calc_positions(db_conn)
    for p in positions:
        assert p.quantity >= 0, f"{p.symbol} has negative quantity {p.quantity}"


def test_sell_validation_prevents_negative_positions(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    import pytest
    with pytest.raises(ValueError):
        ledger.sell(db_conn, "2025-02-15", asset.id, quantity=11, price=170.0)
    positions = calc_positions(db_conn)
    assert len(positions) == 1
    assert positions[0].quantity == 10


def test_net_worth_after_rent_and_expense(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
    asset, prop, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="RENTAL", name="Rental",
        purchase_price=300000.0, down_payment=50000.0)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=250000.0,
        interest_rate=0.0, payment_per_period=1500.0,
    )
    # cash: 50k - 50k = 0
    ledger.receive_rent(db_conn, "2025-03-01", asset.id, 2000.0)
    ledger.pay_property_expense(db_conn, "2025-03-01", asset.id, 800.0)
    # cash: 0 + 2000 - 800 = 1200
    assert calc_cash_balance(db_conn) == 1200.0
    # total assets: 1200 + 300000 = 301200
    # total liabilities: 250000
    # net worth: 51200
    assert calc_net_worth(db_conn) == 51200.0


# ===================================================================
# Sold properties excluded from portfolio totals
# ===================================================================


def test_sold_property_excluded_from_total_assets(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset, prop, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, down_payment=100000.0)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=400000.0,
        interest_rate=0.0, payment_per_period=2000.0,
    )
    assert calc_total_property_value(db_conn) == 500000.0
    ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0)
    assert calc_total_property_value(db_conn) == 0.0


def test_sold_property_mortgage_excluded_from_liabilities(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset, prop, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, down_payment=100000.0)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=400000.0,
        interest_rate=0.0, payment_per_period=2000.0,
    )
    assert calc_total_mortgage(db_conn) == 400000.0
    ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0)
    assert calc_total_mortgage(db_conn) == 0.0


def test_active_property_still_in_total_assets(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
    a1, p1, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="Active",
        purchase_price=300000.0, down_payment=100000.0)
    ledger.add_mortgage(
        db_conn, property_id=p1.id, original_amount=200000.0,
        interest_rate=0.0, payment_per_period=1000.0,
    )
    a2, p2, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H2", name="ToBeSold",
        purchase_price=400000.0, down_payment=100000.0)
    ledger.add_mortgage(
        db_conn, property_id=p2.id, original_amount=300000.0,
        interest_rate=0.0, payment_per_period=1500.0,
    )
    assert calc_total_property_value(db_conn) == 700000.0
    ledger.sell_property(db_conn, "2025-06-01", a2.id, 420000.0)
    assert calc_total_property_value(db_conn) == 300000.0
    assert calc_total_mortgage(db_conn) == 200000.0


def test_portfolio_summary_after_sell(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset, prop, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, down_payment=100000.0)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=400000.0,
        interest_rate=0.0, payment_per_period=2000.0,
    )
    ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0, fees=10000.0)
    summary = get_portfolio_summary(db_conn)
    assert summary["property_value"] == 0.0
    assert summary["mortgage"] == 0.0
    assert summary["real_estate_equity"] == 0.0
