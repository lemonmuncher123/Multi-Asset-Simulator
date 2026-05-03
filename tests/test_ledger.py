import pytest
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.transaction_repo import list_transactions
from src.storage.property_repo import get_property_by_asset
from src.engines import ledger
from src.engines.ledger import first_day_next_month
from src.engines.holdings import get_asset_quantity
from src.engines.portfolio import calc_positions, calc_cash_balance, calc_net_worth
from src.storage.property_repo import list_active_properties


def test_deposit_cash(db_conn):
    txn = ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
    assert txn.txn_type == "deposit_cash"
    assert txn.total_amount == 10000.0


def test_withdraw_cash(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
    txn = ledger.withdraw_cash(db_conn, "2025-01-02", 3000.0)
    assert txn.total_amount == -3000.0


def test_buy_stock(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    txn = ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    assert txn.txn_type == "buy"
    assert txn.quantity == 10
    assert txn.price == 150.0
    assert txn.total_amount == -1500.0


def test_buy_with_fees(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    txn = ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0, fees=9.99)
    assert txn.total_amount == -(10 * 150.0 + 9.99)
    assert txn.fees == 9.99


def test_sell_stock(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    txn = ledger.sell(db_conn, "2025-02-15", asset.id, quantity=5, price=170.0)
    assert txn.txn_type == "sell"
    assert txn.quantity == 5
    assert txn.total_amount == 850.0


def test_sell_with_fees(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
    txn = ledger.sell(db_conn, "2025-02-15", asset.id, quantity=5, price=170.0, fees=9.99)
    assert txn.total_amount == (5 * 170.0 - 9.99)


def test_buy_crypto(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
    asset = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    txn = ledger.buy(db_conn, "2025-01-20", asset.id, quantity=0.5, price=42000.0)
    assert txn.quantity == 0.5
    assert txn.total_amount == -21000.0


def test_add_property(db_conn):
    from src.storage.mortgage_repo import get_mortgage_by_property
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset, prop, txn = ledger.add_property(
        db_conn, "2025-02-01", symbol="HOUSE1", name="My House",
        purchase_price=500000.0, down_payment=100000.0,
        address="123 Main St", monthly_expense=2500.0)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=400000.0,
        interest_rate=0.05, payment_per_period=2000.0)
    assert asset.asset_type == "real_estate"
    assert prop.purchase_price == 500000.0
    mortgage = get_mortgage_by_property(db_conn, prop.id)
    assert mortgage.current_balance == 400000.0
    assert txn.total_amount == -100000.0  # down payment = 500k - 400k


def test_add_property_with_explicit_down_payment(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    _, prop, txn = ledger.add_property(
        db_conn, "2025-02-01", symbol="HOUSE2", name="Condo",
        purchase_price=300000.0,
        down_payment=60000.0)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=250000.0,
        interest_rate=0.05, payment_per_period=1500.0)
    assert txn.total_amount == -60000.0


def test_update_property_value(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 600000.0)
    asset, prop, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0)
    txn = ledger.update_property_value(db_conn, "2025-06-01", asset.id, 550000.0)
    assert txn.total_amount == 0.0
    assert txn.txn_type == "update_property_value"

    from src.storage.property_repo import get_property_by_asset
    updated = get_property_by_asset(db_conn, asset.id)
    assert updated.current_value == 550000.0


def test_receive_rent(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 400000.0)
    asset, _, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="RENTAL", name="Rental",
        purchase_price=300000.0, monthly_rent=2000.0)
    txn = ledger.receive_rent(db_conn, "2025-03-01", asset.id, 2000.0)
    assert txn.total_amount == 2000.0


def test_pay_property_expense(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 600000.0)
    asset, _, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0)
    txn = ledger.pay_property_expense(db_conn, "2025-03-01", asset.id, 800.0)
    assert txn.total_amount == -800.0


def test_pay_mortgage(db_conn):
    from src.storage.mortgage_repo import get_mortgage_by_property
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset, prop, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, down_payment=100000.0)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=400000.0,
        interest_rate=0.05, payment_per_period=2000.0)
    txn = ledger.pay_mortgage(db_conn, "2025-03-01", asset.id, amount=2000.0, principal=1500.0)
    assert txn.total_amount == -2000.0

    mortgage = get_mortgage_by_property(db_conn, prop.id)
    assert mortgage.current_balance == 398500.0


def test_add_debt(db_conn):
    asset, debt, txn = ledger.add_debt(
        db_conn, "2025-01-01", symbol="STUDENT", name="Student Loan",
        amount=50000.0, interest_rate=0.05, minimum_payment=500.0,
        payment_per_period=708.33)
    assert asset.asset_type == "debt"
    assert debt.current_balance == 50000.0
    assert txn.total_amount == 50000.0  # cash received from loan


def test_add_debt_no_cash(db_conn):
    _, _, txn = ledger.add_debt(
        db_conn, "2025-01-01", symbol="CC", name="Credit Card",
        amount=5000.0, cash_received=False,
        payment_per_period=50.0)
    assert txn.total_amount == 0.0


def test_pay_debt(db_conn):
    asset, debt, _ = ledger.add_debt(
        db_conn, "2025-01-01", symbol="CC", name="Credit Card",
        amount=5000.0, interest_rate=0.20,
        payment_per_period=133.33)
    # When principal_portion is omitted and the debt has a non-zero rate,
    # one month of interest is computed from the balance:
    # 5000 * 0.20 / 12 ≈ 83.33; principal reduction = 1000 - 83.33 ≈ 916.67;
    # remaining balance = 5000 - 916.67 ≈ 4083.33.
    txn = ledger.pay_debt(db_conn, "2025-02-01", asset.id, 1000.0)
    assert txn.total_amount == -1000.0

    from src.storage.debt_repo import get_debt_by_asset
    updated = get_debt_by_asset(db_conn, asset.id)
    assert updated.current_balance == pytest.approx(4083.333333, rel=1e-4)


def test_pay_debt_with_explicit_principal(db_conn):
    # When `principal_portion` is provided, it is used verbatim — useful
    # for users who want to model their own amortization schedule.
    asset, _, _ = ledger.add_debt(
        db_conn, "2025-01-01", symbol="CC2", name="Card",
        amount=5000.0, interest_rate=0.20,
        payment_per_period=133.33)
    ledger.pay_debt(db_conn, "2025-02-01", asset.id, 1000.0, principal_portion=900.0)

    from src.storage.debt_repo import get_debt_by_asset
    updated = get_debt_by_asset(db_conn, asset.id)
    assert updated.current_balance == 4100.0


def test_pay_debt_zero_rate_treats_full_amount_as_principal(db_conn):
    # Backwards-compatible path: a debt with no interest rate behaves the
    # way the old code did — full payment reduces principal.
    asset, _, _ = ledger.add_debt(
        db_conn, "2025-01-01", symbol="LOAN", name="Friend Loan",
        amount=5000.0, interest_rate=0.0,
        payment_per_period=50.0)
    ledger.pay_debt(db_conn, "2025-02-01", asset.id, 1000.0)

    from src.storage.debt_repo import get_debt_by_asset
    updated = get_debt_by_asset(db_conn, asset.id)
    assert updated.current_balance == 4000.0


def test_manual_adjustment(db_conn):
    txn = ledger.manual_adjustment(db_conn, "2025-01-01", 500.0, notes="Correction")
    assert txn.txn_type == "manual_adjustment"
    assert txn.total_amount == 500.0


# ===================================================================
# Regression: sell-over-position bug
# ===================================================================


class TestSellRejectsNoHoldings:
    """ledger.sell() must reject selling an asset with no holdings."""

    def test_stock_no_holdings(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        count_before = len(list_transactions(db_conn))
        with pytest.raises(ValueError, match="no position"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 1, 150.0)
        assert len(list_transactions(db_conn)) == count_before

    def test_etf_no_holdings(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="SPY", name="S&P 500 ETF", asset_type="etf"))
        count_before = len(list_transactions(db_conn))
        with pytest.raises(ValueError, match="no position"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 1, 400.0)
        assert len(list_transactions(db_conn)) == count_before

    def test_crypto_no_holdings(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
        count_before = len(list_transactions(db_conn))
        with pytest.raises(ValueError, match="no position"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 0.1, 40000.0)
        assert len(list_transactions(db_conn)) == count_before


class TestSellRejectsOverPosition:
    """ledger.sell() must reject selling more than currently held."""

    @pytest.fixture(autouse=True)
    def _seed_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-01-01", 10_000_000.0)

    def test_sell_11_of_10_held(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
        count_before = len(list_transactions(db_conn))
        with pytest.raises(ValueError, match="Insufficient quantity"):
            ledger.sell(db_conn, "2025-02-15", asset.id, quantity=11, price=170.0)
        assert get_asset_quantity(db_conn, asset.id) == 10
        assert len(list_transactions(db_conn)) == count_before


class TestSellAllowsValidSells:
    """Valid partial and full sells must still work; exhausted position rejects further sells."""

    @pytest.fixture(autouse=True)
    def _seed_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-01-01", 10_000_000.0)

    def test_sell_5_then_5_then_reject(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)

        txn1 = ledger.sell(db_conn, "2025-02-01", asset.id, quantity=5, price=160.0)
        assert txn1.quantity == 5
        txn2 = ledger.sell(db_conn, "2025-02-02", asset.id, quantity=5, price=165.0)
        assert txn2.quantity == 5

        positions = calc_positions(db_conn)
        assert all(p.symbol != "AAPL" for p in positions)

        with pytest.raises(ValueError):
            ledger.sell(db_conn, "2025-02-03", asset.id, quantity=0.0001, price=170.0)


class TestSellFractionalCrypto:
    """Fractional crypto sells must respect available quantity."""

    @pytest.fixture(autouse=True)
    def _seed_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-01-01", 10_000_000.0)

    def test_buy_half_sell_quarter_then_over(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=0.5, price=42000.0)

        txn = ledger.sell(db_conn, "2025-02-01", asset.id, quantity=0.25, price=44000.0)
        assert txn.quantity == pytest.approx(0.25)

        with pytest.raises(ValueError, match="Insufficient quantity"):
            ledger.sell(db_conn, "2025-02-02", asset.id, quantity=0.3, price=44000.0)


class TestSellDateSensitiveHoldings:
    """A sell dated before the buy must fail because the position did not exist yet."""

    @pytest.fixture(autouse=True)
    def _seed_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-01-01", 10_000_000.0)

    def test_sell_before_buy_date(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        ledger.buy(db_conn, "2025-02-01", asset.id, quantity=10, price=150.0)
        with pytest.raises(ValueError, match="no position"):
            ledger.sell(db_conn, "2025-01-15", asset.id, quantity=5, price=145.0)


class TestSellRejectsNonSellableTypes:
    """ledger.sell() must reject real_estate, debt, and cash asset types."""

    @pytest.fixture(autouse=True)
    def _seed_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-01-01", 10_000_000.0)

    def test_real_estate(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-01-01", symbol="HOUSE", name="House",
            purchase_price=500000.0)
        with pytest.raises(ValueError, match="Cannot sell asset type"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 1, 500000.0)

    def test_debt(self, db_conn):
        asset, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="LOAN", name="Loan", amount=10000.0,
            payment_per_period=100.0)
        with pytest.raises(ValueError, match="Cannot sell asset type"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 1, 10000.0)

    def test_cash(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="CASH1", name="Cash", asset_type="cash"))
        with pytest.raises(ValueError, match="Cannot sell asset type"):
            ledger.sell(db_conn, "2025-02-01", asset.id, 1, 1000.0)

    def test_custom_with_holdings_succeeds(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="CUST", name="Custom", asset_type="custom"))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=5, price=100.0)
        txn = ledger.sell(db_conn, "2025-02-01", asset.id, quantity=3, price=120.0)
        assert txn.quantity == 3


# ===================================================================
# sell_property
# ===================================================================


class TestSellProperty:

    @pytest.fixture(autouse=True)
    def _seed_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-01-01", 10_000_000.0)

    def test_creates_sell_property_transaction(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        asset, prop, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0, down_payment=100000.0)
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=400000.0,
            interest_rate=0.0, payment_per_period=2000.0)
        txn = ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0)
        assert txn.txn_type == "sell_property"

    def test_cash_proceeds_equal_price_minus_fees(self, db_conn):
        # As of schema v11, sell_property transaction's total_amount =
        # sale_price - fees. The mortgage is settled by a separate
        # pay_mortgage_in_full transaction. Net cash to seller =
        # sale_price - payoff - fees.
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        asset, prop, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0, down_payment=100000.0)
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=400000.0,
            interest_rate=0.0, payment_per_period=2000.0)
        txn = ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0, fees=10000.0)
        assert txn.total_amount == 550000.0 - 10000.0

    def test_marks_property_sold(self, db_conn):
        asset, prop, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0, down_payment=100000.0)
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=400000.0,
            interest_rate=0.0, payment_per_period=2000.0)
        ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0)
        prop = get_property_by_asset(db_conn, asset.id)
        assert prop.status == "sold"
        assert prop.sold_date == "2025-06-01"
        assert prop.sold_price == 550000.0

    def test_clears_current_value_and_mortgage(self, db_conn):
        from src.storage.mortgage_repo import get_mortgage_by_property
        asset, prop, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0, down_payment=100000.0)
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=400000.0,
            interest_rate=0.0, payment_per_period=2000.0)
        ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0)
        prop = get_property_by_asset(db_conn, asset.id)
        assert prop.current_value == 0
        mortgage = get_mortgage_by_property(db_conn, prop.id)
        assert mortgage is not None
        assert mortgage.current_balance == 0

    def test_sold_property_cannot_be_sold_again(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0)
        ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0)
        with pytest.raises(ValueError, match="already sold"):
            ledger.sell_property(db_conn, "2025-07-01", asset.id, 600000.0)

    def test_invalid_sale_price_raises(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0)
        with pytest.raises(ValueError, match="Sale price must be positive"):
            ledger.sell_property(db_conn, "2025-06-01", asset.id, 0)
        with pytest.raises(ValueError, match="Sale price must be positive"):
            ledger.sell_property(db_conn, "2025-06-01", asset.id, -100000.0)

    def test_negative_fees_raises(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0)
        with pytest.raises(ValueError, match="Fees cannot be negative"):
            ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0, fees=-1.0)

    def test_sell_non_real_estate_raises(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        with pytest.raises(ValueError, match="not a real estate"):
            ledger.sell_property(db_conn, "2025-06-01", asset.id, 100.0)

    def test_sell_property_zero_fees_default(self, db_conn):
        asset, prop, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=300000.0, down_payment=100000.0)
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.0, payment_per_period=1000.0)
        txn = ledger.sell_property(db_conn, "2025-06-01", asset.id, 350000.0)
        # As of schema v11: sell_property total_amount = sale_price - fees
        # (mortgage settled by separate pay_mortgage_in_full txn).
        assert txn.total_amount == 350000.0
        assert txn.fees == 0.0

    def test_receive_rent_still_works_active_property(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="RENTAL", name="Rental",
            purchase_price=300000.0, monthly_rent=2000.0)
        txn = ledger.receive_rent(db_conn, "2025-03-01", asset.id, 2000.0)
        assert txn.total_amount == 2000.0

    def test_pay_expense_still_works_active_property(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0)
        txn = ledger.pay_property_expense(db_conn, "2025-03-01", asset.id, 800.0)
        assert txn.total_amount == -800.0


# ===================================================================
# settle_due_rent
# ===================================================================


class TestSettleDueRent:

    @pytest.fixture(autouse=True)
    def _seed_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-01-01", 10_000_000.0)

    def test_monthly_rent_settles_on_first_of_month(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-01-15", symbol="R1", name="Rental",
            purchase_price=300000.0, monthly_rent=2500.0,
            rent_collection_frequency="monthly",
            cashflow_start_date="2025-02-01")
        created = ledger.settle_due_rent(db_conn, "2025-04-01")
        assert len(created) == 3
        dates = [t.date for t in created]
        assert "2025-02-01" in dates
        assert "2025-03-01" in dates
        assert "2025-04-01" in dates

    def test_annual_rent_settles_on_january_first(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-06-01", symbol="R1", name="Annual Rental",
            purchase_price=300000.0, monthly_rent=2000.0,
            rent_collection_frequency="annual",
            cashflow_start_date="2025-06-01")
        created = ledger.settle_due_rent(db_conn, "2027-02-01")
        assert len(created) == 2
        dates = [t.date for t in created]
        assert "2026-01-01" in dates
        assert "2027-01-01" in dates

    def test_annual_rent_amount_is_12x_monthly(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-06-01", symbol="R1", name="Annual Rental",
            purchase_price=300000.0, monthly_rent=2000.0,
            rent_collection_frequency="annual",
            cashflow_start_date="2025-06-01")
        created = ledger.settle_due_rent(db_conn, "2026-02-01")
        assert len(created) == 1
        assert created[0].total_amount == 2000.0 * 12

    def test_settlement_starts_after_cashflow_start_date(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-03-20", symbol="R1", name="Rental",
            purchase_price=300000.0, monthly_rent=2500.0,
            rent_collection_frequency="monthly",
            cashflow_start_date="2025-04-01")
        created = ledger.settle_due_rent(db_conn, "2025-04-15")
        assert len(created) == 1
        assert created[0].date == "2025-04-01"

    def test_settlement_stops_at_sold_date(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-01-15", symbol="R1", name="Rental",
            purchase_price=300000.0, monthly_rent=2500.0,
            cashflow_start_date="2025-02-01")
        ledger.sell_property(db_conn, "2025-03-15", asset.id, 350000.0)
        created = ledger.settle_due_rent(db_conn, "2025-06-01")
        assert len(created) == 0

    def test_idempotent_no_duplicates(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-01-15", symbol="R1", name="Rental",
            purchase_price=300000.0, monthly_rent=2500.0,
            cashflow_start_date="2025-02-01")
        first = ledger.settle_due_rent(db_conn, "2025-04-01")
        assert len(first) == 3
        second = ledger.settle_due_rent(db_conn, "2025-04-01")
        assert len(second) == 0

    def test_creates_receive_rent_transactions(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-01-15", symbol="R1", name="Rental",
            purchase_price=300000.0, monthly_rent=2500.0,
            cashflow_start_date="2025-02-01")
        created = ledger.settle_due_rent(db_conn, "2025-03-01")
        for txn in created:
            assert txn.txn_type == "receive_rent"
            assert txn.total_amount == 2500.0
            assert txn.asset_id == asset.id
            assert "Scheduled rent" in txn.notes

    def test_zero_rent_creates_no_transactions(self, db_conn):
        ledger.add_property(
            db_conn, "2025-01-15", symbol="R1", name="NoRent",
            purchase_price=300000.0, monthly_rent=0.0)
        created = ledger.settle_due_rent(db_conn, "2025-06-01")
        assert len(created) == 0

    def test_settle_single_property_only(self, db_conn):
        a1, _, _ = ledger.add_property(
            db_conn, "2025-01-15", symbol="R1", name="Rental1",
            purchase_price=300000.0, monthly_rent=2000.0,
            cashflow_start_date="2025-02-01")
        a2, _, _ = ledger.add_property(
            db_conn, "2025-01-15", symbol="R2", name="Rental2",
            purchase_price=400000.0, monthly_rent=3000.0,
            cashflow_start_date="2025-02-01")
        created = ledger.settle_due_rent(db_conn, "2025-03-01", property_asset_id=a1.id)
        assert all(t.asset_id == a1.id for t in created)
        rent_txns_a2 = list_transactions(db_conn, asset_id=a2.id, txn_type="receive_rent")
        scheduled = [t for t in rent_txns_a2 if t.notes and "Scheduled" in t.notes]
        assert len(scheduled) == 0

    def test_cashflow_start_date_on_first_of_month_starts_that_month(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-01-01", symbol="R1", name="Rental",
            purchase_price=300000.0, monthly_rent=1000.0,
            cashflow_start_date="2025-03-01")
        created = ledger.settle_due_rent(db_conn, "2025-04-01")
        assert len(created) == 2
        dates = [t.date for t in created]
        assert "2025-03-01" in dates
        assert "2025-04-01" in dates

    def test_cashflow_start_date_non_first_normalizes_to_next_month(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-01-01", symbol="R1", name="Rental",
            purchase_price=300000.0, monthly_rent=1000.0,
            cashflow_start_date="2025-03-15")
        created = ledger.settle_due_rent(db_conn, "2025-05-01")
        assert len(created) == 2
        dates = [t.date for t in created]
        assert "2025-04-01" in dates
        assert "2025-05-01" in dates

    def test_annual_rent_cashflow_start_jan1(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2020-01-01", symbol="R1", name="Annual",
            purchase_price=300000.0, monthly_rent=2000.0,
            rent_collection_frequency="annual",
            cashflow_start_date="2026-01-01")
        created = ledger.settle_due_rent(db_conn, "2027-06-01")
        assert len(created) == 2
        dates = [t.date for t in created]
        assert "2026-01-01" in dates
        assert "2027-01-01" in dates

    def test_no_backfill_from_old_purchase_date(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2009-01-15", symbol="R1", name="Old Property",
            purchase_price=200000.0, monthly_rent=1500.0,
            cashflow_start_date="2026-05-01")
        created = ledger.settle_due_rent(db_conn, "2026-06-01")
        assert len(created) == 2
        dates = [t.date for t in created]
        assert "2026-05-01" in dates
        assert "2026-06-01" in dates


# ===================================================================
# settle_due_rent: existing property scenario (requirement 5)
# ===================================================================


class TestSettleDueRentExistingPropertyScenario:
    """Specific test scenario: purchase_date=2009-01-15, cashflow_start_date=2026-05-01, monthly_rent=2000."""

    def _create_existing_property(self, db_conn):
        asset, prop, txn = ledger.add_property(
            db_conn, "2009-01-15", symbol="OLD1", name="Existing Rental",
            purchase_price=200000.0, monthly_rent=2000.0,
            acquisition_mode="existing_property",
            cashflow_start_date="2026-05-01")
        return asset

    def test_settle_before_cashflow_start_creates_zero(self, db_conn):
        self._create_existing_property(db_conn)
        created = ledger.settle_due_rent(db_conn, "2026-04-30")
        assert len(created) == 0

    def test_settle_on_cashflow_start_creates_one(self, db_conn):
        self._create_existing_property(db_conn)
        created = ledger.settle_due_rent(db_conn, "2026-05-01")
        assert len(created) == 1
        assert created[0].date == "2026-05-01"
        assert created[0].total_amount == 2000.0

    def test_no_rent_before_cashflow_start_date(self, db_conn):
        self._create_existing_property(db_conn)
        created = ledger.settle_due_rent(db_conn, "2026-07-01")
        dates = [t.date for t in created]
        for d in dates:
            assert d >= "2026-05-01", f"Rent dated {d} is before cashflow_start_date 2026-05-01"

    def test_settle_twice_no_duplicates(self, db_conn):
        self._create_existing_property(db_conn)
        first = ledger.settle_due_rent(db_conn, "2026-06-01")
        assert len(first) == 2
        second = ledger.settle_due_rent(db_conn, "2026-06-01")
        assert len(second) == 0

    def test_annual_rent_starts_jan1_on_or_after_cashflow_start(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2009-01-15", symbol="ANN1", name="Annual Existing",
            purchase_price=200000.0, monthly_rent=2000.0,
            acquisition_mode="existing_property",
            cashflow_start_date="2026-05-01",
            rent_collection_frequency="annual")
        created = ledger.settle_due_rent(db_conn, "2028-06-01")
        dates = [t.date for t in created]
        assert "2027-01-01" in dates
        assert "2028-01-01" in dates
        assert "2026-01-01" not in dates
        assert "2009-01-01" not in dates
        assert "2010-01-01" not in dates

    def test_annual_rent_cashflow_start_on_jan1_includes_that_year(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2009-01-15", symbol="ANN2", name="Annual Jan Start",
            purchase_price=200000.0, monthly_rent=2000.0,
            acquisition_mode="existing_property",
            cashflow_start_date="2027-01-01",
            rent_collection_frequency="annual")
        created = ledger.settle_due_rent(db_conn, "2028-06-01")
        dates = [t.date for t in created]
        assert "2027-01-01" in dates
        assert "2028-01-01" in dates


# ===================================================================
# settle_due_rent: legacy NULL cashflow_start_date (requirement 6)
# ===================================================================


class TestSettleDueRentLegacyNull:
    """A property with cashflow_start_date=NULL must not backfill historical rent."""

    def test_null_cashflow_start_date_no_historical_backfill(self, db_conn, monkeypatch):
        from src.models.property_asset import PropertyAsset
        from src.storage.asset_repo import create_asset
        from src.storage.property_repo import create_property

        asset = create_asset(db_conn, Asset(
            symbol="OLD1", name="Legacy House", asset_type="real_estate", liquidity="illiquid"))
        create_property(db_conn, PropertyAsset(
            asset_id=asset.id,
            purchase_date="2005-03-01",
            purchase_price=150000.0,
            current_value=300000.0,
            monthly_rent=1500.0,
            cashflow_start_date=None))

        monkeypatch.setattr(
            "src.engines.ledger.first_day_next_month",
            lambda today=None: "2026-05-01")

        created = ledger.settle_due_rent(db_conn, "2026-06-01")
        dates = [t.date for t in created]
        assert len(created) == 2
        assert "2026-05-01" in dates
        assert "2026-06-01" in dates
        for d in dates:
            assert d >= "2026-05-01"

    def test_null_cashflow_settle_before_fallback_creates_zero(self, db_conn, monkeypatch):
        from src.models.property_asset import PropertyAsset
        from src.storage.asset_repo import create_asset
        from src.storage.property_repo import create_property

        asset = create_asset(db_conn, Asset(
            symbol="OLD2", name="Legacy House 2", asset_type="real_estate", liquidity="illiquid"))
        create_property(db_conn, PropertyAsset(
            asset_id=asset.id,
            purchase_date="2005-03-01",
            purchase_price=150000.0,
            current_value=300000.0,
            monthly_rent=1500.0,
            cashflow_start_date=None))

        monkeypatch.setattr(
            "src.engines.ledger.first_day_next_month",
            lambda today=None: "2026-05-01")

        created = ledger.settle_due_rent(db_conn, "2026-04-30")
        assert len(created) == 0


# ===================================================================
# first_day_next_month
# ===================================================================


class TestFirstDayNextMonth:

    def test_mid_month(self):
        from datetime import date
        assert first_day_next_month(date(2026, 4, 26)) == "2026-05-01"

    def test_december_rolls_to_january(self):
        from datetime import date
        assert first_day_next_month(date(2026, 12, 15)) == "2027-01-01"

    def test_first_of_month(self):
        from datetime import date
        assert first_day_next_month(date(2025, 1, 1)) == "2025-02-01"

    def test_last_day_of_month(self):
        from datetime import date
        assert first_day_next_month(date(2025, 2, 28)) == "2025-03-01"

    def test_jan_2026(self):
        from datetime import date
        assert first_day_next_month(date(2026, 1, 1)) == "2026-02-01"


# ===================================================================
# add_property acquisition modes
# ===================================================================


class TestAddPropertyAcquisitionModes:

    @pytest.fixture(autouse=True)
    def _seed_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-01-01", 10_000_000.0)

    def test_new_purchase_deducts_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        _, _, txn = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0, down_payment=100000.0,
            acquisition_mode="new_purchase")
        assert txn.total_amount == -100000.0

    def test_existing_property_no_cash_deduction(self, db_conn):
        _, _, txn = ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=200000.0, 
            acquisition_mode="existing_property")
        assert txn.total_amount == 0.0
        assert "Existing property entry" in txn.notes

    def test_existing_property_preserves_user_notes(self, db_conn):
        _, _, txn = ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=200000.0,
            acquisition_mode="existing_property",
            notes="Inherited from family")
        assert "Inherited from family" in txn.notes
        assert "Existing property entry" in txn.notes

    def test_unknown_mode_raises(self, db_conn):
        with pytest.raises(ValueError, match="Unknown acquisition_mode"):
            ledger.add_property(
                db_conn, "2025-01-01", symbol="H1", name="House",
                purchase_price=100000.0,
                acquisition_mode="invalid")

    def test_cashflow_start_date_persisted(self, db_conn):
        asset, prop, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0,
            cashflow_start_date="2025-03-01")
        fetched = get_property_by_asset(db_conn, asset.id)
        assert fetched.cashflow_start_date == "2025-03-01"

    def test_cashflow_start_date_defaults_to_next_month(self, db_conn):
        asset, prop, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0)
        fetched = get_property_by_asset(db_conn, asset.id)
        assert fetched.cashflow_start_date == first_day_next_month()

    def test_existing_property_with_current_value(self, db_conn):
        asset, prop, _ = ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=200000.0, current_value=450000.0,
            acquisition_mode="existing_property")
        fetched = get_property_by_asset(db_conn, asset.id)
        assert fetched.current_value == 450000.0
        assert fetched.purchase_price == 200000.0

    def test_new_purchase_current_value_defaults_to_purchase_price(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0,
            acquisition_mode="new_purchase")
        fetched = get_property_by_asset(db_conn, asset.id)
        assert fetched.current_value == 500000.0

    def test_new_purchase_current_value_saved_if_provided(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0, current_value=520000.0,
            acquisition_mode="new_purchase")
        fetched = get_property_by_asset(db_conn, asset.id)
        assert fetched.current_value == 520000.0

    def test_existing_property_creates_asset_and_property(self, db_conn):
        asset, prop, txn = ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=200000.0,
            acquisition_mode="existing_property")
        assert asset.id is not None
        assert asset.asset_type == "real_estate"
        assert prop.id is not None
        assert prop.asset_id == asset.id

    def test_existing_property_txn_type_is_add_property(self, db_conn):
        _, _, txn = ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=200000.0,
            acquisition_mode="existing_property")
        assert txn.txn_type == "add_property"

    def test_existing_property_cash_balance_unchanged(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        cash_before = calc_cash_balance(db_conn)
        ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        cash_after = calc_cash_balance(db_conn)
        assert cash_after == cash_before

    def test_existing_property_saves_purchase_date(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=200000.0,
            acquisition_mode="existing_property")
        fetched = get_property_by_asset(db_conn, asset.id)
        assert fetched.purchase_date == "2009-01-15"

    def test_existing_property_saves_cashflow_start_date(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=200000.0,
            acquisition_mode="existing_property",
            cashflow_start_date="2026-05-01")
        fetched = get_property_by_asset(db_conn, asset.id)
        assert fetched.cashflow_start_date == "2026-05-01"


class TestPlannedPurchaseMode:
    def test_planned_purchase_accepted(self, db_conn):
        asset, prop, txn = ledger.add_property(
            db_conn, "2026-06-01", symbol="H1", name="Dream House",
            purchase_price=600000.0, 
            acquisition_mode="planned_purchase")
        assert asset.id is not None
        assert prop.status == "planned"

    def test_planned_purchase_zero_cash_impact(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        cash_before = calc_cash_balance(db_conn)
        _, _, txn = ledger.add_property(
            db_conn, "2026-06-01", symbol="H1", name="Dream House",
            purchase_price=600000.0, 
            acquisition_mode="planned_purchase")
        assert txn.total_amount == 0.0
        assert calc_cash_balance(db_conn) == cash_before

    def test_planned_purchase_not_in_active_properties(self, db_conn):
        ledger.add_property(
            db_conn, "2026-06-01", symbol="H1", name="Dream House",
            purchase_price=600000.0,
            acquisition_mode="planned_purchase")
        assert len(list_active_properties(db_conn)) == 0

    def test_planned_purchase_txn_note(self, db_conn):
        _, _, txn = ledger.add_property(
            db_conn, "2026-06-01", symbol="H1", name="Dream House",
            purchase_price=600000.0,
            acquisition_mode="planned_purchase")
        assert "Planned purchase" in txn.notes

    def test_planned_purchase_no_rent_settlement(self, db_conn):
        ledger.add_property(
            db_conn, "2026-06-01", symbol="H1", name="Dream House",
            purchase_price=600000.0, monthly_rent=3000.0,
            acquisition_mode="planned_purchase",
            cashflow_start_date="2026-07-01")
        created = ledger.settle_due_rent(db_conn, "2027-12-31")
        assert len(created) == 0

    def test_planned_purchase_entry_type_stored(self, db_conn):
        asset, prop, _ = ledger.add_property(
            db_conn, "2026-06-01", symbol="H1", name="Dream House",
            purchase_price=600000.0,
            acquisition_mode="planned_purchase")
        fetched = get_property_by_asset(db_conn, asset.id)
        assert fetched.entry_type == "planned_purchase"

    def test_planned_does_not_affect_net_worth(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        nw_before = calc_net_worth(db_conn)
        ledger.add_property(
            db_conn, "2026-06-01", symbol="H1", name="Dream House",
            purchase_price=600000.0, 
            acquisition_mode="planned_purchase")
        nw_after = calc_net_worth(db_conn)
        assert nw_after == nw_before

    def test_unknown_mode_raises(self, db_conn):
        with pytest.raises(ValueError, match="Unknown acquisition_mode"):
            ledger.add_property(
                db_conn, "2026-06-01", symbol="H1", name="X",
                purchase_price=100000.0,
                acquisition_mode="invalid_mode")

    def test_planned_does_not_affect_total_assets(self, db_conn):
        from src.engines.portfolio import calc_total_assets
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        ta_before = calc_total_assets(db_conn)
        ledger.add_property(
            db_conn, "2026-06-01", symbol="H1", name="Dream House",
            purchase_price=600000.0, 
            acquisition_mode="planned_purchase")
        assert calc_total_assets(db_conn) == ta_before

    def test_planned_not_in_analyze_all(self, db_conn):
        from src.engines.real_estate import analyze_all_properties
        ledger.add_property(
            db_conn, "2026-06-01", symbol="H1", name="Dream House",
            purchase_price=600000.0,
            acquisition_mode="planned_purchase")
        assert len(analyze_all_properties(db_conn)) == 0

    def test_planned_not_in_real_estate_warnings(self, db_conn):
        from src.engines.real_estate import get_real_estate_warnings
        ledger.add_property(
            db_conn, "2026-06-01", symbol="H1", name="Dream House",
            purchase_price=600000.0,
            acquisition_mode="planned_purchase")
        warnings = get_real_estate_warnings(db_conn)
        assert len(warnings) == 0

    def test_planned_coexists_with_active(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        a1, _, _ = ledger.add_property(
            db_conn, "2020-01-01", symbol="H1", name="Owned House",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        a2, _, _ = ledger.add_property(
            db_conn, "2026-06-01", symbol="H2", name="Dream House",
            purchase_price=600000.0,
            acquisition_mode="planned_purchase")
        active = list_active_properties(db_conn)
        assert len(active) == 1
        assert active[0].asset_id == a1.id


# ===================================================================
# New Purchase mode: active property with cash impact
# ===================================================================


class TestNewPurchaseModeActiveAndCashImpacting:
    """Verify new_purchase creates an active property that deducts cash."""

    @pytest.fixture(autouse=True)
    def _seed_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-01-01", 10_000_000.0)

    def test_new_purchase_creates_active_property(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        asset, prop, txn = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="New House",
            purchase_price=500000.0, 
            acquisition_mode="new_purchase")
        assert prop.status == "active"

    def test_new_purchase_in_active_properties(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="New House",
            purchase_price=500000.0, 
            acquisition_mode="new_purchase")
        active = list_active_properties(db_conn)
        assert len(active) == 1
        assert active[0].asset_id == asset.id

    def test_new_purchase_in_analyze_all(self, db_conn):
        from src.engines.real_estate import analyze_all_properties
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="New House",
            purchase_price=500000.0, 
            acquisition_mode="new_purchase")
        analyses = analyze_all_properties(db_conn)
        assert len(analyses) == 1
        assert analyses[0].name == "New House"

    def test_new_purchase_txn_has_negative_total(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        _, _, txn = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="New House",
            purchase_price=500000.0, down_payment=100000.0,
            acquisition_mode="new_purchase")
        assert txn.total_amount == pytest.approx(-100000.0)

    def test_new_purchase_with_down_payment_uses_dp(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        _, _, txn = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="New House",
            purchase_price=500000.0, 
            down_payment=120000.0,
            acquisition_mode="new_purchase")
        assert txn.total_amount == pytest.approx(-120000.0)

    def test_new_purchase_without_down_payment_uses_full_price(self, db_conn):
        # As of v11, omitting down_payment means cash_out = full
        # purchase_price (no implicit "price - mortgage" calculation,
        # since mortgage is no longer an arg).
        ledger.deposit_cash(db_conn, "2025-01-01", 600000.0)
        _, _, txn = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="New House",
            purchase_price=500000.0,
            acquisition_mode="new_purchase")
        assert txn.total_amount == pytest.approx(-500000.0)

    def test_new_purchase_cash_balance_decreases(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        cash_before = calc_cash_balance(db_conn)
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="New House",
            purchase_price=500000.0, down_payment=100000.0,
            acquisition_mode="new_purchase")
        cash_after = calc_cash_balance(db_conn)
        assert cash_after == pytest.approx(cash_before - 100000.0)

    def test_new_purchase_entry_type_stored(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="New House",
            purchase_price=500000.0,
            acquisition_mode="new_purchase")
        fetched = get_property_by_asset(db_conn, asset.id)
        assert fetched.entry_type == "new_purchase"

    def test_new_purchase_default_cashflow_start_date(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="New House",
            purchase_price=500000.0,
            acquisition_mode="new_purchase")
        fetched = get_property_by_asset(db_conn, asset.id)
        assert fetched.cashflow_start_date == first_day_next_month()


class TestNewPurchaseRentSettlement:
    """Verify rent settlement behavior for new_purchase properties."""

    def test_new_purchase_no_rent_before_cashflow_start(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        ledger.add_property(
            db_conn, "2026-01-15", symbol="R1", name="New Rental",
            purchase_price=400000.0, down_payment=80000.0,
            monthly_rent=2500.0,
            acquisition_mode="new_purchase",
            cashflow_start_date="2026-06-01")
        created = ledger.settle_due_rent(db_conn, "2026-05-31")
        assert len(created) == 0

    def test_new_purchase_rent_on_cashflow_start(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        ledger.add_property(
            db_conn, "2026-01-15", symbol="R1", name="New Rental",
            purchase_price=400000.0, down_payment=80000.0,
            monthly_rent=2500.0,
            acquisition_mode="new_purchase",
            cashflow_start_date="2026-06-01")
        created = ledger.settle_due_rent(db_conn, "2026-06-01")
        assert len(created) == 1
        assert created[0].date == "2026-06-01"

    def test_new_purchase_no_backfill_from_purchase_date(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        ledger.add_property(
            db_conn, "2026-01-15", symbol="R1", name="New Rental",
            purchase_price=400000.0, down_payment=80000.0,
            monthly_rent=2500.0,
            acquisition_mode="new_purchase",
            cashflow_start_date="2026-06-01")
        created = ledger.settle_due_rent(db_conn, "2026-08-01")
        dates = [t.date for t in created]
        for d in dates:
            assert d >= "2026-06-01"
        assert "2026-02-01" not in dates
        assert "2026-03-01" not in dates


class TestExistingPropertyWithAnalysis:
    def test_existing_appears_in_analyze_all(self, db_conn):
        from src.engines.real_estate import analyze_all_properties
        asset, _, _ = ledger.add_property(
            db_conn, "2010-01-01", symbol="H1", name="My House",
            purchase_price=200000.0, monthly_rent=2000.0,
            acquisition_mode="existing_property")
        analyses = analyze_all_properties(db_conn)
        assert len(analyses) == 1
        assert analyses[0].name == "My House"

    def test_existing_rent_settlement_respects_cashflow_start(self, db_conn):
        ledger.add_property(
            db_conn, "2010-01-01", symbol="H1", name="Rental",
            purchase_price=200000.0, monthly_rent=2000.0,
            acquisition_mode="existing_property",
            cashflow_start_date="2026-07-01")
        before = ledger.settle_due_rent(db_conn, "2026-06-30")
        assert len(before) == 0
        after = ledger.settle_due_rent(db_conn, "2026-07-01")
        assert len(after) == 1


# ===================================================================
# Existing Property: transaction_date separation
# ===================================================================


class TestExistingPropertyTransactionDate:
    """Existing Property transaction uses today's date, not the historical purchase date."""

    @pytest.fixture(autouse=True)
    def _seed_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-01-01", 10_000_000.0)

    def test_existing_property_txn_uses_today_by_default(self, db_conn):
        from datetime import date
        _, _, txn = ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=200000.0,
            acquisition_mode="existing_property")
        assert txn.date == date.today().isoformat()

    def test_existing_property_purchase_date_preserved(self, db_conn):
        asset, _, _ = ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=200000.0,
            acquisition_mode="existing_property")
        fetched = get_property_by_asset(db_conn, asset.id)
        assert fetched.purchase_date == "2009-01-15"

    def test_existing_property_explicit_transaction_date(self, db_conn):
        _, _, txn = ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=200000.0,
            acquisition_mode="existing_property",
            transaction_date="2026-04-01")
        assert txn.date == "2026-04-01"

    def test_existing_property_txn_not_on_historical_date(self, db_conn):
        _, _, txn = ledger.add_property(
            db_conn, "2009-01-15", symbol="H1", name="Old House",
            purchase_price=200000.0,
            acquisition_mode="existing_property")
        assert txn.date != "2009-01-15"

    def test_new_purchase_uses_date_param(self, db_conn):
        _, _, txn = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0,
            acquisition_mode="new_purchase")
        assert txn.date == "2025-02-01"

    def test_new_purchase_with_transaction_date(self, db_conn):
        _, _, txn = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0,
            acquisition_mode="new_purchase",
            transaction_date="2025-02-15")
        assert txn.date == "2025-02-15"

    def test_planned_purchase_ignores_transaction_date(self, db_conn):
        _, _, txn = ledger.add_property(
            db_conn, "2026-06-01", symbol="H1", name="Dream",
            purchase_price=600000.0,
            acquisition_mode="planned_purchase")
        assert txn.date == "2026-06-01"


# ===================================================================
# Cash sufficiency: every spend path refuses to overdraft.
# ===================================================================


class TestCashSufficiency:
    """Verifies that every direct cash-out path raises ValueError when the
    user lacks the cash to cover it. Documents the source-of-truth contract
    that the simulator never lets cash drift negative through normal use.

    The one exception is `manual_adjustment`, which is the documented
    escape hatch and is covered separately.
    """

    def test_buy_without_cash_raises(self, db_conn):
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)

    def test_buy_at_exact_balance_succeeds(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 1500.0)
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        # Total cost is exactly 1500; the check uses _EPSILON tolerance.
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
        assert calc_cash_balance(db_conn) == 0.0

    def test_buy_one_dollar_short_raises(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 1499.0)
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)

    def test_buy_with_fees_factored_into_check(self, db_conn):
        # Deposit covers shares but not fees.
        ledger.deposit_cash(db_conn, "2025-01-01", 1500.0)
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0, fees=10.0)

    def test_withdraw_without_cash_raises(self, db_conn):
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.withdraw_cash(db_conn, "2025-01-01", 100.0)

    def test_withdraw_more_than_balance_raises(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.withdraw_cash(db_conn, "2025-01-02", 200.0)

    def test_add_property_new_purchase_without_cash_raises(self, db_conn):
        # The bug the user originally reported: buying real estate with
        # no cash on hand would silently push the balance negative.
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.add_property(
                db_conn, "2025-02-01", symbol="HOUSE", name="House",
                purchase_price=500000.0)

    def test_add_property_with_explicit_down_payment_without_cash_raises(self, db_conn):
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.add_property(
                db_conn, "2025-02-01", symbol="HOUSE", name="House",
                purchase_price=500000.0, 
                down_payment=100000.0)

    def test_add_property_existing_mode_no_cash_check(self, db_conn):
        # Existing-property entries record total_amount=0 (no cash impact),
        # so they don't need cash on hand.
        asset, _, txn = ledger.add_property(
            db_conn, "2009-01-15", symbol="OLDH", name="Old House",
            purchase_price=200000.0, 
            acquisition_mode="existing_property")
        assert txn.total_amount == 0.0
        assert asset.asset_type == "real_estate"

    def test_add_property_planned_mode_no_cash_check(self, db_conn):
        # Planned-purchase entries also have total_amount=0.
        _, _, txn = ledger.add_property(
            db_conn, "2030-01-01", symbol="PLAN", name="Future House",
            purchase_price=999999.0,
            acquisition_mode="planned_purchase")
        assert txn.total_amount == 0.0

    def test_pay_property_expense_without_cash_raises(self, db_conn):
        # Set up a property using existing_property mode so we have an asset
        # to pay an expense against without spending cash on the purchase.
        asset, _, _ = ledger.add_property(
            db_conn, "2024-01-01", symbol="H1", name="House",
            purchase_price=300000.0,
            acquisition_mode="existing_property")
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.pay_property_expense(db_conn, "2025-03-01", asset.id, 800.0)

    def test_pay_mortgage_without_cash_raises(self, db_conn):
        asset, prop, _ = ledger.add_property(
            db_conn, "2024-01-01", symbol="H1", name="House",
            purchase_price=300000.0,
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.0, payment_per_period=1000.0,
        )
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.pay_mortgage(db_conn, "2025-03-01", asset.id, amount=2000.0)

    def test_pay_debt_without_cash_raises(self, db_conn):
        # add_debt with cash_received=False adds a liability without any cash inflow.
        asset, _, _ = ledger.add_debt(
            db_conn, "2024-01-01", symbol="CC", name="Card",
            amount=5000.0, cash_received=False,
            payment_per_period=50.0)
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.pay_debt(db_conn, "2025-03-01", asset.id, 200.0)

    def test_sell_property_underwater_without_cash_raises(self, db_conn):
        # Mortgage + fees > sale price => seller has to bring cash to closing.
        ledger.deposit_cash(db_conn, "2024-01-01", 100000.0)
        asset, prop, _ = ledger.add_property(
            db_conn, "2024-02-01", symbol="UH", name="Underwater",
            purchase_price=500000.0,
            down_payment=50000.0)
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=450000.0,
            interest_rate=0.0, payment_per_period=2000.0,
        )
        # Cash now: 100k − 50k = 50k. Sale price 100k, mortgage 450k, fees 0.
        # Seller has to bring 350k. They only have 50k.
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.sell_property(db_conn, "2024-03-01", asset.id, sale_price=100000.0)

    def test_sell_property_above_water_succeeds_without_cash(self, db_conn):
        # When sale covers mortgage and fees, no cash check is needed even
        # if the user has spent down to zero.
        ledger.deposit_cash(db_conn, "2024-01-01", 250000.0)
        asset, prop, _ = ledger.add_property(
            db_conn, "2024-02-01", symbol="OK", name="Profitable",
            purchase_price=500000.0,
            down_payment=200000.0)
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=300000.0,
            interest_rate=0.0, payment_per_period=2000.0,
        )
        # Cash after purchase: 250k − 200k = 50k.
        # Sell at 600k − 300k mortgage = 300k net proceeds (positive).
        txn = ledger.sell_property(db_conn, "2024-06-01", asset.id, sale_price=600000.0)
        assert txn.total_amount > 0

    def test_manual_adjustment_can_drive_cash_negative(self, db_conn):
        # The escape hatch. Used for "I started this simulator already in
        # the red" or "fix-it" entries that intentionally bypass the
        # source-of-truth contract.
        ledger.manual_adjustment(db_conn, "2025-01-01", -500.0, notes="seed deficit")
        assert calc_cash_balance(db_conn) == -500.0

    def test_csv_import_running_overdraft_rejected(self, db_conn):
        from src.engines.import_export import import_transactions_csv
        create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        # 1k deposit, then a 1.5k buy — second row overdraws and is refused.
        # Atomicity (Option 3) means the deposit also rolls back via the
        # validation pass: the buy row is rejected before the COMMIT.
        csv_text = (
            "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
            "2025-01-01,deposit_cash,,,,1000,USD,0,\n"
            "2025-01-02,buy,AAPL,10,150,-1500,USD,0,\n"
        )
        result = import_transactions_csv(db_conn, csv_text)
        assert result.imported == 1  # only the deposit
        assert any("insufficient cash" in e.lower() for e in result.errors)
        assert calc_cash_balance(db_conn) == 1000.0

    def test_csv_import_running_cash_seeded_from_existing(self, db_conn):
        from src.engines.import_export import import_transactions_csv
        # Existing balance in the DB carries through to the import's
        # running cash — we don't need to deposit again in the CSV.
        ledger.deposit_cash(db_conn, "2024-01-01", 5000.0)
        create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        csv_text = (
            "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
            "2025-01-15,buy,AAPL,10,150,-1500,USD,0,\n"
        )
        result = import_transactions_csv(db_conn, csv_text)
        assert result.imported == 1
        assert len(result.errors) == 0
