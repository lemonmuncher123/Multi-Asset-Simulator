from src.models.asset import Asset
from src.models.debt import Debt
from src.storage.asset_repo import create_asset
from src.storage.debt_repo import create_debt, get_debt, get_debt_by_asset, list_debts, update_debt


def test_create_and_get_debt(db_conn):
    asset = create_asset(db_conn, Asset(
        symbol="STUDENT_LOAN", name="Student Loan", asset_type="debt",
    ))
    debt = create_debt(db_conn, Debt(
        asset_id=asset.id, name="Student Loan",
        original_amount=50000.0, current_balance=35000.0,
        interest_rate=0.05, minimum_payment=500.0,
    ))
    assert debt.id is not None

    fetched = get_debt(db_conn, debt.id)
    assert fetched.name == "Student Loan"
    assert fetched.original_amount == 50000.0
    assert fetched.current_balance == 35000.0
    assert fetched.interest_rate == 0.05


def test_get_debt_by_asset(db_conn):
    asset = create_asset(db_conn, Asset(symbol="CC", name="Credit Card", asset_type="debt"))
    create_debt(db_conn, Debt(
        asset_id=asset.id, name="Credit Card",
        original_amount=5000.0, current_balance=3000.0,
        interest_rate=0.20,
    ))
    fetched = get_debt_by_asset(db_conn, asset.id)
    assert fetched is not None
    assert fetched.interest_rate == 0.20


def test_list_debts(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="D1", name="Debt 1", asset_type="debt"))
    a2 = create_asset(db_conn, Asset(symbol="D2", name="Debt 2", asset_type="debt"))
    create_debt(db_conn, Debt(asset_id=a1.id, name="Car Loan", original_amount=30000, current_balance=25000))
    create_debt(db_conn, Debt(asset_id=a2.id, name="Student Loan", original_amount=50000, current_balance=40000))

    debts = list_debts(db_conn)
    assert len(debts) == 2
    assert debts[0].name == "Car Loan"
    assert debts[1].name == "Student Loan"


def test_update_debt(db_conn):
    asset = create_asset(db_conn, Asset(symbol="D1", name="Debt", asset_type="debt"))
    debt = create_debt(db_conn, Debt(
        asset_id=asset.id, name="Car Loan",
        original_amount=30000, current_balance=25000,
    ))
    debt.current_balance = 24500.0
    update_debt(db_conn, debt)

    fetched = get_debt(db_conn, debt.id)
    assert fetched.current_balance == 24500.0
