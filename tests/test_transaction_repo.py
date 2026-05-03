from src.models.asset import Asset
from src.models.transaction import Transaction
from src.storage.asset_repo import create_asset
from src.storage.transaction_repo import create_transaction, get_transaction, list_transactions, delete_transaction


def test_create_and_get_transaction(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    txn = create_transaction(db_conn, Transaction(
        date="2025-01-15", txn_type="buy", asset_id=asset.id,
        quantity=10, price=150.0, total_amount=-1500.0,
    ))
    assert txn.id is not None

    fetched = get_transaction(db_conn, txn.id)
    assert fetched.txn_type == "buy"
    assert fetched.quantity == 10
    assert fetched.price == 150.0
    assert fetched.total_amount == -1500.0


def test_list_transactions_empty(db_conn):
    assert list_transactions(db_conn) == []


def test_list_transactions_by_asset(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    a2 = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))

    create_transaction(db_conn, Transaction(
        date="2025-01-15", txn_type="buy", asset_id=a1.id,
        quantity=10, price=150.0, total_amount=-1500.0,
    ))
    create_transaction(db_conn, Transaction(
        date="2025-01-16", txn_type="buy", asset_id=a2.id,
        quantity=0.5, price=40000.0, total_amount=-20000.0,
    ))

    apple_txns = list_transactions(db_conn, asset_id=a1.id)
    assert len(apple_txns) == 1
    assert apple_txns[0].asset_id == a1.id

    all_txns = list_transactions(db_conn)
    assert len(all_txns) == 2


def test_list_transactions_by_type(db_conn):
    create_transaction(db_conn, Transaction(
        date="2025-01-01", txn_type="deposit_cash", total_amount=10000.0,
    ))
    create_transaction(db_conn, Transaction(
        date="2025-01-02", txn_type="withdraw_cash", total_amount=-500.0,
    ))

    deposits = list_transactions(db_conn, txn_type="deposit_cash")
    assert len(deposits) == 1
    assert deposits[0].total_amount == 10000.0


def test_delete_transaction(db_conn):
    txn = create_transaction(db_conn, Transaction(
        date="2025-01-01", txn_type="deposit_cash", total_amount=5000.0,
    ))
    delete_transaction(db_conn, txn.id)
    assert get_transaction(db_conn, txn.id) is None


def test_cash_deposit_no_asset(db_conn):
    txn = create_transaction(db_conn, Transaction(
        date="2025-01-01", txn_type="deposit_cash", total_amount=50000.0,
    ))
    fetched = get_transaction(db_conn, txn.id)
    assert fetched.asset_id is None
    assert fetched.quantity is None
    assert fetched.total_amount == 50000.0
