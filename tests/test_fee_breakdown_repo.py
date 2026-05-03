"""Tests for src/storage/fee_breakdown_repo.py — CRUD for transaction fee breakdowns."""

import pytest
from src.storage.database import init_db, EXPECTED_TABLES
from src.storage.fee_breakdown_repo import (
    FeeBreakdownRow, create_fee_breakdown, list_fee_breakdowns,
)
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.transaction_repo import create_transaction
from src.models.transaction import Transaction


@pytest.fixture
def db_conn():
    conn = init_db(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def sample_txn(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    txn = create_transaction(db_conn, Transaction(
        date="2026-03-15", txn_type="buy", asset_id=asset.id,
        quantity=10, price=150.0, total_amount=-1505.0, fees=5.0,
    ))
    return txn


# --- Table existence ---

class TestTableExists:

    def test_table_in_expected_tables(self):
        assert "transaction_fee_breakdown" in EXPECTED_TABLES

    def test_table_created_in_fresh_db(self, db_conn):
        tables = {row[0] for row in db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "transaction_fee_breakdown" in tables

    def test_table_has_correct_columns(self, db_conn):
        cols = [row[1] for row in db_conn.execute(
            "PRAGMA table_info(transaction_fee_breakdown)"
        ).fetchall()]
        expected = ["id", "transaction_id", "fee_type", "amount", "rate",
                    "notes", "created_at"]
        assert cols == expected


# --- Create ---

class TestCreateFeeBreakdown:

    def test_create_returns_row_with_id(self, db_conn, sample_txn):
        row = create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id,
            fee_type="broker_commission",
            amount=4.95,
        ))
        assert row.id is not None
        assert row.id > 0

    def test_create_stores_all_fields(self, db_conn, sample_txn):
        row = create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id,
            fee_type="sec_section31",
            amount=0.23,
            rate=22.90,
            notes="$22.90 per $1M sold",
        ))
        stored = db_conn.execute(
            "SELECT * FROM transaction_fee_breakdown WHERE id = ?", (row.id,)
        ).fetchone()
        assert stored["transaction_id"] == sample_txn.id
        assert stored["fee_type"] == "sec_section31"
        assert stored["amount"] == 0.23
        assert stored["rate"] == 22.90
        assert stored["notes"] == "$22.90 per $1M sold"
        assert stored["created_at"] is not None

    def test_create_nullable_fields(self, db_conn, sample_txn):
        row = create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id,
            fee_type="additional_fee",
            amount=5.0,
            rate=None,
            notes=None,
        ))
        stored = db_conn.execute(
            "SELECT * FROM transaction_fee_breakdown WHERE id = ?", (row.id,)
        ).fetchone()
        assert stored["rate"] is None
        assert stored["notes"] is None

    def test_create_multiple_for_same_transaction(self, db_conn, sample_txn):
        create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id, fee_type="broker_commission", amount=4.95,
        ))
        create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id, fee_type="finra_taf", amount=0.02,
        ))
        create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id, fee_type="additional_fee", amount=3.0,
        ))
        count = db_conn.execute(
            "SELECT COUNT(*) FROM transaction_fee_breakdown WHERE transaction_id = ?",
            (sample_txn.id,),
        ).fetchone()[0]
        assert count == 3


# --- List ---

class TestListFeeBreakdowns:

    def test_list_returns_empty_for_no_rows(self, db_conn, sample_txn):
        rows = list_fee_breakdowns(db_conn, sample_txn.id)
        assert rows == []

    def test_list_returns_created_rows(self, db_conn, sample_txn):
        create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id, fee_type="broker_commission", amount=4.95,
        ))
        create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id, fee_type="finra_taf", amount=0.02,
        ))
        rows = list_fee_breakdowns(db_conn, sample_txn.id)
        assert len(rows) == 2
        assert rows[0].fee_type == "broker_commission"
        assert rows[1].fee_type == "finra_taf"

    def test_list_does_not_return_other_transactions(self, db_conn, sample_txn):
        asset = create_asset(db_conn, Asset(symbol="MSFT", name="Microsoft", asset_type="stock"))
        txn2 = create_transaction(db_conn, Transaction(
            date="2026-03-16", txn_type="buy", asset_id=asset.id,
            quantity=5, price=200.0, total_amount=-1000.0,
        ))
        create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id, fee_type="broker_commission", amount=4.95,
        ))
        create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=txn2.id, fee_type="broker_commission", amount=9.99,
        ))
        rows_t1 = list_fee_breakdowns(db_conn, sample_txn.id)
        rows_t2 = list_fee_breakdowns(db_conn, txn2.id)
        assert len(rows_t1) == 1
        assert rows_t1[0].amount == 4.95
        assert len(rows_t2) == 1
        assert rows_t2[0].amount == 9.99

    def test_list_ordered_by_id(self, db_conn, sample_txn):
        r1 = create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id, fee_type="broker_commission", amount=1.0,
        ))
        r2 = create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id, fee_type="finra_taf", amount=2.0,
        ))
        r3 = create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id, fee_type="additional_fee", amount=3.0,
        ))
        rows = list_fee_breakdowns(db_conn, sample_txn.id)
        assert [r.id for r in rows] == [r1.id, r2.id, r3.id]

    def test_row_object_has_all_fields(self, db_conn, sample_txn):
        create_fee_breakdown(db_conn, FeeBreakdownRow(
            transaction_id=sample_txn.id,
            fee_type="sec_section31",
            amount=0.23,
            rate=22.90,
            notes="test note",
        ))
        rows = list_fee_breakdowns(db_conn, sample_txn.id)
        assert len(rows) == 1
        row = rows[0]
        assert row.id is not None
        assert row.transaction_id == sample_txn.id
        assert row.fee_type == "sec_section31"
        assert row.amount == 0.23
        assert row.rate == 22.90
        assert row.notes == "test note"
        assert row.created_at is not None
