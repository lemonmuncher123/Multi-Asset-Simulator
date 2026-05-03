from src.models.decision_journal import DecisionJournalEntry
from src.models.transaction import Transaction
from src.storage.journal_repo import (
    create_journal_entry, get_journal_entry, get_journal_by_transaction,
    list_journal_entries, update_journal_entry, delete_journal_entry,
)
from src.storage.transaction_repo import create_transaction


def test_create_and_get_journal_entry(db_conn):
    entry = create_journal_entry(db_conn, DecisionJournalEntry(
        date="2025-01-15", title="Buy AAPL",
        thesis="Strong earnings expected",
        risk_reasoning="Could drop on macro weakness",
    ))
    assert entry.id is not None

    fetched = get_journal_entry(db_conn, entry.id)
    assert fetched.title == "Buy AAPL"
    assert fetched.thesis == "Strong earnings expected"
    assert fetched.risk_reasoning == "Could drop on macro weakness"


def test_create_with_all_new_fields(db_conn):
    entry = create_journal_entry(db_conn, DecisionJournalEntry(
        transaction_id=None,
        date="2025-02-01", title="Buy BTC",
        thesis="Crypto diversification",
        intended_role="speculation",
        risk_reasoning="High volatility",
        exit_plan="Sell if drops 20%",
        confidence_level=3,
        expected_holding_period="6 months",
        pre_trade_notes="Market seems bullish",
        snapshot_before='{"cash": 100000}',
        snapshot_after='{"cash": 90000}',
    ))
    fetched = get_journal_entry(db_conn, entry.id)
    assert fetched.intended_role == "speculation"
    assert fetched.confidence_level == 3
    assert fetched.exit_plan == "Sell if drops 20%"
    assert fetched.expected_holding_period == "6 months"
    assert fetched.snapshot_before == '{"cash": 100000}'
    assert fetched.snapshot_after == '{"cash": 90000}'


def test_get_journal_by_transaction(db_conn):
    txn = create_transaction(db_conn, Transaction(
        date="2025-01-15", txn_type="deposit_cash", total_amount=1000.0,
    ))
    entry = create_journal_entry(db_conn, DecisionJournalEntry(
        transaction_id=txn.id, date="2025-01-15", title="Buy AAPL",
    ))
    fetched = get_journal_by_transaction(db_conn, txn.id)
    assert fetched is not None
    assert fetched.id == entry.id

    assert get_journal_by_transaction(db_conn, 999) is None


def test_list_journal_entries(db_conn):
    create_journal_entry(db_conn, DecisionJournalEntry(
        date="2025-01-10", title="Entry 1",
    ))
    create_journal_entry(db_conn, DecisionJournalEntry(
        date="2025-01-15", title="Entry 2",
    ))

    entries = list_journal_entries(db_conn)
    assert len(entries) == 2
    assert entries[0].date == "2025-01-15"


def test_update_journal_entry(db_conn):
    entry = create_journal_entry(db_conn, DecisionJournalEntry(
        date="2025-01-15", title="Buy AAPL",
        thesis="Growth play",
    ))
    entry.post_trade_review = "Stock rose 8%"
    entry.mistake_tags = "none"
    entry.lesson_learned = "Thesis was correct"
    update_journal_entry(db_conn, entry)

    fetched = get_journal_entry(db_conn, entry.id)
    assert fetched.post_trade_review == "Stock rose 8%"
    assert fetched.lesson_learned == "Thesis was correct"


def test_delete_journal_entry(db_conn):
    entry = create_journal_entry(db_conn, DecisionJournalEntry(
        date="2025-01-15", title="Delete me",
    ))
    delete_journal_entry(db_conn, entry.id)
    assert get_journal_entry(db_conn, entry.id) is None
