import pytest

from src.storage.database import init_db
from src.gui.main_window import MainWindow
from src.models.transaction import Transaction
from src.storage.transaction_repo import create_transaction
from src.storage.report_repo import list_reports, report_exists


@pytest.fixture
def conn():
    c = init_db(":memory:")
    yield c
    c.close()


@pytest.fixture
def conn_with_data():
    c = init_db(":memory:")
    create_transaction(c, Transaction(
        date="2025-03-15", txn_type="deposit_cash",
        total_amount=50000.0, currency="USD",
    ))
    create_transaction(c, Transaction(
        date="2025-04-10", txn_type="deposit_cash",
        total_amount=10000.0, currency="USD",
    ))
    yield c
    c.close()


# --- MainWindow instantiation ---

def test_main_window_instantiates(conn):
    w = MainWindow(conn, enable_startup_sync=False)
    assert w is not None


# --- Startup report generation ---

def test_startup_reports_callable(conn):
    w = MainWindow(conn, enable_startup_sync=False)
    w._startup_reports()


def test_startup_reports_generates_due(conn_with_data):
    from datetime import date
    from src.engines.reports import generate_due_reports

    generated = generate_due_reports(conn_with_data, today=date(2025, 5, 15))
    assert len(generated) > 0
    assert report_exists(conn_with_data, "monthly", "2025-03")
    assert report_exists(conn_with_data, "monthly", "2025-04")


def test_startup_reports_populates_table(conn_with_data):
    from datetime import date
    from src.engines.reports import generate_due_reports

    generate_due_reports(conn_with_data, today=date(2025, 5, 15))
    reports = list_reports(conn_with_data, "monthly")
    assert len(reports) >= 2


def test_repeated_startup_no_duplicates(conn_with_data):
    from datetime import date
    from src.engines.reports import generate_due_reports

    gen1 = generate_due_reports(conn_with_data, today=date(2025, 5, 15))
    gen2 = generate_due_reports(conn_with_data, today=date(2025, 5, 15))
    assert len(gen1) > 0
    assert len(gen2) == 0

    reports = list_reports(conn_with_data)
    monthly_labels = [r.period_label for r in reports if r.report_type == "monthly"]
    for label in set(monthly_labels):
        assert monthly_labels.count(label) == 1


def test_main_window_with_data_instantiates(conn_with_data):
    w = MainWindow(conn_with_data, enable_startup_sync=False)
    assert w is not None
    w._startup_reports()
