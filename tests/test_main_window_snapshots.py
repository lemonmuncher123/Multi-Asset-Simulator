import pytest
from unittest.mock import patch, MagicMock

from src.storage.database import init_db
from src.gui.main_window import MainWindow
from src.models.transaction import Transaction
from src.storage.transaction_repo import create_transaction
from src.storage.snapshot_repo import list_snapshots


@pytest.fixture
def conn():
    c = init_db(":memory:")
    yield c
    c.close()


@pytest.fixture
def conn_with_data():
    c = init_db(":memory:")
    create_transaction(c, Transaction(
        date="2026-04-27", txn_type="deposit_cash",
        total_amount=50000.0, currency="USD",
    ))
    yield c
    c.close()


# --- _record_today_snapshot ---

def test_record_today_snapshot_writes_when_data(conn_with_data):
    w = MainWindow(conn_with_data, enable_startup_sync=False)
    w._record_today_snapshot()
    snapshots = list_snapshots(conn_with_data)
    assert len(snapshots) == 1
    assert snapshots[0].cash == 50000.0

def test_record_today_snapshot_no_write_empty_db(conn):
    w = MainWindow(conn, enable_startup_sync=False)
    w._record_today_snapshot()
    snapshots = list_snapshots(conn)
    assert len(snapshots) == 0

def test_record_today_snapshot_swallows_exception(conn):
    w = MainWindow(conn, enable_startup_sync=False)
    with patch(
        "src.gui.main_window.record_daily_portfolio_snapshot",
        side_effect=RuntimeError("db locked"),
    ):
        w._record_today_snapshot()


# --- _handle_data_changed ---

def test_handle_data_changed_writes_snapshot_and_refreshes(conn_with_data):
    w = MainWindow(conn_with_data, enable_startup_sync=False)
    with patch.object(w, "_refresh_current") as mock_refresh:
        w._handle_data_changed()
        mock_refresh.assert_called_once()
    snapshots = list_snapshots(conn_with_data)
    assert len(snapshots) == 1


def test_handle_data_changed_refreshes_even_on_empty_db(conn):
    w = MainWindow(conn, enable_startup_sync=False)
    with patch.object(w, "_refresh_current") as mock_refresh:
        w._handle_data_changed()
        mock_refresh.assert_called_once()
    snapshots = list_snapshots(conn)
    assert len(snapshots) == 0


# --- _on_startup_sync_finished ---

def test_sync_finished_records_snapshot(conn_with_data):
    w = MainWindow(conn_with_data, enable_startup_sync=False)
    with patch.object(w, "_refresh_current"):
        w._on_startup_sync_finished({"updated": 5})
    snapshots = list_snapshots(conn_with_data)
    assert len(snapshots) == 1


def test_sync_finished_calls_refresh(conn_with_data):
    w = MainWindow(conn_with_data, enable_startup_sync=False)
    with patch.object(w, "_refresh_current") as mock_refresh:
        w._on_startup_sync_finished({"updated": 5})
        mock_refresh.assert_called_once()


# --- wiring: page.data_changed signal triggers _handle_data_changed ---

def test_transactions_page_data_changed_triggers_handler(conn):
    w = MainWindow(conn, enable_startup_sync=False)
    txn_page = w.page_widgets[1]  # Transactions
    with patch.object(w, "_refresh_current") as mock_refresh:
        txn_page.data_changed.emit()
        mock_refresh.assert_called_once()


def test_settings_data_panel_data_changed_triggers_handler(conn):
    w = MainWindow(conn, enable_startup_sync=False)
    settings_page = w.page_widgets[8]  # Settings
    with patch.object(w, "_refresh_current") as mock_refresh:
        settings_page.data_panel.data_changed.emit()
        mock_refresh.assert_called_once()
