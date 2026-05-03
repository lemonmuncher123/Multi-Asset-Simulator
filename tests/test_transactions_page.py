import pytest
from unittest.mock import patch

from PySide6.QtWidgets import QMessageBox
from src.gui.pages.transactions import TXN_TYPES, AMOUNT_REQUIRED, TransactionsPage
from src.models.security_master import SecurityMasterRecord
from src.storage.database import init_db
from src.storage.security_master_repo import upsert_security
from src.storage.asset_repo import get_asset_by_symbol, list_assets


# --- Module-level constants ---

def test_txn_types_does_not_include_add_property():
    assert "add_property" not in TXN_TYPES


def test_amount_required_does_not_include_add_property():
    assert "add_property" not in AMOUNT_REQUIRED


def test_txn_types_includes_expected():
    for t in ("deposit_cash", "withdraw_cash", "buy", "sell", "pay_property_expense"):
        assert t in TXN_TYPES


def test_receive_rent_excluded_from_combo():
    # Rent is auto-credited; manual receive-rent has no UI surface.
    assert "receive_rent" not in TXN_TYPES


def test_add_debt_pay_debt_pay_mortgage_in_combo():
    # The debt operations live on the same combo as cash flows so the
    # whole transactions page shares one selector.
    for t in ("add_debt", "pay_debt", "pay_mortgage"):
        assert t in TXN_TYPES


# --- GUI instantiation ---

@pytest.fixture
def page():
    conn = init_db(":memory:")
    p = TransactionsPage(conn)
    yield p
    conn.close()


def test_transactions_page_instantiates(page):
    assert page is not None


def test_combo_does_not_contain_add_property(page):
    combo = page.txn_type
    items = [combo.itemData(i) for i in range(combo.count())]
    assert "add_property" not in items


# --- Search / Add Selected Asset ---

@pytest.fixture
def page_with_universe():
    conn = init_db(":memory:")
    upsert_security(conn, SecurityMasterRecord(
        symbol="QQQ", name="Invesco QQQ Trust", asset_type="etf", exchange="NASDAQ",
    ))
    p = TransactionsPage(conn)
    yield p
    conn.close()


def _run_search(page):
    page.search_input.setText("QQQ")
    page._search_securities()


def test_search_populates_results_table(page_with_universe):
    _run_search(page_with_universe)
    assert page_with_universe.search_results_table.rowCount() == 1
    assert page_with_universe._search_results[0].symbol == "QQQ"


def test_selecting_row_caches_index(page_with_universe):
    _run_search(page_with_universe)
    page_with_universe.search_results_table.selectRow(0)
    assert page_with_universe._selected_search_result_index == 0


def test_new_search_clears_cached_index(page_with_universe):
    _run_search(page_with_universe)
    page_with_universe.search_results_table.selectRow(0)
    assert page_with_universe._selected_search_result_index == 0
    _run_search(page_with_universe)
    assert page_with_universe._selected_search_result_index is None


def test_empty_search_clears_cached_index(page_with_universe):
    _run_search(page_with_universe)
    page_with_universe.search_results_table.selectRow(0)
    page_with_universe.search_input.setText("")
    page_with_universe._search_securities()
    assert page_with_universe._selected_search_result_index is None


@patch.object(QMessageBox, "information")
def test_add_selected_asset_normal_selection(mock_info, page_with_universe):
    page = page_with_universe
    _run_search(page)
    page.search_results_table.selectRow(0)

    page._add_selected_asset()

    asset = get_asset_by_symbol(page.conn, "QQQ")
    assert asset is not None
    assert asset.symbol == "QQQ"

    combo_symbols = [
        page.asset_combo.itemText(i) for i in range(page.asset_combo.count())
    ]
    assert any("QQQ" in s for s in combo_symbols)
    mock_info.assert_called_once()


@patch.object(QMessageBox, "information")
def test_add_selected_asset_fallback_to_cached_index(mock_info, page_with_universe):
    page = page_with_universe
    _run_search(page)
    page.search_results_table.selectRow(0)
    assert page._selected_search_result_index == 0

    page.search_results_table.clearSelection()
    selected = page.search_results_table.selectionModel().selectedRows()
    assert len(selected) == 0

    page._add_selected_asset()

    asset = get_asset_by_symbol(page.conn, "QQQ")
    assert asset is not None
    assert asset.symbol == "QQQ"

    combo_data = [
        page.asset_combo.itemData(i) for i in range(page.asset_combo.count())
    ]
    assert asset.id in combo_data
    mock_info.assert_called_once()


@patch.object(QMessageBox, "information")
def test_add_selected_asset_reselects_row(mock_info, page_with_universe):
    page = page_with_universe
    _run_search(page)
    page.search_results_table.selectRow(0)

    page._add_selected_asset()

    selected = page.search_results_table.selectionModel().selectedRows()
    assert len(selected) == 1
    assert selected[0].row() == 0


def test_add_selected_asset_noop_when_no_selection_or_cache(page_with_universe):
    page = page_with_universe
    _run_search(page)
    assert page._selected_search_result_index is None

    page._add_selected_asset()

    asset = get_asset_by_symbol(page.conn, "QQQ")
    assert asset is None
    assert page.asset_combo.count() == 0


# --- User-action preflight: auto-settle runs before submit (Phase 7) ---
#
# These tests construct a real MainWindow so the page's `parent.window()`
# resolves to the MainWindow with `ensure_auto_settle_current()`. Without
# the MainWindow, the preflight is a silent no-op (existing standalone-
# Page tests above keep working unchanged).


@pytest.fixture
def conn_for_window():
    c = init_db(":memory:")
    yield c
    c.close()


def _count(conn, txn_type: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE txn_type=?",
        (txn_type,),
    ).fetchone()[0]


def _trigger_deposit_submit(page, amount: float = 100.0):
    """Drive the page's deposit_cash submit path. The submit handler
    runs `guard_transaction_or_warn` which walks `parent.window()` to
    find the MainWindow's `ensure_auto_settle_current` and call it
    before the bankruptcy check."""
    page.txn_type.setCurrentIndex(TXN_TYPES.index("deposit_cash"))
    page.amount_input.setText(str(amount))
    page._submit()


@patch.object(QMessageBox, "warning")
def test_preflight_runs_auto_settle_before_user_submit(
    mock_warn, qapp, conn_for_window,
):
    """Item 4: a user-initiated submit triggers `ensure_auto_settle_current`
    via the bankruptcy guard. The preflight closes the loophole where
    leaving the app open across midnight bypasses scheduled settlement.

    Setup: a debt is scheduled with cashflow_start_date in the past;
    the MainWindow is constructed *without* running startup_auto_settle,
    so the obligation is un-settled. The very first user submit then
    triggers preflight, which processes the overdue payments before
    the deposit is recorded.
    """
    from src.engines import ledger
    from src.gui.main_window import MainWindow

    ledger.deposit_cash(conn_for_window, "2025-01-01", 5000.0)
    ledger.add_debt(
        conn_for_window, "2025-01-01", symbol="L", name="Loan",
        amount=10000.0, interest_rate=0.0,
        payment_per_period=200.0, schedule_frequency="monthly",
        cashflow_start_date="2025-01-01",
        cash_received=False,
    )

    w = MainWindow(
        conn_for_window,
        enable_startup_sync=False,
        enable_auto_settle_timer=False,
    )
    # Pin the simulator's "today" past the obligation's anchor so
    # auto-settle has work to do, but skip _startup_auto_settle so the
    # obligation is still un-processed when the user submits.
    w._current_date = lambda: "2025-03-15"
    assert _count(conn_for_window, "pay_debt") == 0
    assert w._last_auto_settle_date is None

    tx_page = w.page_widgets[w._page_index["Transactions"]]
    tx_page.refresh()

    _trigger_deposit_submit(tx_page, amount=100.0)

    # Three monthly payments now exist (Jan, Feb, Mar) — preflight
    # processed them before the deposit landed.
    assert _count(conn_for_window, "pay_debt") == 3
    # Cache reflects the preflight run.
    assert w._last_auto_settle_date == "2025-03-15"
    # And the user's deposit also went through.
    assert conn_for_window.execute(
        "SELECT COUNT(*) FROM transactions "
        "WHERE txn_type='deposit_cash' AND total_amount=100"
    ).fetchone()[0] == 1
    # No bankruptcy warning fired (portfolio is solvent).
    mock_warn.assert_not_called()
    w.close()


@patch.object(QMessageBox, "warning")
def test_preflight_creates_due_debt_payments_before_user_submit(
    mock_warn, qapp, conn_for_window,
):
    """Item 6: explicit confirmation that the auto debt deduction is
    created BEFORE the user transaction lands. We compare transaction
    timestamps to verify ordering: the auto-settled pay_debt rows have
    earlier ledger ids than the user's deposit_cash row."""
    from src.engines import ledger
    from src.gui.main_window import MainWindow

    ledger.deposit_cash(conn_for_window, "2025-01-01", 5000.0)
    ledger.add_debt(
        conn_for_window, "2025-01-01", symbol="L", name="Card",
        amount=2000.0, interest_rate=0.0,
        payment_per_period=200.0, schedule_frequency="monthly",
        cashflow_start_date="2025-01-01",
        cash_received=False,
    )

    w = MainWindow(
        conn_for_window,
        enable_startup_sync=False,
        enable_auto_settle_timer=False,
    )
    w._current_date = lambda: "2025-02-15"
    tx_page = w.page_widgets[w._page_index["Transactions"]]
    tx_page.refresh()

    _trigger_deposit_submit(tx_page, amount=42.0)

    # All pay_debt rows must have id less than the user's deposit_cash
    # row that landed *after* preflight ran.
    rows = conn_for_window.execute(
        "SELECT id, txn_type, total_amount FROM transactions "
        "ORDER BY id"
    ).fetchall()
    auto_pay_ids = [r["id"] for r in rows if r["txn_type"] == "pay_debt"]
    user_deposit_ids = [
        r["id"] for r in rows
        if r["txn_type"] == "deposit_cash"
        and abs(r["total_amount"] - 42.0) < 1e-6
    ]
    assert len(auto_pay_ids) >= 1
    assert len(user_deposit_ids) == 1
    assert max(auto_pay_ids) < user_deposit_ids[0]
    w.close()


@patch.object(QMessageBox, "warning")
def test_preflight_creates_due_rent_before_user_submit(
    mock_warn, qapp, conn_for_window,
):
    """Item 7: rent obligations are also processed by preflight, not
    just debt. The auto-settle pipeline runs `settle_due_rent` first;
    a user submit triggers it before the user's transaction lands."""
    from src.engines import ledger
    from src.gui.main_window import MainWindow

    ledger.deposit_cash(conn_for_window, "2025-01-01", 5000.0)
    ledger.add_property(
        conn_for_window, "2025-01-01", symbol="H", name="House",
        purchase_price=200000.0,
        monthly_rent=1500.0,
        cashflow_start_date="2025-01-01",
        acquisition_mode="existing_property",
    )

    w = MainWindow(
        conn_for_window,
        enable_startup_sync=False,
        enable_auto_settle_timer=False,
    )
    w._current_date = lambda: "2025-03-15"
    assert _count(conn_for_window, "receive_rent") == 0

    tx_page = w.page_widgets[w._page_index["Transactions"]]
    tx_page.refresh()
    _trigger_deposit_submit(tx_page, amount=10.0)

    # Three months of rent: Jan, Feb, Mar.
    assert _count(conn_for_window, "receive_rent") == 3
    mock_warn.assert_not_called()
    w.close()


@patch.object(QMessageBox, "warning")
def test_repeated_user_submits_do_not_duplicate_scheduled_rows(
    mock_warn, qapp, conn_for_window,
):
    """Item 8 (UI variant): clicking submit multiple times in the same
    session on the same calendar date does not re-create the
    scheduled debt/rent rows preflight already processed."""
    from src.engines import ledger
    from src.gui.main_window import MainWindow

    ledger.deposit_cash(conn_for_window, "2025-01-01", 5000.0)
    ledger.add_debt(
        conn_for_window, "2025-01-01", symbol="L", name="Loan",
        amount=10000.0, interest_rate=0.0,
        payment_per_period=200.0, schedule_frequency="monthly",
        cashflow_start_date="2025-01-01",
        cash_received=False,
    )
    ledger.add_property(
        conn_for_window, "2025-01-01", symbol="H", name="House",
        purchase_price=200000.0, monthly_rent=1500.0,
        cashflow_start_date="2025-01-01",
        acquisition_mode="existing_property",
    )

    w = MainWindow(
        conn_for_window,
        enable_startup_sync=False,
        enable_auto_settle_timer=False,
    )
    w._current_date = lambda: "2025-03-15"
    tx_page = w.page_widgets[w._page_index["Transactions"]]
    tx_page.refresh()

    # Three submits, each one triggers a preflight check.
    for i in range(3):
        _trigger_deposit_submit(tx_page, amount=10.0 + i)

    # Scheduled rows are exactly the ones produced by the FIRST
    # preflight; subsequent submits short-circuited via the cache and
    # the engine-level dedupe.
    assert _count(conn_for_window, "pay_debt") == 3
    assert _count(conn_for_window, "receive_rent") == 3
    # Three user deposits did go through (10, 11, 12).
    assert _count(conn_for_window, "deposit_cash") == 4  # initial 5000 + three submits
    w.close()
