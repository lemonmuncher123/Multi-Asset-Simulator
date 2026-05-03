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


# --- Auto-settle day-rollover + preflight (Phase 7) ---

def _seed_overdue_debt(conn, *, anchor: str = "2025-01-01"):
    """Seed a $200/mo debt anchored at `anchor` so the auto-settle
    pipeline has scheduled work to do once `_current_date` is moved
    past `anchor`."""
    from src.engines import ledger
    ledger.deposit_cash(conn, anchor, 1000.0)
    _, debt, _ = ledger.add_debt(
        conn, anchor, symbol="L", name="Loan",
        amount=10000.0, interest_rate=0.0,
        payment_per_period=200.0, schedule_frequency="monthly",
        cashflow_start_date=anchor,
        cash_received=False,
    )
    return debt


def _count_pay_debt(conn) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE txn_type='pay_debt'"
    ).fetchone()[0]


def _count_receive_rent(conn) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE txn_type='receive_rent'"
    ).fetchone()[0]


def test_startup_auto_settle_still_runs(qapp, conn):
    """Item 1: startup_auto_settle continues to run as before. With
    a scheduled obligation already due, calling _startup_auto_settle
    creates the corresponding pay_debt transactions."""
    _seed_overdue_debt(conn)
    w = MainWindow(
        conn, enable_startup_sync=False, enable_auto_settle_timer=False,
    )
    w._current_date = lambda: "2025-03-15"  # past obligation start
    assert _count_pay_debt(conn) == 0
    w._startup_auto_settle()
    # Three monthly payments due (Jan, Feb, Mar) → three pay_debt rows.
    assert _count_pay_debt(conn) == 3
    # Cache populated so subsequent preflights short-circuit.
    assert w._last_auto_settle_date == "2025-03-15"


def test_rollover_skips_when_date_unchanged(qapp, conn):
    """Item 2: when the calendar date hasn't moved since the last
    successful settle, the rollover timer's check is an O(1) no-op —
    no new ledger writes."""
    _seed_overdue_debt(conn)
    w = MainWindow(
        conn, enable_startup_sync=False, enable_auto_settle_timer=False,
    )
    w._current_date = lambda: "2025-03-15"
    w._startup_auto_settle()
    count_after_startup = _count_pay_debt(conn)
    # Now fire the rollover handler twice on the same date.
    w._check_auto_settle_date_rollover()
    w._check_auto_settle_date_rollover()
    assert _count_pay_debt(conn) == count_after_startup


def test_rollover_runs_exactly_once_on_date_change(qapp, conn):
    """Item 3: when _current_date moves from one day to the next, the
    rollover handler runs auto-settle exactly once for the new date.
    Repeated calls on the new date are no-ops."""
    _, debt, _ = (None, None, None)
    from src.engines import ledger
    ledger.deposit_cash(conn, "2025-05-15", 5000.0)
    ledger.add_debt(
        conn, "2025-05-15", symbol="L", name="Loan",
        amount=10000.0, interest_rate=0.0,
        payment_per_period=200.0, schedule_frequency="monthly",
        cashflow_start_date="2025-06-01",
        cash_received=False,
    )
    w = MainWindow(
        conn, enable_startup_sync=False, enable_auto_settle_timer=False,
    )
    # Day before the obligation comes due.
    w._current_date = lambda: "2025-05-31"
    w._check_auto_settle_date_rollover()
    assert _count_pay_debt(conn) == 0
    assert w._last_auto_settle_date == "2025-05-31"
    # Cross midnight to 2025-06-01.
    w._current_date = lambda: "2025-06-01"
    w._check_auto_settle_date_rollover()
    assert _count_pay_debt(conn) == 1
    assert w._last_auto_settle_date == "2025-06-01"
    # Repeated firings on the new date produce no further rows.
    w._check_auto_settle_date_rollover()
    w._check_auto_settle_date_rollover()
    assert _count_pay_debt(conn) == 1


def test_preflight_idempotent_on_same_date(qapp, conn):
    """Item 8: ensure_auto_settle_current called repeatedly on the
    same date does not duplicate scheduled debt/rent/mortgage rows.
    Idempotency is layered: the cache short-circuit + the engine's
    own dedupe (note-prefix matching in settle_due_*)."""
    _seed_overdue_debt(conn)
    w = MainWindow(
        conn, enable_startup_sync=False, enable_auto_settle_timer=False,
    )
    w._current_date = lambda: "2025-03-15"
    w.ensure_auto_settle_current()
    count = _count_pay_debt(conn)
    assert count == 3  # Jan, Feb, Mar
    # Five more preflights on the same date — count must not move.
    for _ in range(5):
        w.ensure_auto_settle_current()
    assert _count_pay_debt(conn) == count


def test_preflight_runs_when_cache_is_stale(qapp, conn):
    """Companion to item 8: when the cache value is from yesterday
    (e.g., the user left the app open across midnight and is now
    triggering a submit on the new day), ensure_auto_settle_current
    actually runs the pipeline."""
    _seed_overdue_debt(conn)
    w = MainWindow(
        conn, enable_startup_sync=False, enable_auto_settle_timer=False,
    )
    w._current_date = lambda: "2025-02-15"
    w._startup_auto_settle()
    assert _count_pay_debt(conn) == 2  # Jan, Feb
    # Cross to a new day with another obligation now due.
    w._current_date = lambda: "2025-03-01"
    w.ensure_auto_settle_current()
    assert _count_pay_debt(conn) == 3  # Mar now also processed


def test_run_auto_settle_today_param_overrides_current_date(qapp, conn):
    """Sanity check on the testable abstraction: passing today=...
    explicitly to _run_auto_settle uses that date even when
    _current_date returns something different. Lets tests pin the
    pipeline to an exact date without monkeypatching."""
    _seed_overdue_debt(conn)
    w = MainWindow(
        conn, enable_startup_sync=False, enable_auto_settle_timer=False,
    )
    w._current_date = lambda: "2099-01-01"  # nonsense
    w._run_auto_settle(today="2025-01-15")
    # Only the January payment should have processed (anchor is 2025-01-01).
    assert _count_pay_debt(conn) == 1
    assert w._last_auto_settle_date == "2025-01-15"


def test_auto_settle_timer_default_constructs_active(qapp, conn):
    """The day-boundary timer is created and active by default.
    Production deployments should have it running; tests opt out via
    enable_auto_settle_timer=False so the real 30-minute interval
    never fires during the test."""
    w = MainWindow(conn, enable_startup_sync=False)
    assert w._date_rollover_timer is not None
    assert w._date_rollover_timer.isActive()
    assert w._date_rollover_timer.interval() == w.AUTO_SETTLE_TIMER_INTERVAL_MS
    w.close()


# ===================================================================
# Reports tab — Quarterly + Semi-Annual + new tabs + staleness +
# overwrite confirm + how-to-read + falsy-price fix
# ===================================================================


@pytest.fixture
def import_export_page(qapp, conn_with_data):
    from src.gui.pages.import_export import ImportExportPage
    page = ImportExportPage(conn_with_data)
    yield page
    page.deleteLater()


def test_reports_tab_supports_four_cadences(import_export_page):
    items = [
        import_export_page.report_type_combo.itemData(i)
        for i in range(import_export_page.report_type_combo.count())
    ]
    assert items == ["monthly", "quarterly", "semi_annual", "annual"]


def test_stats_label_shows_all_four_counts(import_export_page):
    # The stats label only updates inside _refresh_report_list, which
    # the refresh() entry point gates on the Reports tab being visible.
    import_export_page.page_tabs.setCurrentIndex(1)  # Reports tab
    import_export_page.refresh()
    text = import_export_page._report_stats_label.text()
    assert "Monthly:" in text
    assert "Quarterly:" in text
    assert "Semi-Annual:" in text
    assert "Annual:" in text


def test_report_detail_renders_snapshots_and_fees_tabs(qapp, conn):
    """Generate a monthly report with a snapshot + a fee-breakdown row,
    then ensure both new tabs populate when the row is selected."""
    from datetime import date
    from src.engines.ledger import deposit_cash, buy
    from src.engines.snapshots import record_daily_portfolio_snapshot
    from src.engines.reports import generate_monthly_report
    from src.gui.pages.import_export import ImportExportPage
    from src.storage.asset_repo import create_asset
    from src.models.asset import Asset
    from src.storage.fee_breakdown_repo import (
        create_fee_breakdown, FeeBreakdownRow,
    )

    a = create_asset(conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    deposit_cash(conn, "2026-03-01", 10000.0)
    record_daily_portfolio_snapshot(conn, snapshot_date=date(2026, 3, 1))
    txn = buy(conn, "2026-03-15", a.id, 5, 100.0, fees=2.5)
    create_fee_breakdown(conn, FeeBreakdownRow(
        transaction_id=txn.id, fee_type="broker_commission", amount=2.5,
    ))
    record_daily_portfolio_snapshot(conn, snapshot_date=date(2026, 4, 1))
    generate_monthly_report(conn, 2026, 3)

    page = ImportExportPage(conn)
    page.page_tabs.setCurrentIndex(1)
    page.refresh()
    page.report_list_table.setCurrentCell(0, 0)

    # Snapshots tab populated.
    assert page.report_snapshots_table.rowCount() >= 10
    assert "Beginning" in page.report_snapshots_table.item(0, 0).text()

    # Fees Breakdown tab populated.
    assert page.report_fees_table.rowCount() == 1
    assert page.report_fees_table.item(0, 0).text() == "broker_commission"

    page.deleteLater()


def test_report_list_marks_stale_after_backdated_txn(qapp, conn):
    from src.engines.ledger import deposit_cash
    from src.engines.reports import generate_monthly_report
    from src.gui.pages.import_export import ImportExportPage

    deposit_cash(conn, "2026-03-05", 100.0)
    generate_monthly_report(conn, 2026, 3)

    page = ImportExportPage(conn)
    page.page_tabs.setCurrentIndex(1)
    page.refresh()
    period_item = page.report_list_table.item(0, 0)
    assert period_item is not None
    assert not period_item.font().italic()  # fresh

    deposit_cash(conn, "2026-03-20", 50.0)  # backdated within March
    page.refresh()
    period_item = page.report_list_table.item(0, 0)
    assert period_item.font().italic()
    assert "Stale" in period_item.toolTip()

    page.deleteLater()


def test_show_how_to_read_opens_scrollable_dialog(qapp, conn, monkeypatch):
    """The help is shown in a sized, scrollable QDialog so long text never
    pushes the close affordance off the bottom of the screen."""
    from PySide6.QtWidgets import QDialog, QPlainTextEdit
    from src.gui.pages.import_export import ImportExportPage

    captured = {}

    def fake_exec(self):
        # Capture the dialog's contents instead of actually showing it
        # so the test stays headless.
        captured["title"] = self.windowTitle()
        edits = self.findChildren(QPlainTextEdit)
        assert edits, "expected a QPlainTextEdit holding the help text"
        captured["text"] = edits[0].toPlainText()
        captured["readonly"] = edits[0].isReadOnly()
        captured["size"] = (self.width(), self.height())
        return QDialog.DialogCode.Accepted

    monkeypatch.setattr(QDialog, "exec", fake_exec)

    page = ImportExportPage(conn)
    page._show_how_to_read()

    assert captured["title"] == "How to read this report"
    assert "HOW TO READ THIS REPORT" in captured["text"]
    assert captured["readonly"] is True
    # Reasonable, bounded default — must not be auto-grown to fit content.
    w, h = captured["size"]
    assert 400 <= w <= 1200
    assert 300 <= h <= 1000

    page.deleteLater()


def test_render_zero_price_explicitly(qapp, conn):
    """A buy with price 0 (synthetic) should render as $0.00, not blank."""
    from src.gui.pages.import_export import ImportExportPage

    txn_data = {
        "summary": {
            "report_type": "monthly", "period_label": "2026-03",
            "period_start": "2026-03-01", "period_end": "2026-04-01",
            "generated_at": "2026-04-01T00:00:00", "transaction_count": 1,
            "beginning_cash": 0, "ending_cash": 0, "net_cash_flow": 0,
            "operating_net_income": 0, "total_inflow": 0, "total_outflow": 0,
            "total_fees": 0,
        },
        "operations": [], "transactions": [{
            "date": "2026-03-15", "txn_type": "buy", "asset_symbol": "X",
            "asset_name": "X", "quantity": 1, "price": 0.0,
            "total_amount": 0.0, "fees": 0.0, "notes": "",
        }],
        "trades": [{
            "date": "2026-03-15", "txn_type": "buy", "asset_symbol": "X",
            "asset_name": "X", "quantity": 1, "price": 0.0,
            "total_amount": 0.0, "fees": 0.0, "notes": "",
        }],
        "real_estate": [], "debt": [], "journal": [],
        "current_snapshot": {}, "beginning_snapshot": {}, "ending_snapshot": {},
        "cash_flow_breakdown": {}, "performance": {}, "allocation": {},
        "risk_summary": {},
        "fees_breakdown": {"by_type": [], "grand_total": 0.0},
    }
    page = ImportExportPage(conn)
    page._render_report_detail(txn_data)
    assert page.report_txns_table.item(0, 4).text() == "$0.00"
    assert page.report_trades_table.item(0, 4).text() == "$0.00"

    page.deleteLater()


def _stub_picker(monkeypatch, year: int, sub: int):
    """Patch PeriodPickerDialog to return Accepted with (year, sub) without
    showing UI."""
    from PySide6.QtWidgets import QDialog
    from src.gui import pages as _pages  # noqa: F401  (ensures package imported)
    import src.gui.widgets.period_picker as picker_mod

    class _StubDialog:
        def __init__(self, parent, cadence):
            self.cadence = cadence
        def exec(self):
            return QDialog.DialogCode.Accepted
        def values(self):
            return (year, sub)

    monkeypatch.setattr(picker_mod, "PeriodPickerDialog", _StubDialog)


def test_generate_selected_period_quarterly_via_dispatcher(qapp, conn, monkeypatch):
    from src.engines.ledger import deposit_cash
    from src.gui.pages.import_export import ImportExportPage
    from PySide6.QtWidgets import QMessageBox

    deposit_cash(conn, "2026-02-15", 100.0)

    page = ImportExportPage(conn)
    idx = page.report_type_combo.findData("quarterly")
    page.report_type_combo.setCurrentIndex(idx)

    _stub_picker(monkeypatch, 2026, 1)
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **kw: None)

    page._generate_selected_period()

    from src.storage.report_repo import report_exists
    assert report_exists(conn, "quarterly", "2026-Q1")

    page.deleteLater()


def test_generate_selected_period_overwrite_confirm_no_aborts(qapp, conn, monkeypatch):
    from src.engines.ledger import deposit_cash
    from src.engines.reports import generate_monthly_report
    from src.gui.pages.import_export import ImportExportPage
    from src.storage.report_repo import get_report
    from PySide6.QtWidgets import QMessageBox

    deposit_cash(conn, "2026-03-15", 100.0)
    initial = generate_monthly_report(conn, 2026, 3)
    initial_generated_at = initial.generated_at

    page = ImportExportPage(conn)
    page.report_type_combo.setCurrentIndex(page.report_type_combo.findData("monthly"))

    _stub_picker(monkeypatch, 2026, 3)
    # Confirm dialog → No
    monkeypatch.setattr(
        QMessageBox, "question",
        lambda *a, **kw: QMessageBox.StandardButton.No,
    )
    monkeypatch.setattr(QMessageBox, "information", lambda *a, **kw: None)

    page._generate_selected_period()

    # Report was NOT regenerated.
    after = get_report(conn, "monthly", "2026-03")
    assert after.generated_at == initial_generated_at

    page.deleteLater()
