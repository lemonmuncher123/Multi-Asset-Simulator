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
