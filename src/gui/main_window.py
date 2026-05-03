import logging
import sqlite3
from PySide6.QtWidgets import (
    QMainWindow,
    QHBoxLayout,
    QVBoxLayout,
    QWidget,
    QListWidget,
    QStackedWidget,
    QListWidgetItem,
    QLabel,
)
from PySide6.QtCore import QSize, QTimer

from src.gui.pages.dashboard import DashboardPage
from src.gui.pages.transactions import TransactionsPage
from src.gui.pages.asset_analysis import AssetAnalysisPage
from src.gui.pages.risk import RiskPage
from src.gui.pages.real_estate import RealEstatePage
from src.gui.pages.journal import JournalPage
from src.gui.pages.import_export import ImportExportPage
from src.gui.pages.data_sync import DataSyncPage
from src.gui.pages.settings import SettingsPage
from src.engines.price_sync_worker import PriceSyncController
from src.engines.snapshots import record_daily_portfolio_snapshot

_log = logging.getLogger(__name__)


PAGE_LABELS = [
    "Dashboard",
    "Transactions",
    "Asset Analysis",
    "Risk",
    "Real Estate",
    "Decision Journal",
    "Import / Export",
    "Data Sync",
    "Settings",
]


class MainWindow(QMainWindow):
    # 30 minutes between day-boundary checks. The check itself is
    # cheap (string compare against `_last_auto_settle_date`), and the
    # actual auto-settle pipeline is idempotent within the same day, so
    # the overhead of a fast wakeup is just the equality check. Any
    # interval ≤ 1 hour catches a midnight rollover before users tend
    # to start their first action of the day.
    AUTO_SETTLE_TIMER_INTERVAL_MS = 30 * 60 * 1000

    def __init__(
        self,
        conn: sqlite3.Connection,
        enable_startup_sync: bool = True,
        enable_auto_settle_timer: bool = True,
    ):
        super().__init__()
        self.conn = conn
        self._closing = False
        self._in_auto_settle = False
        # Tracks the calendar date of the last *successful* auto-settle
        # run. Used by `ensure_auto_settle_current` (preflight before
        # user actions) and `_check_auto_settle_date_rollover` (timer)
        # to skip redundant work within the same day. None at startup so
        # the first preflight or timer tick will run a settle.
        self._last_auto_settle_date: str | None = None
        self._sync_controller = PriceSyncController()
        self.setWindowTitle("Multi-Asset Portfolio Trainer")
        self.setMinimumSize(1100, 700)

        central = QWidget()
        self.setCentralWidget(central)
        layout = QHBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # --- Sidebar ---
        sidebar = QWidget()
        sidebar.setFixedWidth(200)
        sidebar.setStyleSheet("""
            QWidget { background-color: #2b2b2b; }
            QLabel { color: #ffffff; font-size: 16px; font-weight: bold; padding: 16px 12px 8px 12px; }
            QListWidget {
                background-color: #2b2b2b;
                color: #cccccc;
                border: none;
                font-size: 14px;
                outline: none;
            }
            QListWidget::item {
                padding: 10px 16px;
            }
            QListWidget::item:selected {
                background-color: #3c3c3c;
                color: #ffffff;
            }
            QListWidget::item:hover {
                background-color: #353535;
            }
        """)
        sidebar_layout = QVBoxLayout(sidebar)
        sidebar_layout.setContentsMargins(0, 0, 0, 0)
        sidebar_layout.setSpacing(0)

        title = QLabel("Portfolio Trainer")
        sidebar_layout.addWidget(title)

        self.nav_list = QListWidget()
        for label in PAGE_LABELS:
            item = QListWidgetItem(label)
            item.setSizeHint(QSize(200, 40))
            self.nav_list.addItem(item)
        sidebar_layout.addWidget(self.nav_list)

        layout.addWidget(sidebar)

        # --- Pages ---
        self.page_widgets = [
            DashboardPage(conn),
            TransactionsPage(conn),
            AssetAnalysisPage(conn),
            RiskPage(conn),
            RealEstatePage(conn),
            JournalPage(conn),
            ImportExportPage(conn),
            DataSyncPage(conn, sync_controller=self._sync_controller),
            SettingsPage(conn),
        ]

        self.pages = QStackedWidget()
        for page in self.page_widgets:
            self.pages.addWidget(page)

        # Right panel: top-level bankruptcy banner stacked above the
        # pages. The banner self-hides when the portfolio is solvent, so
        # it consumes no vertical space outside of bankruptcy. Spec §8 /
        # §12 require a clear bankruptcy warning on every page; this is
        # the single source of that warning across the whole app (the
        # Dashboard also keeps an in-page banner because Dashboard tests
        # depend on it — they show the same content).
        from PySide6.QtWidgets import QWidget as _QW
        from src.gui.widgets.bankruptcy_banner import BankruptcyBanner
        right_panel = _QW()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(0)
        self.bankruptcy_banner = BankruptcyBanner(conn)
        right_layout.addWidget(self.bankruptcy_banner)
        right_layout.addWidget(self.pages)
        layout.addWidget(right_panel)

        self._page_index = {
            label: i for i, label in enumerate(PAGE_LABELS)
        }

        def _connect(label):
            page = self.page_widgets[self._page_index[label]]
            page.data_changed.connect(self._handle_data_changed)

        _connect("Transactions")
        _connect("Real Estate")
        _connect("Decision Journal")
        _connect("Import / Export")
        _connect("Data Sync")
        settings_page = self.page_widgets[self._page_index["Settings"]]
        settings_page.data_panel.data_changed.connect(self._handle_data_changed)

        self.nav_list.currentRowChanged.connect(self._on_page_changed)
        self.nav_list.setCurrentRow(0)

        if enable_startup_sync:
            QTimer.singleShot(0, self._startup_sync)
        # Auto-settle runs after price sync (so force-sell can use fresh
        # quotes) but before reports (so the day's books are clean).
        QTimer.singleShot(250, self._startup_auto_settle)
        QTimer.singleShot(500, self._startup_reports)
        QTimer.singleShot(1000, self._record_today_snapshot)

        # Day-boundary rollover timer: catches the case where the app
        # stays open across midnight. Without this, scheduled debt /
        # mortgage / rent settlements could be silently bypassed by
        # leaving the window open. Tests can pass
        # `enable_auto_settle_timer=False` to suppress; production
        # always wants it on.
        self._date_rollover_timer: QTimer | None = None
        if enable_auto_settle_timer:
            self._date_rollover_timer = QTimer(self)
            self._date_rollover_timer.setInterval(
                self.AUTO_SETTLE_TIMER_INTERVAL_MS,
            )
            self._date_rollover_timer.timeout.connect(
                self._check_auto_settle_date_rollover,
            )
            self._date_rollover_timer.start()

    def closeEvent(self, event):
        self._closing = True
        # Stop the day-boundary timer so it doesn't fire after the
        # connection is closed (would surface as a confusing error).
        if self._date_rollover_timer is not None:
            self._date_rollover_timer.stop()
            try:
                self._date_rollover_timer.timeout.disconnect(
                    self._check_auto_settle_date_rollover,
                )
            except (TypeError, RuntimeError):
                pass
        try:
            self.nav_list.currentRowChanged.disconnect(self._on_page_changed)
        except (TypeError, RuntimeError):
            pass
        self._sync_controller.shutdown()
        for page in self.page_widgets:
            sig = getattr(page, "data_changed", None)
            if sig is not None:
                try:
                    sig.disconnect(self._handle_data_changed)
                except (TypeError, RuntimeError):
                    pass
            if hasattr(page, '_cleanup_figures'):
                page._cleanup_figures()
        # Settings hosts data_panel separately; disconnect that too.
        settings_page = self.page_widgets[self._page_index["Settings"]]
        try:
            settings_page.data_panel.data_changed.disconnect(self._handle_data_changed)
        except (TypeError, RuntimeError, AttributeError):
            pass
        super().closeEvent(event)

    def _startup_sync(self):
        from src.utils.deps import is_yfinance_available
        if not is_yfinance_available():
            return

        # Skip if there are no syncable assets — sync would just write a
        # no-op row to price_sync_log and emit a startup notification for
        # nothing.
        from src.engines.pricing_engine import SYNCABLE_TYPES
        from src.storage.asset_repo import list_assets
        if not any(a.asset_type in SYNCABLE_TYPES for a in list_assets(self.conn)):
            return

        from src.storage.database import DEFAULT_DB_PATH
        self._sync_controller.start_sync(
            db_path=str(DEFAULT_DB_PATH),
            on_finished=self._on_startup_sync_finished,
        )

    def _on_startup_sync_finished(self, result: dict):
        if self._closing:
            return
        self._record_today_snapshot()
        self._refresh_current()

    def _startup_reports(self):
        # Stays on the GUI thread: background workers can't share the
        # connection with the UI, and a separate-connection worker writing
        # to DEFAULT_DB_PATH conflicts with tests that operate on `:memory:`
        # connections. Reports are run synchronously and only execute the
        # work that's actually due (see generate_due_reports).
        if self._closing:
            return
        from datetime import date
        from src.engines.reports import generate_due_reports
        try:
            generated = generate_due_reports(self.conn, today=date.today())
            if generated:
                _log.info("Startup auto-generated %d missing report(s)", len(generated))
                self._refresh_current()
        except Exception:
            _log.exception("Startup report generation failed")

    def _record_today_snapshot(self):
        # Stays synchronous on the GUI thread: a single snapshot row is
        # cheap to write, and keeping it sync lets `_handle_data_changed`
        # refresh the dashboard after the snapshot lands. Reports, which
        # can be slow, are deferred to the background worker instead.
        if self._closing:
            return
        try:
            record_daily_portfolio_snapshot(self.conn)
        except Exception:
            _log.exception("Daily portfolio snapshot failed")

    def _startup_auto_settle(self):
        if self._closing:
            return
        try:
            self._run_auto_settle()
            # Banner refresh runs before the page refresh so navigation
            # in the same tick sees the correct state.
            self.bankruptcy_banner.refresh()
            self._refresh_current()
        except Exception:
            _log.exception("Startup auto-settle failed")

    def _current_date(self) -> str:
        """The simulator's "today" for auto-settle purposes — ISO date.

        Single override point so tests can simulate a midnight crossing
        without sleeping; production always returns the system date.
        """
        from datetime import date
        return date.today().isoformat()

    def ensure_auto_settle_current(self) -> None:
        """Preflight: run auto-settle if today hasn't been processed yet.

        Called from every user-action GUI submit path before the
        bankruptcy guard. Idempotent within a day: the second call on
        the same date is an O(1) string compare against
        ``_last_auto_settle_date`` and returns immediately. Cross-day
        (the user left the window open since yesterday) triggers a
        fresh auto-settle run, which can declare bankruptcy and let the
        existing guards abort the user's action.

        Re-entrancy: if auto-settle is already running on the stack
        (e.g., a `data_changed` signal racing with a submit), this is
        a no-op so we never recurse.
        """
        if self._closing or self._in_auto_settle:
            return
        today = self._current_date()
        if self._last_auto_settle_date == today:
            return
        try:
            self._run_auto_settle(today=today)
        except Exception:
            _log.exception("Preflight auto-settle failed")
        # The bankruptcy banner depends on the post-auto-settle state;
        # refresh so the user-action that follows reads the truth.
        self.bankruptcy_banner.refresh()

    def _check_auto_settle_date_rollover(self) -> None:
        """QTimer callback: every ``AUTO_SETTLE_TIMER_INTERVAL_MS``,
        check whether the calendar date has changed since the last
        successful auto-settle and run one if so.

        Same idempotency as ``ensure_auto_settle_current`` — but driven
        by elapsed time instead of a user action. Combined the two
        catch the "app stays open across midnight" case from both
        directions: timer fires within ~30 min of midnight; the next
        user submit triggers a preflight even sooner.
        """
        if self._closing or self._in_auto_settle:
            return
        today = self._current_date()
        if self._last_auto_settle_date == today:
            return
        try:
            self._run_auto_settle(today=today)
        except Exception:
            _log.exception("Date-rollover auto-settle failed")
        self.bankruptcy_banner.refresh()
        self._refresh_current()

    def _run_auto_settle(self, today: str | None = None):
        """Run the rent-credit / debt-deduct / mortgage-deduct pipeline.

        Order is load-bearing:
          1. Settle rent (income lands first so it can fund obligations).
          2. Settle scheduled debt + mortgage payments. Items that lack
             cash are returned in ``deferred`` lists and not yet missed.
          3. If anything was deferred, force-sell to raise the total
             shortfall so the retry has cash to spend.
          4. Retry the deferred items.
          5. Anything still deferred after retry is recorded as a
             **bankruptcy_event** — terminal state, not a recoverable
             "overdue payment". The dashboard banner reads from that.
          6. Final force-sell pass to clear any residual negative cash.

        Re-entrancy guarded: each generated transaction fires
        ``data_changed``, which calls back here. Without the flag this
        would loop forever.
        """
        if self._in_auto_settle:
            return
        self._in_auto_settle = True
        try:
            from src.engines import ledger
            from src.engines.portfolio import calc_cash_balance
            from src.storage.bankruptcy_event_repo import record_bankruptcy_event
            # Caller-supplied `today` lets the day-rollover timer and
            # tests pin a specific date. Default falls back to
            # `_current_date()` (system date in production).
            if today is None:
                today = self._current_date()

            ledger.settle_due_rent(self.conn, today)
            # Property opex (tax/insurance/HOA/maintenance/management)
            # accrues monthly and posts as `pay_property_expense`. Run
            # before debt settlement so the analysis pages' projected
            # net cash flow matches the cash ledger (Finding 4 fix).
            ledger.settle_due_property_expenses(self.conn, today)

            _, debts_deferred = ledger.settle_due_debt_payments(self.conn, today)
            _, mort_deferred = ledger.settle_due_mortgage_payments(self.conn, today)

            all_deferred = list(debts_deferred) + list(mort_deferred)
            if all_deferred:
                # Raise enough cash to cover the *additional* shortfall on
                # top of whatever's already in cash. Without the +cash term,
                # a positive starting balance would be ignored and we'd
                # over-sell; without the deferred sum, target=0 only fixes
                # already-negative balances and won't pre-empt a miss.
                shortfall = sum(item["amount"] for item in all_deferred)
                target = calc_cash_balance(self.conn) + shortfall
                # Build a debt-name label that names the specific
                # obligation(s) this force-sell is funding so the
                # generated `sell` transactions read clearly in the
                # history (spec §4 #4). When several deferred items
                # share one force-sell, they all flow through the same
                # `debt_name`, producing a concise combined label.
                obligation_label = self._combined_obligation_label(all_deferred)
                ledger.force_sell_to_raise_cash(
                    self.conn, today, target_cash=target,
                    reason="auto debt/mortgage deduction",
                    debt_name=obligation_label,
                    # Audit metadata: the actual obligation amount, not
                    # the cash-level threshold. Spec §11 / §4 #5.
                    required_payment=shortfall,
                )

            still_deferred: list[dict] = []
            if debts_deferred:
                _, dd_left = ledger.retry_deferred(self.conn, debts_deferred)
                still_deferred.extend(dd_left)
            if mort_deferred:
                _, md_left = ledger.retry_deferred(self.conn, mort_deferred)
                still_deferred.extend(md_left)

            if still_deferred:
                # An obligation remains unfunded after force-selling all
                # sellable assets. Per simulator rule, this is bankruptcy —
                # not a recoverable "overdue" state.
                cash_now = calc_cash_balance(self.conn)
                for item in still_deferred:
                    kind = item.get("kind") or "debt"
                    record_bankruptcy_event(
                        self.conn,
                        event_date=today,
                        trigger_kind=kind,
                        asset_id=item.get("asset_id"),
                        due_date=item.get("date"),
                        amount_due=item.get("amount", 0.0),
                        cash_balance=cash_now,
                        shortfall_amount=item.get("amount", 0.0),
                        notes=item.get("label"),
                    )

            # Final cleanup: a manual_adjustment or other path may have
            # driven cash negative outside of auto-settle's scheduled set;
            # this last pass catches that without changing target semantics.
            ledger.force_sell_to_cover_negative_cash(self.conn, today)
            # If the mop-up could not restore cash to >= 0 because no
            # sellable holdings remain, that is also bankruptcy
            # (`risk.check_bankruptcy` path 2: cash<0 + no sellables).
            # Persist a `negative_cash` bankruptcy_event so the audit
            # trail captures this case — `record_bankruptcy_event`
            # dedupes on (trigger_kind, status='active') for this kind,
            # so repeated runs don't stack.
            cash_after_mopup = calc_cash_balance(self.conn)
            if cash_after_mopup < 0 and not self._has_sellable_holdings():
                record_bankruptcy_event(
                    self.conn,
                    event_date=today,
                    trigger_kind="negative_cash",
                    cash_balance=cash_after_mopup,
                    shortfall_amount=-cash_after_mopup,
                    notes=(
                        "Cash balance went negative with no sellable "
                        "assets remaining to liquidate."
                    ),
                )
            # Record success: the preflight + rollover paths short-
            # circuit on the next call within the same date. Set only
            # after the pipeline completes without raising — a partial
            # run leaves the cache stale so the next call retries.
            self._last_auto_settle_date = today
        finally:
            self._in_auto_settle = False

    def _has_sellable_holdings(self) -> bool:
        """True iff any asset of a sellable type has net qty > 0.

        Mirrors the SQL in `risk.check_bankruptcy` path 2 — same
        predicate, same epsilon. Kept here as a private helper so the
        bankruptcy_event recording in `_run_auto_settle` agrees with the
        risk check by construction.
        """
        row = self.conn.execute(
            """
            SELECT 1
            FROM assets a
            WHERE a.asset_type IN ('stock', 'etf', 'crypto', 'custom')
              AND COALESCE((
                  SELECT SUM(CASE
                      WHEN txn_type = 'buy' THEN quantity
                      WHEN txn_type = 'sell' THEN -quantity
                      WHEN txn_type = 'manual_adjustment'
                           AND quantity IS NOT NULL THEN quantity
                      ELSE 0
                  END)
                  FROM transactions WHERE asset_id = a.id
              ), 0) > 1e-9
            LIMIT 1
            """
        ).fetchone()
        return row is not None

    def _handle_data_changed(self):
        if self._closing:
            return
        # Auto-settle is idempotent within a calendar day, so a `data_changed`
        # signal from a non-financial write (journal entry, asset metadata
        # add, settings tweak) doesn't actually need to re-run rent/debt/
        # mortgage settlement after the first run that day. Skip the
        # pipeline when we already settled today; the snapshot + banner +
        # page refresh below still run so the UI reflects the change.
        if self._last_auto_settle_date != self._current_date():
            try:
                self._run_auto_settle()
            except Exception:
                _log.exception("Auto-settle after data change failed")
        self._record_today_snapshot()
        # Refresh the top-level bankruptcy banner before the page
        # refreshes, so the banner reflects the post-auto-settle state.
        self.bankruptcy_banner.refresh()
        self._refresh_current()

    def _combined_obligation_label(self, deferred_items) -> str | None:
        """Human-readable label naming the deferred obligation(s) a
        force-sell is funding. Used as the ``debt_name`` for the
        generated ``sell`` transactions so the history reads e.g.
        ``"...sold to cover scheduled debt payment for debt 'Auto Loan'
        + mortgage on 'House': ..."`` (spec §4 #4).

        Returns ``None`` when no deferred items have a resolvable asset
        name — the caller falls back to the generic reason string.
        """
        if not deferred_items:
            return None
        from src.storage.asset_repo import get_asset
        parts = []
        seen = set()
        for item in deferred_items:
            asset_id = item.get("asset_id")
            if asset_id is None:
                continue
            kind = item.get("kind") or "debt"
            asset = get_asset(self.conn, asset_id)
            name = (asset.name if asset else None) or "unnamed"
            if kind == "mortgage":
                part = f"mortgage on '{name}'"
            else:
                part = f"debt '{name}'"
            # De-dup if the same (kind, asset_id) appears twice in
            # deferred (one per period across a long auto-settle
            # window).
            key = (kind, asset_id)
            if key in seen:
                continue
            seen.add(key)
            parts.append(part)
        if not parts:
            return None
        return " + ".join(parts)

    def guard_transactions_if_bankrupt(self, parent=None) -> bool:
        """If the portfolio is bankrupt, show a warning and return True.

        Called from every UI submit handler that creates a transaction.
        Caller aborts the submit when this returns ``True``. Spec §6 #25
        ("When bankrupt, all transactions are banned"). Defense-in-depth
        on top of the per-page banner — the user might miss a banner,
        but the warning dialog is unmissable.
        """
        from src.engines.risk import is_bankrupt
        from PySide6.QtWidgets import QMessageBox
        if not is_bankrupt(self.conn):
            return False
        QMessageBox.warning(
            parent,
            "Account is bankrupt",
            "All transactions are disabled because the simulator could "
            "not cover a required debt or mortgage payment from cash, "
            "and force-selling all available assets did not raise "
            "enough to make up the shortfall. See the Dashboard for the "
            "specific obligation that triggered bankruptcy.",
        )
        return True

    def _on_page_changed(self, index):
        if self._closing:
            return
        self.pages.setCurrentIndex(index)
        self._refresh_page(index)

    def _refresh_page(self, index):
        page = self.page_widgets[index]
        if hasattr(page, "refresh"):
            page.refresh()

    def _refresh_current(self):
        self._refresh_page(self.pages.currentIndex())
