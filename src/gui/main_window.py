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
    def __init__(self, conn: sqlite3.Connection, enable_startup_sync: bool = True):
        super().__init__()
        self.conn = conn
        self._closing = False
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
        layout.addWidget(self.pages)

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
        QTimer.singleShot(500, self._startup_reports)
        QTimer.singleShot(1000, self._record_today_snapshot)

    def closeEvent(self, event):
        self._closing = True
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

    def _handle_data_changed(self):
        if self._closing:
            return
        self._record_today_snapshot()
        self._refresh_current()

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
