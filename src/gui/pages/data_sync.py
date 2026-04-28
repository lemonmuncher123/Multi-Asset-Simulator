import sqlite3
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QMessageBox, QTableWidgetItem, QScrollArea, QGroupBox, QComboBox,
)
from PySide6.QtGui import QColor
from PySide6.QtCore import QProcess, Signal
from src.gui.widgets.common import make_header, make_table, fmt_money, configure_expanding_table, resize_table_to_contents
from src.utils.display import format_asset_type, format_price_source, format_sync_status
from src.utils.deps import is_yfinance_available, get_install_args
from src.storage.asset_repo import list_assets
from src.storage.quote_repo import list_latest_market_data
from src.storage.sync_log_repo import list_sync_logs, get_latest_sync_log
from src.data_sources.price_provider import ProviderUnavailableError
from src.engines.pricing_engine import sync_asset_market_data, SYNCABLE_TYPES
from src.engines.price_sync_worker import PriceSyncController


class DataSyncPage(QWidget):
    data_changed = Signal()

    def __init__(self, conn: sqlite3.Connection, sync_controller: PriceSyncController | None = None, parent=None):
        super().__init__(parent)
        self.conn = conn
        self._install_process = None
        self._sync_controller = sync_controller or PriceSyncController()

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(12)

        layout.addWidget(make_header("Data Sync"))
        layout.addWidget(QLabel(
            "Sync market data and execution quotes for stocks, ETFs, and crypto. "
            "All data is fetched and stored locally."
        ))

        # --- Dependency status ---
        dep_group = QGroupBox("Dependencies")
        dep_layout = QHBoxLayout(dep_group)

        self.dep_label = QLabel()
        self.dep_label.setStyleSheet("font-size: 14px; padding: 4px;")
        dep_layout.addWidget(self.dep_label)

        self.btn_install = QPushButton("Install Dependencies")
        self.btn_install.setStyleSheet("padding: 8px 20px; font-size: 14px;")
        self.btn_install.clicked.connect(self._install_deps)
        dep_layout.addWidget(self.btn_install)

        self.install_status_label = QLabel()
        self.install_status_label.setStyleSheet("font-size: 13px; padding: 4px;")
        dep_layout.addWidget(self.install_status_label)

        dep_layout.addStretch()
        layout.addWidget(dep_group)

        # --- Sync status ---
        self.status_label = QLabel("No sync performed yet.")
        self.status_label.setStyleSheet("font-size: 14px; padding: 4px;")
        layout.addWidget(self.status_label)

        # --- Sync buttons ---
        btn_group = QGroupBox("Sync Controls")
        btn_layout = QHBoxLayout(btn_group)

        self.btn_sync_all = QPushButton("Sync All Market Data")
        self.btn_sync_all.setStyleSheet("padding: 8px 20px; font-size: 14px;")
        self.btn_sync_all.clicked.connect(self._sync_all)
        btn_layout.addWidget(self.btn_sync_all)

        self.asset_combo = QComboBox()
        self.asset_combo.setMinimumWidth(200)
        self.asset_combo.setStyleSheet("padding: 6px; font-size: 14px;")
        btn_layout.addWidget(self.asset_combo)

        self.btn_sync_one = QPushButton("Sync Selected Asset")
        self.btn_sync_one.setStyleSheet("padding: 8px 20px; font-size: 14px;")
        self.btn_sync_one.clicked.connect(self._sync_selected)
        btn_layout.addWidget(self.btn_sync_one)

        btn_refresh = QPushButton("Refresh Table")
        btn_refresh.setStyleSheet("padding: 8px 20px; font-size: 14px;")
        btn_refresh.clicked.connect(self._refresh_tables)
        btn_layout.addWidget(btn_refresh)

        btn_layout.addStretch()
        layout.addWidget(btn_group)

        # --- Market data table ---
        layout.addWidget(QLabel("Market Data & Execution Quotes"))
        self.market_table = make_table([
            "Symbol", "Name", "Type",
            "Sell Price (Bid)", "Buy Price (Ask)", "Last",
            "Quote Time", "Quote Source",
            "Valuation Price", "Valuation Date",
        ])
        configure_expanding_table(self.market_table)
        layout.addWidget(self.market_table)

        # --- Sync log table ---
        layout.addWidget(QLabel("Sync History"))
        self.log_table = make_table([
            "Time", "Status", "Source", "Attempted", "Succeeded", "Failed", "Errors",
        ])
        configure_expanding_table(self.log_table)
        layout.addWidget(self.log_table)

        layout.addStretch()
        scroll.setWidget(content)
        outer.addWidget(scroll)

    def refresh(self):
        self._load_asset_combo()
        self._refresh_tables()
        self._update_dep_status()
        self._update_sync_button_state()

    def _update_dep_status(self):
        available = is_yfinance_available()
        if available:
            self.dep_label.setText("yfinance: installed")
            self.dep_label.setStyleSheet(
                "font-size: 14px; padding: 4px; color: #2e7d32; font-weight: bold;"
            )
            self.btn_install.setVisible(False)
        else:
            self.dep_label.setText("yfinance: not installed")
            self.dep_label.setStyleSheet(
                "font-size: 14px; padding: 4px; color: #c62828; font-weight: bold;"
            )
            self.btn_install.setVisible(True)
        self._update_sync_button_state()

    def _update_sync_button_state(self):
        available = is_yfinance_available()
        syncing = self._sync_controller.is_running
        self.btn_sync_all.setEnabled(available and not syncing)
        self.btn_sync_one.setEnabled(available and not syncing)

    def _install_deps(self):
        if self._install_process is not None:
            return

        self.btn_install.setEnabled(False)
        self.install_status_label.setText("Installing dependencies...")
        self.install_status_label.setStyleSheet(
            "font-size: 13px; padding: 4px; color: #1565c0;"
        )

        args = get_install_args()
        self._install_process = QProcess(self)
        self._install_process.finished.connect(self._on_install_finished)
        self._install_process.setProgram(args[0])
        self._install_process.setArguments(args[1:])
        self._install_process.start()

    def _on_install_finished(self, exit_code, exit_status):
        if self._install_process is None:
            return
        proc = self._install_process
        self._install_process = None
        self.btn_install.setEnabled(True)

        if exit_code == 0:
            self.install_status_label.setText("Installation complete.")
            self.install_status_label.setStyleSheet(
                "font-size: 13px; padding: 4px; color: #2e7d32;"
            )
            QMessageBox.information(
                self, "Installation Complete",
                "Dependencies installed successfully. Sync buttons are now enabled."
            )
        else:
            self.install_status_label.setText(f"Installation failed (exit code {exit_code}).")
            self.install_status_label.setStyleSheet(
                "font-size: 13px; padding: 4px; color: #c62828;"
            )
            stderr = bytes(proc.readAllStandardError()).decode() if proc else ""
            QMessageBox.warning(
                self, "Installation Failed",
                f"pip exited with code {exit_code}.\n\n{stderr}" if stderr
                else f"pip exited with code {exit_code}."
            )

        self._update_dep_status()

    def _load_asset_combo(self):
        self.asset_combo.clear()
        assets = list_assets(self.conn)
        self._syncable_assets = [
            a for a in assets if a.asset_type in SYNCABLE_TYPES
        ]
        for a in self._syncable_assets:
            self.asset_combo.addItem(f"{a.symbol} ({format_asset_type(a.asset_type)})", a.id)

    def _refresh_tables(self):
        self._load_market_data_table()
        self._load_log_table()
        self._update_status()

    def _update_status(self):
        last = get_latest_sync_log(self.conn)
        if last is None:
            self.status_label.setText("No sync performed yet.")
            self.status_label.setStyleSheet("font-size: 14px; padding: 4px;")
            return
        parts = [
            f"Last sync: {last['finished_at'] or last['started_at']}",
            f"Status: {format_sync_status(last['status'])}",
            f"{last['assets_succeeded']}/{last['assets_attempted']} synced",
        ]
        if last["assets_failed"]:
            parts.append(f"{last['assets_failed']} failed")
        self.status_label.setText(" | ".join(parts))
        status = last["status"]
        if status == "success":
            color = "#2e7d32"
        elif status == "failed":
            color = "#c62828"
        else:
            color = "#e65100"
        self.status_label.setStyleSheet(
            f"font-size: 14px; padding: 4px; color: {color}; font-weight: bold;"
        )

    def _load_market_data_table(self):
        rows = list_latest_market_data(self.conn)
        self.market_table.setRowCount(len(rows))
        for i, r in enumerate(rows):
            self.market_table.setItem(i, 0, QTableWidgetItem(r["symbol"]))
            self.market_table.setItem(i, 1, QTableWidgetItem(r["name"]))
            self.market_table.setItem(i, 2, QTableWidgetItem(format_asset_type(r["asset_type"])))
            self.market_table.setItem(i, 3, QTableWidgetItem(
                fmt_money(r["bid"]) if r["bid"] is not None else "—"
            ))
            self.market_table.setItem(i, 4, QTableWidgetItem(
                fmt_money(r["ask"]) if r["ask"] is not None else "—"
            ))
            self.market_table.setItem(i, 5, QTableWidgetItem(
                fmt_money(r["last"]) if r["last"] is not None else "—"
            ))
            self.market_table.setItem(i, 6, QTableWidgetItem(r["quote_time"] or ""))
            self.market_table.setItem(i, 7, QTableWidgetItem(r["quote_source"] or ""))
            self.market_table.setItem(i, 8, QTableWidgetItem(
                fmt_money(r["valuation_price"]) if r["valuation_price"] is not None else "—"
            ))
            self.market_table.setItem(i, 9, QTableWidgetItem(r["valuation_date"] or ""))
        resize_table_to_contents(self.market_table)

    def _load_log_table(self):
        logs = list_sync_logs(self.conn)
        self.log_table.setRowCount(len(logs))
        for i, log in enumerate(logs):
            self.log_table.setItem(i, 0, QTableWidgetItem(
                log["finished_at"] or log["started_at"]
            ))
            status_item = QTableWidgetItem(format_sync_status(log["status"]))
            if log["status"] == "success":
                status_item.setForeground(QColor("#2e7d32"))
            elif log["status"] == "failed":
                status_item.setForeground(QColor("#c62828"))
            else:
                status_item.setForeground(QColor("#e65100"))
            self.log_table.setItem(i, 1, status_item)
            self.log_table.setItem(i, 2, QTableWidgetItem(log["source"] or ""))
            self.log_table.setItem(i, 3, QTableWidgetItem(str(log["assets_attempted"])))
            self.log_table.setItem(i, 4, QTableWidgetItem(str(log["assets_succeeded"])))
            self.log_table.setItem(i, 5, QTableWidgetItem(str(log["assets_failed"])))
            self.log_table.setItem(i, 6, QTableWidgetItem(log["error_message"] or ""))
        resize_table_to_contents(self.log_table)

    def _sync_all(self):
        if self._sync_controller.is_running:
            return

        self.btn_sync_all.setEnabled(False)
        self.btn_sync_one.setEnabled(False)
        self.status_label.setText("Syncing market data...")
        self.status_label.setStyleSheet(
            "font-size: 14px; padding: 4px; color: #1565c0; font-weight: bold;"
        )

        from src.storage.database import DEFAULT_DB_PATH
        self._sync_controller.start_sync(
            db_path=str(DEFAULT_DB_PATH),
            on_finished=self._on_sync_all_finished,
        )

    def _on_sync_all_finished(self, result: dict):
        self._update_sync_button_state()
        self._refresh_tables()

        status = result["status"]
        parts = [f"{result['succeeded']}/{result['attempted']} synced"]
        if result["failed"]:
            parts.append(f"{result['failed']} failed")
        msg = " | ".join(parts)
        if result["errors"]:
            msg += "\n\nErrors:\n" + "\n".join(result["errors"])

        if status == "success":
            QMessageBox.information(self, "Sync Complete", msg)
        elif status == "partial":
            QMessageBox.warning(self, "Sync Partially Complete", msg)
        elif len(result["errors"]) == 1 and "pip install" in result["errors"][0]:
            QMessageBox.warning(self, "Missing Dependency", result["errors"][0])
        else:
            QMessageBox.warning(self, "Sync Failed", msg)

        self.data_changed.emit()

    def _sync_selected(self):
        idx = self.asset_combo.currentIndex()
        if idx < 0 or idx >= len(self._syncable_assets):
            QMessageBox.warning(self, "No Asset", "Select an asset to sync.")
            return

        asset = self._syncable_assets[idx]
        try:
            result = sync_asset_market_data(self.conn, asset)
            parts = [f"Synced market data for {asset.symbol}."]
            if result["quote_synced"]:
                parts.append("Executable quote available.")
            else:
                parts.append("No executable quote (missing bid/ask).")
            if not result["price_synced"]:
                parts.append("Daily prices failed to sync.")
            QMessageBox.information(self, "Sync Complete", " ".join(parts))
        except ProviderUnavailableError as e:
            QMessageBox.warning(self, "Missing Dependency", str(e))
        except Exception as e:
            QMessageBox.warning(
                self, "Sync Error",
                f"Failed to sync {asset.symbol}: {e}"
            )

        self._refresh_tables()
        self.data_changed.emit()
