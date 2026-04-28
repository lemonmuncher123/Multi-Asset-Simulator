import sqlite3
from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QMessageBox, QTableWidgetItem, QScrollArea, QGroupBox, QInputDialog,
)
from src.gui.widgets.common import make_header, make_table, configure_expanding_table, resize_table_to_contents
from src.utils.display import format_asset_type
from src.storage.asset_repo import list_assets
from src.engines.data_management import (
    get_asset_usage_summary,
    delete_asset_with_related_data,
    clear_all_assets,
    clear_all_user_data,
)


class DataManagementPanel(QWidget):
    data_changed = Signal()

    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        layout.addWidget(make_header("Data Management"))
        layout.addWidget(QLabel(
            "View, delete, and manage assets and their related data. "
            "Deletions are permanent and cannot be undone."
        ))

        # --- Asset table ---
        layout.addWidget(QLabel("Assets"))
        self.asset_table = make_table([
            "ID", "Symbol", "Name", "Type", "Transactions",
            "Prices", "Has Property", "Has Debt",
        ])
        configure_expanding_table(self.asset_table)
        layout.addWidget(self.asset_table)

        # --- Asset actions ---
        asset_btn_layout = QHBoxLayout()

        self.btn_delete_asset = QPushButton("Delete Selected Asset")
        self.btn_delete_asset.setStyleSheet(
            "padding: 8px 20px; font-size: 14px; color: #c62828;"
        )
        self.btn_delete_asset.clicked.connect(self._delete_selected_asset)
        asset_btn_layout.addWidget(self.btn_delete_asset)

        asset_btn_layout.addStretch()
        layout.addLayout(asset_btn_layout)

        # --- Bulk operations ---
        bulk_group = QGroupBox("Bulk Operations")
        bulk_group.setStyleSheet("QGroupBox { font-weight: bold; }")
        bulk_layout = QVBoxLayout(bulk_group)

        bulk_layout.addWidget(QLabel(
            "These operations are destructive and cannot be undone."
        ))

        row1 = QHBoxLayout()
        self.btn_clear_assets = QPushButton("Clear All Assets")
        self.btn_clear_assets.setStyleSheet(
            "padding: 8px 20px; font-size: 14px; color: #c62828;"
        )
        self.btn_clear_assets.clicked.connect(self._clear_all_assets)
        row1.addWidget(self.btn_clear_assets)

        row1.addWidget(QLabel(
            "Removes all assets and related data. "
            "Preserves cash-only transactions, settings, and securities master."
        ))
        row1.addStretch()
        bulk_layout.addLayout(row1)

        row2 = QHBoxLayout()
        self.btn_clear_all = QPushButton("Clear All Data")
        self.btn_clear_all.setStyleSheet(
            "padding: 8px 20px; font-size: 14px; color: #c62828; font-weight: bold;"
        )
        self.btn_clear_all.clicked.connect(self._clear_all_data)
        row2.addWidget(self.btn_clear_all)

        row2.addWidget(QLabel(
            "Removes ALL local application data, including the securities master. "
            "The securities universe will be rebuilt automatically when needed."
        ))
        row2.addStretch()
        bulk_layout.addLayout(row2)

        layout.addWidget(bulk_group)

    def refresh(self):
        self._load_asset_table()

    def _load_asset_table(self):
        assets = list_assets(self.conn)
        self.asset_table.setRowCount(len(assets))
        for i, asset in enumerate(assets):
            summary = get_asset_usage_summary(self.conn, asset.id)
            self.asset_table.setItem(i, 0, QTableWidgetItem(str(asset.id)))
            self.asset_table.setItem(i, 1, QTableWidgetItem(asset.symbol))
            self.asset_table.setItem(i, 2, QTableWidgetItem(asset.name))
            self.asset_table.setItem(i, 3, QTableWidgetItem(
                format_asset_type(asset.asset_type)
            ))
            self.asset_table.setItem(i, 4, QTableWidgetItem(
                str(summary["transactions"])
            ))
            self.asset_table.setItem(i, 5, QTableWidgetItem(
                str(summary["prices"])
            ))
            self.asset_table.setItem(i, 6, QTableWidgetItem(
                "Yes" if summary["has_property"] else ""
            ))
            self.asset_table.setItem(i, 7, QTableWidgetItem(
                "Yes" if summary["has_debt"] else ""
            ))
        resize_table_to_contents(self.asset_table)

    def _delete_selected_asset(self):
        row = self.asset_table.currentRow()
        if row < 0:
            QMessageBox.warning(self, "No Selection", "Select an asset to delete.")
            return

        asset_id = int(self.asset_table.item(row, 0).text())
        symbol = self.asset_table.item(row, 1).text()
        name = self.asset_table.item(row, 2).text()

        summary = get_asset_usage_summary(self.conn, asset_id)

        parts = [f"Delete {symbol} ({name}) and all related data?\n"]
        if summary["transactions"]:
            parts.append(f"  - {summary['transactions']} transaction(s)")
        if summary["prices"]:
            parts.append(f"  - {summary['prices']} price record(s)")
        if summary["has_property"]:
            parts.append("  - Property record")
        if summary["has_debt"]:
            parts.append("  - Debt record")
        if summary["journal_entries"]:
            parts.append(f"  - {summary['journal_entries']} journal entry/entries")
        parts.append("\nThis cannot be undone.")

        reply = QMessageBox.warning(
            self, "Confirm Delete",
            "\n".join(parts),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        try:
            deleted = delete_asset_with_related_data(self.conn, asset_id)
            QMessageBox.information(
                self, "Asset Deleted",
                f"Deleted {symbol} and {deleted['transactions']} transaction(s), "
                f"{deleted['prices']} price(s)."
            )
        except ValueError as e:
            QMessageBox.warning(self, "Cannot Delete", str(e))
            return
        except Exception as e:
            QMessageBox.warning(self, "Delete Error", str(e))
            return

        self._load_asset_table()
        self.data_changed.emit()

    def _confirm_typed(self, title: str, message: str, confirm_text: str) -> bool:
        text, ok = QInputDialog.getText(
            self, title,
            f"{message}\n\nType \"{confirm_text}\" to confirm:",
        )
        return ok and text.strip() == confirm_text

    def _clear_all_assets(self):
        if not self._confirm_typed(
            "Clear All Assets",
            "This will delete all assets and their related data "
            "(transactions, prices, properties, debts, snapshots).\n\n"
            "Cash-only transactions, settings, and the securities master catalog "
            "will be preserved.",
            "CLEAR ASSETS",
        ):
            return

        try:
            deleted = clear_all_assets(self.conn)
            parts = [f"Cleared {deleted['assets']} asset(s)."]
            if deleted["transactions"]:
                parts.append(f"{deleted['transactions']} transaction(s) removed.")
            if deleted["market_prices"]:
                parts.append(f"{deleted['market_prices']} price record(s) removed.")
            QMessageBox.information(self, "Assets Cleared", " ".join(parts))
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))
            return

        self._load_asset_table()
        self.data_changed.emit()

    def _clear_all_data(self):
        if not self._confirm_typed(
            "Clear All Data",
            "This will delete ALL local application data:\n"
            "  - All assets\n"
            "  - All transactions (including cash) and fee breakdowns\n"
            "  - All prices and sync history\n"
            "  - All properties, debts\n"
            "  - All journal entries\n"
            "  - All generated reports\n"
            "  - All snapshots and settings\n"
            "  - The securities master catalog\n\n"
            "Table structure is preserved. The securities universe will be "
            "rebuilt automatically when needed.",
            "DELETE EVERYTHING",
        ):
            return

        try:
            deleted = clear_all_user_data(self.conn)
            total = sum(deleted.values())
            QMessageBox.information(
                self, "All Data Cleared",
                f"Removed {total} record(s) across all tables."
            )
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))
            return

        self._load_asset_table()
        self.data_changed.emit()


class DataManagementPage(QWidget):
    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)

        wrapper = QWidget()
        wrapper_layout = QVBoxLayout(wrapper)
        wrapper_layout.setContentsMargins(24, 16, 24, 16)
        wrapper_layout.setSpacing(0)

        self._panel = DataManagementPanel(conn)
        wrapper_layout.addWidget(self._panel)
        wrapper_layout.addStretch()

        scroll.setWidget(wrapper)
        outer.addWidget(scroll)

    @property
    def data_changed(self):
        return self._panel.data_changed

    def refresh(self):
        self._panel.refresh()
