import json
import sqlite3
from pathlib import Path
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QPushButton, QLabel,
    QMessageBox, QTableWidgetItem, QGroupBox, QFileDialog,
    QInputDialog, QListWidget, QTabWidget, QComboBox, QHeaderView,
)
from PySide6.QtGui import QColor
from PySide6.QtCore import Qt, QSignalBlocker, Signal
from src.gui.widgets.common import make_header, make_table, configure_expanding_table, resize_table_to_contents
from src.engines.import_export import (
    export_assets_csv,
    export_transactions_csv,
    export_summary_csv,
    import_assets_csv,
    import_transactions_csv,
)
from src.engines.full_data_io import (
    export_full_data,
    import_full_data,
    inspect_full_export,
    read_csv_table,
)
from src.storage.report_repo import (
    list_reports, get_report, list_report_summaries, report_count,
    delete_reports_before_date, delete_report, delete_reports_by_type,
    delete_reports_in_period_range, delete_all_reports, get_report_stats,
)
from src.engines.reports import (
    generate_due_reports, generate_monthly_report, generate_annual_report,
    get_auto_report_start_date, count_due_reports,
)
from src.engines.report_export import export_report_txt, export_report_xlsx

BTN_STYLE = "padding: 8px 20px; font-size: 14px;"
MAX_READER_ROWS = 1000
DEFAULT_REPORT_LIST_LIMIT = 60
REPORTS_TAB_INDEX = 1


def _money_or_na(val) -> str:
    if val is None:
        return "N/A"
    return f"${val:,.2f}"


def _pct_or_na(val) -> str:
    if val is None:
        return "N/A"
    return f"{val:.2f}%"


def _alloc_pct_or_na(val) -> str:
    """Allocation percentages are stored as fractions (0.10 = 10%)."""
    if val is None:
        return "N/A"
    return f"{val * 100:.2f}%"


class ImportExportPage(QWidget):
    data_changed = Signal()

    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn
        self._reader_path = None
        self._reports_dirty = True
        self._report_summary_cache = []

        outer = QVBoxLayout(self)
        outer.setContentsMargins(24, 16, 24, 16)
        outer.setSpacing(12)

        outer.addWidget(make_header("Import / Export"))
        outer.addWidget(QLabel("All files are saved and loaded locally. No data is uploaded."))

        self.page_tabs = QTabWidget()
        self.page_tabs.currentChanged.connect(self._on_tab_changed)
        outer.addWidget(self.page_tabs)

        # ── Tab 1: Import / Export ──
        io_tab = QWidget()
        io_layout = QVBoxLayout(io_tab)
        io_layout.setContentsMargins(8, 12, 8, 8)
        io_layout.setSpacing(12)

        export_group = QGroupBox("Export")
        export_layout = QHBoxLayout(export_group)

        btn_export_assets = QPushButton("Export Assets")
        btn_export_assets.setStyleSheet(BTN_STYLE)
        btn_export_assets.clicked.connect(self._export_assets)
        export_layout.addWidget(btn_export_assets)

        btn_export_txns = QPushButton("Export Transactions")
        btn_export_txns.setStyleSheet(BTN_STYLE)
        btn_export_txns.clicked.connect(self._export_transactions)
        export_layout.addWidget(btn_export_txns)

        btn_export_summary = QPushButton("Export Summary")
        btn_export_summary.setStyleSheet(BTN_STYLE)
        btn_export_summary.clicked.connect(self._export_summary)
        export_layout.addWidget(btn_export_summary)

        export_layout.addStretch()
        io_layout.addWidget(export_group)

        import_group = QGroupBox("Import")
        import_layout = QHBoxLayout(import_group)

        btn_import_assets = QPushButton("Import Assets")
        btn_import_assets.setStyleSheet(BTN_STYLE)
        btn_import_assets.clicked.connect(self._import_assets)
        import_layout.addWidget(btn_import_assets)

        btn_import_txns = QPushButton("Import Transactions")
        btn_import_txns.setStyleSheet(BTN_STYLE)
        btn_import_txns.clicked.connect(self._import_transactions)
        import_layout.addWidget(btn_import_txns)

        import_layout.addStretch()
        io_layout.addWidget(import_group)

        io_layout.addWidget(QLabel("Import Results"))
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("font-size: 14px; padding: 4px;")
        io_layout.addWidget(self.status_label)

        self.result_table = make_table(["Type", "Detail"])
        configure_expanding_table(self.result_table)
        io_layout.addWidget(self.result_table)

        backup_group = QGroupBox("Full Backup")
        backup_layout = QHBoxLayout(backup_group)

        btn_full_export = QPushButton("Export Full Data")
        btn_full_export.setStyleSheet(BTN_STYLE)
        btn_full_export.clicked.connect(self._full_export)
        backup_layout.addWidget(btn_full_export)

        btn_full_import = QPushButton("Import Full Data")
        btn_full_import.setStyleSheet(BTN_STYLE)
        btn_full_import.clicked.connect(self._full_import)
        backup_layout.addWidget(btn_full_import)

        backup_layout.addStretch()
        io_layout.addWidget(backup_group)

        io_layout.addStretch()
        self.page_tabs.addTab(io_tab, "Import / Export")

        # ── Tab 2: Reports ──
        reports_tab = QWidget()
        reports_layout = QVBoxLayout(reports_tab)
        reports_layout.setContentsMargins(8, 12, 8, 8)
        reports_layout.setSpacing(12)

        type_row = QHBoxLayout()
        type_row.addWidget(QLabel("Type:"))
        self.report_type_combo = QComboBox()
        self.report_type_combo.addItem("Monthly", "monthly")
        self.report_type_combo.addItem("Annual", "annual")
        self.report_type_combo.currentIndexChanged.connect(self._on_report_type_changed)
        type_row.addWidget(self.report_type_combo)
        type_row.addStretch()
        reports_layout.addLayout(type_row)

        btn_grid = QGridLayout()
        btn_grid.setSpacing(8)

        btn_gen_missing = QPushButton("Generate Missing Reports")
        btn_gen_missing.setStyleSheet(BTN_STYLE)
        btn_gen_missing.clicked.connect(self._generate_missing_reports)
        btn_grid.addWidget(btn_gen_missing, 0, 0)

        btn_gen_selected = QPushButton("Generate Selected Period")
        btn_gen_selected.setStyleSheet(BTN_STYLE)
        btn_gen_selected.clicked.connect(self._generate_selected_period)
        btn_grid.addWidget(btn_gen_selected, 0, 1)

        btn_export_report = QPushButton("Export Selected Report")
        btn_export_report.setStyleSheet(BTN_STYLE)
        btn_export_report.clicked.connect(self._export_selected_report)
        btn_grid.addWidget(btn_export_report, 0, 2)

        btn_refresh_reports = QPushButton("Refresh Reports")
        btn_refresh_reports.setStyleSheet(BTN_STYLE)
        btn_refresh_reports.clicked.connect(self._refresh_report_list)
        btn_grid.addWidget(btn_refresh_reports, 1, 0)

        btn_delete_selected = QPushButton("Delete Selected Report")
        btn_delete_selected.setStyleSheet(BTN_STYLE + " color: #c62828;")
        btn_delete_selected.clicked.connect(self._delete_selected_report)
        btn_grid.addWidget(btn_delete_selected, 1, 1)

        btn_delete_type = QPushButton("Delete Current Type Reports")
        btn_delete_type.setStyleSheet(BTN_STYLE + " color: #c62828;")
        btn_delete_type.clicked.connect(self._delete_current_type_reports)
        btn_grid.addWidget(btn_delete_type, 1, 2)

        btn_delete_old = QPushButton("Delete Reports Before Auto Start")
        btn_delete_old.setStyleSheet(BTN_STYLE + " color: #c62828;")
        btn_delete_old.clicked.connect(self._delete_old_reports)
        btn_grid.addWidget(btn_delete_old, 2, 0)

        btn_delete_range = QPushButton("Delete Reports By Date Range")
        btn_delete_range.setStyleSheet(BTN_STYLE + " color: #c62828;")
        btn_delete_range.clicked.connect(self._delete_reports_by_date_range)
        btn_grid.addWidget(btn_delete_range, 2, 1)

        btn_delete_all = QPushButton("Delete All Reports")
        btn_delete_all.setStyleSheet(BTN_STYLE + " color: #c62828; font-weight: bold;")
        btn_delete_all.clicked.connect(self._delete_all_reports)
        btn_grid.addWidget(btn_delete_all, 2, 2)

        btn_grid.setColumnStretch(3, 1)
        reports_layout.addLayout(btn_grid)

        self._report_stats_label = QLabel("")
        self._report_stats_label.setStyleSheet("color: #666; font-size: 12px; padding: 2px 4px;")
        reports_layout.addWidget(self._report_stats_label)

        self._report_info_label = QLabel("")
        self._report_info_label.setStyleSheet("color: #999; font-size: 12px; padding: 2px 4px;")
        reports_layout.addWidget(self._report_info_label)

        self.report_list_table = make_table(
            [
                "Period", "Generated",
                "Net Cash Flow", "Operating Net Income",
                "Net Worth Change", "Funding Flow", "Approx Return %",
            ],
            stretch_last=True,
        )
        self.report_list_table.setMinimumHeight(150)
        self.report_list_table.setMaximumHeight(250)
        self.report_list_table.setSelectionBehavior(self.report_list_table.SelectionBehavior.SelectRows)
        self.report_list_table.currentCellChanged.connect(self._on_report_selected)
        reports_layout.addWidget(self.report_list_table)

        self.report_detail_tabs = QTabWidget()
        self.report_summary_table = make_table(["Metric", "Value"])
        self.report_perf_table = make_table(["Metric", "Value"])
        self.report_cfb_table = make_table(["Category", "Sub-item", "Amount"])
        self.report_ops_table = make_table(["Type", "Count", "Total Amount", "Total Fees"])
        self.report_txns_table = make_table(["Date", "Type", "Asset", "Qty", "Price", "Amount", "Fees", "Notes"])
        hdr_txns = self.report_txns_table.horizontalHeader()
        for col in range(7):
            hdr_txns.setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)
        hdr_txns.setSectionResizeMode(7, QHeaderView.ResizeMode.Stretch)
        self.report_trades_table = make_table(
            ["Date", "Type", "Asset", "Qty", "Price", "Amount", "Fees", "Notes"],
            stretch_last=True,
        )
        self.report_re_table = make_table(
            ["Date", "Type", "Asset", "Amount", "Notes"], stretch_last=True,
        )
        self.report_debt_table = make_table(
            ["Date", "Type", "Asset", "Amount", "Notes"], stretch_last=True,
        )
        self.report_journal_table = make_table(["Date", "Title", "Thesis", "Confidence", "Tags"])
        self.report_snapshot_table = make_table(["Metric", "Value"])
        self.report_alloc_table = make_table(["Metric", "Value"], stretch_last=True)

        # Risk Summary tab is a wrapper widget: a header label (counts +
        # data-quality note) above a Severity / Category / Message table.
        risk_tab = QWidget()
        risk_tab_layout = QVBoxLayout(risk_tab)
        risk_tab_layout.setContentsMargins(4, 4, 4, 4)
        risk_tab_layout.setSpacing(6)
        self.report_risk_label = QLabel("")
        self.report_risk_label.setWordWrap(True)
        self.report_risk_label.setStyleSheet("font-size: 12px; color: #666; padding: 2px 4px;")
        risk_tab_layout.addWidget(self.report_risk_label)
        self.report_risk_table = make_table(
            ["Severity", "Category", "Message"], stretch_last=True,
        )
        risk_tab_layout.addWidget(self.report_risk_table)

        self.report_detail_tabs.addTab(self.report_summary_table, "Summary")
        self.report_detail_tabs.addTab(self.report_perf_table, "Performance")
        self.report_detail_tabs.addTab(self.report_cfb_table, "Cash Flow Breakdown")
        self.report_detail_tabs.addTab(self.report_alloc_table, "Allocation")
        self.report_detail_tabs.addTab(risk_tab, "Risk Summary")
        self.report_detail_tabs.addTab(self.report_ops_table, "Operations")
        self.report_detail_tabs.addTab(self.report_txns_table, "Transactions")
        self.report_detail_tabs.addTab(self.report_trades_table, "Trades")
        self.report_detail_tabs.addTab(self.report_re_table, "Real Estate")
        self.report_detail_tabs.addTab(self.report_debt_table, "Debt")
        self.report_detail_tabs.addTab(self.report_journal_table, "Journal")
        self.report_detail_tabs.addTab(self.report_snapshot_table, "Current Snapshot")
        reports_layout.addWidget(self.report_detail_tabs, stretch=1)

        self.page_tabs.addTab(reports_tab, "Reports")

        # ── Tab 3: Export Reader ──
        reader_tab = QWidget()
        reader_layout = QVBoxLayout(reader_tab)
        reader_layout.setContentsMargins(8, 12, 8, 8)
        reader_layout.setSpacing(12)

        reader_btn_row = QHBoxLayout()
        btn_open_export = QPushButton("Open Full Export")
        btn_open_export.setStyleSheet(BTN_STYLE)
        btn_open_export.clicked.connect(self._open_full_export)
        reader_btn_row.addWidget(btn_open_export)

        btn_open_csv = QPushButton("Open CSV File")
        btn_open_csv.setStyleSheet(BTN_STYLE)
        btn_open_csv.clicked.connect(self._open_single_csv)
        reader_btn_row.addWidget(btn_open_csv)

        reader_btn_row.addStretch()
        reader_layout.addLayout(reader_btn_row)

        self.reader_info = QLabel("")
        self.reader_info.setStyleSheet("font-size: 13px; color: #999; padding: 4px;")
        reader_layout.addWidget(self.reader_info)

        reader_content = QHBoxLayout()

        self.reader_table_list = QListWidget()
        self.reader_table_list.setMaximumWidth(200)
        self.reader_table_list.currentTextChanged.connect(self._reader_table_selected)
        reader_content.addWidget(self.reader_table_list)

        self.reader_data_table = make_table([], stretch_last=False)
        reader_content.addWidget(self.reader_data_table, stretch=1)

        reader_layout.addLayout(reader_content, stretch=1)

        self.page_tabs.addTab(reader_tab, "Export Reader")

    def refresh(self):
        self._reports_dirty = True
        if self.page_tabs.currentIndex() == REPORTS_TAB_INDEX:
            self._refresh_report_list()

    def _on_tab_changed(self, index):
        if index == REPORTS_TAB_INDEX and self._reports_dirty:
            self._refresh_report_list()

    def _on_report_type_changed(self, index):
        self._reports_dirty = True
        if self.page_tabs.currentIndex() == REPORTS_TAB_INDEX:
            self._refresh_report_list()

    # --- Desktop path helpers ---

    def _desktop_dir(self) -> str:
        desktop = Path.home() / "Desktop"
        return str(desktop if desktop.is_dir() else Path.home())

    def _desktop_file(self, filename: str) -> str:
        return str(Path(self._desktop_dir()) / filename)

    # --- Simple CSV export/import (unchanged logic) ---

    def _export_assets(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Assets", self._desktop_file("assets.csv"), "CSV Files (*.csv)"
        )
        if not path:
            return
        csv_text = export_assets_csv(self.conn)
        with open(path, "w", newline="") as f:
            f.write(csv_text)
        QMessageBox.information(self, "Export", f"Assets exported to {path}")

    def _export_transactions(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Transactions", self._desktop_file("transactions.csv"), "CSV Files (*.csv)"
        )
        if not path:
            return
        csv_text = export_transactions_csv(self.conn)
        with open(path, "w", newline="") as f:
            f.write(csv_text)
        QMessageBox.information(self, "Export", f"Transactions exported to {path}")

    def _export_summary(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Summary", self._desktop_file("portfolio_summary.csv"), "CSV Files (*.csv)"
        )
        if not path:
            return
        csv_text = export_summary_csv(self.conn)
        with open(path, "w", newline="") as f:
            f.write(csv_text)
        QMessageBox.information(self, "Export", f"Summary exported to {path}")

    def _import_assets(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Import Assets", self._desktop_dir(), "CSV Files (*.csv)"
        )
        if not path:
            return
        # `utf-8-sig` strips an Excel-emitted BOM if present; falls back to
        # plain UTF-8 on files without one. Without the explicit encoding,
        # Windows defaults to the system locale and BOM-prefixed CSVs raise
        # UnicodeDecodeError.
        with open(path, "r", encoding="utf-8-sig") as f:
            csv_text = f.read()
        result = import_assets_csv(self.conn, csv_text)
        self._show_result("Asset Import", result)
        if result.imported > 0:
            self.data_changed.emit()

    def _import_transactions(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Import Transactions", self._desktop_dir(), "CSV Files (*.csv)"
        )
        if not path:
            return
        with open(path, "r", encoding="utf-8-sig") as f:
            csv_text = f.read()
        result = import_transactions_csv(self.conn, csv_text)
        self._show_result("Transaction Import", result)
        if result.imported > 0:
            self.data_changed.emit()

    def _show_result(self, label, result):
        status_parts = [f"{label}: {result.imported} imported"]
        if result.skipped:
            status_parts.append(f"{result.skipped} skipped")
        if result.errors:
            status_parts.append(f"{len(result.errors)} issue(s)")
        self.status_label.setText(" | ".join(status_parts))

        if result.errors:
            self.status_label.setStyleSheet(
                "font-size: 14px; padding: 4px; color: #e65100; font-weight: bold;"
            )
        else:
            self.status_label.setStyleSheet(
                "font-size: 14px; padding: 4px; color: #2e7d32; font-weight: bold;"
            )

        self.result_table.setRowCount(len(result.errors))
        for i, err in enumerate(result.errors):
            if "skipped" in err.lower():
                type_text = "Skip"
                color = "#f57f17"
            else:
                type_text = "Error"
                color = "#c62828"
            type_item = QTableWidgetItem(type_text)
            type_item.setForeground(QColor(color))
            self.result_table.setItem(i, 0, type_item)
            self.result_table.setItem(i, 1, QTableWidgetItem(err))
        resize_table_to_contents(self.result_table)

    # --- Full Backup ---

    def _full_export(self):
        fmt, ok = QInputDialog.getItem(
            self, "Export Format", "Export as:",
            ["Zip file (.zip)", "Folder"], 0, False,
        )
        if not ok:
            return

        if fmt.startswith("Zip"):
            path, _ = QFileDialog.getSaveFileName(
                self, "Export Full Data",
                self._desktop_file("portfolio_backup.zip"),
                "Zip Files (*.zip);;All Files (*)",
            )
        else:
            parent = QFileDialog.getExistingDirectory(
                self, "Select Parent Directory", self._desktop_dir(),
            )
            if not parent:
                return
            name, ok2 = QInputDialog.getText(
                self, "Folder Name", "Export folder name:",
                text="portfolio_backup",
            )
            if not ok2 or not name.strip():
                return
            path = str(Path(parent) / name.strip())

        if not path:
            return
        try:
            result = export_full_data(self.conn, path)
            if result.success:
                QMessageBox.information(self, "Full Export", result.message)
            else:
                QMessageBox.warning(self, "Full Export Failed", result.message)
        except Exception as e:
            QMessageBox.critical(self, "Full Export Error", str(e))

    def _full_import(self):
        fmt, ok = QInputDialog.getItem(
            self, "Import Source", "Import from:",
            ["Zip file (.zip)", "Folder"], 0, False,
        )
        if not ok:
            return

        if fmt.startswith("Zip"):
            path, _ = QFileDialog.getOpenFileName(
                self, "Import Full Data", self._desktop_dir(),
                "Zip Files (*.zip);;All Files (*)",
            )
        else:
            path = QFileDialog.getExistingDirectory(
                self, "Select Export Folder", self._desktop_dir(),
            )
        if not path:
            return

        manifest = inspect_full_export(path)
        if manifest is None:
            QMessageBox.warning(
                self, "Invalid Export",
                "Select either a portfolio backup .zip file or an export folder"
                " that contains manifest.csv.",
            )
            return

        table_summary = "\n".join(
            f"  {t.name}: {t.row_count} rows" for t in manifest.tables
        )
        confirm_text, ok = QInputDialog.getText(
            self, "Confirm Full Import",
            f"This will REPLACE ALL data in the database.\n\n"
            f"Export from: {manifest.exported_at}\n"
            f"Schema version: {manifest.schema_version}\n\n"
            f"Tables:\n{table_summary}\n\n"
            f"Type REPLACE DATA to confirm:",
        )
        if not ok or confirm_text.strip() != "REPLACE DATA":
            return

        try:
            result = import_full_data(self.conn, path, mode="replace")
            if result.success:
                QMessageBox.information(self, "Full Import", result.message)
                self.data_changed.emit()
            else:
                detail = "\n".join(result.details) if result.details else ""
                QMessageBox.warning(
                    self, "Full Import Failed",
                    f"{result.message}\n\n{detail}".strip(),
                )
        except Exception as e:
            QMessageBox.critical(self, "Full Import Error", str(e))

    # --- Export Reader ---

    def _open_full_export(self):
        fmt, ok = QInputDialog.getItem(
            self, "Open Export", "Open from:",
            ["Zip file (.zip)", "Folder"], 0, False,
        )
        if not ok:
            return

        if fmt.startswith("Zip"):
            path, _ = QFileDialog.getOpenFileName(
                self, "Open Full Export", self._desktop_dir(),
                "Zip Files (*.zip);;All Files (*)",
            )
        else:
            path = QFileDialog.getExistingDirectory(
                self, "Select Export Folder", self._desktop_dir(),
            )
        if not path:
            return
        self._load_export_reader(path)

    def _open_single_csv(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open CSV File", self._desktop_dir(), "CSV Files (*.csv)"
        )
        if not path:
            return
        self._reader_path = None
        self.reader_table_list.clear()
        self.reader_table_list.addItem(path.split("/")[-1])
        self._reader_single_csv_path = path
        self.reader_info.setText(f"CSV file: {path}")
        self.reader_table_list.setCurrentRow(0)

    def _load_export_reader(self, path: str):
        manifest = inspect_full_export(path)
        if manifest is None:
            QMessageBox.warning(
                self, "Invalid Export",
                "Select either a portfolio backup .zip file or an export folder"
                " that contains manifest.csv.",
            )
            return

        self._reader_path = path
        self._reader_single_csv_path = None
        self.reader_table_list.clear()
        self.reader_info.setText(
            f"Export: {path}\n"
            f"Schema: {manifest.schema_version}  |  "
            f"Exported: {manifest.exported_at}  |  "
            f"Tables: {len(manifest.tables)}"
        )

        for t in manifest.tables:
            self.reader_table_list.addItem(f"{t.name} ({t.row_count} rows)")

        self.reader_data_table.setColumnCount(0)
        self.reader_data_table.setRowCount(0)

    def _reader_table_selected(self, text: str):
        if not text:
            return

        if hasattr(self, "_reader_single_csv_path") and self._reader_single_csv_path:
            import csv as csv_mod
            import io
            try:
                with open(self._reader_single_csv_path, "r") as f:
                    content = f.read()
                reader = csv_mod.reader(io.StringIO(content))
                headers = next(reader, [])
                rows = []
                for i, row in enumerate(reader):
                    if i >= MAX_READER_ROWS:
                        break
                    rows.append(row)
                self._populate_reader_table(headers, rows)
            except Exception as e:
                self.reader_info.setText(f"Error reading CSV: {e}")
            return

        if self._reader_path is None:
            return

        table_name = text.split(" (")[0]
        result = read_csv_table(self._reader_path, table_name, max_rows=MAX_READER_ROWS)
        if result is None:
            self.reader_data_table.setColumnCount(1)
            self.reader_data_table.setHorizontalHeaderLabels(["Error"])
            self.reader_data_table.setRowCount(1)
            self.reader_data_table.setItem(0, 0, QTableWidgetItem("Could not read table"))
            return

        headers, rows = result
        self._populate_reader_table(headers, rows)

    # --- Reports ---

    def _refresh_report_list(self, *_args):
        rtype = self.report_type_combo.currentData()
        total = report_count(self.conn, report_type=rtype)
        summaries = list_report_summaries(
            self.conn, report_type=rtype, limit=DEFAULT_REPORT_LIST_LIMIT,
        )
        self._report_summary_cache = summaries
        self._reports_dirty = False

        with QSignalBlocker(self.report_list_table):
            self.report_list_table.setRowCount(len(summaries))
            for i, s in enumerate(summaries):
                self.report_list_table.setItem(i, 0, QTableWidgetItem(s.period_label))
                self.report_list_table.setItem(i, 1, QTableWidgetItem(s.generated_at[:19]))
                self.report_list_table.setItem(i, 2, QTableWidgetItem(f"${s.net_cash_flow:,.2f}"))
                self.report_list_table.setItem(i, 3, QTableWidgetItem(f"${s.operating_net_income:,.2f}"))
                self.report_list_table.setItem(i, 4, QTableWidgetItem(_money_or_na(s.net_worth_change)))
                self.report_list_table.setItem(i, 5, QTableWidgetItem(_money_or_na(s.funding_flow)))
                self.report_list_table.setItem(i, 6, QTableWidgetItem(_pct_or_na(s.approximate_return_pct)))

        if total > len(summaries):
            self._report_info_label.setText(
                f"Showing latest {len(summaries)} of {total} reports"
            )
        else:
            self._report_info_label.setText("")

        stats = get_report_stats(self.conn)
        self._report_stats_label.setText(
            f"Total: {stats['total']} reports  |  Monthly: {stats['monthly']}  |  Annual: {stats['annual']}"
        )

    def _on_report_selected(self, row, col, prev_row, prev_col):
        if row < 0 or row >= len(self._report_summary_cache):
            return
        summary = self._report_summary_cache[row]
        rtype = summary.report_type
        label = summary.period_label
        report = get_report(self.conn, rtype, label)
        if report is None:
            return
        try:
            data = json.loads(report.report_json)
        except json.JSONDecodeError:
            return
        self._render_report_detail(data)

    def _render_report_detail(self, data: dict):
        s = data.get("summary", {})
        summary_rows = [
            ("Report Type", str(s.get("report_type", ""))),
            ("Period", f"{s.get('period_start', '')} to {s.get('period_end', '')}"),
            ("Generated At", str(s.get("generated_at", ""))),
            ("Transaction Count", str(s.get("transaction_count", 0))),
            ("Beginning Cash", f"${s.get('beginning_cash', 0):,.2f}"),
            ("Ending Cash", f"${s.get('ending_cash', 0):,.2f}"),
            ("Net Cash Flow (cash movement, not profit)",
             f"${s.get('net_cash_flow', 0):,.2f}"),
            ("Operating Net Income", f"${s.get('operating_net_income', 0):,.2f}"),
            ("Total Inflow", f"${s.get('total_inflow', 0):,.2f}"),
            ("Total Outflow", f"${s.get('total_outflow', 0):,.2f}"),
            ("Total Fees", f"${s.get('total_fees', 0):,.2f}"),
        ]
        self.report_summary_table.setRowCount(len(summary_rows))
        for i, (metric, value) in enumerate(summary_rows):
            self.report_summary_table.setItem(i, 0, QTableWidgetItem(metric))
            self.report_summary_table.setItem(i, 1, QTableWidgetItem(value))

        perf = data.get("performance", {}) or {}
        perf_rows = [
            ("Beginning Net Worth", _money_or_na(perf.get("beginning_net_worth"))),
            ("Ending Net Worth", _money_or_na(perf.get("ending_net_worth"))),
            ("Net Worth Change", _money_or_na(perf.get("net_worth_change"))),
            ("Funding Flow (deposits - withdrawals)",
             _money_or_na(perf.get("funding_flow"))),
            ("Approximate Investment Result",
             _money_or_na(perf.get("approximate_investment_result"))),
            ("Approximate Return %", _pct_or_na(perf.get("approximate_return_pct"))),
            ("Data Quality Note", perf.get("data_quality_note", "")),
        ]
        self.report_perf_table.setRowCount(len(perf_rows))
        for i, (metric, value) in enumerate(perf_rows):
            self.report_perf_table.setItem(i, 0, QTableWidgetItem(metric))
            self.report_perf_table.setItem(i, 1, QTableWidgetItem(value))

        cfb = data.get("cash_flow_breakdown", {}) or {}
        ff = cfb.get("funding_flow", {}) or {}
        tcf = cfb.get("trade_cash_flow", {}) or {}
        rcf = cfb.get("real_estate_cash_flow", {}) or {}
        dcf = cfb.get("debt_cash_flow", {}) or {}
        cfb_rows = [
            ("Funding Flow", "Deposits", _money_or_na(ff.get("deposits"))),
            ("Funding Flow", "Withdrawals", _money_or_na(ff.get("withdrawals"))),
            ("Funding Flow", "Net", _money_or_na(ff.get("net"))),
            ("Trade Cash Flow", "Buys", _money_or_na(tcf.get("buys"))),
            ("Trade Cash Flow", "Sells", _money_or_na(tcf.get("sells"))),
            ("Trade Cash Flow", "Net", _money_or_na(tcf.get("net"))),
            ("Real Estate Cash Flow", "Rent Received",
             _money_or_na(rcf.get("rent_received"))),
            ("Real Estate Cash Flow", "Property Expenses",
             _money_or_na(rcf.get("property_expenses"))),
            ("Real Estate Cash Flow", "Property Purchases",
             _money_or_na(rcf.get("property_purchases"))),
            ("Real Estate Cash Flow", "Property Sales",
             _money_or_na(rcf.get("property_sales"))),
            ("Real Estate Cash Flow", "Net", _money_or_na(rcf.get("net"))),
            ("Debt Cash Flow", "Borrowed", _money_or_na(dcf.get("borrowed"))),
            ("Debt Cash Flow", "Debt Payments",
             _money_or_na(dcf.get("debt_payments"))),
            ("Debt Cash Flow", "Mortgage Payments",
             _money_or_na(dcf.get("mortgage_payments"))),
            ("Debt Cash Flow", "Net", _money_or_na(dcf.get("net"))),
            ("Fees Total", "", _money_or_na(cfb.get("fees_total"))),
            ("Other Cash Flow", "", _money_or_na(cfb.get("other_cash_flow"))),
        ]
        self.report_cfb_table.setRowCount(len(cfb_rows))
        for i, (cat, sub, amt) in enumerate(cfb_rows):
            self.report_cfb_table.setItem(i, 0, QTableWidgetItem(cat))
            self.report_cfb_table.setItem(i, 1, QTableWidgetItem(sub))
            self.report_cfb_table.setItem(i, 2, QTableWidgetItem(amt))

        alloc = data.get("allocation", {}) or {}
        alloc_rows = [
            ("Source", str(alloc.get("source", "")) or "N/A"),
            ("As Of", str(alloc.get("as_of") or "N/A (current state)")),
            ("Cash Amount", _money_or_na(alloc.get("cash_amount"))),
            ("Cash %", _alloc_pct_or_na(alloc.get("cash_pct"))),
            ("Total Assets", _money_or_na(alloc.get("total_assets"))),
            ("Total Liabilities", _money_or_na(alloc.get("total_liabilities"))),
            ("Net Worth", _money_or_na(alloc.get("net_worth"))),
            ("Liquid Assets", _money_or_na(alloc.get("liquid_assets"))),
            ("Illiquid Assets", _money_or_na(alloc.get("illiquid_assets"))),
            ("Real Estate Equity %",
             _alloc_pct_or_na(alloc.get("real_estate_equity_pct"))),
            ("Debt Ratio", _alloc_pct_or_na(alloc.get("debt_ratio"))),
        ]
        for atype, info in (alloc.get("by_asset_type") or {}).items():
            info = info or {}
            value = _money_or_na(info.get("value"))
            pct = _alloc_pct_or_na(info.get("pct"))
            alloc_rows.append((f"By Asset Type — {atype}", f"{value} ({pct})"))
        for i, item in enumerate((alloc.get("top_assets") or []), start=1):
            item = item or {}
            value = _money_or_na(item.get("value"))
            pct = _alloc_pct_or_na(item.get("pct"))
            alloc_rows.append((
                f"Top Asset {i} — {item.get('name', '')}",
                f"{value} ({pct})",
            ))
        for cat, info in (alloc.get("by_liquidity") or {}).items():
            info = info or {}
            value = _money_or_na(info.get("value"))
            pct = _alloc_pct_or_na(info.get("pct"))
            alloc_rows.append((f"By Liquidity — {cat}", f"{value} ({pct})"))
        alloc_rows.append(("Data Quality Note", alloc.get("data_quality_note", "")))
        self.report_alloc_table.setRowCount(len(alloc_rows))
        for i, (metric, value) in enumerate(alloc_rows):
            self.report_alloc_table.setItem(i, 0, QTableWidgetItem(metric))
            self.report_alloc_table.setItem(i, 1, QTableWidgetItem(value))

        risk = data.get("risk_summary", {}) or {}
        total_count = risk.get("total_count", 0)
        warning_count = risk.get("warning_count", 0)
        info_count = risk.get("info_count", 0)
        note = risk.get("data_quality_note", "")
        self.report_risk_label.setText(
            f"Total: {total_count} warnings "
            f"({warning_count} actionable, {info_count} info)\n{note}"
        )
        warnings_list = risk.get("warnings", []) or []
        self.report_risk_table.setRowCount(len(warnings_list))
        for i, w in enumerate(warnings_list):
            w = w or {}
            self.report_risk_table.setItem(
                i, 0, QTableWidgetItem(str(w.get("severity", ""))),
            )
            self.report_risk_table.setItem(
                i, 1, QTableWidgetItem(str(w.get("category", ""))),
            )
            self.report_risk_table.setItem(
                i, 2, QTableWidgetItem(str(w.get("message", ""))),
            )

        ops = data.get("operations", [])
        self.report_ops_table.setRowCount(len(ops))
        for i, op in enumerate(ops):
            self.report_ops_table.setItem(i, 0, QTableWidgetItem(op.get("txn_type", "")))
            self.report_ops_table.setItem(i, 1, QTableWidgetItem(str(op.get("count", 0))))
            self.report_ops_table.setItem(i, 2, QTableWidgetItem(f"${op.get('total_amount', 0):,.2f}"))
            self.report_ops_table.setItem(i, 3, QTableWidgetItem(f"${op.get('total_fees', 0):,.2f}"))

        txns = data.get("transactions", [])
        self.report_txns_table.setRowCount(len(txns))
        for i, t in enumerate(txns):
            self.report_txns_table.setItem(i, 0, QTableWidgetItem(t.get("date", "")))
            self.report_txns_table.setItem(i, 1, QTableWidgetItem(t.get("txn_type", "")))
            self.report_txns_table.setItem(i, 2, QTableWidgetItem(t.get("asset_symbol", "")))
            qty = t.get("quantity")
            self.report_txns_table.setItem(i, 3, QTableWidgetItem(str(qty) if qty is not None else ""))
            price = t.get("price")
            self.report_txns_table.setItem(i, 4, QTableWidgetItem(f"${price:,.2f}" if price else ""))
            self.report_txns_table.setItem(i, 5, QTableWidgetItem(f"${t.get('total_amount', 0):,.2f}"))
            self.report_txns_table.setItem(i, 6, QTableWidgetItem(f"${t.get('fees', 0):,.2f}"))
            self.report_txns_table.setItem(i, 7, QTableWidgetItem(t.get("notes", "")))

        trades = data.get("trades", []) or []
        self.report_trades_table.setRowCount(len(trades))
        for i, t in enumerate(trades):
            self.report_trades_table.setItem(i, 0, QTableWidgetItem(t.get("date", "")))
            self.report_trades_table.setItem(i, 1, QTableWidgetItem(t.get("txn_type", "")))
            self.report_trades_table.setItem(i, 2, QTableWidgetItem(t.get("asset_symbol", "")))
            qty = t.get("quantity")
            self.report_trades_table.setItem(i, 3, QTableWidgetItem(str(qty) if qty is not None else ""))
            price = t.get("price")
            self.report_trades_table.setItem(i, 4, QTableWidgetItem(f"${price:,.2f}" if price else ""))
            self.report_trades_table.setItem(i, 5, QTableWidgetItem(f"${t.get('total_amount', 0):,.2f}"))
            self.report_trades_table.setItem(i, 6, QTableWidgetItem(f"${t.get('fees', 0):,.2f}"))
            self.report_trades_table.setItem(i, 7, QTableWidgetItem(t.get("notes", "")))

        re_ops = data.get("real_estate", []) or []
        self.report_re_table.setRowCount(len(re_ops))
        for i, t in enumerate(re_ops):
            self.report_re_table.setItem(i, 0, QTableWidgetItem(t.get("date", "")))
            self.report_re_table.setItem(i, 1, QTableWidgetItem(t.get("txn_type", "")))
            self.report_re_table.setItem(i, 2, QTableWidgetItem(t.get("asset_symbol", "")))
            self.report_re_table.setItem(i, 3, QTableWidgetItem(f"${t.get('total_amount', 0):,.2f}"))
            self.report_re_table.setItem(i, 4, QTableWidgetItem(t.get("notes", "")))

        debt_ops = data.get("debt", []) or []
        self.report_debt_table.setRowCount(len(debt_ops))
        for i, t in enumerate(debt_ops):
            self.report_debt_table.setItem(i, 0, QTableWidgetItem(t.get("date", "")))
            self.report_debt_table.setItem(i, 1, QTableWidgetItem(t.get("txn_type", "")))
            self.report_debt_table.setItem(i, 2, QTableWidgetItem(t.get("asset_symbol", "")))
            self.report_debt_table.setItem(i, 3, QTableWidgetItem(f"${t.get('total_amount', 0):,.2f}"))
            self.report_debt_table.setItem(i, 4, QTableWidgetItem(t.get("notes", "")))

        journal = data.get("journal", [])
        self.report_journal_table.setRowCount(len(journal))
        for i, j in enumerate(journal):
            self.report_journal_table.setItem(i, 0, QTableWidgetItem(j.get("date", "")))
            self.report_journal_table.setItem(i, 1, QTableWidgetItem(j.get("title", "")))
            self.report_journal_table.setItem(i, 2, QTableWidgetItem(j.get("thesis", "")))
            cl = j.get("confidence_level")
            self.report_journal_table.setItem(i, 3, QTableWidgetItem(str(cl) if cl is not None else ""))
            self.report_journal_table.setItem(i, 4, QTableWidgetItem(j.get("tags", "")))

        snap = data.get("current_snapshot", {})
        snap_rows = [
            ("Note", snap.get("note", "")),
            ("Cash", _money_or_na(snap.get("cash"))),
            ("Total Assets", _money_or_na(snap.get("total_assets"))),
            ("Total Liabilities", _money_or_na(snap.get("total_liabilities"))),
            ("Net Worth", _money_or_na(snap.get("net_worth"))),
        ]
        self.report_snapshot_table.setRowCount(len(snap_rows))
        for i, (metric, value) in enumerate(snap_rows):
            self.report_snapshot_table.setItem(i, 0, QTableWidgetItem(metric))
            self.report_snapshot_table.setItem(i, 1, QTableWidgetItem(str(value)))

    def _generate_missing_reports(self):
        from datetime import date
        LARGE_GENERATION_THRESHOLD = 24
        try:
            pending = count_due_reports(self.conn, today=date.today())
            if pending == 0:
                QMessageBox.information(self, "Reports", "All reports are up to date.")
                return

            if pending > LARGE_GENERATION_THRESHOLD:
                reply = QMessageBox.question(
                    self, "Generate Reports",
                    f"This will generate {pending} reports, which may take a moment.\n\n"
                    f"Continue?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No,
                )
                if reply != QMessageBox.StandardButton.Yes:
                    return

            generated = generate_due_reports(self.conn, today=date.today())
            self._refresh_report_list()
            QMessageBox.information(
                self, "Reports",
                f"Generated {len(generated)} report(s).",
            )
        except Exception as e:
            QMessageBox.critical(self, "Report Error", str(e))

    def _generate_selected_period(self):
        rtype = self.report_type_combo.currentData()
        if rtype == "monthly":
            label, ok = QInputDialog.getText(
                self, "Generate Monthly Report",
                "Enter period label (YYYY-MM):",
            )
            if not ok or not label.strip():
                return
            label = label.strip()
            try:
                parts = label.split("-")
                year, month = int(parts[0]), int(parts[1])
                generate_monthly_report(self.conn, year, month)
                self._refresh_report_list()
                QMessageBox.information(self, "Reports", f"Generated monthly report for {label}.")
            except Exception as e:
                QMessageBox.critical(self, "Report Error", str(e))
        else:
            label, ok = QInputDialog.getText(
                self, "Generate Annual Report",
                "Enter year (YYYY):",
            )
            if not ok or not label.strip():
                return
            try:
                year = int(label.strip())
                generate_annual_report(self.conn, year)
                self._refresh_report_list()
                QMessageBox.information(self, "Reports", f"Generated annual report for {year}.")
            except Exception as e:
                QMessageBox.critical(self, "Report Error", str(e))

    def _delete_old_reports(self):
        start_date = get_auto_report_start_date(self.conn)
        if start_date is None:
            QMessageBox.warning(
                self, "Delete Old Reports",
                "No meaningful transaction activity found. Cannot determine cutoff date.",
            )
            return

        cutoff = start_date.isoformat()
        total = report_count(self.conn)
        reply = QMessageBox.question(
            self, "Delete Old Reports",
            f"Delete all reports with period_start before {cutoff}?\n\n"
            f"This cutoff is based on the earliest meaningful transaction.\n"
            f"Only report rows will be deleted. Transactions and other data are not affected.\n\n"
            f"Total reports currently: {total}",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        deleted = delete_reports_before_date(self.conn, cutoff)
        self._refresh_report_list()
        self._clear_report_detail()
        QMessageBox.information(
            self, "Delete Old Reports",
            f"Deleted {deleted} report(s) before {cutoff}.",
        )

    def _delete_selected_report(self):
        row = self.report_list_table.currentRow()
        if row < 0 or row >= len(self._report_summary_cache):
            QMessageBox.warning(self, "Delete", "Select a report first.")
            return
        summary = self._report_summary_cache[row]
        reply = QMessageBox.question(
            self, "Delete Report",
            f"Delete the {summary.report_type} report for {summary.period_label}?\n\n"
            f"Only this report will be deleted. Transactions and other data are not affected.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        delete_report(self.conn, summary.id)
        self._refresh_report_list()
        self._clear_report_detail()
        QMessageBox.information(
            self, "Report Deleted",
            f"Deleted {summary.report_type} report for {summary.period_label}.",
        )

    def _delete_current_type_reports(self):
        rtype = self.report_type_combo.currentData()
        type_count = report_count(self.conn, report_type=rtype)
        if type_count == 0:
            QMessageBox.information(self, "Delete Reports", f"No {rtype} reports to delete.")
            return
        reply = QMessageBox.question(
            self, "Delete Reports",
            f"Delete all {type_count} {rtype} report(s)?\n\n"
            f"Only report rows will be deleted. Transactions and other data are not affected.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        deleted = delete_reports_by_type(self.conn, rtype)
        self._refresh_report_list()
        self._clear_report_detail()
        QMessageBox.information(
            self, "Reports Deleted",
            f"Deleted {deleted} {rtype} report(s).",
        )

    def _delete_reports_by_date_range(self):
        start, ok = QInputDialog.getText(
            self, "Date Range Start",
            "Enter start date (YYYY-MM-DD), inclusive:",
        )
        if not ok or not start.strip():
            return
        end, ok = QInputDialog.getText(
            self, "Date Range End",
            "Enter end date (YYYY-MM-DD), exclusive:",
        )
        if not ok or not end.strip():
            return
        start = start.strip()
        end = end.strip()

        reply = QMessageBox.question(
            self, "Delete Reports",
            f"Delete all reports with period_start >= {start} and < {end}?\n\n"
            f"Only report rows will be deleted. Transactions and other data are not affected.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        deleted = delete_reports_in_period_range(self.conn, start, end)
        self._refresh_report_list()
        self._clear_report_detail()
        QMessageBox.information(
            self, "Reports Deleted",
            f"Deleted {deleted} report(s) in range [{start}, {end}).",
        )

    def _delete_all_reports(self):
        stats = get_report_stats(self.conn)
        if stats["total"] == 0:
            QMessageBox.information(self, "Delete All Reports", "No reports to delete.")
            return
        text, ok = QInputDialog.getText(
            self, "Delete All Reports",
            f"This will delete ALL {stats['total']} reports "
            f"({stats['monthly']} monthly, {stats['annual']} annual).\n\n"
            f"Only report rows will be deleted. Transactions and other data are not affected.\n\n"
            f'Type "DELETE REPORTS" to confirm:',
        )
        if not ok or text.strip() != "DELETE REPORTS":
            return
        deleted = delete_all_reports(self.conn)
        self._refresh_report_list()
        self._clear_report_detail()
        QMessageBox.information(
            self, "All Reports Deleted",
            f"Deleted {deleted} report(s).",
        )

    def _clear_report_detail(self):
        for table in (
            self.report_summary_table,
            self.report_perf_table,
            self.report_cfb_table,
            self.report_alloc_table,
            self.report_risk_table,
            self.report_ops_table,
            self.report_txns_table,
            self.report_trades_table,
            self.report_re_table,
            self.report_debt_table,
            self.report_journal_table,
            self.report_snapshot_table,
        ):
            table.setRowCount(0)
        self.report_risk_label.setText("")

    def _export_selected_report(self):
        row = self.report_list_table.currentRow()
        if row < 0 or row >= len(self._report_summary_cache):
            QMessageBox.warning(self, "Export", "Select a report first.")
            return
        summary = self._report_summary_cache[row]
        report = get_report(self.conn, summary.report_type, summary.period_label)
        if report is None:
            QMessageBox.warning(self, "Export", "Report not found.")
            return
        try:
            data = json.loads(report.report_json)
        except json.JSONDecodeError:
            QMessageBox.critical(self, "Export Error", "Could not parse report data.")
            return

        rtype = report.report_type
        label = report.period_label
        if rtype == "monthly":
            default_base = f"monthly_report_{label.replace('-', '_')}"
        else:
            default_base = f"annual_report_{label}"

        path, _ = QFileDialog.getSaveFileName(
            self, "Export Report", self._desktop_file(f"{default_base}.xlsx"),
            "Excel Workbook (*.xlsx);;Text File (*.txt)",
        )
        if not path:
            return

        try:
            if path.endswith(".txt"):
                export_report_txt(data, path)
            else:
                export_report_xlsx(data, path)
            QMessageBox.information(self, "Export", f"Report exported to {path}")
        except Exception as e:
            QMessageBox.critical(self, "Export Error", str(e))

    def _populate_reader_table(self, headers: list[str], rows: list[list[str]]):
        self.reader_data_table.setColumnCount(len(headers))
        self.reader_data_table.setHorizontalHeaderLabels(headers)
        self.reader_data_table.setRowCount(len(rows))

        for r, row in enumerate(rows):
            for c, val in enumerate(row):
                self.reader_data_table.setItem(r, c, QTableWidgetItem(str(val)))

        info_parts = [self.reader_info.text().split("\n")[0]]
        info_parts.append(
            f"Showing {len(rows)} rows, {len(headers)} columns"
            + (f" (limited to {MAX_READER_ROWS})" if len(rows) == MAX_READER_ROWS else "")
        )
        self.reader_info.setText("\n".join(info_parts))
