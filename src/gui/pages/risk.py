import sqlite3
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QTableWidgetItem, QScrollArea,
)
from PySide6.QtGui import QColor
from src.gui.widgets.common import make_header, make_table, configure_expanding_table, resize_table_to_contents
from src.engines.risk import get_all_warnings
from src.utils.display import format_severity, format_category


SEVERITY_COLORS = {
    "critical": "#c62828",
    "high": "#e65100",
    "medium": "#f9a825",
    "low": "#558b2f",
    "info": "#1565c0",
}


class RiskPage(QWidget):
    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(12)

        layout.addWidget(make_header("Risk Warnings"))
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("font-size: 13px; color: #999;")
        layout.addWidget(self.status_label)

        self.table = make_table(["Severity", "Category", "Message"])
        configure_expanding_table(self.table)
        layout.addWidget(self.table)
        layout.addStretch()

        scroll.setWidget(content)
        outer.addWidget(scroll)

    def refresh(self):
        warnings = get_all_warnings(self.conn)
        non_info = [w for w in warnings if w.severity != "info"]

        if not warnings:
            self.status_label.setText("No risk warnings detected.")
        else:
            self.status_label.setText(
                f"{len(non_info)} warning(s), {len(warnings) - len(non_info)} info notice(s)"
            )

        self.table.setRowCount(len(warnings))
        for i, w in enumerate(warnings):
            sev_item = QTableWidgetItem(format_severity(w.severity))
            color = SEVERITY_COLORS.get(w.severity, "#cccccc")
            sev_item.setForeground(QColor(color))

            self.table.setItem(i, 0, sev_item)
            self.table.setItem(i, 1, QTableWidgetItem(format_category(w.category)))
            self.table.setItem(i, 2, QTableWidgetItem(w.message))
        resize_table_to_contents(self.table)
