from PySide6.QtWidgets import (
    QLabel, QFrame, QVBoxLayout, QHBoxLayout, QWidget, QTableWidget,
    QTableWidgetItem, QHeaderView, QSizePolicy,
)
from PySide6.QtCore import Qt


def make_header(text: str) -> QLabel:
    label = QLabel(text)
    label.setStyleSheet("font-size: 20px; font-weight: bold; padding: 8px 0;")
    return label


CARD_BG = "#353535"
CARD_BORDER = "#4a4a4a"
CARD_TITLE_COLOR = "#aaaaaa"
CARD_VALUE_COLOR = "#e0e0e0"
LABEL_MUTED_COLOR = "#999999"
STATUS_LABEL_STYLE = f"font-size: 13px; color: {LABEL_MUTED_COLOR}; padding: 4px;"


def make_stat_card(title: str, value: str, color: str = CARD_VALUE_COLOR) -> QFrame:
    card = QFrame()
    card.setFrameShape(QFrame.Shape.StyledPanel)
    card.setStyleSheet(f"""
        QFrame {{
            background-color: {CARD_BG};
            border: 1px solid {CARD_BORDER};
            border-radius: 6px;
            padding: 12px;
        }}
    """)
    layout = QVBoxLayout(card)
    layout.setSpacing(4)

    title_label = QLabel(title)
    title_label.setStyleSheet(f"font-size: 13px; color: {CARD_TITLE_COLOR}; border: none;")

    value_label = QLabel(value)
    value_label.setObjectName("value")
    value_label.setStyleSheet(f"font-size: 18px; font-weight: bold; color: {color}; border: none;")

    layout.addWidget(title_label)
    layout.addWidget(value_label)
    return card


def make_table(headers: list[str], stretch_last: bool = True) -> QTableWidget:
    table = QTableWidget()
    table.setColumnCount(len(headers))
    table.setHorizontalHeaderLabels(headers)
    table.setAlternatingRowColors(True)
    table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
    table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
    table.verticalHeader().setVisible(False)
    header = table.horizontalHeader()
    if stretch_last:
        header.setStretchLastSection(True)
    for i in range(len(headers)):
        header.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
    return table


def configure_expanding_table(table: QTableWidget) -> None:
    table.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
    table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
    header_h = table.horizontalHeader().height() or table.verticalHeader().defaultSectionSize()
    table.setFixedHeight(header_h + table.verticalHeader().defaultSectionSize() + 2 * table.frameWidth())


def resize_table_to_contents(table: QTableWidget, min_visible_rows: int = 0) -> None:
    table.resizeRowsToContents()
    row_count = max(table.rowCount(), min_visible_rows)
    height = table.horizontalHeader().height() + 2 * table.frameWidth()
    if row_count == 0:
        height += table.verticalHeader().defaultSectionSize()
    else:
        for r in range(table.rowCount()):
            height += table.rowHeight(r)
        default_h = table.verticalHeader().defaultSectionSize()
        for _ in range(row_count - table.rowCount()):
            height += default_h
    if table.horizontalScrollBar() and table.horizontalScrollBar().isVisible():
        height += table.horizontalScrollBar().height()
    table.setFixedHeight(height)


def fmt_money(value: float | None, prefix: str = "$") -> str:
    if value is None:
        return "N/A"
    return f"{prefix}{value:,.2f}"


def fmt_money_compact(value: float | None, prefix: str = "$") -> str:
    """Compact money for cramped UI surfaces (e.g. Dashboard cards, tooltips
    elsewhere supply the exact value).

    Returns examples: "$0.00", "$999.99", "$1.2K", "$3.4M", "$5.6B", "$7.8T".
    Negatives use a leading "-" before the prefix, e.g. "-$1.2K".

    Do NOT use in tables, exports, or reports — those need exact values from
    fmt_money().
    """
    if value is None:
        return "N/A"
    if value == 0:
        return f"{prefix}0.00"
    sign = "-" if value < 0 else ""
    abs_v = abs(value)
    if abs_v < 1_000:
        return f"{sign}{prefix}{abs_v:,.2f}"
    if abs_v < 1_000_000:
        return f"{sign}{prefix}{abs_v / 1_000:.1f}K"
    if abs_v < 1_000_000_000:
        return f"{sign}{prefix}{abs_v / 1_000_000:.1f}M"
    if abs_v < 1_000_000_000_000:
        return f"{sign}{prefix}{abs_v / 1_000_000_000:.1f}B"
    if abs_v < 1_000_000_000_000_000:
        return f"{sign}{prefix}{abs_v / 1_000_000_000_000:.1f}T"
    return f"{sign}{prefix}{abs_v / 1_000_000_000_000:,.0f}T"


def fmt_pct(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.1%}"


def fmt_qty(value: float | None) -> str:
    if value is None:
        return "N/A"
    if value == int(value):
        return f"{int(value)}"
    return f"{value:,.4f}"
