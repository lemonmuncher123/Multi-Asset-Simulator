import sqlite3
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QGridLayout, QLabel, QScrollArea,
    QFrame, QTableWidgetItem, QSizePolicy,
)
from PySide6.QtCore import Qt, QEvent
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg

from src.gui.widgets.common import (
    make_header, make_stat_card, make_table, fmt_money, fmt_money_compact, fmt_pct,
    configure_expanding_table, resize_table_to_contents,
    CARD_BG, CARD_BORDER, CARD_TITLE_COLOR, CARD_VALUE_COLOR, LABEL_MUTED_COLOR,
)
from src.utils.display import format_transaction_type
from src.engines.dashboard import (
    get_dashboard_summary,
    get_net_worth_trend,
    get_cash_flow_snapshot,
    get_return_drivers,
    get_recent_activity,
    get_real_estate_snapshot,
)
from src.engines.allocation import calc_asset_pie_breakdown
from src.engines.risk import get_all_warnings
from src.charts.dashboard import (
    create_net_worth_trend_figure,
    create_asset_mix_figure,
    create_return_drivers_figure,
)

_GREEN = "#2e7d32"
_RED = "#c62828"
_SECTION_STYLE = f"""
    QFrame {{
        background-color: {CARD_BG};
        border: 1px solid {CARD_BORDER};
        border-radius: 6px;
        padding: 10px;
    }}
"""
_SECTION_LABEL = f"font-size: 13px; color: {CARD_TITLE_COLOR}; border: none;"
_WARN_COLORS = {
    "critical": "#d32f2f",
    "high": "#e53935",
    "medium": "#f9a825",
    "low": CARD_VALUE_COLOR,
}

# --- Responsive layout breakpoints (page width in pixels) ---
_WIDE_THRESHOLD = 1100
_MEDIUM_THRESHOLD = 800
_NARROW_THRESHOLD = 500

# Column counts by mode for sections that reflow row-major.
_HERO_COLUMNS = {"wide": 4, "medium": 2, "narrow": 1, "compact": 1}
_RE_COLUMNS = {"wide": 6, "medium": 3, "narrow": 2, "compact": 1}


def _layout_mode_for_width(width: int) -> str:
    """Pick a layout mode for the current Dashboard width.

    "wide" >=1100, "medium" >=800, "narrow" >=500, "compact" otherwise.
    """
    if width >= _WIDE_THRESHOLD:
        return "wide"
    if width >= _MEDIUM_THRESHOLD:
        return "medium"
    if width >= _NARROW_THRESHOLD:
        return "narrow"
    return "compact"


def _section_header(text: str) -> QLabel:
    lbl = QLabel(text)
    lbl.setStyleSheet(f"font-size: 14px; font-weight: bold; color: {CARD_VALUE_COLOR}; padding: 4px 0 2px 0;")
    return lbl


def _make_kv_label(title: str, value: str, color: str = CARD_VALUE_COLOR) -> QWidget:
    w = QWidget()
    lay = QVBoxLayout(w)
    lay.setContentsMargins(0, 0, 0, 0)
    lay.setSpacing(1)
    t = QLabel(title)
    t.setStyleSheet(_SECTION_LABEL)
    v = QLabel(value)
    v.setObjectName("value")
    v.setStyleSheet(f"font-size: 15px; font-weight: bold; color: {color}; border: none;")
    lay.addWidget(t)
    lay.addWidget(v)
    return w


class DashboardPage(QWidget):
    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFrameShape(QScrollArea.Shape.NoFrame)
        self.scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        # Layout reflows for narrow widths (see _apply_responsive_layout), so
        # the page itself never needs to scroll horizontally.
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        content = QWidget()
        self.main_layout = QVBoxLayout(content)
        self.main_layout.setContentsMargins(24, 16, 24, 16)
        self.main_layout.setSpacing(14)

        self.main_layout.addWidget(make_header("Dashboard"))

        # --- Top: hero cards (responsive grid; populated by reflow) ---
        self.top_row = QGridLayout()
        self.top_row.setSpacing(12)
        self.nw_card = make_stat_card("Net Worth", "$0.00")
        self.change_card = make_stat_card("30D Change", "--")
        self.cash_card = make_stat_card("Cash", "$0.00")
        self.risk_card = make_stat_card("Risk Status", "OK")
        self.main_layout.addLayout(self.top_row)

        # --- Charts row (responsive grid) ---
        self.charts_row = QGridLayout()
        self.charts_row.setSpacing(12)
        self._trend_canvas = None
        self._trend_placeholder = QWidget()
        self._trend_placeholder.setStyleSheet("background: transparent;")
        self._mix_canvas = None
        self._mix_placeholder = QWidget()
        self._mix_placeholder.setStyleSheet("background: transparent;")
        self.main_layout.addLayout(self.charts_row)

        # --- Return drivers + cash flow (responsive grid) ---
        self.perf_row = QGridLayout()
        self.perf_row.setSpacing(12)
        self._drivers_canvas = None
        self._drivers_placeholder = QWidget()
        self._drivers_placeholder.setStyleSheet("background: transparent;")

        self.cf_frame = QFrame()
        self.cf_frame.setStyleSheet(_SECTION_STYLE)
        self.cf_layout = QVBoxLayout(self.cf_frame)
        self.cf_layout.setSpacing(4)
        self.cf_layout.addWidget(_section_header("Cash Flow (30D)"))
        self.cf_inflow = _make_kv_label("Inflow", "$0.00", _GREEN)
        self.cf_outflow = _make_kv_label("Outflow", "$0.00", _RED)
        self.cf_net = _make_kv_label("Net", "$0.00")
        self.cf_fees = _make_kv_label("Fees", "$0.00")
        self.cf_count = _make_kv_label("Transactions", "0")
        for w in (self.cf_inflow, self.cf_outflow, self.cf_net, self.cf_fees, self.cf_count):
            self.cf_layout.addWidget(w)
        self.main_layout.addLayout(self.perf_row)

        # --- Real estate (conditional, responsive metric grid) ---
        self.re_frame = QFrame()
        self.re_frame.setObjectName("re_frame")
        self.re_frame.setStyleSheet(_SECTION_STYLE)
        self.re_layout = QVBoxLayout(self.re_frame)
        self.re_layout.setSpacing(4)
        self.re_layout.addWidget(_section_header("Real Estate Snapshot"))
        self.re_metrics = QGridLayout()
        self.re_metrics.setSpacing(10)
        self.re_props = _make_kv_label("Properties", "0")
        self.re_value = _make_kv_label("Value", "$0")
        self.re_equity = _make_kv_label("Equity", "$0")
        self.re_mortgage = _make_kv_label("Mortgage", "$0")
        self.re_ncf = _make_kv_label("Monthly NCF", "$0")
        self.re_ltv = _make_kv_label("Avg LTV", "N/A")
        self.re_layout.addLayout(self.re_metrics)
        self.re_frame.setVisible(False)
        self.main_layout.addWidget(self.re_frame)

        # --- Risk warnings ---
        self.risk_frame = QFrame()
        self.risk_frame.setStyleSheet(_SECTION_STYLE)
        self.risk_layout = QVBoxLayout(self.risk_frame)
        self.risk_layout.setSpacing(4)
        self.risk_layout.addWidget(_section_header("Risk Warnings"))
        self.risk_labels: list[QLabel] = []
        for _ in range(3):
            lbl = QLabel("")
            lbl.setWordWrap(True)
            lbl.setStyleSheet(f"font-size: 12px; color: {CARD_VALUE_COLOR}; border: none; padding: 2px 0;")
            self.risk_labels.append(lbl)
            self.risk_layout.addWidget(lbl)
        self.risk_ok_label = QLabel("No active risk warnings")
        self.risk_ok_label.setStyleSheet(f"font-size: 12px; color: {LABEL_MUTED_COLOR}; border: none; padding: 2px 0;")
        self.risk_layout.addWidget(self.risk_ok_label)
        self.main_layout.addWidget(self.risk_frame)

        # --- Recent activity ---
        self.main_layout.addWidget(_section_header("Recent Activity"))
        self.activity_table = make_table(["Date", "Type", "Asset", "Amount", "Fees"])
        configure_expanding_table(self.activity_table)
        self.main_layout.addWidget(self.activity_table)

        self.main_layout.addStretch()
        self.scroll_area.setWidget(content)

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(self.scroll_area)

        # Force initial responsive layout so the section grids are populated
        # before first refresh / resize. Defaults to "wide" — resizeEvent will
        # downshift if the page is mounted in a narrower viewport.
        self._current_layout_mode: str | None = None
        self._apply_responsive_layout(_WIDE_THRESHOLD + 100)

    # --- Responsive layout ---

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._reflow_dashboard_layout_if_needed()

    def _reflow_dashboard_layout_if_needed(self) -> None:
        """Public entry point for reflow. Uses the page width — close enough
        to viewport width (within scrollbar margin) and reliable across the
        widget's lifecycle, including before show()."""
        self._apply_responsive_layout(self.width())

    def _apply_responsive_layout(self, width: int) -> None:
        """Reflow each responsive section if the layout mode has changed.

        No-op when the mode is unchanged, so this is safe to call from
        resizeEvent on every pixel of drag.
        """
        mode = _layout_mode_for_width(width)
        if mode == self._current_layout_mode:
            return
        self._current_layout_mode = mode

        self.setUpdatesEnabled(False)
        try:
            self._reflow_top_row(mode)
            self._reflow_charts_row(mode)
            self._reflow_perf_row(mode)
            self._reflow_re_metrics(mode)
        finally:
            self.setUpdatesEnabled(True)

    def _reflow_top_row(self, mode: str) -> None:
        cards = [self.nw_card, self.change_card, self.cash_card, self.risk_card]
        self._populate_grid(self.top_row, cards, _HERO_COLUMNS[mode], equal_stretch=True)

    def _reflow_charts_row(self, mode: str) -> None:
        # Use whichever of canvas/placeholder is currently the live widget.
        # _swap_canvas keeps these attributes in sync.
        trend_widget = self._trend_canvas or self._trend_placeholder
        mix_widget = self._mix_canvas or self._mix_placeholder
        if mode in ("wide", "medium"):
            self._populate_grid(
                self.charts_row, [trend_widget, mix_widget], 2,
                col_stretches=[3, 2],
            )
        else:
            self._populate_grid(self.charts_row, [trend_widget, mix_widget], 1)

    def _reflow_perf_row(self, mode: str) -> None:
        drivers_widget = self._drivers_canvas or self._drivers_placeholder
        if mode in ("wide", "medium"):
            self._populate_grid(
                self.perf_row, [drivers_widget, self.cf_frame], 2,
                col_stretches=[3, 2],
            )
        else:
            self._populate_grid(self.perf_row, [drivers_widget, self.cf_frame], 1)

    def _reflow_re_metrics(self, mode: str) -> None:
        metrics = [
            self.re_props, self.re_value, self.re_equity,
            self.re_mortgage, self.re_ncf, self.re_ltv,
        ]
        self._populate_grid(self.re_metrics, metrics, _RE_COLUMNS[mode], equal_stretch=True)

    @staticmethod
    def _populate_grid(grid: QGridLayout, widgets, cols: int,
                       col_stretches=None, equal_stretch: bool = False) -> None:
        """Detach all current widgets from `grid`, reset stretches, then
        re-add `widgets` row-major in `cols` columns. Widgets are not
        deleted — their parent stays the layout's owning widget."""
        existing = []
        for i in range(grid.count()):
            item = grid.itemAt(i)
            w = item.widget() if item else None
            if w is not None:
                existing.append(w)
        for w in existing:
            grid.removeWidget(w)

        # Reset stretches over a generous range so a previous wider mode
        # doesn't leak column-stretch values into the new layout.
        for c in range(8):
            grid.setColumnStretch(c, 0)
        if equal_stretch:
            for c in range(cols):
                grid.setColumnStretch(c, 1)
        elif col_stretches:
            for c, s in enumerate(col_stretches):
                grid.setColumnStretch(c, s)

        for idx, w in enumerate(widgets):
            if w is None:
                continue
            row = idx // cols
            col = idx % cols
            grid.addWidget(w, row, col)

    # --- Card helpers ---

    def _update_card(self, card: QFrame, value: str, color: str = CARD_VALUE_COLOR, tooltip: str = ""):
        label = card.findChild(QLabel, "value")
        if label:
            label.setText(value)
            label.setStyleSheet(f"font-size: 18px; font-weight: bold; color: {color}; border: none;")
            label.setToolTip(tooltip)
            # Allow the label to shrink rather than push the card wider when
            # the value text is long (e.g. trillions or signed change values).
            label.setMinimumWidth(0)
            label.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)

    def _update_kv(self, widget: QWidget, value: str, color: str = CARD_VALUE_COLOR, tooltip: str = ""):
        label = widget.findChild(QLabel, "value")
        if label:
            label.setText(value)
            label.setStyleSheet(f"font-size: 15px; font-weight: bold; color: {color}; border: none;")
            label.setToolTip(tooltip)
            label.setMinimumWidth(0)
            label.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)

    # --- Event filter for chart scroll forwarding ---

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Type.Wheel:
            delta = event.pixelDelta()
            if delta.isNull():
                delta = event.angleDelta()
            if not delta.isNull():
                sb = self.scroll_area.verticalScrollBar()
                sb.setValue(sb.value() - delta.y())
            return True
        return super().eventFilter(obj, event)

    # --- Canvas helpers ---

    def _cleanup_figures(self):
        for attr in ('_trend_canvas', '_mix_canvas', '_drivers_canvas'):
            canvas = getattr(self, attr, None)
            if canvas is not None:
                canvas.removeEventFilter(self)
                fig = canvas.figure
                if fig is not None:
                    fig.clf()
                    fig.canvas = None
                canvas.deleteLater()
                setattr(self, attr, None)

    def _swap_canvas(self, attr: str, placeholder_attr: str, fig, min_height: int = 260, stretch: int = 3):
        new_canvas = FigureCanvasQTAgg(fig)
        new_canvas.setMinimumHeight(min_height)
        new_canvas.setStyleSheet("background: transparent;")
        new_canvas.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        new_canvas.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        new_canvas.installEventFilter(self)
        if attr == "_drivers_canvas":
            new_canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        old = getattr(self, attr)
        placeholder = getattr(self, placeholder_attr)
        layout = self.charts_row if attr != "_drivers_canvas" else self.perf_row

        if old is not None:
            old.removeEventFilter(self)
            old_fig = old.figure
            layout.replaceWidget(old, new_canvas)
            if old_fig is not None:
                old_fig.clf()
                old_fig.canvas = None
            old.deleteLater()
        else:
            layout.replaceWidget(placeholder, new_canvas)
            placeholder.deleteLater()

        setattr(self, attr, new_canvas)

    # --- Refresh ---

    def refresh(self):
        warnings = get_all_warnings(self.conn)
        self._refresh_summary(warnings)
        self._refresh_trend_chart()
        self._refresh_mix_chart()
        self._refresh_drivers_chart()
        self._refresh_cash_flow()
        self._refresh_real_estate()
        self._refresh_risk(warnings)
        self._refresh_activity()

    def _refresh_summary(self, warnings):
        s = get_dashboard_summary(self.conn, warnings=warnings)

        nw_color = _GREEN if s["net_worth"] >= 0 else _RED
        self._update_card(
            self.nw_card,
            fmt_money_compact(s["net_worth"]),
            nw_color,
            tooltip=fmt_money(s["net_worth"]),
        )

        cash_color = _RED if s["cash"] < 0 else CARD_VALUE_COLOR
        self._update_card(
            self.cash_card,
            fmt_money_compact(s["cash"]),
            cash_color,
            tooltip=fmt_money(s["cash"]),
        )

        trend = get_net_worth_trend(self.conn, days=30)
        if len(trend) >= 2:
            old_nw = trend[0]["net_worth"]
            cur_nw = trend[-1]["net_worth"]
            change = cur_nw - old_nw
            c_color = _GREEN if change >= 0 else _RED
            # fmt_money_compact already supplies the leading "-" for negatives;
            # we only prepend "+" for positive deltas to match the original
            # 30D-change behaviour.
            sign = "+" if change > 0 else ""
            self._update_card(
                self.change_card,
                f"{sign}{fmt_money_compact(change)}",
                c_color,
                tooltip=fmt_money(change),
            )
        else:
            self._update_card(self.change_card, "--", LABEL_MUTED_COLOR)

        wc = s["risk_warning_count"]
        if wc == 0:
            self._update_card(self.risk_card, "OK", _GREEN)
        else:
            self._update_card(self.risk_card, f"{wc} warning{'s' if wc != 1 else ''}", _RED)

    def _is_compact_chart_mode(self) -> bool:
        """Charts use the compact rendering profile in narrow / compact
        layout modes — fewer ticks, tighter margins, legends repositioned."""
        return self._current_layout_mode in ("narrow", "compact")

    def _refresh_trend_chart(self):
        trend = get_net_worth_trend(self.conn)
        fig = create_net_worth_trend_figure(trend, compact=self._is_compact_chart_mode())
        self._swap_canvas("_trend_canvas", "_trend_placeholder", fig, 260, 3)

    def _refresh_mix_chart(self):
        items = calc_asset_pie_breakdown(self.conn)
        fig = create_asset_mix_figure(items, compact=self._is_compact_chart_mode())
        self._swap_canvas("_mix_canvas", "_mix_placeholder", fig, 260, 2)

    def _refresh_drivers_chart(self):
        drivers = get_return_drivers(self.conn)
        fig = create_return_drivers_figure(
            drivers["gainers"], drivers["losers"],
            compact=self._is_compact_chart_mode(),
        )
        self._swap_canvas("_drivers_canvas", "_drivers_placeholder", fig, 200, 3)

    def _refresh_cash_flow(self):
        cf = get_cash_flow_snapshot(self.conn)
        self._update_kv(
            self.cf_inflow, fmt_money_compact(cf["inflow"]), _GREEN,
            tooltip=fmt_money(cf["inflow"]),
        )
        self._update_kv(
            self.cf_outflow, fmt_money_compact(cf["outflow"]), _RED,
            tooltip=fmt_money(cf["outflow"]),
        )
        net_color = _GREEN if cf["net_cash_flow"] >= 0 else _RED
        self._update_kv(
            self.cf_net, fmt_money_compact(cf["net_cash_flow"]), net_color,
            tooltip=fmt_money(cf["net_cash_flow"]),
        )
        self._update_kv(
            self.cf_fees, fmt_money_compact(cf["fees"]),
            tooltip=fmt_money(cf["fees"]),
        )
        self._update_kv(self.cf_count, str(cf["transaction_count"]))

    def _refresh_real_estate(self):
        snap = get_real_estate_snapshot(self.conn)
        if snap is None:
            self.re_frame.setVisible(False)
            return

        self.re_frame.setVisible(True)
        self._update_kv(self.re_props, str(snap["property_count"]))
        self._update_kv(
            self.re_value, fmt_money_compact(snap["total_property_value"]),
            tooltip=fmt_money(snap["total_property_value"]),
        )
        self._update_kv(
            self.re_equity, fmt_money_compact(snap["total_equity"]),
            tooltip=fmt_money(snap["total_equity"]),
        )
        self._update_kv(
            self.re_mortgage, fmt_money_compact(snap["total_mortgage"]),
            tooltip=fmt_money(snap["total_mortgage"]),
        )
        ncf = snap["monthly_net_cash_flow"]
        ncf_color = _GREEN if ncf >= 0 else _RED
        self._update_kv(
            self.re_ncf, fmt_money_compact(ncf), ncf_color,
            tooltip=fmt_money(ncf),
        )
        self._update_kv(self.re_ltv, fmt_pct(snap["average_ltv"]))

    def _refresh_risk(self, warnings):
        actionable = [w for w in warnings if w.severity != "info"][:3]

        if not actionable:
            self.risk_ok_label.setVisible(True)
            for lbl in self.risk_labels:
                lbl.setVisible(False)
            return

        self.risk_ok_label.setVisible(False)
        for i, lbl in enumerate(self.risk_labels):
            if i < len(actionable):
                w = actionable[i]
                color = _WARN_COLORS.get(w.severity, CARD_VALUE_COLOR)
                sev = w.severity.upper()
                lbl.setText(f"[{sev}] {w.message}")
                lbl.setStyleSheet(f"font-size: 12px; color: {color}; border: none; padding: 2px 0;")
                lbl.setVisible(True)
            else:
                lbl.setVisible(False)

    def _refresh_activity(self):
        txns = get_recent_activity(self.conn)
        self.activity_table.setRowCount(len(txns))
        for i, t in enumerate(txns):
            self.activity_table.setItem(i, 0, QTableWidgetItem(t["date"]))
            self.activity_table.setItem(i, 1, QTableWidgetItem(format_transaction_type(t["txn_type"])))
            asset_str = t["asset_symbol"] or ""
            self.activity_table.setItem(i, 2, QTableWidgetItem(asset_str))
            self.activity_table.setItem(i, 3, QTableWidgetItem(fmt_money(t["amount"])))
            self.activity_table.setItem(i, 4, QTableWidgetItem(fmt_money(t["fees"])))
        resize_table_to_contents(self.activity_table)
