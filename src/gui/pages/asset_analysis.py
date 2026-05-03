import sqlite3
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QTableWidgetItem, QScrollArea,
    QRadioButton, QButtonGroup,
)
from PySide6.QtCore import Qt
from PySide6.QtGui import QColor

from src.gui.widgets.scroll_friendly_canvas import ScrollFriendlyCanvas
from src.gui.widgets.common import (
    make_header, make_table, fmt_money, fmt_pct, fmt_qty,
    configure_expanding_table, resize_table_to_contents,
)
from src.utils.display import format_asset_type
from src.engines.portfolio import calc_positions, get_portfolio_summary
from src.engines.allocation import (
    calc_allocation_by_asset_type,
    calc_allocation_by_asset,
    calc_allocation_by_liquidity,
    calc_asset_pie_breakdown,
    get_full_allocation,
)
from src.engines.debt_math import compute_debt_schedule, normalize_period_to_months
from src.engines.cashflow import compute_cashflow_series
from src.storage.debt_repo import list_debts
from src.charts.allocation_pie import create_asset_pie_figure, connect_pie_hover
from src.charts.cashflow import create_cashflow_bar_figure

_GREEN = "#2e7d32"
_RED = "#c62828"


class AssetAnalysisPage(QWidget):
    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)
        content = QWidget()
        self.layout = QVBoxLayout(content)
        self.layout.setContentsMargins(24, 16, 24, 16)
        self.layout.setSpacing(16)

        self.layout.addWidget(make_header("Asset Analysis"))

        # Granularity toggle for cashflow analysis (controls both the
        # chart and the table at the bottom of the page).
        self._granularity = "monthly"
        toggle_row = QHBoxLayout()
        toggle_row.setSpacing(8)
        toggle_row.addWidget(QLabel("Cashflow:"))
        self._monthly_btn = QRadioButton("Monthly")
        self._yearly_btn = QRadioButton("Yearly")
        self._monthly_btn.setChecked(True)
        self._granularity_group = QButtonGroup(self)
        self._granularity_group.addButton(self._monthly_btn)
        self._granularity_group.addButton(self._yearly_btn)
        toggle_row.addWidget(self._monthly_btn)
        toggle_row.addWidget(self._yearly_btn)
        toggle_row.addStretch()
        # Connect both buttons so we react to either edge of the toggle.
        # The handler short-circuits on the False half so each click does
        # work exactly once.
        self._monthly_btn.toggled.connect(self._on_granularity_toggled)
        self._yearly_btn.toggled.connect(self._on_granularity_toggled)
        self.layout.addLayout(toggle_row)

        # Pie chart and cashflow chart sit side-by-side in an HBox so the
        # right-of-pie space is used. Both share min-height 400 so the pie
        # — constrained by `set_aspect("equal")` — can render at a usable
        # diameter inside its half of the row.
        charts_row = QHBoxLayout()
        charts_row.setSpacing(12)
        self.canvas = None
        self._chart_placeholder = QWidget()
        self._chart_placeholder.setStyleSheet("background: transparent;")
        self._chart_placeholder.setMinimumHeight(400)
        charts_row.addWidget(self._chart_placeholder, 1)

        self._cashflow_canvas = None
        self._cashflow_chart_placeholder = QWidget()
        self._cashflow_chart_placeholder.setStyleSheet("background: transparent;")
        self._cashflow_chart_placeholder.setMinimumHeight(400)
        charts_row.addWidget(self._cashflow_chart_placeholder, 1)
        self._charts_row = charts_row
        self.layout.addLayout(charts_row)

        # Balance Sheet Breakdown
        self.layout.addWidget(QLabel("Balance Sheet Breakdown"))
        self.bs_table = make_table(["Metric", "Value"], stretch_last=True)
        configure_expanding_table(self.bs_table)
        self.layout.addWidget(self.bs_table)

        # Positions table
        self.layout.addWidget(QLabel("Positions"))
        self.pos_table = make_table([
            "Symbol", "Name", "Type", "Quantity", "Avg Price",
            "Cost Basis", "Current Price", "Market Value", "Unrealized P&L",
        ])
        configure_expanding_table(self.pos_table)
        self.layout.addWidget(self.pos_table)

        # Allocation by asset type
        self.layout.addWidget(QLabel("By Asset Type"))
        self.type_table = make_table(["Asset Type", "Value", "Percentage"])
        configure_expanding_table(self.type_table)
        self.layout.addWidget(self.type_table)

        # Allocation by individual asset
        self.layout.addWidget(QLabel("By Individual Asset"))
        self.asset_table = make_table(["Name", "Type", "Value", "Percentage"])
        configure_expanding_table(self.asset_table)
        self.layout.addWidget(self.asset_table)

        # Liquid vs illiquid
        self.layout.addWidget(QLabel("Liquid vs Illiquid"))
        self.liq_table = make_table(["Category", "Value", "Percentage"])
        configure_expanding_table(self.liq_table)
        self.layout.addWidget(self.liq_table)

        # Debts
        self.layout.addWidget(QLabel("Debts"))
        self.debt_table = make_table([
            "Name", "Balance", "Annual Rate", "Schedule",
            "Per-period Payment", "Periods Left", "Months to Payoff",
        ])
        configure_expanding_table(self.debt_table)
        self.layout.addWidget(self.debt_table)

        # Mortgages — parallel to Debts. Same columns minus Schedule
        # (mortgages are monthly-only).
        self.layout.addWidget(QLabel("Mortgages"))
        self.mortgage_table = make_table([
            "Property", "Balance", "Annual Rate",
            "Monthly Payment", "Months Left", "Total Paid (plan)",
            "Total Interest (plan)",
        ])
        configure_expanding_table(self.mortgage_table)
        self.layout.addWidget(self.mortgage_table)

        # Cashflow Breakdown table — period-by-period totals split into
        # the same 5 categories shown in the chart above. Net column is
        # color-coded green for positive, red for negative.
        self.layout.addWidget(QLabel("Cashflow Breakdown"))
        self.cashflow_table = make_table([
            "Period", "Funding", "Trades", "Real Estate",
            "Debt", "Other", "Net",
        ])
        configure_expanding_table(self.cashflow_table)
        self.layout.addWidget(self.cashflow_table)

        self.layout.addStretch()
        scroll.setWidget(content)
        outer.addWidget(scroll)

    def _cleanup_figures(self):
        if self.canvas is not None:
            cid = getattr(self.canvas, "_pie_hover_cid", None)
            if cid is not None:
                try:
                    self.canvas.mpl_disconnect(cid)
                except Exception:
                    pass
            fig = self.canvas.figure
            if fig is not None:
                if hasattr(fig, '_pie_hover_data'):
                    del fig._pie_hover_data
                fig.clf()
                fig.canvas = None
            self.canvas.deleteLater()
            self.canvas = None
        if self._cashflow_canvas is not None:
            fig = self._cashflow_canvas.figure
            if fig is not None:
                fig.clf()
                fig.canvas = None
            self._cashflow_canvas.deleteLater()
            self._cashflow_canvas = None

    def _refresh_chart(self):
        items = calc_asset_pie_breakdown(self.conn)
        # Compact mode (legend below) since the pie now shares its row with
        # the cashflow chart — keeping the side legend would crush the pie.
        fig = create_asset_pie_figure(items, compact=True)

        new_canvas = ScrollFriendlyCanvas(fig)
        new_canvas.setMinimumHeight(400)
        new_canvas.setStyleSheet("background: transparent;")
        new_canvas.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        cid = connect_pie_hover(new_canvas)
        if cid is not None:
            new_canvas._pie_hover_cid = cid

        if self.canvas is not None:
            old_canvas = self.canvas
            old_fig = old_canvas.figure
            old_cid = getattr(old_canvas, "_pie_hover_cid", None)
            self.layout.replaceWidget(old_canvas, new_canvas)
            if old_cid is not None:
                try:
                    old_canvas.mpl_disconnect(old_cid)
                except Exception:
                    pass
            if old_fig is not None:
                if hasattr(old_fig, '_pie_hover_data'):
                    del old_fig._pie_hover_data
                old_fig.clf()
                old_fig.canvas = None
            old_canvas.deleteLater()
        else:
            self.layout.replaceWidget(self._chart_placeholder, new_canvas)
            self._chart_placeholder.deleteLater()

        self.canvas = new_canvas

    def _refresh_positions(self):
        positions = calc_positions(self.conn)
        self.pos_table.setRowCount(len(positions))
        for i, p in enumerate(positions):
            self.pos_table.setItem(i, 0, QTableWidgetItem(p.symbol))
            self.pos_table.setItem(i, 1, QTableWidgetItem(p.name))
            self.pos_table.setItem(i, 2, QTableWidgetItem(format_asset_type(p.asset_type)))
            self.pos_table.setItem(i, 3, QTableWidgetItem(fmt_qty(p.quantity)))
            self.pos_table.setItem(i, 4, QTableWidgetItem(fmt_money(p.average_price)))
            self.pos_table.setItem(i, 5, QTableWidgetItem(fmt_money(p.cost_basis)))
            self.pos_table.setItem(i, 6, QTableWidgetItem(fmt_money(p.current_price)))
            self.pos_table.setItem(i, 7, QTableWidgetItem(fmt_money(p.market_value)))
            self.pos_table.setItem(i, 8, QTableWidgetItem(fmt_money(p.unrealized_pnl)))
        resize_table_to_contents(self.pos_table)

    def _refresh_allocation(self):
        by_type = calc_allocation_by_asset_type(self.conn)
        self.type_table.setRowCount(len(by_type))
        for i, (atype, data) in enumerate(by_type.items()):
            self.type_table.setItem(i, 0, QTableWidgetItem(format_asset_type(atype)))
            self.type_table.setItem(i, 1, QTableWidgetItem(fmt_money(data["value"])))
            self.type_table.setItem(i, 2, QTableWidgetItem(fmt_pct(data["pct"])))
        resize_table_to_contents(self.type_table)

        by_asset = calc_allocation_by_asset(self.conn)
        self.asset_table.setRowCount(len(by_asset))
        for i, item in enumerate(by_asset):
            self.asset_table.setItem(i, 0, QTableWidgetItem(item["name"]))
            self.asset_table.setItem(i, 1, QTableWidgetItem(format_asset_type(item["asset_type"])))
            self.asset_table.setItem(i, 2, QTableWidgetItem(fmt_money(item["value"])))
            self.asset_table.setItem(i, 3, QTableWidgetItem(fmt_pct(item["pct"])))
        resize_table_to_contents(self.asset_table)

        liq = calc_allocation_by_liquidity(self.conn)
        self.liq_table.setRowCount(2)
        for i, (cat, data) in enumerate(liq.items()):
            self.liq_table.setItem(i, 0, QTableWidgetItem(cat.title()))
            self.liq_table.setItem(i, 1, QTableWidgetItem(fmt_money(data["value"])))
            self.liq_table.setItem(i, 2, QTableWidgetItem(fmt_pct(data["pct"])))
        resize_table_to_contents(self.liq_table)

    def _refresh_balance_sheet(self):
        summary = get_portfolio_summary(self.conn)
        alloc = get_full_allocation(self.conn)

        rows = [
            ("Cash", fmt_money(summary["cash"])),
            ("Positions Value", fmt_money(summary["positions_value"])),
            ("Property Value", fmt_money(summary["property_value"])),
            ("Total Assets", fmt_money(summary["total_assets"])),
            ("Mortgage", fmt_money(summary["mortgage"])),
            ("Other Debt", fmt_money(summary["debt"])),
            ("Total Liabilities", fmt_money(summary["total_liabilities"])),
            ("Net Worth", fmt_money(summary["net_worth"])),
            ("Liquid Assets", fmt_money(alloc["liquid_assets"])),
            ("Illiquid Assets", fmt_money(alloc["illiquid_assets"])),
            ("Debt Ratio", fmt_pct(alloc["debt_ratio"])),
            ("Cash %", fmt_pct(alloc["cash_pct"])),
            ("Crypto %", fmt_pct(alloc["crypto_pct"])),
            ("Real Estate Equity %", fmt_pct(alloc["real_estate_equity_pct"])),
        ]

        self.bs_table.setRowCount(len(rows))
        for i, (metric, value) in enumerate(rows):
            self.bs_table.setItem(i, 0, QTableWidgetItem(metric))
            self.bs_table.setItem(i, 1, QTableWidgetItem(value))
        resize_table_to_contents(self.bs_table)

    def _refresh_debts(self):
        debts = list_debts(self.conn)
        self.debt_table.setRowCount(len(debts))
        for i, d in enumerate(debts):
            sched = (
                compute_debt_schedule(
                    principal=d.current_balance,
                    annual_rate=d.interest_rate,
                    schedule=d.schedule_frequency,
                    payment=d.monthly_payment_amount,
                )
                if d.current_balance > 0 and d.monthly_payment_amount > 0
                else None
            )
            if sched and sched.feasible:
                periods_left = str(sched.num_periods)
                months_left = str(normalize_period_to_months(
                    sched.num_periods, d.schedule_frequency,
                ))
            elif d.current_balance <= 0:
                periods_left = "0"
                months_left = "0"
            else:
                periods_left = "∞"
                months_left = "∞"
            self.debt_table.setItem(i, 0, QTableWidgetItem(d.name or "(unnamed)"))
            self.debt_table.setItem(i, 1, QTableWidgetItem(fmt_money(d.current_balance)))
            self.debt_table.setItem(i, 2, QTableWidgetItem(fmt_pct(d.interest_rate)))
            self.debt_table.setItem(i, 3, QTableWidgetItem(d.schedule_frequency.title()))
            self.debt_table.setItem(i, 4, QTableWidgetItem(fmt_money(d.monthly_payment_amount)))
            self.debt_table.setItem(i, 5, QTableWidgetItem(periods_left))
            self.debt_table.setItem(i, 6, QTableWidgetItem(months_left))
        resize_table_to_contents(self.debt_table)

    def _refresh_mortgages(self):
        from src.storage.mortgage_repo import list_mortgages
        from src.storage.property_repo import get_property
        from src.storage.asset_repo import get_asset
        mortgages = list_mortgages(self.conn)
        self.mortgage_table.setRowCount(len(mortgages))
        for i, m in enumerate(mortgages):
            prop = get_property(self.conn, m.property_id)
            asset = get_asset(self.conn, prop.asset_id) if prop is not None else None
            display_name = (
                asset.name if asset is not None
                else (m.name or f"Mortgage {m.id}")
            )
            # Use the stored 5 preview values rather than recomputing on
            # every render. Paid-off mortgages have all zeros (∞ display
            # not needed since the row's balance == 0 distinguishes it).
            months_left = (
                str(m.preview_period_count) if m.current_balance > 0 else "0"
            )
            self.mortgage_table.setItem(i, 0, QTableWidgetItem(display_name))
            self.mortgage_table.setItem(i, 1, QTableWidgetItem(fmt_money(m.current_balance)))
            self.mortgage_table.setItem(i, 2, QTableWidgetItem(fmt_pct(m.interest_rate)))
            self.mortgage_table.setItem(i, 3, QTableWidgetItem(fmt_money(m.monthly_payment_amount)))
            self.mortgage_table.setItem(i, 4, QTableWidgetItem(months_left))
            self.mortgage_table.setItem(i, 5, QTableWidgetItem(fmt_money(m.preview_total_paid)))
            self.mortgage_table.setItem(i, 6, QTableWidgetItem(fmt_money(m.preview_total_interest)))
        resize_table_to_contents(self.mortgage_table)

    def _on_granularity_toggled(self, checked: bool):
        # Both QRadioButton.toggled signals fire on each change (one True,
        # one False); only act on the True transition to avoid double work.
        if not checked:
            return
        self._granularity = "monthly" if self._monthly_btn.isChecked() else "yearly"
        self._refresh_cashflow_chart()
        self._refresh_cashflow_table()

    def _refresh_cashflow_chart(self):
        periods = compute_cashflow_series(self.conn, self._granularity)
        fig = create_cashflow_bar_figure(periods, granularity=self._granularity)

        new_canvas = ScrollFriendlyCanvas(fig)
        new_canvas.setMinimumHeight(400)
        new_canvas.setStyleSheet("background: transparent;")
        new_canvas.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

        if self._cashflow_canvas is not None:
            old = self._cashflow_canvas
            old_fig = old.figure
            self._charts_row.replaceWidget(old, new_canvas)
            if old_fig is not None:
                old_fig.clf()
                old_fig.canvas = None
            old.deleteLater()
        else:
            self._charts_row.replaceWidget(self._cashflow_chart_placeholder, new_canvas)
            self._cashflow_chart_placeholder.deleteLater()

        self._cashflow_canvas = new_canvas

    def _refresh_cashflow_table(self):
        periods = compute_cashflow_series(self.conn, self._granularity)
        self.cashflow_table.setRowCount(len(periods))
        for i, p in enumerate(periods):
            self.cashflow_table.setItem(i, 0, QTableWidgetItem(p.label))
            self.cashflow_table.setItem(i, 1, QTableWidgetItem(fmt_money(p.funding_flow)))
            self.cashflow_table.setItem(i, 2, QTableWidgetItem(fmt_money(p.trade_cash_flow)))
            self.cashflow_table.setItem(i, 3, QTableWidgetItem(fmt_money(p.real_estate_cash_flow)))
            self.cashflow_table.setItem(i, 4, QTableWidgetItem(fmt_money(p.debt_cash_flow)))
            self.cashflow_table.setItem(i, 5, QTableWidgetItem(fmt_money(p.other_cash_flow)))
            net_item = QTableWidgetItem(fmt_money(p.net))
            if p.net > 0:
                net_item.setForeground(QColor(_GREEN))
            elif p.net < 0:
                net_item.setForeground(QColor(_RED))
            self.cashflow_table.setItem(i, 6, net_item)
        resize_table_to_contents(self.cashflow_table)

    def refresh(self):
        self._refresh_chart()
        self._refresh_balance_sheet()
        self._refresh_positions()
        self._refresh_allocation()
        self._refresh_debts()
        self._refresh_mortgages()
        self._refresh_cashflow_chart()
        self._refresh_cashflow_table()
