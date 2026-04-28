import sqlite3
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QTableWidgetItem, QScrollArea,
)
from PySide6.QtCore import Qt
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg

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
from src.charts.allocation_pie import create_asset_pie_figure, connect_pie_hover


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

        self.canvas = None
        self._chart_placeholder = QWidget()
        self._chart_placeholder.setStyleSheet("background: transparent;")
        self.layout.addWidget(self._chart_placeholder)

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

    def _refresh_chart(self):
        items = calc_asset_pie_breakdown(self.conn)
        fig = create_asset_pie_figure(items)

        new_canvas = FigureCanvasQTAgg(fig)
        new_canvas.setMinimumHeight(320)
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

    def refresh(self):
        self._refresh_chart()
        self._refresh_balance_sheet()
        self._refresh_positions()
        self._refresh_allocation()
