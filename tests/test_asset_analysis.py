import pytest
from matplotlib.figure import Figure

from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.price_repo import upsert_price
from src.engines import ledger
from src.engines.allocation import calc_asset_pie_breakdown, calc_allocation_by_asset
from src.engines.portfolio import calc_positions
from src.charts.allocation_pie import create_asset_pie_figure


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def portfolio(db_conn):
    """Mixed portfolio: cash 50k, VTI 20k, BTC 30k, house 500k (400k mortgage)."""
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)

    vti = create_asset(db_conn, Asset(symbol="VTI", name="Vanguard Total", asset_type="etf"))
    ledger.buy(db_conn, "2025-01-15", vti.id, quantity=100, price=200.0)

    btc = create_asset(db_conn, Asset(
        symbol="BTC", name="Bitcoin", asset_type="crypto", currency="USD", region="Global",
    ))
    ledger.buy(db_conn, "2025-01-20", btc.id, quantity=1, price=30000.0)

    ledger.add_property(
        db_conn, "2025-02-01", symbol="HOUSE", name="My House",
        purchase_price=500000.0, mortgage_balance=400000.0,
        monthly_expense=2500.0,
    )

    return db_conn


# ===================================================================
# 1. Engine tests — calc_asset_pie_breakdown
# ===================================================================

class TestPieBreakdownMixedPortfolio:
    def test_returns_four_items(self, portfolio):
        items = calc_asset_pie_breakdown(portfolio)
        assert len(items) == 4

    def test_item_names(self, portfolio):
        items = calc_asset_pie_breakdown(portfolio)
        names = {i["name"] for i in items}
        assert "Cash" in names
        assert any("VTI" in n for n in names)
        assert any("BTC" in n for n in names)
        assert any("HOUSE" in n or "House" in n for n in names)

    def test_percentages_sum_to_one(self, portfolio):
        items = calc_asset_pie_breakdown(portfolio)
        total = sum(i["pct"] for i in items)
        assert abs(total - 1.0) < 1e-9

    def test_sorted_descending_by_value(self, portfolio):
        items = calc_asset_pie_breakdown(portfolio)
        values = [i["value"] for i in items]
        assert values == sorted(values, reverse=True)

    def test_all_values_positive(self, portfolio):
        items = calc_asset_pie_breakdown(portfolio)
        for item in items:
            assert item["value"] > 0

    def test_expected_values(self, portfolio):
        items = calc_asset_pie_breakdown(portfolio)
        by_name = {i["name"]: i["value"] for i in items}
        assert by_name["Cash"] == 50000.0
        vti_val = next(v for k, v in by_name.items() if "VTI" in k)
        assert vti_val == 20000.0
        btc_val = next(v for k, v in by_name.items() if "BTC" in k)
        assert btc_val == 30000.0
        house_val = next(v for k, v in by_name.items() if "HOUSE" in k or "House" in k)
        assert house_val == 500000.0


class TestPieBreakdownMarketValue:
    def test_uses_market_value_when_price_exists(self, portfolio):
        vti_id = portfolio.execute(
            "SELECT id FROM assets WHERE symbol = 'VTI'"
        ).fetchone()["id"]
        upsert_price(portfolio, vti_id, "2025-03-01", 250.0)

        items = calc_asset_pie_breakdown(portfolio)
        vti_item = next(i for i in items if "VTI" in i["name"])
        assert vti_item["value"] == 25000.0  # 100 * 250

    def test_falls_back_to_cost_basis_without_price(self, portfolio):
        items = calc_asset_pie_breakdown(portfolio)
        vti_item = next(i for i in items if "VTI" in i["name"])
        assert vti_item["value"] == 20000.0  # 100 * 200 (cost basis)


class TestPieBreakdownExclusions:
    def test_excludes_zero_cash(self, db_conn):
        items = calc_asset_pie_breakdown(db_conn)
        assert len(items) == 0

    def test_excludes_negative_cash(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="X", name="Expensive", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-02", asset.id, quantity=1000, price=100.0)
        # cash = 100k - 100k = 0, position = 100k
        items = calc_asset_pie_breakdown(db_conn)
        names = [i["name"] for i in items]
        assert "Cash" not in names

    def test_debt_not_in_pie(self, portfolio):
        ledger.add_debt(
            portfolio, "2025-03-01", symbol="CC", name="Credit Card",
            amount=10000.0, cash_received=False,
        )
        items = calc_asset_pie_breakdown(portfolio)
        for item in items:
            assert item["asset_type"] != "debt"

    def test_debt_with_cash_received_affects_cash_not_pie_type(self, db_conn):
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="LOAN", name="Personal Loan",
            amount=50000.0, cash_received=True,
        )
        items = calc_asset_pie_breakdown(db_conn)
        assert len(items) == 1
        assert items[0]["name"] == "Cash"
        assert items[0]["value"] == 50000.0

    def test_percentages_recomputed_for_positive_only(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        asset = create_asset(db_conn, Asset(symbol="A", name="Stock A", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-02", asset.id, quantity=250, price=200.0)
        # cash = 50k - 50k = 0, position = 50k
        # only positive item is the position
        items = calc_asset_pie_breakdown(db_conn)
        assert len(items) == 1
        assert abs(items[0]["pct"] - 1.0) < 1e-9


class TestPieBreakdownEmpty:
    def test_empty_portfolio_returns_empty(self, db_conn):
        items = calc_asset_pie_breakdown(db_conn)
        assert items == []

    def test_cash_only(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
        items = calc_asset_pie_breakdown(db_conn)
        assert len(items) == 1
        assert items[0]["name"] == "Cash"
        assert items[0]["value"] == 10000.0
        assert abs(items[0]["pct"] - 1.0) < 1e-9


# ===================================================================
# 2. Chart tests — create_asset_pie_figure
# ===================================================================

class TestPieChart:
    def test_empty_returns_figure(self):
        fig = create_asset_pie_figure([])
        assert isinstance(fig, Figure)

    def test_empty_does_not_crash(self):
        fig = create_asset_pie_figure([])
        assert len(fig.get_axes()) == 1

    def test_non_empty_returns_figure(self):
        items = [
            {"name": "Cash", "value": 50000, "pct": 0.5, "asset_type": "cash"},
            {"name": "VTI", "value": 50000, "pct": 0.5, "asset_type": "etf"},
        ]
        fig = create_asset_pie_figure(items)
        assert isinstance(fig, Figure)

    def test_non_empty_has_axes(self):
        items = [
            {"name": "Cash", "value": 50000, "pct": 0.5, "asset_type": "cash"},
            {"name": "VTI", "value": 50000, "pct": 0.5, "asset_type": "etf"},
        ]
        fig = create_asset_pie_figure(items)
        assert len(fig.get_axes()) >= 1

    def test_single_item(self):
        items = [{"name": "Cash", "value": 100, "pct": 1.0, "asset_type": "cash"}]
        fig = create_asset_pie_figure(items)
        assert isinstance(fig, Figure)

    def test_many_items_wraps_palette(self):
        items = [
            {"name": f"Asset {i}", "value": 100 - i, "pct": 1 / 25, "asset_type": "stock"}
            for i in range(25)
        ]
        fig = create_asset_pie_figure(items)
        assert isinstance(fig, Figure)


# ===================================================================
# 3. GUI smoke tests — AssetAnalysisPage
# ===================================================================

class TestAssetAnalysisPageSmoke:
    def test_instantiation(self, qapp, db_conn):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(db_conn)
        assert page is not None

    def test_refresh_empty_db(self, qapp, db_conn):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(db_conn)
        page.refresh()
        assert page.pos_table.rowCount() == 0
        assert page.asset_table.rowCount() == 0

    def test_refresh_mixed_portfolio(self, qapp, portfolio):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(portfolio)
        page.refresh()

        positions = calc_positions(portfolio)
        assert page.pos_table.rowCount() == len(positions)

        by_asset = calc_allocation_by_asset(portfolio)
        assert page.asset_table.rowCount() == len(by_asset)

    def test_canvas_exists_after_refresh(self, qapp, portfolio):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(portfolio)
        page.refresh()
        assert page.canvas is not None

    def test_double_refresh_no_crash(self, qapp, portfolio):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(portfolio)
        page.refresh()
        page.refresh()
        assert page.canvas is not None

    def test_canvas_exists_empty_db(self, qapp, db_conn):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(db_conn)
        page.refresh()
        assert page.canvas is not None


# ===================================================================
# 4. Main window / navigation smoke tests
# ===================================================================

class TestMainWindowNavigation:
    def test_page_labels_contains_asset_analysis(self):
        from src.gui.main_window import PAGE_LABELS
        assert "Asset Analysis" in PAGE_LABELS

    def test_page_labels_no_positions(self):
        from src.gui.main_window import PAGE_LABELS
        assert "Positions" not in PAGE_LABELS

    def test_page_labels_no_allocation(self):
        from src.gui.main_window import PAGE_LABELS
        assert "Allocation" not in PAGE_LABELS

    def test_main_window_instantiation(self, qapp, db_conn):
        from src.gui.main_window import MainWindow, PAGE_LABELS
        window = MainWindow(db_conn, enable_startup_sync=False)
        assert window is not None

    def test_page_count_matches_labels(self, qapp, db_conn):
        from src.gui.main_window import MainWindow, PAGE_LABELS
        window = MainWindow(db_conn, enable_startup_sync=False)
        assert len(window.page_widgets) == len(PAGE_LABELS)
        assert window.pages.count() == len(PAGE_LABELS)


# ===================================================================
# 5. Balance Sheet Breakdown tests
# ===================================================================

class TestBalanceSheetBreakdown:
    def test_bs_table_exists(self, qapp, db_conn):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(db_conn)
        assert hasattr(page, "bs_table")

    def test_bs_table_empty_db(self, qapp, db_conn):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(db_conn)
        page.refresh()
        assert page.bs_table.rowCount() == 14

    def test_bs_table_row_count(self, qapp, portfolio):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(portfolio)
        page.refresh()
        assert page.bs_table.rowCount() == 14

    def test_bs_table_metric_labels(self, qapp, db_conn):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(db_conn)
        page.refresh()
        metrics = [
            page.bs_table.item(i, 0).text()
            for i in range(page.bs_table.rowCount())
        ]
        assert "Cash" in metrics
        assert "Positions Value" in metrics
        assert "Property Value" in metrics
        assert "Total Assets" in metrics
        assert "Mortgage" in metrics
        assert "Other Debt" in metrics
        assert "Total Liabilities" in metrics
        assert "Net Worth" in metrics
        assert "Liquid Assets" in metrics
        assert "Illiquid Assets" in metrics
        assert "Debt Ratio" in metrics
        assert "Cash %" in metrics
        assert "Crypto %" in metrics
        assert "Real Estate Equity %" in metrics

    def test_bs_table_values_mixed_portfolio(self, qapp, portfolio):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(portfolio)
        page.refresh()
        values = {
            page.bs_table.item(i, 0).text(): page.bs_table.item(i, 1).text()
            for i in range(page.bs_table.rowCount())
        }
        assert values["Cash"] == "$50,000.00"
        assert values["Total Assets"] == "$600,000.00"
        assert values["Mortgage"] == "$400,000.00"
        assert values["Net Worth"] == "$200,000.00"

    def test_bs_table_empty_portfolio_values(self, qapp, db_conn):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(db_conn)
        page.refresh()
        values = {
            page.bs_table.item(i, 0).text(): page.bs_table.item(i, 1).text()
            for i in range(page.bs_table.rowCount())
        }
        assert values["Cash"] == "$0.00"
        assert values["Net Worth"] == "$0.00"
        assert values["Debt Ratio"] == "0.0%"

    def test_bs_table_before_positions(self, qapp, db_conn):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(db_conn)
        bs_idx = page.layout.indexOf(page.bs_table)
        pos_idx = page.layout.indexOf(page.pos_table)
        assert bs_idx < pos_idx

    def test_existing_tables_still_present(self, qapp, portfolio):
        from src.gui.pages.asset_analysis import AssetAnalysisPage
        page = AssetAnalysisPage(portfolio)
        page.refresh()
        assert page.pos_table.rowCount() > 0
        assert page.type_table.rowCount() > 0
        assert page.asset_table.rowCount() > 0
        assert page.liq_table.rowCount() == 2
        assert page.canvas is not None
