import pytest
from datetime import date

from matplotlib.figure import Figure

from src.models.asset import Asset
from src.models.portfolio_snapshot import PortfolioSnapshot
from src.storage.asset_repo import create_asset
from src.storage.price_repo import upsert_price
from src.storage.snapshot_repo import create_snapshot
from src.engines import ledger
from src.engines.dashboard import (
    get_dashboard_summary,
    get_net_worth_trend,
    get_cash_flow_snapshot,
    get_return_drivers,
    get_recent_activity,
    get_real_estate_snapshot,
)


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
# 1. get_dashboard_summary
# ===================================================================

class TestDashboardSummary:
    def test_keys(self, portfolio):
        s = get_dashboard_summary(portfolio)
        expected = [
            "net_worth", "total_assets", "total_liabilities", "cash",
            "position_value", "property_value", "mortgage", "debt",
            "risk_warning_count", "top_risk_message",
        ]
        for k in expected:
            assert k in s, f"Missing key: {k}"

    def test_values_mixed_portfolio(self, portfolio):
        s = get_dashboard_summary(portfolio)
        assert s["cash"] == 50000.0
        assert s["position_value"] == 50000.0  # VTI 20k + BTC 30k at cost
        assert s["property_value"] == 500000.0
        assert s["total_assets"] == 600000.0
        assert s["mortgage"] == 400000.0
        assert s["debt"] == 0.0
        assert s["total_liabilities"] == 400000.0
        assert s["net_worth"] == 200000.0

    def test_empty_portfolio(self, db_conn):
        s = get_dashboard_summary(db_conn)
        assert s["net_worth"] == 0.0
        assert s["total_assets"] == 0.0
        assert s["cash"] == 0.0
        assert s["risk_warning_count"] == 0
        assert s["top_risk_message"] is None

    def test_risk_warning_count_excludes_info(self, portfolio):
        s = get_dashboard_summary(portfolio)
        from src.engines.risk import get_all_warnings
        all_warnings = get_all_warnings(portfolio)
        info_count = len([w for w in all_warnings if w.severity == "info"])
        non_info_count = len(all_warnings) - info_count
        assert s["risk_warning_count"] == non_info_count

    def test_top_risk_message_is_string_when_present(self, portfolio):
        s = get_dashboard_summary(portfolio)
        if s["risk_warning_count"] > 0:
            assert isinstance(s["top_risk_message"], str)
            assert len(s["top_risk_message"]) > 0

    def test_with_debt(self, portfolio):
        ledger.add_debt(
            portfolio, "2025-03-01", symbol="CC", name="Credit Card",
            amount=10000.0, cash_received=False,
        )
        s = get_dashboard_summary(portfolio)
        assert s["debt"] == 10000.0
        assert s["total_liabilities"] == 410000.0


# ===================================================================
# 2. get_net_worth_trend
# ===================================================================

class TestNetWorthTrend:
    def test_empty_no_snapshots(self, db_conn):
        result = get_net_worth_trend(db_conn)
        assert result == []

    def test_returns_snapshots_within_window(self, db_conn):
        today = date.today()
        create_snapshot(db_conn, PortfolioSnapshot(
            date=(today.replace(day=1)).isoformat(),
            cash=10000, total_assets=10000, total_liabilities=0, net_worth=10000,
        ))
        result = get_net_worth_trend(db_conn, days=90)
        assert len(result) == 1
        assert result[0]["net_worth"] == 10000

    def test_keys_in_result(self, db_conn):
        today = date.today()
        create_snapshot(db_conn, PortfolioSnapshot(
            date=today.isoformat(),
            cash=5000, total_assets=5000, total_liabilities=0, net_worth=5000,
        ))
        result = get_net_worth_trend(db_conn)
        item = result[0]
        for k in ["date", "cash", "total_assets", "total_liabilities", "net_worth"]:
            assert k in item

    def test_excludes_old_snapshots(self, db_conn):
        create_snapshot(db_conn, PortfolioSnapshot(
            date="2020-01-01",
            cash=1000, total_assets=1000, total_liabilities=0, net_worth=1000,
        ))
        result = get_net_worth_trend(db_conn, days=90)
        assert result == []

    def test_multiple_snapshots_ordered(self, db_conn):
        today = date.today()
        from datetime import timedelta
        d1 = (today - timedelta(days=30)).isoformat()
        d2 = (today - timedelta(days=15)).isoformat()
        d3 = today.isoformat()
        create_snapshot(db_conn, PortfolioSnapshot(
            date=d1, cash=1000, total_assets=1000, total_liabilities=0, net_worth=1000,
        ))
        create_snapshot(db_conn, PortfolioSnapshot(
            date=d2, cash=2000, total_assets=2000, total_liabilities=0, net_worth=2000,
        ))
        create_snapshot(db_conn, PortfolioSnapshot(
            date=d3, cash=3000, total_assets=3000, total_liabilities=0, net_worth=3000,
        ))
        result = get_net_worth_trend(db_conn, days=90)
        assert len(result) == 3
        assert result[0]["date"] == d1
        assert result[2]["date"] == d3


# ===================================================================
# 3. get_cash_flow_snapshot
# ===================================================================

class TestCashFlowSnapshot:
    def test_keys(self, db_conn):
        result = get_cash_flow_snapshot(db_conn, today=date(2025, 3, 1))
        expected = [
            "start_date", "end_date", "inflow", "outflow",
            "net_cash_flow", "fees", "transaction_count",
        ]
        for k in expected:
            assert k in result

    def test_empty_portfolio(self, db_conn):
        result = get_cash_flow_snapshot(db_conn, today=date(2025, 3, 1))
        assert result["inflow"] == 0.0
        assert result["outflow"] == 0.0
        assert result["net_cash_flow"] == 0.0
        assert result["fees"] == 0.0
        assert result["transaction_count"] == 0

    def test_deposit_is_inflow(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-02-15", 50000.0)
        result = get_cash_flow_snapshot(db_conn, days=30, today=date(2025, 3, 1))
        assert result["inflow"] == 50000.0
        assert result["outflow"] == 0.0
        assert result["net_cash_flow"] == 50000.0
        assert result["transaction_count"] == 1

    def test_withdraw_is_outflow(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        ledger.withdraw_cash(db_conn, "2025-02-15", 20000.0)
        result = get_cash_flow_snapshot(db_conn, days=30, today=date(2025, 3, 1))
        assert result["outflow"] == 20000.0

    def test_buy_is_outflow(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="X", name="Stock X", asset_type="stock"))
        ledger.buy(db_conn, "2025-02-20", asset.id, quantity=10, price=100.0, fees=5.0)
        result = get_cash_flow_snapshot(db_conn, days=30, today=date(2025, 3, 1))
        assert result["outflow"] == 1005.0  # 10 * 100 + 5
        assert result["fees"] == 5.0

    def test_sell_is_inflow(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="X", name="Stock X", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=100.0)
        ledger.sell(db_conn, "2025-02-20", asset.id, quantity=5, price=120.0, fees=3.0)
        result = get_cash_flow_snapshot(db_conn, days=30, today=date(2025, 3, 1))
        assert result["inflow"] == 597.0  # 5 * 120 - 3
        assert result["fees"] == 3.0

    def test_date_range(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
        ledger.deposit_cash(db_conn, "2025-02-15", 5000.0)
        result = get_cash_flow_snapshot(db_conn, days=30, today=date(2025, 3, 1))
        # Only the Feb 15 deposit is within [Feb 1, Mar 1]
        assert result["inflow"] == 5000.0
        assert result["transaction_count"] == 1
        assert result["start_date"] == "2025-01-30"
        assert result["end_date"] == "2025-03-01"

    def test_net_cash_flow_is_inflow_minus_outflow(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-02-15", 50000.0)
        ledger.withdraw_cash(db_conn, "2025-02-20", 10000.0)
        result = get_cash_flow_snapshot(db_conn, days=30, today=date(2025, 3, 1))
        assert result["net_cash_flow"] == 40000.0
        assert result["inflow"] == 50000.0
        assert result["outflow"] == 10000.0


# ===================================================================
# 4. get_return_drivers
# ===================================================================

class TestReturnDrivers:
    def test_keys(self, db_conn):
        result = get_return_drivers(db_conn)
        assert "gainers" in result
        assert "losers" in result
        assert "missing_price_count" in result

    def test_empty_portfolio(self, db_conn):
        result = get_return_drivers(db_conn)
        assert result["gainers"] == []
        assert result["losers"] == []
        assert result["missing_price_count"] == 0

    def test_no_prices_all_missing(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="X", name="Stock X", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=100.0)
        result = get_return_drivers(db_conn)
        assert result["gainers"] == []
        assert result["losers"] == []
        assert result["missing_price_count"] == 1

    def test_gainer(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
        upsert_price(db_conn, asset.id, "2025-03-01", 180.0)

        result = get_return_drivers(db_conn)
        assert len(result["gainers"]) == 1
        g = result["gainers"][0]
        assert g["symbol"] == "AAPL"
        assert g["market_value"] == 1800.0
        assert g["cost_basis"] == 1500.0
        assert g["unrealized_pnl"] == 300.0
        assert g["unrealized_pnl_pct"] == pytest.approx(0.20)

    def test_loser(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="X", name="Stock X", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=100.0)
        upsert_price(db_conn, asset.id, "2025-03-01", 80.0)

        result = get_return_drivers(db_conn)
        assert len(result["losers"]) == 1
        assert result["losers"][0]["unrealized_pnl"] == -200.0

    def test_gainers_sorted_desc(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        a1 = create_asset(db_conn, Asset(symbol="A", name="Small Gain", asset_type="stock"))
        a2 = create_asset(db_conn, Asset(symbol="B", name="Big Gain", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-15", a1.id, quantity=10, price=100.0)
        ledger.buy(db_conn, "2025-01-15", a2.id, quantity=10, price=100.0)
        upsert_price(db_conn, a1.id, "2025-03-01", 110.0)
        upsert_price(db_conn, a2.id, "2025-03-01", 150.0)

        result = get_return_drivers(db_conn)
        assert len(result["gainers"]) == 2
        assert result["gainers"][0]["symbol"] == "B"
        assert result["gainers"][1]["symbol"] == "A"

    def test_losers_sorted_asc(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        a1 = create_asset(db_conn, Asset(symbol="A", name="Small Loss", asset_type="stock"))
        a2 = create_asset(db_conn, Asset(symbol="B", name="Big Loss", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-15", a1.id, quantity=10, price=100.0)
        ledger.buy(db_conn, "2025-01-15", a2.id, quantity=10, price=100.0)
        upsert_price(db_conn, a1.id, "2025-03-01", 90.0)
        upsert_price(db_conn, a2.id, "2025-03-01", 50.0)

        result = get_return_drivers(db_conn)
        assert len(result["losers"]) == 2
        assert result["losers"][0]["symbol"] == "B"
        assert result["losers"][1]["symbol"] == "A"

    def test_limit(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 5000000.0)
        for i in range(10):
            a = create_asset(db_conn, Asset(symbol=f"S{i}", name=f"Stock {i}", asset_type="stock"))
            ledger.buy(db_conn, "2025-01-15", a.id, quantity=10, price=100.0)
            upsert_price(db_conn, a.id, "2025-03-01", 110.0 + i)

        result = get_return_drivers(db_conn, limit=3)
        assert len(result["gainers"]) == 3

    def test_item_keys(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="X", name="Stock X", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=100.0)
        upsert_price(db_conn, asset.id, "2025-03-01", 120.0)

        result = get_return_drivers(db_conn)
        item = result["gainers"][0]
        expected_keys = [
            "symbol", "name", "asset_type", "market_value",
            "cost_basis", "unrealized_pnl", "unrealized_pnl_pct",
        ]
        for k in expected_keys:
            assert k in item

    def test_zero_cost_basis_pnl_pct_is_none(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="FREE", name="Free Stock", asset_type="stock"))
        # Simulate zero cost basis via manual_adjustment
        db_conn.execute(
            "INSERT INTO transactions (date, txn_type, asset_id, quantity, price, total_amount) "
            "VALUES ('2025-01-15', 'buy', ?, 10, 0.0, 0.0)",
            (asset.id,),
        )
        db_conn.commit()
        upsert_price(db_conn, asset.id, "2025-03-01", 50.0)

        result = get_return_drivers(db_conn)
        assert len(result["gainers"]) == 1
        assert result["gainers"][0]["unrealized_pnl_pct"] is None


# ===================================================================
# 5. get_recent_activity
# ===================================================================

class TestRecentActivity:
    def test_empty(self, db_conn):
        result = get_recent_activity(db_conn)
        assert result == []

    def test_returns_transactions(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        ledger.withdraw_cash(db_conn, "2025-01-15", 5000.0)
        result = get_recent_activity(db_conn)
        assert len(result) == 2

    def test_ordered_desc(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        ledger.deposit_cash(db_conn, "2025-02-01", 10000.0)
        ledger.deposit_cash(db_conn, "2025-03-01", 5000.0)
        result = get_recent_activity(db_conn)
        assert result[0]["date"] == "2025-03-01"
        assert result[1]["date"] == "2025-02-01"
        assert result[2]["date"] == "2025-01-01"

    def test_limit(self, db_conn):
        for i in range(10):
            ledger.deposit_cash(db_conn, f"2025-01-{i+1:02d}", 1000.0)
        result = get_recent_activity(db_conn, limit=3)
        assert len(result) == 3

    def test_item_keys(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        result = get_recent_activity(db_conn)
        item = result[0]
        expected_keys = [
            "date", "txn_type", "asset_symbol", "asset_name",
            "amount", "fees", "notes",
        ]
        for k in expected_keys:
            assert k in item

    def test_includes_asset_info(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=10, price=150.0)
        result = get_recent_activity(db_conn)
        buy_txn = result[0]
        assert buy_txn["asset_symbol"] == "AAPL"
        assert buy_txn["asset_name"] == "Apple"

    def test_cash_transaction_no_asset(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        result = get_recent_activity(db_conn)
        assert result[0]["asset_symbol"] is None
        assert result[0]["asset_name"] is None

    def test_fees_default_to_zero(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        result = get_recent_activity(db_conn)
        assert result[0]["fees"] == 0.0


# ===================================================================
# 6. get_real_estate_snapshot
# ===================================================================

class TestRealEstateSnapshot:
    def test_no_properties_returns_none(self, db_conn):
        result = get_real_estate_snapshot(db_conn)
        assert result is None

    def test_single_property(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="Rental",
            purchase_price=300000.0, current_value=320000.0,
            mortgage_balance=200000.0,
            monthly_rent=2500.0, monthly_mortgage_payment=1200.0,
            monthly_property_tax=200.0, monthly_insurance=100.0,
            vacancy_rate=0.05,
        )
        result = get_real_estate_snapshot(db_conn)
        assert result is not None
        assert result["property_count"] == 1
        assert result["total_property_value"] == 320000.0
        assert result["total_mortgage"] == 200000.0
        assert result["total_equity"] == 120000.0

    def test_keys(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=300000.0, mortgage_balance=200000.0,
        )
        result = get_real_estate_snapshot(db_conn)
        expected = [
            "property_count", "total_property_value", "total_mortgage",
            "total_equity", "monthly_net_cash_flow", "annual_net_cash_flow",
            "average_ltv", "negative_cash_flow_count",
        ]
        for k in expected:
            assert k in result

    def test_sold_property_returns_none(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=300000.0, mortgage_balance=200000.0,
        )
        ledger.sell_property(db_conn, "2025-06-01", asset.id, 350000.0)
        result = get_real_estate_snapshot(db_conn)
        assert result is None

    def test_multiple_properties(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 1000000.0)
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House 1",
            purchase_price=300000.0, mortgage_balance=200000.0,
            monthly_rent=2000.0, monthly_mortgage_payment=1000.0,
            vacancy_rate=0.0,
        )
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H2", name="House 2",
            purchase_price=400000.0, mortgage_balance=300000.0,
            monthly_rent=2500.0, monthly_mortgage_payment=1500.0,
            vacancy_rate=0.0,
        )
        result = get_real_estate_snapshot(db_conn)
        assert result["property_count"] == 2
        assert result["total_property_value"] == 700000.0
        assert result["total_mortgage"] == 500000.0
        assert result["total_equity"] == 200000.0

    def test_negative_cash_flow_count(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 1000000.0)
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="Good House",
            purchase_price=300000.0, mortgage_balance=200000.0,
            monthly_rent=3000.0, monthly_mortgage_payment=1000.0,
            vacancy_rate=0.0,
        )
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H2", name="Bad House",
            purchase_price=400000.0, mortgage_balance=300000.0,
            monthly_rent=500.0, monthly_mortgage_payment=3000.0,
            vacancy_rate=0.0,
        )
        result = get_real_estate_snapshot(db_conn)
        assert result["negative_cash_flow_count"] == 1

    def test_average_ltv(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 1000000.0)
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House 1",
            purchase_price=200000.0, current_value=200000.0,
            mortgage_balance=160000.0,
        )
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H2", name="House 2",
            purchase_price=400000.0, current_value=400000.0,
            mortgage_balance=200000.0,
        )
        result = get_real_estate_snapshot(db_conn)
        # LTV1 = 160k/200k = 0.80, LTV2 = 200k/400k = 0.50, avg = 0.65
        assert result["average_ltv"] == pytest.approx(0.65)

    def test_planned_property_excluded(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        ledger.add_property(
            db_conn, "2025-02-01", symbol="PLAN", name="Planned House",
            purchase_price=300000.0, mortgage_balance=200000.0,
            acquisition_mode="planned_purchase",
        )
        result = get_real_estate_snapshot(db_conn)
        assert result is None

    def test_mixed_active_and_sold(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 1000000.0)
        a1, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="Active",
            purchase_price=300000.0, mortgage_balance=200000.0,
        )
        a2, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="H2", name="Sold",
            purchase_price=400000.0, mortgage_balance=300000.0,
        )
        ledger.sell_property(db_conn, "2025-06-01", a2.id, 450000.0)
        result = get_real_estate_snapshot(db_conn)
        assert result is not None
        assert result["property_count"] == 1
        assert result["total_property_value"] == 300000.0
        assert result["total_mortgage"] == 200000.0


# ===================================================================
# 7. Chart: create_net_worth_trend_figure
# ===================================================================

from src.charts.dashboard import (
    create_net_worth_trend_figure,
    create_asset_mix_figure,
    create_return_drivers_figure,
)


class TestNetWorthTrendFigure:
    def test_empty_returns_figure(self):
        fig = create_net_worth_trend_figure([])
        assert isinstance(fig, Figure)

    def test_empty_shows_message(self):
        fig = create_net_worth_trend_figure([])
        ax = fig.get_axes()[0]
        texts = [t.get_text() for t in ax.texts]
        assert any("No snapshot" in t for t in texts)

    def test_single_point(self):
        rows = [{"date": "2025-01-01", "cash": 1000, "total_assets": 1000,
                 "total_liabilities": 0, "net_worth": 1000}]
        fig = create_net_worth_trend_figure(rows)
        assert isinstance(fig, Figure)
        assert len(fig.get_axes()) == 1

    def test_multiple_points(self):
        rows = [
            {"date": f"2025-0{i}-01", "cash": 1000 * i, "total_assets": 2000 * i,
             "total_liabilities": 500 * i, "net_worth": 1500 * i}
            for i in range(1, 7)
        ]
        fig = create_net_worth_trend_figure(rows)
        assert isinstance(fig, Figure)
        ax = fig.get_axes()[0]
        assert len(ax.get_lines()) >= 2

    def test_with_liabilities(self):
        rows = [
            {"date": "2025-01-01", "cash": 5000, "total_assets": 10000,
             "total_liabilities": 3000, "net_worth": 7000},
            {"date": "2025-02-01", "cash": 6000, "total_assets": 12000,
             "total_liabilities": 4000, "net_worth": 8000},
        ]
        fig = create_net_worth_trend_figure(rows)
        ax = fig.get_axes()[0]
        assert len(ax.get_lines()) == 3

    def test_zero_liabilities_omits_line(self):
        rows = [
            {"date": "2025-01-01", "cash": 5000, "total_assets": 5000,
             "total_liabilities": 0, "net_worth": 5000},
        ]
        fig = create_net_worth_trend_figure(rows)
        ax = fig.get_axes()[0]
        assert len(ax.get_lines()) == 2

    def test_many_points_reduces_ticks(self):
        rows = [
            {"date": f"2025-01-{i+1:02d}", "cash": 1000, "total_assets": 1000,
             "total_liabilities": 0, "net_worth": 1000}
            for i in range(20)
        ]
        fig = create_net_worth_trend_figure(rows)
        ax = fig.get_axes()[0]
        assert len(ax.get_xticks()) <= 10


# ===================================================================
# 8. Chart: create_asset_mix_figure
# ===================================================================

class TestAssetMixFigure:
    def test_empty_returns_figure(self):
        fig = create_asset_mix_figure([])
        assert isinstance(fig, Figure)

    def test_empty_shows_message(self):
        fig = create_asset_mix_figure([])
        ax = fig.get_axes()[0]
        texts = [t.get_text() for t in ax.texts]
        assert any("No asset data" in t for t in texts)

    def test_single_item(self):
        items = [{"name": "Cash", "value": 50000, "pct": 1.0, "asset_type": "cash"}]
        fig = create_asset_mix_figure(items)
        assert isinstance(fig, Figure)

    def test_five_items_no_other(self):
        items = [
            {"name": f"Asset {i}", "value": 100 - i * 10, "pct": 0.2, "asset_type": "stock"}
            for i in range(5)
        ]
        fig = create_asset_mix_figure(items)
        assert isinstance(fig, Figure)
        ax = fig.get_axes()[0]
        legend = ax.get_legend()
        legend_labels = [t.get_text() for t in legend.get_texts()]
        assert not any("Other" in l for l in legend_labels)

    def test_more_than_five_items_creates_other(self):
        items = [
            {"name": f"Asset {i}", "value": 100 - i * 5, "pct": 1 / 8, "asset_type": "stock"}
            for i in range(8)
        ]
        fig = create_asset_mix_figure(items)
        assert isinstance(fig, Figure)
        ax = fig.get_axes()[0]
        legend = ax.get_legend()
        legend_labels = [t.get_text() for t in legend.get_texts()]
        assert any("Other" in l for l in legend_labels)
        assert len(legend_labels) == 6

    def test_donut_wedge_width(self):
        items = [
            {"name": "Cash", "value": 50000, "pct": 0.5, "asset_type": "cash"},
            {"name": "VTI", "value": 50000, "pct": 0.5, "asset_type": "etf"},
        ]
        fig = create_asset_mix_figure(items)
        ax = fig.get_axes()[0]
        patches = ax.patches
        assert len(patches) == 2

    def test_long_name_truncated(self):
        items = [
            {"name": "A Very Long Asset Name That Should Be Truncated",
             "value": 100, "pct": 1.0, "asset_type": "stock"}
        ]
        fig = create_asset_mix_figure(items)
        ax = fig.get_axes()[0]
        legend = ax.get_legend()
        label = legend.get_texts()[0].get_text()
        assert len(label.split("\n")[0]) <= 20


# ===================================================================
# 9. Chart: create_return_drivers_figure
# ===================================================================

class TestReturnDriversFigure:
    def test_empty_returns_figure(self):
        fig = create_return_drivers_figure([], [])
        assert isinstance(fig, Figure)

    def test_empty_shows_message(self):
        fig = create_return_drivers_figure([], [])
        ax = fig.get_axes()[0]
        texts = [t.get_text() for t in ax.texts]
        assert any("No priced positions" in t for t in texts)

    def test_gainers_only(self):
        gainers = [
            {"symbol": "AAPL", "name": "Apple", "asset_type": "stock",
             "market_value": 1800, "cost_basis": 1500, "unrealized_pnl": 300,
             "unrealized_pnl_pct": 0.20},
        ]
        fig = create_return_drivers_figure(gainers, [])
        assert isinstance(fig, Figure)
        ax = fig.get_axes()[0]
        assert len(ax.patches) == 1

    def test_losers_only(self):
        losers = [
            {"symbol": "META", "name": "Meta", "asset_type": "stock",
             "market_value": 800, "cost_basis": 1000, "unrealized_pnl": -200,
             "unrealized_pnl_pct": -0.20},
        ]
        fig = create_return_drivers_figure([], losers)
        assert isinstance(fig, Figure)

    def test_mixed_gainers_and_losers(self):
        gainers = [
            {"symbol": "AAPL", "name": "Apple", "asset_type": "stock",
             "market_value": 1800, "cost_basis": 1500, "unrealized_pnl": 300,
             "unrealized_pnl_pct": 0.20},
        ]
        losers = [
            {"symbol": "META", "name": "Meta", "asset_type": "stock",
             "market_value": 800, "cost_basis": 1000, "unrealized_pnl": -200,
             "unrealized_pnl_pct": -0.20},
        ]
        fig = create_return_drivers_figure(gainers, losers)
        assert isinstance(fig, Figure)
        ax = fig.get_axes()[0]
        assert len(ax.patches) == 2

    def test_bar_labels_present(self):
        gainers = [
            {"symbol": "AAPL", "name": "Apple", "asset_type": "stock",
             "market_value": 1800, "cost_basis": 1500, "unrealized_pnl": 300,
             "unrealized_pnl_pct": 0.20},
        ]
        fig = create_return_drivers_figure(gainers, [])
        ax = fig.get_axes()[0]
        text_values = [t.get_text() for t in ax.texts]
        assert any("$" in t for t in text_values)

    def test_many_items_scales_figure(self):
        gainers = [
            {"symbol": f"S{i}", "name": f"Stock {i}", "asset_type": "stock",
             "market_value": 1100 + i * 100, "cost_basis": 1000,
             "unrealized_pnl": 100 + i * 100, "unrealized_pnl_pct": 0.1 + i * 0.1}
            for i in range(8)
        ]
        fig = create_return_drivers_figure(gainers, [])
        assert isinstance(fig, Figure)
        assert fig.get_figheight() > 2


# ===================================================================
# 10. DashboardPage GUI smoke tests
# ===================================================================

from PySide6.QtWidgets import QLabel


class TestDashboardPageSmoke:
    def test_instantiation(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        assert page is not None

    def test_refresh_empty_db(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page.refresh()

    def test_double_refresh_no_crash(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page.refresh()
        page.refresh()

    def test_net_worth_card_exists(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page.refresh()
        assert page.nw_card is not None

    def test_real_estate_hidden_no_properties(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page.refresh()
        assert page.re_frame.isHidden()

    def test_real_estate_visible_with_property(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="Test House",
            purchase_price=300000.0, mortgage_balance=200000.0,
        )
        page = DashboardPage(db_conn)
        page.refresh()
        assert not page.re_frame.isHidden()

    def test_risk_ok_label_when_no_warnings(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page.refresh()
        assert not page.risk_ok_label.isHidden()

    def test_activity_table_empty_db(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page.refresh()
        assert page.activity_table.rowCount() == 0

    def test_activity_table_populated(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 1000.0)
        page = DashboardPage(db_conn)
        page.refresh()
        assert page.activity_table.rowCount() >= 1

    def test_trend_canvas_exists_after_refresh(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page.refresh()
        assert page._trend_canvas is not None

    def test_mix_canvas_exists_after_refresh(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page.refresh()
        assert page._mix_canvas is not None

    def test_drivers_canvas_exists_after_refresh(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page.refresh()
        assert page._drivers_canvas is not None

    def test_refresh_with_mixed_portfolio(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        asset = create_asset(db_conn, Asset(symbol="VTI", name="Vanguard Total", asset_type="etf"))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=100, price=200.0)
        upsert_price(db_conn, asset.id, "2025-03-01", 220.0)
        page = DashboardPage(db_conn)
        page.refresh()
        assert page.activity_table.rowCount() >= 2

    def test_risk_warnings_shown_when_present(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset = create_asset(db_conn, Asset(
            symbol="YOLO", name="Big Bet", asset_type="stock",
        ))
        ledger.buy(db_conn, "2025-01-15", asset.id, quantity=500, price=190.0)
        page = DashboardPage(db_conn)
        page.refresh()
        visible_warnings = [lbl for lbl in page.risk_labels if not lbl.isHidden()]
        if visible_warnings:
            assert page.risk_ok_label.isHidden()

    def test_cash_flow_section_populated(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-04-01", 50000.0)
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.cf_inflow.findChild(QLabel, "value")
        assert val is not None

    def test_real_estate_hidden_after_sell(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        asset, _, _ = ledger.add_property(
            db_conn, "2025-02-01", symbol="SELL1", name="Sell House",
            purchase_price=300000.0, mortgage_balance=200000.0,
        )
        ledger.sell_property(db_conn, "2025-06-01", asset.id, 350000.0)
        page = DashboardPage(db_conn)
        page.refresh()
        assert page.re_frame.isHidden()


# ===================================================================
# 11. Dashboard snapshot display behavior
# ===================================================================

from src.storage.snapshot_repo import list_snapshots
from src.engines.snapshots import record_daily_portfolio_snapshot
from src.gui.widgets.common import LABEL_MUTED_COLOR


class TestDashboardSnapshotDisplay:
    def test_one_snapshot_30d_change_is_dash(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        from datetime import timedelta
        ledger.deposit_cash(db_conn, "2026-04-20", 50000.0)
        record_daily_portfolio_snapshot(db_conn, date.today())
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.change_card.findChild(QLabel, "value")
        assert val.text() == "--"

    def test_two_snapshots_30d_change_shows_value(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        from datetime import timedelta
        today = date.today()
        ledger.deposit_cash(db_conn, "2026-04-01", 50000.0)
        record_daily_portfolio_snapshot(db_conn, today - timedelta(days=15))
        ledger.deposit_cash(db_conn, "2026-04-20", 10000.0)
        record_daily_portfolio_snapshot(db_conn, today)
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.change_card.findChild(QLabel, "value")
        assert val.text() != "--"
        assert "$" in val.text()

    def test_refresh_does_not_write_snapshots(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2026-04-20", 50000.0)
        before = len(list_snapshots(db_conn))
        page = DashboardPage(db_conn)
        page.refresh()
        page.refresh()
        page.refresh()
        after = len(list_snapshots(db_conn))
        assert after == before


# ===================================================================
# 12. Dashboard scroll area and event filter
# ===================================================================

from PySide6.QtCore import QEvent
from PySide6.QtWidgets import QScrollArea


class TestDashboardScrollForwarding:
    def test_scroll_area_is_instance_attribute(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        assert hasattr(page, "scroll_area")
        assert isinstance(page.scroll_area, QScrollArea)

    def test_canvas_has_event_filter_after_refresh(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page.refresh()
        assert page._trend_canvas is not None
        assert page._mix_canvas is not None
        assert page._drivers_canvas is not None

    def test_event_filter_handles_wheel(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        from PySide6.QtGui import QWheelEvent
        from PySide6.QtCore import QPointF, QPoint, Qt as QtCore
        page = DashboardPage(db_conn)
        page.refresh()
        event = QWheelEvent(
            QPointF(0, 0), QPointF(0, 0),
            QPoint(0, 0), QPoint(0, -120),
            QtCore.MouseButton.NoButton, QtCore.KeyboardModifier.NoModifier,
            QtCore.ScrollPhase.NoScrollPhase, False,
        )
        result = page.eventFilter(page._trend_canvas, event)
        assert result is True


# ===================================================================
# 13. Return drivers chart improvements
# ===================================================================

class TestReturnDriversChartImprovements:
    def test_tiny_values_no_crash(self):
        losers = [
            {"symbol": "X", "name": "Tiny Loss", "asset_type": "stock",
             "market_value": 999, "cost_basis": 1000, "unrealized_pnl": -1,
             "unrealized_pnl_pct": -0.001},
        ]
        fig = create_return_drivers_figure([], losers)
        assert isinstance(fig, Figure)
        ax = fig.get_axes()[0]
        assert len(ax.patches) == 1

    def test_zero_tick_label_no_sign(self):
        from src.charts.dashboard import _fmt_pnl_tick
        assert _fmt_pnl_tick(0, None) == "$0"

    def test_positive_tick_label_has_plus(self):
        from src.charts.dashboard import _fmt_pnl_tick
        assert _fmt_pnl_tick(500, None) == "$+500"

    def test_negative_tick_label_has_minus(self):
        from src.charts.dashboard import _fmt_pnl_tick
        assert "-" in _fmt_pnl_tick(-500, None)

    def test_large_tick_uses_k_suffix(self):
        from src.charts.dashboard import _fmt_pnl_tick
        assert "K" in _fmt_pnl_tick(5000, None)

    def test_bar_labels_use_annotate(self):
        gainers = [
            {"symbol": "AAPL", "name": "Apple", "asset_type": "stock",
             "market_value": 1800, "cost_basis": 1500, "unrealized_pnl": 300,
             "unrealized_pnl_pct": 0.20},
        ]
        fig = create_return_drivers_figure(gainers, [])
        ax = fig.get_axes()[0]
        annotations = [c for c in ax.get_children()
                       if hasattr(c, 'get_text') and hasattr(c, 'xyann')]
        assert len(annotations) >= 1

    def test_all_losers_renders(self):
        losers = [
            {"symbol": f"L{i}", "name": f"Loser {i}", "asset_type": "stock",
             "market_value": 900 - i * 50, "cost_basis": 1000,
             "unrealized_pnl": -100 - i * 50, "unrealized_pnl_pct": -0.1 - i * 0.05}
            for i in range(5)
        ]
        fig = create_return_drivers_figure([], losers)
        assert isinstance(fig, Figure)
        ax = fig.get_axes()[0]
        assert len(ax.patches) == 5

    def test_single_item_renders(self):
        gainers = [
            {"symbol": "SOLO", "name": "Solo", "asset_type": "stock",
             "market_value": 1100, "cost_basis": 1000, "unrealized_pnl": 100,
             "unrealized_pnl_pct": 0.10},
        ]
        fig = create_return_drivers_figure(gainers, [])
        assert isinstance(fig, Figure)
        ax = fig.get_axes()[0]
        xlim = ax.get_xlim()
        assert xlim[1] > 100


# ===================================================================
# 14. Phase 1: compact money formatter
# ===================================================================

from src.gui.widgets.common import fmt_money, fmt_money_compact


class TestFmtMoneyCompact:
    def test_none(self):
        assert fmt_money_compact(None) == "N/A"

    def test_zero(self):
        assert fmt_money_compact(0) == "$0.00"

    def test_zero_float(self):
        assert fmt_money_compact(0.0) == "$0.00"

    def test_sub_thousand_keeps_two_decimals(self):
        assert fmt_money_compact(999.99) == "$999.99"

    def test_sub_thousand_small_value(self):
        assert fmt_money_compact(12.5) == "$12.50"

    def test_thousands(self):
        result = fmt_money_compact(1234.0)
        assert result.endswith("K")
        assert "1.2" in result
        assert "$" in result

    def test_thousands_round_number(self):
        assert fmt_money_compact(1000) == "$1.0K"

    def test_millions(self):
        result = fmt_money_compact(3_400_000)
        assert result.endswith("M")
        assert "3.4" in result

    def test_billions(self):
        result = fmt_money_compact(5_600_000_000)
        assert result.endswith("B")
        assert "5.6" in result

    def test_trillions(self):
        result = fmt_money_compact(7_800_000_000_000)
        assert result.endswith("T")
        assert "7.8" in result

    def test_negative_thousands(self):
        result = fmt_money_compact(-1_500)
        assert result.startswith("-")
        assert result.endswith("K")
        assert "1.5" in result

    def test_negative_millions(self):
        result = fmt_money_compact(-2_500_000)
        assert result.startswith("-$")
        assert result.endswith("M")

    def test_negative_sub_thousand(self):
        assert fmt_money_compact(-50) == "-$50.00"

    def test_above_trillion_safe_fallback(self):
        # 1 quadrillion: above the highest standard suffix. Must still
        # return a string with the $ prefix and not raise.
        result = fmt_money_compact(1_000_000_000_000_000)
        assert isinstance(result, str)
        assert "$" in result
        assert result.endswith("T")

    def test_custom_prefix(self):
        assert fmt_money_compact(1500, prefix="€").startswith("€")
        assert fmt_money_compact(-1500, prefix="€") == "-€1.5K"


class TestFmtMoneyUnchanged:
    """Phase 1 must not regress fmt_money() — tables, exports, and reports
    still depend on its exact-value contract."""

    def test_full_exact_value(self):
        assert fmt_money(1_234_567.89) == "$1,234,567.89"

    def test_huge_value_keeps_full_digits(self):
        assert fmt_money(5_500_000_000.0) == "$5,500,000,000.00"

    def test_none(self):
        assert fmt_money(None) == "N/A"

    def test_zero(self):
        assert fmt_money(0) == "$0.00"

    def test_negative_keeps_existing_format(self):
        # Existing dashboard wiring depends on this exact form.
        assert fmt_money(-1234.56) == "$-1,234.56"


# ===================================================================
# 15. Phase 1: Dashboard compact display + tooltips
# ===================================================================


class TestDashboardCompactDisplay:
    def test_huge_cash_displays_compact_text(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 5_500_000_000.0)
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.cash_card.findChild(QLabel, "value")
        text = val.text()
        assert "B" in text
        assert "$" in text
        # Compact form must not include the full digit sequence.
        assert "5,500,000,000" not in text

    def test_huge_cash_tooltip_has_full_exact_value(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 5_500_000_000.0)
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.cash_card.findChild(QLabel, "value")
        assert val.toolTip() == "$5,500,000,000.00"

    def test_huge_net_worth_displays_compact(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 1_500_000_000.0)
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.nw_card.findChild(QLabel, "value")
        text = val.text()
        assert "B" in text
        assert "1,500,000,000" not in text

    def test_huge_net_worth_tooltip_full(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 1_500_000_000.0)
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.nw_card.findChild(QLabel, "value")
        assert val.toolTip() == "$1,500,000,000.00"

    def test_modest_cash_uses_subthousand_form(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 750.0)
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.cash_card.findChild(QLabel, "value")
        # Sub-1k preserves two decimals — same look as before for normal use.
        assert val.text() == "$750.00"
        assert val.toolTip() == "$750.00"

    def test_cash_flow_inflow_tooltip_has_exact_value(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2026-04-15", 12_345_678.90)
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.cf_inflow.findChild(QLabel, "value")
        assert "M" in val.text()
        assert val.toolTip() == "$12,345,678.90"

    def test_cash_flow_transaction_count_unchanged(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2026-04-15", 1000.0)
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.cf_count.findChild(QLabel, "value")
        # transaction_count is not money — no compact suffix, no tooltip
        assert val.text().isdigit()
        assert val.toolTip() == ""

    def test_real_estate_money_compact_with_full_tooltip(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 10_000_000_000.0)
        ledger.add_property(
            db_conn, "2025-02-01", symbol="MEGA", name="Mega Tower",
            purchase_price=2_500_000_000.0, current_value=2_500_000_000.0,
            mortgage_balance=1_000_000_000.0,
        )
        page = DashboardPage(db_conn)
        page.refresh()
        v_value = page.re_value.findChild(QLabel, "value")
        v_mortgage = page.re_mortgage.findChild(QLabel, "value")
        assert "B" in v_value.text()
        assert "B" in v_mortgage.text()
        assert v_value.toolTip() == "$2,500,000,000.00"
        assert v_mortgage.toolTip() == "$1,000,000,000.00"

    def test_real_estate_property_count_unchanged(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 500_000.0)
        ledger.add_property(
            db_conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=300_000.0, mortgage_balance=200_000.0,
        )
        page = DashboardPage(db_conn)
        page.refresh()
        v = page.re_props.findChild(QLabel, "value")
        # property_count is a plain integer — not formatted as money.
        assert v.text() == "1"
        assert v.toolTip() == ""

    def test_risk_status_card_unchanged(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.risk_card.findChild(QLabel, "value")
        # Risk Status is plain text, not money — no $, no tooltip.
        assert "$" not in val.text()
        assert val.toolTip() == ""

    def test_negative_change_uses_compact_minus(self, qapp, db_conn):
        # 30D Change card naturally goes negative when net worth drops; this
        # exercises the full dashboard wiring with a negative compact value.
        from src.gui.pages.dashboard import DashboardPage
        from src.engines.snapshots import record_daily_portfolio_snapshot
        from datetime import timedelta
        today = date.today()
        early = (today - timedelta(days=20)).isoformat()
        later = (today - timedelta(days=5)).isoformat()
        ledger.deposit_cash(db_conn, early, 5_000_000.0)
        record_daily_portfolio_snapshot(db_conn, today - timedelta(days=15))
        ledger.withdraw_cash(db_conn, later, 4_500_000.0)
        record_daily_portfolio_snapshot(db_conn, today)
        page = DashboardPage(db_conn)
        page.refresh()
        val = page.change_card.findChild(QLabel, "value")
        text = val.text()
        assert text.startswith("-")
        assert "$" in text
        # Tooltip should still expose the exact value (fmt_money's "$-..." form).
        assert val.toolTip().startswith("$-")


# ===================================================================
# 16. Phase 1: Chart compact tick formatter
# ===================================================================


class TestCompactMoneyTick:
    def test_zero(self):
        from src.charts.dashboard import _compact_money_tick
        assert _compact_money_tick(0, None) == "$0"

    def test_sub_thousand(self):
        from src.charts.dashboard import _compact_money_tick
        assert _compact_money_tick(500, None) == "$500"

    def test_thousands(self):
        from src.charts.dashboard import _compact_money_tick
        result = _compact_money_tick(5_000, None)
        assert "K" in result
        assert "$5K" == result

    def test_millions(self):
        from src.charts.dashboard import _compact_money_tick
        result = _compact_money_tick(5_000_000, None)
        assert "M" in result

    def test_huge_billions_no_overflow(self):
        from src.charts.dashboard import _compact_money_tick
        result = _compact_money_tick(100_000_000_000, None)
        # The original $xxxk formatter would have produced "$100000000k".
        # Compact must collapse to a billions suffix instead.
        assert "100000000k" not in result
        assert "100000000K" not in result
        assert "B" in result

    def test_trillions(self):
        from src.charts.dashboard import _compact_money_tick
        result = _compact_money_tick(7_500_000_000_000, None)
        assert "T" in result

    def test_negative(self):
        from src.charts.dashboard import _compact_money_tick
        result = _compact_money_tick(-2_500_000, None)
        assert result.startswith("-")
        assert "M" in result

    def test_pnl_tick_huge_no_overflow(self):
        from src.charts.dashboard import _fmt_pnl_tick
        result = _fmt_pnl_tick(100_000_000_000, None)
        # Avoid "$+100000000k"-style labels.
        assert "100000000k" not in result
        assert "100000000K" not in result
        assert "B" in result
        assert "+" in result

    def test_pnl_tick_negative_billions(self):
        from src.charts.dashboard import _fmt_pnl_tick
        result = _fmt_pnl_tick(-3_400_000_000, None)
        assert "B" in result
        assert "-" in result

    def test_trend_chart_y_axis_uses_compact_for_huge_values(self):
        # End-to-end: build a trend with billions and confirm the figure's
        # y-axis formatter produces compact labels (not "$xxxk" overflow).
        rows = [
            {"date": "2025-01-01", "cash": 0, "total_assets": 5_000_000_000,
             "total_liabilities": 0, "net_worth": 5_000_000_000},
            {"date": "2025-02-01", "cash": 0, "total_assets": 5_500_000_000,
             "total_liabilities": 0, "net_worth": 5_500_000_000},
        ]
        fig = create_net_worth_trend_figure(rows)
        ax = fig.get_axes()[0]
        formatter = ax.yaxis.get_major_formatter()
        sample = formatter(5_000_000_000, None)
        assert "B" in sample
        assert "5000000000" not in sample
        assert "5000000k" not in sample


# ===================================================================
# 17. Phase 2: responsive layout
# ===================================================================

from src.gui.pages.dashboard import _layout_mode_for_width


class TestLayoutModeForWidth:
    def test_wide_threshold(self):
        assert _layout_mode_for_width(1200) == "wide"
        assert _layout_mode_for_width(1100) == "wide"

    def test_medium_threshold(self):
        assert _layout_mode_for_width(1099) == "medium"
        assert _layout_mode_for_width(900) == "medium"
        assert _layout_mode_for_width(800) == "medium"

    def test_narrow_threshold(self):
        assert _layout_mode_for_width(799) == "narrow"
        assert _layout_mode_for_width(600) == "narrow"
        assert _layout_mode_for_width(500) == "narrow"

    def test_compact_threshold(self):
        assert _layout_mode_for_width(499) == "compact"
        assert _layout_mode_for_width(320) == "compact"
        assert _layout_mode_for_width(0) == "compact"


class TestDashboardResponsiveHeroRow:
    def test_wide_uses_4_hero_columns(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(1200)
        assert page._current_layout_mode == "wide"
        assert page.top_row.itemAtPosition(0, 0).widget() is page.nw_card
        assert page.top_row.itemAtPosition(0, 1).widget() is page.change_card
        assert page.top_row.itemAtPosition(0, 2).widget() is page.cash_card
        assert page.top_row.itemAtPosition(0, 3).widget() is page.risk_card

    def test_medium_uses_2_hero_columns(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(900)
        assert page._current_layout_mode == "medium"
        assert page.top_row.itemAtPosition(0, 0).widget() is page.nw_card
        assert page.top_row.itemAtPosition(0, 1).widget() is page.change_card
        assert page.top_row.itemAtPosition(1, 0).widget() is page.cash_card
        assert page.top_row.itemAtPosition(1, 1).widget() is page.risk_card

    def test_narrow_uses_1_hero_column(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(600)
        assert page._current_layout_mode == "narrow"
        assert page.top_row.itemAtPosition(0, 0).widget() is page.nw_card
        assert page.top_row.itemAtPosition(1, 0).widget() is page.change_card
        assert page.top_row.itemAtPosition(2, 0).widget() is page.cash_card
        assert page.top_row.itemAtPosition(3, 0).widget() is page.risk_card

    def test_compact_uses_1_hero_column(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(400)
        assert page._current_layout_mode == "compact"
        for row, card in enumerate(
            [page.nw_card, page.change_card, page.cash_card, page.risk_card]
        ):
            assert page.top_row.itemAtPosition(row, 0).widget() is card


class TestDashboardResponsiveCharts:
    def test_wide_charts_side_by_side(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(1200)
        page.refresh()
        assert page.charts_row.itemAtPosition(0, 0).widget() is page._trend_canvas
        assert page.charts_row.itemAtPosition(0, 1).widget() is page._mix_canvas

    def test_medium_charts_side_by_side(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(900)
        page.refresh()
        assert page.charts_row.itemAtPosition(0, 0).widget() is page._trend_canvas
        assert page.charts_row.itemAtPosition(0, 1).widget() is page._mix_canvas

    def test_narrow_charts_stacked(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(600)
        page.refresh()
        # Trend above, mix below
        assert page.charts_row.itemAtPosition(0, 0).widget() is page._trend_canvas
        assert page.charts_row.itemAtPosition(1, 0).widget() is page._mix_canvas

    def test_compact_charts_stacked(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(400)
        page.refresh()
        assert page.charts_row.itemAtPosition(0, 0).widget() is page._trend_canvas
        assert page.charts_row.itemAtPosition(1, 0).widget() is page._mix_canvas

    def test_wide_to_narrow_reflow_preserves_canvas(self, qapp, db_conn):
        # Refresh once at wide; reflow to narrow; the canvas widget instance
        # should be the same and now sit at (1, 0) for mix_canvas.
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(1200)
        page.refresh()
        trend = page._trend_canvas
        mix = page._mix_canvas
        page._apply_responsive_layout(600)
        assert page._trend_canvas is trend
        assert page._mix_canvas is mix
        assert page.charts_row.itemAtPosition(0, 0).widget() is trend
        assert page.charts_row.itemAtPosition(1, 0).widget() is mix


class TestDashboardResponsivePerfRow:
    def test_wide_perf_side_by_side(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(1200)
        page.refresh()
        assert page.perf_row.itemAtPosition(0, 0).widget() is page._drivers_canvas
        assert page.perf_row.itemAtPosition(0, 1).widget() is page.cf_frame

    def test_narrow_perf_stacked(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(600)
        page.refresh()
        assert page.perf_row.itemAtPosition(0, 0).widget() is page._drivers_canvas
        assert page.perf_row.itemAtPosition(1, 0).widget() is page.cf_frame

    def test_compact_perf_stacked(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(400)
        page.refresh()
        assert page.perf_row.itemAtPosition(0, 0).widget() is page._drivers_canvas
        assert page.perf_row.itemAtPosition(1, 0).widget() is page.cf_frame


class TestDashboardResponsiveRealEstate:
    def _re_widgets(self, page):
        return [
            page.re_props, page.re_value, page.re_equity,
            page.re_mortgage, page.re_ncf, page.re_ltv,
        ]

    def _assert_grid(self, grid, widgets, cols):
        for idx, w in enumerate(widgets):
            row = idx // cols
            col = idx % cols
            assert grid.itemAtPosition(row, col).widget() is w, (
                f"expected {w!r} at ({row}, {col})"
            )

    def test_wide_uses_6_columns(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(1200)
        self._assert_grid(page.re_metrics, self._re_widgets(page), 6)

    def test_medium_uses_3_columns(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(900)
        self._assert_grid(page.re_metrics, self._re_widgets(page), 3)

    def test_narrow_uses_2_columns(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(600)
        self._assert_grid(page.re_metrics, self._re_widgets(page), 2)

    def test_compact_uses_1_column(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(400)
        self._assert_grid(page.re_metrics, self._re_widgets(page), 1)


class TestDashboardHorizontalScroll:
    def test_horizontal_scrollbar_policy_is_off(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        from PySide6.QtCore import Qt as _Qt
        page = DashboardPage(db_conn)
        assert (
            page.scroll_area.horizontalScrollBarPolicy()
            == _Qt.ScrollBarPolicy.ScrollBarAlwaysOff
        )

    def test_no_horizontal_scrollbar_at_small_width(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        from PySide6.QtCore import Qt as _Qt
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(400)
        page.refresh()
        qapp.processEvents()
        h_sb = page.scroll_area.horizontalScrollBar()
        # Policy is AlwaysOff and the scroll area should never expose a
        # horizontal scrollbar, regardless of inner content width.
        assert not h_sb.isVisible()
        assert (
            page.scroll_area.horizontalScrollBarPolicy()
            == _Qt.ScrollBarPolicy.ScrollBarAlwaysOff
        )


class TestDashboardReflowSafety:
    def test_reflow_without_refresh_keeps_placeholders(self, qapp, db_conn):
        # Reflow before any refresh — placeholders stay in the grid and
        # canvases are still None, so the next refresh's _swap_canvas can
        # still find them.
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(400)  # compact
        assert page._trend_canvas is None
        assert page.charts_row.itemAtPosition(0, 0).widget() is page._trend_placeholder
        assert page.charts_row.itemAtPosition(1, 0).widget() is page._mix_placeholder
        # Refresh now should populate canvases without raising.
        page.refresh()
        assert page._trend_canvas is not None
        assert page._mix_canvas is not None

    def test_repeated_apply_same_mode_is_noop(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        page._apply_responsive_layout(900)
        before = page._current_layout_mode
        # Same mode: should early-return; no reordering.
        page._apply_responsive_layout(950)
        assert page._current_layout_mode == before
        assert page.top_row.itemAtPosition(0, 0).widget() is page.nw_card

    def test_refresh_after_mode_changes_does_not_crash(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        for w in (1200, 600, 400, 900, 1200):
            page._apply_responsive_layout(w)
            page.refresh()
        assert page._trend_canvas is not None
        assert page._mix_canvas is not None
        assert page._drivers_canvas is not None


# ===================================================================
# 18. Phase 3: chart compact-mode rendering
# ===================================================================


class TestChartFunctionsAcceptCompact:
    """Each chart function must accept a `compact` keyword and still return
    a Figure instance — no surprise crashes when DashboardPage forwards the
    flag."""

    def test_trend_compact_returns_figure(self):
        rows = [{"date": "2025-01-01", "cash": 1000, "total_assets": 1000,
                 "total_liabilities": 0, "net_worth": 1000}]
        fig = create_net_worth_trend_figure(rows, compact=True)
        assert isinstance(fig, Figure)

    def test_trend_compact_empty_returns_figure(self):
        fig = create_net_worth_trend_figure([], compact=True)
        assert isinstance(fig, Figure)

    def test_mix_compact_returns_figure(self):
        items = [{"name": "Cash", "value": 50000, "pct": 1.0, "asset_type": "cash"}]
        fig = create_asset_mix_figure(items, compact=True)
        assert isinstance(fig, Figure)

    def test_mix_compact_empty_returns_figure(self):
        fig = create_asset_mix_figure([], compact=True)
        assert isinstance(fig, Figure)

    def test_drivers_compact_returns_figure(self):
        gainers = [{"symbol": "AAPL", "name": "Apple", "asset_type": "stock",
                    "market_value": 1800, "cost_basis": 1500, "unrealized_pnl": 300,
                    "unrealized_pnl_pct": 0.20}]
        fig = create_return_drivers_figure(gainers, [], compact=True)
        assert isinstance(fig, Figure)

    def test_drivers_compact_empty_returns_figure(self):
        fig = create_return_drivers_figure([], [], compact=True)
        assert isinstance(fig, Figure)


class TestNetWorthTrendCompact:
    def _make_rows(self, n):
        return [
            {"date": f"2025-01-{i+1:02d}", "cash": 1000 * (i + 1),
             "total_assets": 1000 * (i + 1), "total_liabilities": 0,
             "net_worth": 1000 * (i + 1)}
            for i in range(n)
        ]

    def test_compact_creates_fewer_tick_labels(self):
        rows = self._make_rows(20)
        ax_default = create_net_worth_trend_figure(rows).get_axes()[0]
        ax_compact = create_net_worth_trend_figure(rows, compact=True).get_axes()[0]
        assert len(ax_compact.get_xticks()) < len(ax_default.get_xticks())

    def test_compact_caps_xticks_at_low_count(self):
        rows = self._make_rows(30)
        fig = create_net_worth_trend_figure(rows, compact=True)
        ax = fig.get_axes()[0]
        # Keeping the cap loose since matplotlib may add edge ticks later;
        # the point is it's tighter than the non-compact "<=10" target.
        assert len(ax.get_xticks()) <= 6

    def test_compact_uses_compact_money_y_formatter(self):
        rows = self._make_rows(5)
        # Make values huge to confirm the formatter still produces compact text.
        for r in rows:
            r["net_worth"] = r["total_assets"] = 5_000_000_000
        fig = create_net_worth_trend_figure(rows, compact=True)
        ax = fig.get_axes()[0]
        formatter = ax.yaxis.get_major_formatter()
        sample = formatter(5_000_000_000, None)
        assert "B" in sample
        assert "5000000000" not in sample

    def test_compact_few_points_shows_all_ticks(self):
        # Below the compact cap we still show every date.
        rows = self._make_rows(5)
        fig = create_net_worth_trend_figure(rows, compact=True)
        ax = fig.get_axes()[0]
        assert len(ax.get_xticks()) == 5


class TestAssetMixCompact:
    def _items(self, n):
        return [
            {"name": f"Asset {i}", "value": 100, "pct": 1 / n, "asset_type": "stock"}
            for i in range(n)
        ]

    def test_compact_truncates_names_more_aggressively(self):
        items = [{"name": "A Very Long Asset Name That Should Be Truncated",
                  "value": 100, "pct": 1.0, "asset_type": "stock"}]
        default_label = create_asset_mix_figure(items).get_axes()[0].get_legend(
        ).get_texts()[0].get_text().split("\n")[0]
        compact_label = create_asset_mix_figure(items, compact=True).get_axes()[0].get_legend(
        ).get_texts()[0].get_text().split("\n")[0]
        # Default truncation budget is 18; compact is 10. Compact must be
        # strictly shorter to leave room for the donut.
        assert len(compact_label) < len(default_label)
        assert len(compact_label) <= 12  # 8 + ".."

    def test_compact_legend_below_donut_via_anchor_y(self):
        # In compact mode the legend is anchored below the axes (y < 0).
        # In default mode the legend sits to the right (anchor at axes coords
        # (1.0, 0.5)). Comparing the *bbox y0* in axes coords distinguishes
        # the two without depending on private legend attributes.
        from matplotlib.transforms import Bbox
        items = self._items(3)
        leg_default = create_asset_mix_figure(items).get_axes()[0].get_legend()
        leg_compact = create_asset_mix_figure(items, compact=True).get_axes()[0].get_legend()
        # bbox_to_anchor returns a Bbox in display coords once attached;
        # but get_bbox_to_anchor() initially returns the user Bbox in
        # the legend's bbox_transform (axes coords here). Either way we
        # can compare the y-extent: compact is below (smaller y).
        b_default: Bbox = leg_default.get_bbox_to_anchor()
        b_compact: Bbox = leg_compact.get_bbox_to_anchor()
        # The compact anchor's y-center is below the axes (0.5 vs negative).
        # Use y0 to compare; in display coords both are numeric.
        assert b_compact.y0 < b_default.y0

    def test_compact_keeps_all_categories(self):
        # 8 items → top 5 + "Other" in both modes; compact must not drop
        # categories.
        items = self._items(8)
        legend_default = create_asset_mix_figure(items).get_axes()[0].get_legend()
        legend_compact = create_asset_mix_figure(items, compact=True).get_axes()[0].get_legend()
        assert len(legend_default.get_texts()) == len(legend_compact.get_texts())

    def test_compact_short_names_are_not_over_truncated(self):
        items = [{"name": "Cash", "value": 100, "pct": 1.0, "asset_type": "cash"}]
        fig = create_asset_mix_figure(items, compact=True)
        legend = fig.get_axes()[0].get_legend()
        # "Cash" (4 chars) is below the truncation budget; should appear intact.
        first_line = legend.get_texts()[0].get_text().split("\n")[0]
        assert first_line == "Cash"


class TestReturnDriversCompact:
    def _gainer(self, symbol, pnl):
        return {"symbol": symbol, "name": symbol, "asset_type": "stock",
                "market_value": 1000 + pnl, "cost_basis": 1000,
                "unrealized_pnl": pnl, "unrealized_pnl_pct": pnl / 1000.0}

    def test_compact_keeps_bar_labels(self):
        gainers = [self._gainer("AAPL", 300), self._gainer("MSFT", 500)]
        fig = create_return_drivers_figure(gainers, [], compact=True)
        ax = fig.get_axes()[0]
        # Annotations (the bar value labels) must still be present.
        annotations = [c for c in ax.get_children()
                       if hasattr(c, "get_text") and hasattr(c, "xyann")]
        assert len(annotations) >= len(gainers)

    def test_compact_keeps_color_semantics(self):
        gainers = [self._gainer("UP", 300)]
        losers = [{"symbol": "DOWN", "name": "DOWN", "asset_type": "stock",
                   "market_value": 700, "cost_basis": 1000,
                   "unrealized_pnl": -300, "unrealized_pnl_pct": -0.3}]
        fig = create_return_drivers_figure(gainers, losers, compact=True)
        ax = fig.get_axes()[0]
        # 2 bars (patches), one positive (green) and one negative (red).
        # We only need to confirm both colors are still in use — semantics
        # haven't been collapsed to a single neutral color.
        face_colors = {tuple(p.get_facecolor()) for p in ax.patches}
        assert len(face_colors) == 2

    def test_compact_keeps_tiny_loss_visible(self):
        # A $1 loss should not vanish below quantization in compact mode.
        losers = [{"symbol": "TINY", "name": "Tiny Loss", "asset_type": "stock",
                   "market_value": 999, "cost_basis": 1000, "unrealized_pnl": -1,
                   "unrealized_pnl_pct": -0.001}]
        fig = create_return_drivers_figure([], losers, compact=True)
        ax = fig.get_axes()[0]
        assert len(ax.patches) == 1
        # X-limit must still bracket the bar at -1 with some pad.
        xlim = ax.get_xlim()
        assert xlim[0] < -1
        assert xlim[1] > 0

    def test_compact_uses_smaller_annotation_font(self):
        gainers = [self._gainer("AAPL", 300)]
        ax_default = create_return_drivers_figure(gainers, []).get_axes()[0]
        ax_compact = create_return_drivers_figure(gainers, [], compact=True).get_axes()[0]
        anns_default = [c for c in ax_default.get_children()
                        if hasattr(c, "xyann") and hasattr(c, "get_fontsize")]
        anns_compact = [c for c in ax_compact.get_children()
                        if hasattr(c, "xyann") and hasattr(c, "get_fontsize")]
        assert anns_default and anns_compact
        assert anns_compact[0].get_fontsize() < anns_default[0].get_fontsize()

    def test_compact_huge_pnl_still_compact_label(self):
        gainers = [self._gainer("HUGE", 5_000_000_000)]
        fig = create_return_drivers_figure(gainers, [], compact=True)
        ax = fig.get_axes()[0]
        annotations = [c for c in ax.get_children()
                       if hasattr(c, "get_text") and hasattr(c, "xyann")]
        texts = [a.get_text() for a in annotations]
        assert any("B" in t for t in texts)
        assert not any("5000000000" in t for t in texts)


class TestDashboardPassesCompactToCharts:
    def test_compact_helper_for_layout_modes(self, qapp, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        page = DashboardPage(db_conn)
        for mode, expected in [
            ("wide", False), ("medium", False),
            ("narrow", True), ("compact", True),
        ]:
            page._current_layout_mode = mode
            assert page._is_compact_chart_mode() is expected

    def test_refresh_at_narrow_uses_compact_trend(self, qapp, db_conn):
        # Fewer x-ticks in the rendered trend canvas when refresh runs in
        # narrow mode vs wide mode.
        from src.gui.pages.dashboard import DashboardPage
        from src.engines.snapshots import record_daily_portfolio_snapshot
        from datetime import timedelta
        today = date.today()
        ledger.deposit_cash(db_conn, (today - timedelta(days=25)).isoformat(), 5_000.0)
        # 20 daily snapshots so the tick reducer kicks in.
        for d in range(20):
            record_daily_portfolio_snapshot(db_conn, today - timedelta(days=20 - d))

        page = DashboardPage(db_conn)
        page._apply_responsive_layout(1200)  # wide
        page.refresh()
        wide_xticks = len(page._trend_canvas.figure.get_axes()[0].get_xticks())

        page = DashboardPage(db_conn)
        page._apply_responsive_layout(600)  # narrow
        page.refresh()
        narrow_xticks = len(page._trend_canvas.figure.get_axes()[0].get_xticks())

        assert narrow_xticks < wide_xticks
