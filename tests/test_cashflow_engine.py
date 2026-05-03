"""Tests for the multi-period cashflow engine.

These tests cover:
1. The extracted `compute_cash_flow_breakdown(txns)` helper in reports.py
2. The new `compute_cashflow_series(conn, granularity, ...)` engine in cashflow.py

Both share the same category mapping; the series builder is a per-period loop
over the helper.
"""
from datetime import date

import pytest

from src.models.asset import Asset
from src.models.transaction import Transaction
from src.storage.asset_repo import create_asset
from src.storage.transaction_repo import create_transaction


# ----------------------------------------------------------------------
# Helper: compute_cash_flow_breakdown(txns)
# ----------------------------------------------------------------------

def test_breakdown_empty_txns_returns_zero_structure():
    from src.engines.reports import compute_cash_flow_breakdown
    result = compute_cash_flow_breakdown([])
    assert result["funding_flow"] == {
        "deposits": 0.0, "withdrawals": 0.0, "net": 0.0,
    }
    assert result["trade_cash_flow"] == {
        "buys": 0.0, "sells": 0.0, "net": 0.0,
    }
    assert result["real_estate_cash_flow"]["net"] == 0.0
    assert result["debt_cash_flow"]["net"] == 0.0
    assert result["other_cash_flow"] == 0.0
    assert result["fees_total"] == 0.0


def test_breakdown_categorizes_funding():
    from src.engines.reports import compute_cash_flow_breakdown
    txns = [
        {"txn_type": "deposit_cash", "total_amount": 1000.0, "fees": 0.0},
        {"txn_type": "deposit_cash", "total_amount": 500.0, "fees": 0.0},
        {"txn_type": "withdraw_cash", "total_amount": -200.0, "fees": 0.0},
    ]
    result = compute_cash_flow_breakdown(txns)
    assert result["funding_flow"]["deposits"] == 1500.0
    assert result["funding_flow"]["withdrawals"] == -200.0
    assert result["funding_flow"]["net"] == 1300.0


def test_breakdown_categorizes_trades():
    from src.engines.reports import compute_cash_flow_breakdown
    txns = [
        {"txn_type": "buy", "total_amount": -500.0, "fees": 5.0},
        {"txn_type": "sell", "total_amount": 800.0, "fees": 5.0},
    ]
    result = compute_cash_flow_breakdown(txns)
    assert result["trade_cash_flow"]["buys"] == -500.0
    assert result["trade_cash_flow"]["sells"] == 800.0
    assert result["trade_cash_flow"]["net"] == 300.0
    assert result["fees_total"] == 10.0


def test_breakdown_categorizes_real_estate():
    from src.engines.reports import compute_cash_flow_breakdown
    txns = [
        {"txn_type": "receive_rent", "total_amount": 1500.0, "fees": 0.0},
        {"txn_type": "pay_property_expense", "total_amount": -300.0, "fees": 0.0},
        {"txn_type": "add_property", "total_amount": -100000.0, "fees": 0.0},
        {"txn_type": "sell_property", "total_amount": 120000.0, "fees": 0.0},
    ]
    result = compute_cash_flow_breakdown(txns)
    re = result["real_estate_cash_flow"]
    assert re["rent_received"] == 1500.0
    assert re["property_expenses"] == -300.0
    assert re["property_purchases"] == -100000.0
    assert re["property_sales"] == 120000.0
    assert re["net"] == 1500.0 - 300.0 - 100000.0 + 120000.0


def test_breakdown_categorizes_debt():
    from src.engines.reports import compute_cash_flow_breakdown
    txns = [
        {"txn_type": "add_debt", "total_amount": 50000.0, "fees": 0.0},
        {"txn_type": "pay_debt", "total_amount": -1000.0, "fees": 0.0},
        {"txn_type": "pay_mortgage", "total_amount": -2000.0, "fees": 0.0},
    ]
    result = compute_cash_flow_breakdown(txns)
    dc = result["debt_cash_flow"]
    assert dc["borrowed"] == 50000.0
    assert dc["debt_payments"] == -1000.0
    assert dc["mortgage_payments"] == -2000.0
    assert dc["net"] == 50000.0 - 1000.0 - 2000.0


def test_breakdown_routes_unknown_to_other():
    from src.engines.reports import compute_cash_flow_breakdown
    txns = [
        {"txn_type": "manual_adjustment", "total_amount": -50.0, "fees": 0.0},
        {"txn_type": "manual_adjustment", "total_amount": 25.0, "fees": 0.0},
    ]
    result = compute_cash_flow_breakdown(txns)
    assert result["other_cash_flow"] == -25.0


def test_breakdown_pay_mortgage_lives_in_debt_not_real_estate():
    """pay_mortgage is debt-servicing, not RE operating cash flow.
    Reports.py comment at lines 19-22 is explicit about this."""
    from src.engines.reports import compute_cash_flow_breakdown
    txns = [
        {"txn_type": "pay_mortgage", "total_amount": -1500.0, "fees": 0.0},
    ]
    result = compute_cash_flow_breakdown(txns)
    assert result["real_estate_cash_flow"]["net"] == 0.0
    assert result["debt_cash_flow"]["mortgage_payments"] == -1500.0
    assert result["debt_cash_flow"]["net"] == -1500.0


def test_breakdown_works_with_sqlite_rows():
    """The helper must accept sqlite3.Row objects (the existing call-site
    in build_period_report passes them directly)."""
    from src.engines.reports import compute_cash_flow_breakdown
    from src.storage.database import init_db

    conn = init_db(":memory:")
    create_transaction(conn, Transaction(
        date="2026-03-01", txn_type="deposit_cash",
        total_amount=1000.0, currency="USD",
    ))
    rows = conn.execute(
        "SELECT * FROM transactions",
    ).fetchall()
    result = compute_cash_flow_breakdown(rows)
    assert result["funding_flow"]["deposits"] == 1000.0
    conn.close()


# ----------------------------------------------------------------------
# Series: compute_cashflow_series(conn, granularity, start_date, end_date)
# ----------------------------------------------------------------------

def test_series_monthly_default_returns_12_periods(db_conn):
    """With no transactions, default monthly call still returns 12 zero
    periods so the chart has a continuous x-axis."""
    from src.engines.cashflow import compute_cashflow_series
    series = compute_cashflow_series(db_conn, granularity="monthly")
    assert len(series) == 12
    assert all(p.net == 0.0 for p in series)
    assert all(p.funding_flow == 0.0 for p in series)


def test_series_yearly_default_returns_5_periods(db_conn):
    from src.engines.cashflow import compute_cashflow_series
    series = compute_cashflow_series(db_conn, granularity="yearly")
    assert len(series) == 5
    assert all(p.net == 0.0 for p in series)


def test_series_monthly_explicit_range_inclusive(db_conn):
    """Explicit start/end snap to period boundaries; both endpoints included."""
    from src.engines.cashflow import compute_cashflow_series
    series = compute_cashflow_series(
        db_conn, granularity="monthly",
        start_date=date(2026, 1, 15),
        end_date=date(2026, 4, 10),
    )
    labels = [p.label for p in series]
    assert labels == ["2026-01", "2026-02", "2026-03", "2026-04"]


def test_series_yearly_explicit_range_inclusive(db_conn):
    from src.engines.cashflow import compute_cashflow_series
    series = compute_cashflow_series(
        db_conn, granularity="yearly",
        start_date=date(2023, 6, 1),
        end_date=date(2026, 1, 1),
    )
    labels = [p.label for p in series]
    assert labels == ["2023", "2024", "2025", "2026"]


def test_series_monthly_period_boundaries(db_conn):
    """Each period's start/end are 1st-of-month, end exclusive."""
    from src.engines.cashflow import compute_cashflow_series
    series = compute_cashflow_series(
        db_conn, granularity="monthly",
        start_date=date(2026, 2, 1),
        end_date=date(2026, 3, 31),
    )
    feb = series[0]
    mar = series[1]
    assert feb.period_start == date(2026, 2, 1)
    assert feb.period_end == date(2026, 3, 1)
    assert mar.period_start == date(2026, 3, 1)
    assert mar.period_end == date(2026, 4, 1)


def test_series_aggregates_transactions_per_period(db_conn):
    """Transactions inside a period contribute to that period's totals."""
    from src.engines.cashflow import compute_cashflow_series
    create_transaction(db_conn, Transaction(
        date="2026-02-15", txn_type="deposit_cash",
        total_amount=1000.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-10", txn_type="receive_rent",
        total_amount=2000.0, currency="USD",
    ))
    series = compute_cashflow_series(
        db_conn, granularity="monthly",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 4, 1),
    )
    by_label = {p.label: p for p in series}
    assert by_label["2026-01"].net == 0.0
    assert by_label["2026-02"].funding_flow == 1000.0
    assert by_label["2026-02"].net == 1000.0
    assert by_label["2026-03"].real_estate_cash_flow == 2000.0
    assert by_label["2026-03"].net == 2000.0


def test_series_signed_amounts_per_category(db_conn):
    """Verify signs: deposits +, withdrawals -, buys -, sells +,
    receive_rent +, pay_property_expense -, pay_debt -, pay_mortgage -."""
    from src.engines.cashflow import compute_cashflow_series
    asset = create_asset(db_conn, Asset(
        symbol="AAPL", name="Apple", asset_type="stock",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-01", txn_type="deposit_cash",
        total_amount=10000.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-02", txn_type="withdraw_cash",
        total_amount=-500.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-05", txn_type="buy", asset_id=asset.id,
        quantity=10, price=150.0, total_amount=-1500.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-10", txn_type="sell", asset_id=asset.id,
        quantity=5, price=160.0, total_amount=800.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-15", txn_type="receive_rent",
        total_amount=2000.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-16", txn_type="pay_property_expense",
        total_amount=-300.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-20", txn_type="pay_debt",
        total_amount=-400.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-21", txn_type="pay_mortgage",
        total_amount=-1200.0, currency="USD",
    ))

    series = compute_cashflow_series(
        db_conn, granularity="monthly",
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 31),
    )
    march = series[0]
    assert march.funding_flow == 10000.0 - 500.0
    assert march.trade_cash_flow == -1500.0 + 800.0
    assert march.real_estate_cash_flow == 2000.0 - 300.0
    # pay_mortgage is debt cash flow, not RE
    assert march.debt_cash_flow == -400.0 - 1200.0


def test_series_net_equals_sum_of_categories(db_conn):
    from src.engines.cashflow import compute_cashflow_series
    create_transaction(db_conn, Transaction(
        date="2026-03-01", txn_type="deposit_cash",
        total_amount=1000.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-02", txn_type="receive_rent",
        total_amount=2000.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-03", txn_type="pay_mortgage",
        total_amount=-1500.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-04", txn_type="manual_adjustment",
        total_amount=50.0, currency="USD",
    ))
    series = compute_cashflow_series(
        db_conn, granularity="monthly",
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 31),
    )
    p = series[0]
    expected_sum = (
        p.funding_flow + p.trade_cash_flow
        + p.real_estate_cash_flow + p.debt_cash_flow
        + p.other_cash_flow
    )
    assert p.net == pytest.approx(expected_sum)


def test_series_lumpy_payment_isolated_to_one_month(db_conn):
    """A single annual property tax payment in March produces a strongly
    negative real_estate_cash_flow that month, while neighboring months
    are zero. Validates the cash-basis honesty contract."""
    from src.engines.cashflow import compute_cashflow_series
    create_transaction(db_conn, Transaction(
        date="2026-03-15", txn_type="pay_property_expense",
        total_amount=-12000.0, currency="USD",
        notes="Annual property tax",
    ))
    series = compute_cashflow_series(
        db_conn, granularity="monthly",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 4, 30),
    )
    by_label = {p.label: p for p in series}
    assert by_label["2026-03"].real_estate_cash_flow == -12000.0
    assert by_label["2026-02"].real_estate_cash_flow == 0.0
    assert by_label["2026-04"].real_estate_cash_flow == 0.0


def test_series_yearly_aggregates_full_calendar_year(db_conn):
    """Transactions across multiple months of one year roll into one row."""
    from src.engines.cashflow import compute_cashflow_series
    create_transaction(db_conn, Transaction(
        date="2025-02-01", txn_type="deposit_cash",
        total_amount=1000.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2025-08-15", txn_type="deposit_cash",
        total_amount=2000.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-03-01", txn_type="deposit_cash",
        total_amount=500.0, currency="USD",
    ))
    series = compute_cashflow_series(
        db_conn, granularity="yearly",
        start_date=date(2025, 1, 1),
        end_date=date(2026, 12, 31),
    )
    by_label = {p.label: p for p in series}
    assert by_label["2025"].funding_flow == 3000.0
    assert by_label["2026"].funding_flow == 500.0


def test_series_skips_transactions_outside_range(db_conn):
    from src.engines.cashflow import compute_cashflow_series
    create_transaction(db_conn, Transaction(
        date="2025-12-31", txn_type="deposit_cash",
        total_amount=999.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-05-01", txn_type="deposit_cash",
        total_amount=999.0, currency="USD",
    ))
    series = compute_cashflow_series(
        db_conn, granularity="monthly",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 4, 30),
    )
    assert sum(p.funding_flow for p in series) == 0.0
