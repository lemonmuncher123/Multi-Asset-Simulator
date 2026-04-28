import json
import tempfile
from datetime import date
from pathlib import Path

import pytest

from src.storage.database import init_db
from src.models.asset import Asset
from src.models.report import Report
from src.storage.asset_repo import create_asset
from src.storage.transaction_repo import create_transaction
from src.models.transaction import Transaction
from src.storage.report_repo import (
    create_or_replace_report,
    get_report,
    list_reports,
    report_exists,
    delete_report,
)
from src.engines.reports import (
    build_period_report,
    generate_monthly_report,
    generate_annual_report,
    generate_due_reports,
    get_auto_report_start_date,
    count_due_reports,
)
from src.engines.report_export import export_report_txt, export_report_xlsx


@pytest.fixture
def db_conn():
    conn = init_db(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def populated_db(db_conn):
    a = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    create_transaction(db_conn, Transaction(
        date="2025-06-01", txn_type="deposit_cash",
        total_amount=100000.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2025-06-15", txn_type="buy", asset_id=a.id,
        quantity=10, price=150.0, total_amount=-1500.0, currency="USD", fees=10.0,
    ))
    create_transaction(db_conn, Transaction(
        date="2025-07-01", txn_type="deposit_cash",
        total_amount=5000.0, currency="USD",
    ))
    return db_conn


# ===================================================================
# 2. Report repo tests
# ===================================================================

def test_create_and_get_report(db_conn):
    r = Report(
        report_type="monthly",
        period_start="2025-06-01",
        period_end="2025-07-01",
        period_label="2025-06",
        title="Monthly Report - 2025-06",
        report_json='{"summary": {}}',
    )
    created = create_or_replace_report(db_conn, r)
    assert created.id is not None

    fetched = get_report(db_conn, "monthly", "2025-06")
    assert fetched is not None
    assert fetched.title == "Monthly Report - 2025-06"


def test_get_report_returns_none_for_missing(db_conn):
    assert get_report(db_conn, "monthly", "2099-01") is None


def test_report_exists(db_conn):
    assert not report_exists(db_conn, "monthly", "2025-06")
    create_or_replace_report(db_conn, Report(
        report_type="monthly", period_start="2025-06-01",
        period_end="2025-07-01", period_label="2025-06",
        title="test", report_json="{}",
    ))
    assert report_exists(db_conn, "monthly", "2025-06")


def test_list_reports_filters_by_type(db_conn):
    create_or_replace_report(db_conn, Report(
        report_type="monthly", period_start="2025-06-01",
        period_end="2025-07-01", period_label="2025-06",
        title="m1", report_json="{}",
    ))
    create_or_replace_report(db_conn, Report(
        report_type="annual", period_start="2025-01-01",
        period_end="2026-01-01", period_label="2025",
        title="a1", report_json="{}",
    ))
    all_reports = list_reports(db_conn)
    assert len(all_reports) == 2
    monthly = list_reports(db_conn, "monthly")
    assert len(monthly) == 1
    assert monthly[0].report_type == "monthly"
    annual = list_reports(db_conn, "annual")
    assert len(annual) == 1
    assert annual[0].report_type == "annual"


def test_list_reports_no_filter_returns_all(db_conn):
    for m in range(1, 4):
        create_or_replace_report(db_conn, Report(
            report_type="monthly", period_start=f"2025-{m:02d}-01",
            period_end=f"2025-{m+1:02d}-01", period_label=f"2025-{m:02d}",
            title=f"m{m}", report_json="{}",
        ))
    assert len(list_reports(db_conn)) == 3


def test_create_or_replace_is_idempotent(db_conn):
    for i in range(3):
        create_or_replace_report(db_conn, Report(
            report_type="monthly", period_start="2025-06-01",
            period_end="2025-07-01", period_label="2025-06",
            title=f"version {i}", report_json=f'{{"v": {i}}}',
        ))
    reports = list_reports(db_conn, "monthly")
    assert len(reports) == 1
    assert reports[0].title == "version 2"
    data = json.loads(reports[0].report_json)
    assert data["v"] == 2


def test_same_period_label_different_types_allowed(db_conn):
    create_or_replace_report(db_conn, Report(
        report_type="monthly", period_start="2025-01-01",
        period_end="2025-02-01", period_label="2025-01",
        title="m", report_json="{}",
    ))
    create_or_replace_report(db_conn, Report(
        report_type="annual", period_start="2025-01-01",
        period_end="2026-01-01", period_label="2025-01",
        title="a", report_json="{}",
    ))
    assert len(list_reports(db_conn)) == 2


def test_delete_report(db_conn):
    r = create_or_replace_report(db_conn, Report(
        report_type="monthly", period_start="2025-06-01",
        period_end="2025-07-01", period_label="2025-06",
        title="test", report_json="{}",
    ))
    delete_report(db_conn, r.id)
    assert not report_exists(db_conn, "monthly", "2025-06")


def test_report_preserves_all_fields(db_conn):
    r = Report(
        report_type="annual",
        period_start="2025-01-01",
        period_end="2026-01-01",
        period_label="2025",
        generated_at="2026-01-01T00:00:00",
        title="Annual Report - 2025",
        report_json='{"key": "val"}',
        notes="Some notes",
    )
    create_or_replace_report(db_conn, r)
    fetched = get_report(db_conn, "annual", "2025")
    assert fetched.period_start == "2025-01-01"
    assert fetched.period_end == "2026-01-01"
    assert fetched.generated_at == "2026-01-01T00:00:00"
    assert fetched.notes == "Some notes"
    assert json.loads(fetched.report_json) == {"key": "val"}


# ===================================================================
# 3. Report calculation tests — period boundaries
# ===================================================================

class TestMonthlyPeriodBoundaries:

    def test_monthly_includes_first_day(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
        data = json.loads(report.report_json)
        assert data["summary"]["transaction_count"] == 1

    def test_monthly_includes_last_day(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-30", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
        data = json.loads(report.report_json)
        assert data["summary"]["transaction_count"] == 1

    def test_monthly_excludes_next_month_first(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="deposit_cash",
            total_amount=500.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-05-01", txn_type="deposit_cash",
            total_amount=999.0, currency="USD",
        ))
        report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
        data = json.loads(report.report_json)
        assert data["summary"]["transaction_count"] == 1
        assert data["summary"]["net_cash_flow"] == 500.0

    def test_monthly_includes_full_april(self, db_conn):
        for day in (1, 10, 20, 30):
            create_transaction(db_conn, Transaction(
                date=f"2026-04-{day:02d}", txn_type="deposit_cash",
                total_amount=100.0, currency="USD",
            ))
        report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
        data = json.loads(report.report_json)
        assert data["summary"]["transaction_count"] == 4
        assert data["summary"]["net_cash_flow"] == 400.0


class TestAnnualPeriodBoundaries:

    def test_annual_includes_jan_1(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-01-01", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        report = build_period_report(db_conn, "2026-01-01", "2027-01-01", "2026", "annual")
        data = json.loads(report.report_json)
        assert data["summary"]["transaction_count"] == 1

    def test_annual_includes_dec_31(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-12-31", txn_type="deposit_cash",
            total_amount=2000.0, currency="USD",
        ))
        report = build_period_report(db_conn, "2026-01-01", "2027-01-01", "2026", "annual")
        data = json.loads(report.report_json)
        assert data["summary"]["transaction_count"] == 1
        assert data["summary"]["net_cash_flow"] == 2000.0

    def test_annual_excludes_next_year_jan_1(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-06-15", txn_type="deposit_cash",
            total_amount=500.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2027-01-01", txn_type="deposit_cash",
            total_amount=9999.0, currency="USD",
        ))
        report = build_period_report(db_conn, "2026-01-01", "2027-01-01", "2026", "annual")
        data = json.loads(report.report_json)
        assert data["summary"]["transaction_count"] == 1
        assert data["summary"]["net_cash_flow"] == 500.0


# ===================================================================
# 3. Report calculation tests — financials
# ===================================================================

class TestNetCashFlow:

    def test_net_cash_flow_equals_sum_total_amount(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=10000.0, currency="USD",
        ))
        a = create_asset(db_conn, Asset(symbol="X", name="X", asset_type="stock"))
        create_transaction(db_conn, Transaction(
            date="2026-04-10", txn_type="buy", asset_id=a.id,
            quantity=5, price=100.0, total_amount=-500.0, currency="USD", fees=5.0,
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-20", txn_type="sell", asset_id=a.id,
            quantity=3, price=120.0, total_amount=360.0, currency="USD", fees=3.0,
        ))
        report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
        data = json.loads(report.report_json)
        expected = 10000.0 - 500.0 + 360.0
        assert data["summary"]["net_cash_flow"] == expected


class TestOperatingNetIncome:

    def test_operating_net_income_rent_minus_expense(self, db_conn):
        create_asset(db_conn, Asset(
            symbol="PROP", name="Property", asset_type="real_estate",
            liquidity="illiquid",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-05", txn_type="receive_rent",
            total_amount=3000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-10", txn_type="pay_property_expense",
            total_amount=-800.0, currency="USD",
        ))
        report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
        data = json.loads(report.report_json)
        assert data["summary"]["operating_net_income"] == 3000.0 - 800.0

    def test_deposits_do_not_affect_operating_net_income(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=50000.0, currency="USD",
        ))
        report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
        data = json.loads(report.report_json)
        assert data["summary"]["operating_net_income"] == 0.0
        assert data["summary"]["net_cash_flow"] == 50000.0

    def test_withdrawals_do_not_affect_operating_net_income(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=50000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="withdraw_cash",
            total_amount=-10000.0, currency="USD",
        ))
        report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
        data = json.loads(report.report_json)
        assert data["summary"]["operating_net_income"] == 0.0
        assert data["summary"]["net_cash_flow"] == 40000.0

    def test_buys_sells_do_not_affect_operating_net_income(self, db_conn):
        a = create_asset(db_conn, Asset(symbol="X", name="X", asset_type="stock"))
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=50000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-05", txn_type="buy", asset_id=a.id,
            quantity=10, price=100.0, total_amount=-1000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-20", txn_type="sell", asset_id=a.id,
            quantity=5, price=120.0, total_amount=600.0, currency="USD",
        ))
        report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
        data = json.loads(report.report_json)
        assert data["summary"]["operating_net_income"] == 0.0
        assert data["summary"]["net_cash_flow"] == 50000.0 - 1000.0 + 600.0

    def test_sell_property_in_re_ops_not_in_operating_income(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-05", txn_type="receive_rent",
            total_amount=2000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="sell_property",
            total_amount=500000.0, currency="USD",
        ))
        report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
        data = json.loads(report.report_json)
        assert data["summary"]["operating_net_income"] == 2000.0
        re_types = {t["txn_type"] for t in data["real_estate"]}
        assert "sell_property" in re_types
        assert "receive_rent" in re_types


class TestFees:

    def test_fees_summed_correctly(self, db_conn):
        a = create_asset(db_conn, Asset(symbol="X", name="X", asset_type="stock"))
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=100000.0, currency="USD", fees=0.0,
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-05", txn_type="buy", asset_id=a.id,
            quantity=10, price=100.0, total_amount=-1000.0,
            currency="USD", fees=9.99,
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-20", txn_type="sell", asset_id=a.id,
            quantity=5, price=120.0, total_amount=600.0,
            currency="USD", fees=5.50,
        ))
        report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
        data = json.loads(report.report_json)
        assert abs(data["summary"]["total_fees"] - 15.49) < 0.01


# ===================================================================
# 3. Report calculation — empty period, journal, snapshot, sections
# ===================================================================

def test_build_period_report_empty_period(db_conn):
    report = build_period_report(db_conn, "2025-01-01", "2025-02-01", "2025-01", "monthly")
    data = json.loads(report.report_json)
    assert data["summary"]["transaction_count"] == 0
    assert data["summary"]["net_cash_flow"] == 0.0
    assert data["summary"]["operating_net_income"] == 0.0
    assert data["summary"]["total_fees"] == 0.0
    assert data["summary"]["beginning_cash"] == 0.0
    assert data["summary"]["ending_cash"] == 0.0
    assert data["operations"] == []
    assert data["transactions"] == []
    assert data["trades"] == []
    assert data["real_estate"] == []
    assert data["debt"] == []
    assert data["journal"] == []
    assert "current_snapshot" in data


def test_report_includes_journal_entries(populated_db):
    populated_db.execute(
        "INSERT INTO decision_journal (date, title, thesis) VALUES (?, ?, ?)",
        ("2025-06-15", "Buy AAPL", "Growth thesis"),
    )
    populated_db.commit()
    report = build_period_report(
        populated_db, "2025-06-01", "2025-07-01", "2025-06", "monthly"
    )
    data = json.loads(report.report_json)
    assert len(data["journal"]) == 1
    assert data["journal"][0]["title"] == "Buy AAPL"
    assert data["journal"][0]["thesis"] == "Growth thesis"


def test_journal_entries_outside_period_excluded(populated_db):
    populated_db.execute(
        "INSERT INTO decision_journal (date, title) VALUES (?, ?)",
        ("2025-05-31", "Before period"),
    )
    populated_db.execute(
        "INSERT INTO decision_journal (date, title) VALUES (?, ?)",
        ("2025-07-01", "After period"),
    )
    populated_db.commit()
    report = build_period_report(
        populated_db, "2025-06-01", "2025-07-01", "2025-06", "monthly"
    )
    data = json.loads(report.report_json)
    assert len(data["journal"]) == 0


def test_report_current_snapshot_section(populated_db):
    report = build_period_report(
        populated_db, "2025-06-01", "2025-07-01", "2025-06", "monthly"
    )
    data = json.loads(report.report_json)
    snap = data["current_snapshot"]
    assert "note" in snap
    assert "cash" in snap
    assert "total_assets" in snap
    assert "total_liabilities" in snap
    assert "net_worth" in snap
    assert snap["cash"] is not None


def test_report_operations_breakdown(populated_db):
    report = build_period_report(
        populated_db, "2025-06-01", "2025-07-01", "2025-06", "monthly"
    )
    data = json.loads(report.report_json)
    ops = {o["txn_type"]: o for o in data["operations"]}
    assert "deposit_cash" in ops
    assert "buy" in ops
    assert ops["deposit_cash"]["count"] == 1
    assert ops["buy"]["count"] == 1
    assert ops["deposit_cash"]["total_amount"] == 100000.0
    assert ops["buy"]["total_amount"] == -1500.0


def test_report_trades_section(populated_db):
    report = build_period_report(
        populated_db, "2025-06-01", "2025-07-01", "2025-06", "monthly"
    )
    data = json.loads(report.report_json)
    assert len(data["trades"]) == 1
    assert data["trades"][0]["txn_type"] == "buy"
    assert data["trades"][0]["asset_symbol"] == "AAPL"


def test_report_debt_section(db_conn):
    create_transaction(db_conn, Transaction(
        date="2026-04-01", txn_type="add_debt",
        total_amount=-10000.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-04-15", txn_type="pay_debt",
        total_amount=-500.0, currency="USD",
    ))
    report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
    data = json.loads(report.report_json)
    assert len(data["debt"]) == 2
    debt_types = {d["txn_type"] for d in data["debt"]}
    assert debt_types == {"add_debt", "pay_debt"}


def test_report_beginning_ending_cash(db_conn):
    create_transaction(db_conn, Transaction(
        date="2026-03-15", txn_type="deposit_cash",
        total_amount=10000.0, currency="USD",
    ))
    create_transaction(db_conn, Transaction(
        date="2026-04-10", txn_type="deposit_cash",
        total_amount=5000.0, currency="USD",
    ))
    report = build_period_report(db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly")
    data = json.loads(report.report_json)
    assert data["summary"]["beginning_cash"] == 10000.0
    assert data["summary"]["ending_cash"] == 15000.0


# ===================================================================
# 3. Generate monthly/annual
# ===================================================================

def test_generate_monthly_report(populated_db):
    report = generate_monthly_report(populated_db, 2025, 6)
    assert report.report_type == "monthly"
    assert report.period_label == "2025-06"
    assert report.period_start == "2025-06-01"
    assert report.period_end == "2025-07-01"
    assert report_exists(populated_db, "monthly", "2025-06")


def test_generate_monthly_report_december(db_conn):
    create_transaction(db_conn, Transaction(
        date="2025-12-15", txn_type="deposit_cash",
        total_amount=1000.0, currency="USD",
    ))
    report = generate_monthly_report(db_conn, 2025, 12)
    assert report.period_start == "2025-12-01"
    assert report.period_end == "2026-01-01"
    data = json.loads(report.report_json)
    assert data["summary"]["transaction_count"] == 1


def test_generate_annual_report(populated_db):
    report = generate_annual_report(populated_db, 2025)
    assert report.report_type == "annual"
    assert report.period_label == "2025"
    assert report.period_start == "2025-01-01"
    assert report.period_end == "2026-01-01"
    data = json.loads(report.report_json)
    assert data["summary"]["transaction_count"] == 3


# ===================================================================
# 4. Automatic due report tests
# ===================================================================

class TestGenerateDueReports:

    def test_on_may_1_generates_april(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        generated = generate_due_reports(db_conn, today=date(2026, 5, 1))
        labels = [r.period_label for r in generated]
        assert "2026-04" in labels
        assert "2026-05" not in labels

    def test_on_jan_1_2027_generates_dec_monthly_and_2026_annual(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-12-01", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        generated = generate_due_reports(db_conn, today=date(2027, 1, 1))
        labels = [r.period_label for r in generated]
        assert "2026-12" in labels
        assert "2026" in labels
        monthly_types = [r for r in generated if r.report_type == "monthly"]
        annual_types = [r for r in generated if r.report_type == "annual"]
        assert any(r.period_label == "2026-12" for r in monthly_types)
        assert any(r.period_label == "2026" for r in annual_types)

    def test_backfills_missing_completed_reports(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-01-15", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        generated = generate_due_reports(db_conn, today=date(2026, 4, 15))
        labels = [r.period_label for r in generated]
        assert "2026-01" in labels
        assert "2026-02" in labels
        assert "2026-03" in labels
        assert "2026-04" not in labels

    def test_idempotent_no_duplicates(self, populated_db):
        gen1 = generate_due_reports(populated_db, today=date(2025, 8, 15))
        gen2 = generate_due_reports(populated_db, today=date(2025, 8, 15))
        assert len(gen1) > 0
        assert len(gen2) == 0
        all_reports = list_reports(populated_db)
        labels = [r.period_label for r in all_reports]
        for label in labels:
            assert labels.count(label) == 1 or \
                sum(1 for r in all_reports if r.period_label == label) <= 2

    def test_no_report_for_incomplete_current_month(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        generated = generate_due_reports(db_conn, today=date(2026, 4, 20))
        labels = [r.period_label for r in generated]
        assert "2026-04" not in labels

    def test_no_report_for_incomplete_current_year(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-06-15", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        generated = generate_due_reports(db_conn, today=date(2026, 11, 1))
        labels = [r.period_label for r in generated]
        assert "2026" not in labels

    def test_no_transactions_returns_empty(self, db_conn):
        generated = generate_due_reports(db_conn, today=date(2025, 8, 15))
        assert generated == []

    def test_backfills_annual_for_multiple_years(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2024-03-01", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        generated = generate_due_reports(db_conn, today=date(2026, 2, 1))
        labels = [r.period_label for r in generated]
        assert "2024" in labels
        assert "2025" in labels
        assert "2026" not in labels


# ===================================================================
# 1. Storage/schema tests
# ===================================================================

def test_reports_in_expected_tables():
    from src.storage.database import EXPECTED_TABLES
    assert "reports" in EXPECTED_TABLES


def test_reports_table_exists(db_conn):
    from src.storage.database import verify_tables
    tables = verify_tables(db_conn)
    assert "reports" in tables


def test_unique_constraint_prevents_raw_duplicates(db_conn):
    db_conn.execute(
        "INSERT INTO reports (report_type, period_start, period_end, period_label, "
        "generated_at, title, report_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("monthly", "2025-06-01", "2025-07-01", "2025-06", "now", "t", "{}"),
    )
    db_conn.commit()
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError):
        db_conn.execute(
            "INSERT INTO reports (report_type, period_start, period_end, period_label, "
            "generated_at, title, report_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("monthly", "2025-06-01", "2025-07-01", "2025-06", "now", "t2", "{}"),
        )
    db_conn.rollback()


def test_full_backup_includes_reports():
    from src.engines.full_data_io import EXPORT_TABLES, IMPORT_ORDER
    assert "reports" in EXPORT_TABLES
    assert "reports" in IMPORT_ORDER


# ===================================================================
# Full backup roundtrip
# ===================================================================

def test_full_export_import_preserves_reports(populated_db):
    from src.engines.full_data_io import export_full_data, import_full_data

    generate_monthly_report(populated_db, 2025, 6)
    assert report_exists(populated_db, "monthly", "2025-06")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "backup.zip"
        result = export_full_data(populated_db, out)
        assert result.success

        conn2 = init_db(":memory:")
        result = import_full_data(conn2, out)
        assert result.success, result.message
        assert report_exists(conn2, "monthly", "2025-06")
        r = get_report(conn2, "monthly", "2025-06")
        data = json.loads(r.report_json)
        assert data["summary"]["transaction_count"] == 2
        conn2.close()


# ===================================================================
# get_auto_report_start_date
# ===================================================================

class TestAutoReportStartDate:

    def test_returns_none_for_empty_db(self, db_conn):
        assert get_auto_report_start_date(db_conn) is None

    def test_returns_date_for_normal_transaction(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-03-15", txn_type="deposit_cash",
            total_amount=10000.0, currency="USD",
        ))
        result = get_auto_report_start_date(db_conn)
        assert result == date(2026, 3, 15)

    def test_excludes_existing_property_zero_amount(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2009-01-15", txn_type="add_property",
            total_amount=0.0, currency="USD",
            notes="Existing property entry - no purchase cash impact.",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-03-15", txn_type="deposit_cash",
            total_amount=10000.0, currency="USD",
        ))
        result = get_auto_report_start_date(db_conn)
        assert result == date(2026, 3, 15)

    def test_only_existing_property_returns_none(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2009-01-15", txn_type="add_property",
            total_amount=0.0, currency="USD",
            notes="Existing property entry - no purchase cash impact.",
        ))
        assert get_auto_report_start_date(db_conn) is None

    def test_new_purchase_add_property_not_excluded(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2025-02-01", txn_type="add_property",
            total_amount=-100000.0, currency="USD",
        ))
        result = get_auto_report_start_date(db_conn)
        assert result == date(2025, 2, 1)

    def test_planned_purchase_marker_is_excluded(self, db_conn):
        # Planned-purchase markers (total_amount=0 with the planned-scenario
        # note) are excluded from the auto-report start date so a
        # forward-dated planning row cannot pin the backfill to a future
        # date.
        create_transaction(db_conn, Transaction(
            date="2026-06-01", txn_type="add_property",
            total_amount=0.0, currency="USD",
            notes="Planned purchase scenario - no cash impact.",
        ))
        assert get_auto_report_start_date(db_conn) is None

    def test_planned_purchase_with_real_cash_not_excluded(self, db_conn):
        # If somehow a non-zero amount is recorded with the planned note,
        # the row IS treated as a real cash event and pins the start date.
        create_transaction(db_conn, Transaction(
            date="2026-06-01", txn_type="add_property",
            total_amount=-50000.0, currency="USD",
            notes="Planned purchase scenario - no cash impact.",
        ))
        result = get_auto_report_start_date(db_conn)
        assert result == date(2026, 6, 1)


class TestDueReportsExcludesExistingProperty:

    def test_existing_property_does_not_cause_historical_backfill(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2009-01-15", txn_type="add_property",
            total_amount=0.0, currency="USD",
            notes="Existing property entry - no purchase cash impact.",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-03-15", txn_type="deposit_cash",
            total_amount=10000.0, currency="USD",
        ))
        generated = generate_due_reports(db_conn, today=date(2026, 5, 1))
        labels = [r.period_label for r in generated]
        assert "2026-03" in labels
        assert "2026-04" in labels
        assert "2009-01" not in labels
        assert "2009" not in labels

    def test_only_existing_property_generates_no_reports(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2009-01-15", txn_type="add_property",
            total_amount=0.0, currency="USD",
            notes="Existing property entry - no purchase cash impact.",
        ))
        generated = generate_due_reports(db_conn, today=date(2026, 5, 1))
        assert generated == []


# ===================================================================
# Report summary columns
# ===================================================================

class TestCountDueReports:

    def test_count_matches_generated(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2025-10-15", txn_type="deposit_cash",
            total_amount=5000.0, currency="USD",
        ))
        today = date(2026, 2, 1)
        count = count_due_reports(db_conn, today=today)
        generated = generate_due_reports(db_conn, today=today)
        assert count == len(generated)

    def test_count_zero_when_all_generated(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2025-11-15", txn_type="deposit_cash",
            total_amount=5000.0, currency="USD",
        ))
        today = date(2026, 2, 1)
        generate_due_reports(db_conn, today=today)
        assert count_due_reports(db_conn, today=today) == 0

    def test_count_zero_for_empty_db(self, db_conn):
        assert count_due_reports(db_conn) == 0


class TestReportSummaryColumns:

    def test_create_report_populates_summary_columns(self, db_conn):
        from src.storage.report_repo import list_report_summaries
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=10000.0, currency="USD",
        ))
        report = generate_monthly_report(db_conn, 2026, 4)
        assert report.net_cash_flow == 10000.0
        assert report.transaction_count == 1

        summaries = list_report_summaries(db_conn, report_type="monthly")
        assert len(summaries) == 1
        assert summaries[0].net_cash_flow == 10000.0
        assert summaries[0].transaction_count == 1

    def test_list_report_summaries_limit(self, db_conn):
        from src.storage.report_repo import list_report_summaries
        for m in range(1, 6):
            create_transaction(db_conn, Transaction(
                date=f"2025-{m:02d}-01", txn_type="deposit_cash",
                total_amount=1000.0, currency="USD",
            ))
            generate_monthly_report(db_conn, 2025, m)

        all_summaries = list_report_summaries(db_conn, report_type="monthly")
        assert len(all_summaries) == 5

        limited = list_report_summaries(db_conn, report_type="monthly", limit=3)
        assert len(limited) == 3

    def test_delete_reports_before_date(self, db_conn):
        from src.storage.report_repo import delete_reports_before_date, report_count
        for m in range(1, 7):
            create_transaction(db_conn, Transaction(
                date=f"2025-{m:02d}-01", txn_type="deposit_cash",
                total_amount=1000.0, currency="USD",
            ))
            generate_monthly_report(db_conn, 2025, m)

        assert report_count(db_conn, "monthly") == 6
        deleted = delete_reports_before_date(db_conn, "2025-04-01")
        assert deleted == 3
        assert report_count(db_conn, "monthly") == 3


# ===================================================================
# Phase 1: Cash Flow Breakdown
# ===================================================================


class TestCashFlowBreakdown:

    def test_breakdown_section_exists_with_required_keys(self, db_conn):
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        cfb = data["cash_flow_breakdown"]
        for key in (
            "funding_flow", "trade_cash_flow", "real_estate_cash_flow",
            "debt_cash_flow", "fees_total", "other_cash_flow",
        ):
            assert key in cfb

    def test_funding_flow_separates_deposits_and_withdrawals(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=10000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="withdraw_cash",
            total_amount=-3000.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        ff = data["cash_flow_breakdown"]["funding_flow"]
        assert ff["deposits"] == 10000.0
        assert ff["withdrawals"] == -3000.0
        assert ff["net"] == 7000.0

    def test_trade_cash_flow_separated_from_funding(self, db_conn):
        a = create_asset(db_conn, Asset(symbol="X", name="X", asset_type="stock"))
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=100000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-05", txn_type="buy", asset_id=a.id,
            quantity=10, price=100.0, total_amount=-1000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-20", txn_type="sell", asset_id=a.id,
            quantity=5, price=120.0, total_amount=600.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        cfb = data["cash_flow_breakdown"]
        assert cfb["funding_flow"]["net"] == 100000.0
        assert cfb["trade_cash_flow"]["buys"] == -1000.0
        assert cfb["trade_cash_flow"]["sells"] == 600.0
        assert cfb["trade_cash_flow"]["net"] == -400.0

    def test_real_estate_cash_flow_includes_rent_expenses_buys_sells(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-05", txn_type="receive_rent",
            total_amount=2000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-10", txn_type="pay_property_expense",
            total_amount=-500.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="add_property",
            total_amount=-200000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-20", txn_type="sell_property",
            total_amount=300000.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        rcf = data["cash_flow_breakdown"]["real_estate_cash_flow"]
        assert rcf["rent_received"] == 2000.0
        assert rcf["property_expenses"] == -500.0
        assert rcf["property_purchases"] == -200000.0
        assert rcf["property_sales"] == 300000.0
        assert rcf["net"] == 2000.0 - 500.0 - 200000.0 + 300000.0

    def test_pay_mortgage_lives_in_debt_cash_flow_not_real_estate(self, db_conn):
        # pay_mortgage is debt-servicing cash flow even though it remains in
        # the real_estate display section for backward compatibility.
        create_transaction(db_conn, Transaction(
            date="2026-04-10", txn_type="pay_mortgage",
            total_amount=-2000.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        cfb = data["cash_flow_breakdown"]
        assert cfb["debt_cash_flow"]["mortgage_payments"] == -2000.0
        assert cfb["debt_cash_flow"]["net"] == -2000.0
        assert cfb["real_estate_cash_flow"]["net"] == 0.0

    def test_debt_cash_flow_tracks_borrow_repay_mortgage(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="add_debt",
            total_amount=10000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="pay_debt",
            total_amount=-500.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-20", txn_type="pay_mortgage",
            total_amount=-1000.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        dcf = data["cash_flow_breakdown"]["debt_cash_flow"]
        assert dcf["borrowed"] == 10000.0
        assert dcf["debt_payments"] == -500.0
        assert dcf["mortgage_payments"] == -1000.0
        assert dcf["net"] == 8500.0

    def test_other_cash_flow_catches_uncategorized_types(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-10", txn_type="manual_adjustment",
            total_amount=42.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        assert data["cash_flow_breakdown"]["other_cash_flow"] == 42.0

    def test_fees_total_matches_summary(self, db_conn):
        a = create_asset(db_conn, Asset(symbol="X", name="X", asset_type="stock"))
        create_transaction(db_conn, Transaction(
            date="2026-04-05", txn_type="buy", asset_id=a.id,
            quantity=10, price=100.0, total_amount=-1000.0,
            currency="USD", fees=9.99,
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        cfb_fees = data["cash_flow_breakdown"]["fees_total"]
        assert abs(cfb_fees - 9.99) < 0.001
        assert abs(cfb_fees - data["summary"]["total_fees"]) < 0.001

    def test_breakdown_nets_sum_to_net_cash_flow(self, db_conn):
        # Fees are informational; bucket nets + other should equal net_cash_flow.
        a = create_asset(db_conn, Asset(symbol="X", name="X", asset_type="stock"))
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=10000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-05", txn_type="buy", asset_id=a.id,
            quantity=10, price=100.0, total_amount=-1000.0,
            currency="USD", fees=5.0,
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-10", txn_type="receive_rent",
            total_amount=500.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="pay_mortgage",
            total_amount=-200.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-20", txn_type="manual_adjustment",
            total_amount=7.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        cfb = data["cash_flow_breakdown"]
        total = (
            cfb["funding_flow"]["net"]
            + cfb["trade_cash_flow"]["net"]
            + cfb["real_estate_cash_flow"]["net"]
            + cfb["debt_cash_flow"]["net"]
            + cfb["other_cash_flow"]
        )
        assert abs(total - data["summary"]["net_cash_flow"]) < 0.001


# ===================================================================
# Phase 1: Beginning / Ending Snapshots
# ===================================================================


class TestBeginningEndingSnapshots:

    def _add_snapshot(self, conn, date_str, net_worth, cash=0.0):
        from src.storage.snapshot_repo import create_snapshot
        from src.models.portfolio_snapshot import PortfolioSnapshot
        create_snapshot(conn, PortfolioSnapshot(
            date=date_str, cash=cash, total_assets=net_worth,
            total_liabilities=0.0, net_worth=net_worth,
        ))

    def test_missing_snapshots_do_not_crash(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        assert "beginning_snapshot" in data
        assert "ending_snapshot" in data
        assert data["beginning_snapshot"]["net_worth"] is None
        assert data["ending_snapshot"]["net_worth"] is None
        # Both have explanatory notes so a reader knows why fields are None.
        assert data["beginning_snapshot"]["note"]
        assert data["ending_snapshot"]["note"]

    def test_beginning_snapshot_uses_latest_at_or_before_start(self, db_conn):
        self._add_snapshot(db_conn, "2026-03-15", net_worth=15000.0, cash=15000.0)
        self._add_snapshot(db_conn, "2026-03-31", net_worth=18000.0, cash=18000.0)
        # A snapshot inside the period must NOT be picked for the beginning.
        self._add_snapshot(db_conn, "2026-04-15", net_worth=25000.0, cash=25000.0)
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        bs = data["beginning_snapshot"]
        assert bs["snapshot_date"] == "2026-03-31"
        assert bs["net_worth"] == 18000.0

    def test_ending_snapshot_uses_latest_at_or_before_end(self, db_conn):
        self._add_snapshot(db_conn, "2026-04-30", net_worth=20000.0, cash=20000.0)
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        es = data["ending_snapshot"]
        assert es["snapshot_date"] == "2026-04-30"
        assert es["net_worth"] == 20000.0

    def test_current_snapshot_mirrors_ending_when_available(self, db_conn):
        self._add_snapshot(db_conn, "2026-04-30", net_worth=20000.0, cash=8000.0)
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        cs = data["current_snapshot"]
        # Current snapshot mirrors ending when one is available.
        assert cs["net_worth"] == 20000.0
        assert cs["cash"] == 8000.0
        assert cs["snapshot_date"] == "2026-04-30"

    def test_current_snapshot_falls_back_to_live_summary_when_missing(self, db_conn):
        # No snapshots stored — current_snapshot still has non-None fields
        # via the live portfolio summary fallback (backward compatible).
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=1234.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        cs = data["current_snapshot"]
        assert cs["cash"] is not None
        assert cs["net_worth"] is not None
        assert "current state" in cs["note"].lower()


# ===================================================================
# Phase 1: Performance section
# ===================================================================


class TestPerformanceSection:

    def _add_snapshot(self, conn, date_str, net_worth, cash=0.0):
        from src.storage.snapshot_repo import create_snapshot
        from src.models.portfolio_snapshot import PortfolioSnapshot
        create_snapshot(conn, PortfolioSnapshot(
            date=date_str, cash=cash, total_assets=net_worth,
            total_liabilities=0.0, net_worth=net_worth,
        ))

    def test_performance_section_has_required_keys(self, db_conn):
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        perf = data["performance"]
        for key in (
            "beginning_net_worth", "ending_net_worth", "net_worth_change",
            "funding_flow", "approximate_investment_result",
            "approximate_return_pct", "data_quality_note",
        ):
            assert key in perf

    def test_performance_does_not_label_as_strict_twr_or_irr(self, db_conn):
        """Phase 1 must not introduce strict TWR/IRR labeling."""
        self._add_snapshot(db_conn, "2026-03-31", net_worth=10000.0)
        self._add_snapshot(db_conn, "2026-04-30", net_worth=12000.0)
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="deposit_cash",
            total_amount=500.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        perf = data["performance"]
        for key in perf.keys():
            low = key.lower()
            assert "twr" not in low
            assert "irr" not in low
            assert "time_weighted" not in low
            assert "internal_rate" not in low
        note = (perf.get("data_quality_note") or "").lower()
        assert "twr" not in note
        assert "irr" not in note

    def test_performance_with_missing_snapshots_funding_flow_still_set(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=10000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="withdraw_cash",
            total_amount=-2000.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        perf = data["performance"]
        assert perf["beginning_net_worth"] is None
        assert perf["ending_net_worth"] is None
        assert perf["net_worth_change"] is None
        assert perf["approximate_investment_result"] is None
        assert perf["approximate_return_pct"] is None
        # Funding flow is still computed from transactions.
        assert perf["funding_flow"] == 8000.0
        assert "snapshot" in perf["data_quality_note"].lower()

    def test_performance_with_both_snapshots_computes_approx_result(self, db_conn):
        self._add_snapshot(db_conn, "2026-03-31", net_worth=10000.0, cash=10000.0)
        self._add_snapshot(db_conn, "2026-04-30", net_worth=12000.0, cash=11000.0)
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        perf = data["performance"]
        assert perf["beginning_net_worth"] == 10000.0
        assert perf["ending_net_worth"] == 12000.0
        assert perf["net_worth_change"] == 2000.0
        assert perf["funding_flow"] == 1000.0
        # 2000 change minus 1000 funding = 1000 portfolio movement
        assert perf["approximate_investment_result"] == 1000.0
        # 1000 / 10000 * 100 = 10%
        assert abs(perf["approximate_return_pct"] - 10.0) < 0.001

    def test_performance_return_pct_none_when_beginning_zero(self, db_conn):
        self._add_snapshot(db_conn, "2026-03-31", net_worth=0.0)
        self._add_snapshot(db_conn, "2026-04-30", net_worth=100.0)
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        perf = data["performance"]
        assert perf["beginning_net_worth"] == 0.0
        assert perf["approximate_return_pct"] is None

    def test_performance_data_quality_note_marks_metrics_approximate(self, db_conn):
        self._add_snapshot(db_conn, "2026-03-31", net_worth=10000.0)
        self._add_snapshot(db_conn, "2026-04-30", net_worth=11000.0)
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        note = data["performance"]["data_quality_note"].lower()
        assert "approximate" in note


# ===================================================================
# Phase 1: Backward compatibility — generated reports keep old fields
# ===================================================================


def test_generated_reports_still_contain_existing_summary_fields(db_conn):
    create_transaction(db_conn, Transaction(
        date="2026-04-01", txn_type="deposit_cash",
        total_amount=1000.0, currency="USD",
    ))
    monthly = generate_monthly_report(db_conn, 2026, 4)
    annual = generate_annual_report(db_conn, 2025)
    for r in (monthly, annual):
        data = json.loads(r.report_json)
        for key in (
            "report_type", "period_label", "period_start", "period_end",
            "generated_at", "transaction_count", "beginning_cash",
            "ending_cash", "net_cash_flow", "operating_net_income",
            "total_inflow", "total_outflow", "total_fees",
        ):
            assert key in data["summary"]
        for key in (
            "operations", "transactions", "trades", "real_estate",
            "debt", "journal", "current_snapshot",
        ):
            assert key in data


# ===================================================================
# Phase 2: Allocation section
# ===================================================================


class TestAllocationSection:

    def test_section_present_in_monthly_report(self, populated_db):
        report = generate_monthly_report(populated_db, 2025, 6)
        data = json.loads(report.report_json)
        assert "allocation" in data

    def test_section_present_in_annual_report(self, populated_db):
        report = generate_annual_report(populated_db, 2025)
        data = json.loads(report.report_json)
        assert "allocation" in data

    def test_allocation_has_required_keys(self, populated_db):
        report = build_period_report(
            populated_db, "2025-06-01", "2025-07-01", "2025-06", "monthly"
        )
        data = json.loads(report.report_json)
        alloc = data["allocation"]
        for key in (
            "source", "as_of", "data_quality_note", "cash_amount",
            "total_assets", "total_liabilities", "net_worth",
            "cash_pct", "by_asset_type", "top_assets", "by_liquidity",
            "real_estate_equity_pct", "debt_ratio",
            "liquid_assets", "illiquid_assets",
        ):
            assert key in alloc

    def test_missing_snapshot_falls_back_to_current(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=10000.0, currency="USD",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        alloc = data["allocation"]
        assert alloc["source"] == "current"
        assert alloc["as_of"] is None
        # Note clearly identifies current-state fallback.
        assert "current portfolio state" in alloc["data_quality_note"].lower()

    def test_snapshot_with_allocation_used_as_source(self, db_conn):
        from src.storage.snapshot_repo import create_snapshot
        from src.models.portfolio_snapshot import PortfolioSnapshot
        alloc_payload = {
            "by_asset_type": {"stock": {"value": 5000.0, "pct": 0.5}},
            "by_asset": [{
                "name": "AAPL", "asset_type": "stock",
                "value": 5000.0, "pct": 0.5,
            }],
            "by_liquidity": {"liquid": {"value": 5000.0, "pct": 0.5}},
            "by_currency": {"USD": {"value": 5000.0, "pct": 0.5}},
            "by_region": {"US": {"value": 5000.0, "pct": 0.5}},
            "cash_pct": 0.5,
            "crypto_pct": 0.0,
            "real_estate_equity_pct": 0.0,
            "debt_ratio": 0.0,
            "liquid_assets": 5000.0,
            "illiquid_assets": 0.0,
        }
        create_snapshot(db_conn, PortfolioSnapshot(
            date="2026-04-30", cash=5000.0, total_assets=10000.0,
            total_liabilities=0.0, net_worth=10000.0,
            allocation_json=json.dumps(alloc_payload),
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        alloc = data["allocation"]
        assert alloc["source"] == "snapshot"
        assert alloc["as_of"] == "2026-04-30"
        assert alloc["cash_amount"] == 5000.0
        assert alloc["total_assets"] == 10000.0
        assert alloc["cash_pct"] == 0.5
        assert "stock" in alloc["by_asset_type"]
        assert "snapshot" in alloc["data_quality_note"].lower()

    def test_snapshot_without_allocation_json_falls_back(self, db_conn):
        from src.storage.snapshot_repo import create_snapshot
        from src.models.portfolio_snapshot import PortfolioSnapshot
        create_snapshot(db_conn, PortfolioSnapshot(
            date="2026-04-30", cash=5000.0, total_assets=5000.0,
            total_liabilities=0.0, net_worth=5000.0,
            allocation_json=None,
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        alloc = data["allocation"]
        assert alloc["source"] == "current"
        # Note clearly explains the fallback reason.
        note = alloc["data_quality_note"].lower()
        assert "current portfolio state" in note

    def test_snapshot_with_invalid_allocation_json_falls_back(self, db_conn):
        from src.storage.snapshot_repo import create_snapshot
        from src.models.portfolio_snapshot import PortfolioSnapshot
        create_snapshot(db_conn, PortfolioSnapshot(
            date="2026-04-30", cash=5000.0, total_assets=5000.0,
            total_liabilities=0.0, net_worth=5000.0,
            allocation_json="not-valid-json",
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        alloc = data["allocation"]
        assert alloc["source"] == "current"
        note = alloc["data_quality_note"].lower()
        assert "could not be parsed" in note or "fallback" in note

    def test_top_assets_capped(self, db_conn):
        # Create many positions; top_assets should be capped (limit = 10).
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=100000.0, currency="USD",
        ))
        for i in range(15):
            a = create_asset(db_conn, Asset(
                symbol=f"S{i:02d}", name=f"S{i:02d}", asset_type="stock",
            ))
            create_transaction(db_conn, Transaction(
                date="2026-04-15", txn_type="buy", asset_id=a.id,
                quantity=10, price=100.0 + i,
                total_amount=-(10 * (100.0 + i)), currency="USD",
            ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        top = data["allocation"]["top_assets"]
        assert len(top) <= 10


# ===================================================================
# Phase 2: Risk summary section
# ===================================================================


class TestRiskSummarySection:

    def test_section_present_in_monthly_report(self, populated_db):
        report = generate_monthly_report(populated_db, 2025, 6)
        data = json.loads(report.report_json)
        assert "risk_summary" in data

    def test_section_present_in_annual_report(self, populated_db):
        report = generate_annual_report(populated_db, 2025)
        data = json.loads(report.report_json)
        assert "risk_summary" in data

    def test_risk_summary_has_required_keys(self, populated_db):
        report = build_period_report(
            populated_db, "2025-06-01", "2025-07-01", "2025-06", "monthly"
        )
        data = json.loads(report.report_json)
        risk = data["risk_summary"]
        for key in (
            "source", "warning_count", "info_count", "total_count",
            "by_severity", "by_category", "warnings", "data_quality_note",
        ):
            assert key in risk

    def test_risk_summary_reuses_get_all_warnings(self, populated_db):
        # Risk summary must reuse the existing engine, not duplicate rules.
        from src.engines.risk import get_all_warnings
        engine_warnings = get_all_warnings(populated_db)
        report = build_period_report(
            populated_db, "2025-06-01", "2025-07-01", "2025-06", "monthly"
        )
        data = json.loads(report.report_json)
        risk = data["risk_summary"]
        assert risk["total_count"] == len(engine_warnings)
        report_messages = sorted(w["message"] for w in risk["warnings"])
        engine_messages = sorted(w.message for w in engine_warnings)
        assert report_messages == engine_messages

    def test_risk_summary_marks_current_state(self, populated_db):
        report = build_period_report(
            populated_db, "2025-06-01", "2025-07-01", "2025-06", "monthly"
        )
        data = json.loads(report.report_json)
        risk = data["risk_summary"]
        assert risk["source"] == "current"
        assert "current" in risk["data_quality_note"].lower()

    def test_risk_summary_warning_fields(self, populated_db):
        report = build_period_report(
            populated_db, "2025-06-01", "2025-07-01", "2025-06", "monthly"
        )
        data = json.loads(report.report_json)
        for w in data["risk_summary"]["warnings"]:
            for key in ("severity", "category", "message"):
                assert key in w

    def test_risk_summary_actionable_excludes_info(self, populated_db):
        from src.engines.risk import get_all_warnings
        engine_warnings = get_all_warnings(populated_db)
        actionable = [w for w in engine_warnings if w.severity != "info"]
        info_only = [w for w in engine_warnings if w.severity == "info"]
        report = build_period_report(
            populated_db, "2025-06-01", "2025-07-01", "2025-06", "monthly"
        )
        data = json.loads(report.report_json)
        risk = data["risk_summary"]
        assert risk["warning_count"] == len(actionable)
        assert risk["info_count"] == len(info_only)

    def test_risk_summary_zero_counts_on_empty_db(self, db_conn):
        report = build_period_report(
            db_conn, "2025-01-01", "2025-02-01", "2025-01", "monthly"
        )
        data = json.loads(report.report_json)
        risk = data["risk_summary"]
        assert risk["warning_count"] == 0
        assert risk["info_count"] == 0
        assert risk["total_count"] == 0
        assert risk["warnings"] == []

    def test_risk_summary_marks_snapshot_divergence_when_present(self, db_conn):
        # When an ending snapshot exists, the data quality note must call
        # out that risk warnings are still current-state and may diverge.
        from src.storage.snapshot_repo import create_snapshot
        from src.models.portfolio_snapshot import PortfolioSnapshot
        create_snapshot(db_conn, PortfolioSnapshot(
            date="2026-04-30", cash=10000.0, total_assets=10000.0,
            total_liabilities=0.0, net_worth=10000.0,
        ))
        report = build_period_report(
            db_conn, "2026-04-01", "2026-05-01", "2026-04", "monthly"
        )
        data = json.loads(report.report_json)
        note = data["risk_summary"]["data_quality_note"].lower()
        assert "current" in note
        # Either explicitly mentions divergence or "not historically stored".
        assert "differ" in note or "not historically stored" in note


# ===================================================================
# Phase 2: Generation does not crash without snapshots
# ===================================================================


class TestPhase2GenerationDoesNotCrash:

    def test_monthly_report_without_any_snapshot(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        report = generate_monthly_report(db_conn, 2026, 4)
        data = json.loads(report.report_json)
        assert "allocation" in data
        assert "risk_summary" in data
        assert data["allocation"]["source"] == "current"

    def test_annual_report_without_any_snapshot(self, db_conn):
        create_transaction(db_conn, Transaction(
            date="2025-04-01", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        report = generate_annual_report(db_conn, 2025)
        data = json.loads(report.report_json)
        assert "allocation" in data
        assert "risk_summary" in data
        assert data["allocation"]["source"] == "current"

    def test_report_with_snapshot_no_alloc_json_does_not_crash(self, db_conn):
        from src.storage.snapshot_repo import create_snapshot
        from src.models.portfolio_snapshot import PortfolioSnapshot
        create_snapshot(db_conn, PortfolioSnapshot(
            date="2026-04-30", cash=0.0, total_assets=0.0,
            total_liabilities=0.0, net_worth=0.0,
            allocation_json=None,
        ))
        report = generate_monthly_report(db_conn, 2026, 4)
        data = json.loads(report.report_json)
        assert "allocation" in data
        assert data["allocation"]["source"] == "current"


# ===================================================================
# Phase 3: Lightweight summary fields
# ===================================================================


class TestPhase3SummaryColumnsOnSave:
    """`create_or_replace_report` extracts the new performance-derived
    metrics from report_json and writes them into the new columns."""

    def test_funding_flow_extracted_when_no_snapshots(self, db_conn):
        # Without snapshots, performance.funding_flow is computed from
        # transactions; net_worth_change / approx_return_pct are None.
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=10000.0, currency="USD",
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="withdraw_cash",
            total_amount=-2000.0, currency="USD",
        ))
        report = generate_monthly_report(db_conn, 2026, 4)
        assert report.funding_flow == 8000.0
        assert report.net_worth_change is None
        assert report.approximate_return_pct is None

    def test_perf_columns_populated_when_snapshots_exist(self, db_conn):
        from src.storage.snapshot_repo import create_snapshot
        from src.models.portfolio_snapshot import PortfolioSnapshot
        create_snapshot(db_conn, PortfolioSnapshot(
            date="2026-03-31", cash=10000.0, total_assets=10000.0,
            total_liabilities=0.0, net_worth=10000.0,
        ))
        create_snapshot(db_conn, PortfolioSnapshot(
            date="2026-04-30", cash=11000.0, total_assets=12000.0,
            total_liabilities=0.0, net_worth=12000.0,
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        report = generate_monthly_report(db_conn, 2026, 4)
        assert report.net_worth_change == 2000.0
        assert report.funding_flow == 1000.0
        # 1000 / 10000 * 100 = 10%
        assert abs(report.approximate_return_pct - 10.0) < 0.001

    def test_pre_phase1_report_json_does_not_crash_save(self, db_conn):
        # report_json without `performance` (pre-Phase-1 shape) still
        # round-trips. New columns get safe defaults / NULL.
        from src.storage.report_repo import (
            create_or_replace_report, get_report,
        )
        from src.models.report import Report
        old_json = json.dumps({
            "summary": {
                "net_cash_flow": 100.0,
                "operating_net_income": 50.0,
                "transaction_count": 2,
            },
        })
        r = Report(
            report_type="monthly",
            period_start="2024-01-01",
            period_end="2024-02-01",
            period_label="2024-01",
            generated_at="2024-02-01T00:00:00",
            title="Old",
            report_json=old_json,
        )
        create_or_replace_report(db_conn, r)
        assert r.funding_flow == 0.0
        assert r.net_worth_change is None
        assert r.approximate_return_pct is None
        # Round-trip via get_report.
        fetched = get_report(db_conn, "monthly", "2024-01")
        assert fetched.funding_flow == 0.0
        assert fetched.net_worth_change is None
        assert fetched.approximate_return_pct is None

    def test_invalid_report_json_does_not_crash_save(self, db_conn):
        from src.storage.report_repo import create_or_replace_report
        from src.models.report import Report
        r = Report(
            report_type="monthly",
            period_start="2024-01-01",
            period_end="2024-02-01",
            period_label="2024-01",
            generated_at="2024-02-01T00:00:00",
            title="Bad",
            report_json="not-valid-json",
        )
        # Must not crash; new columns fall back to safe defaults.
        create_or_replace_report(db_conn, r)
        assert r.funding_flow == 0.0
        assert r.net_worth_change is None
        assert r.approximate_return_pct is None


class TestPhase3ListReportSummaries:

    def test_list_summaries_returns_new_fields(self, db_conn):
        from src.storage.report_repo import list_report_summaries
        from src.storage.snapshot_repo import create_snapshot
        from src.models.portfolio_snapshot import PortfolioSnapshot
        create_snapshot(db_conn, PortfolioSnapshot(
            date="2026-03-31", cash=10000.0, total_assets=10000.0,
            total_liabilities=0.0, net_worth=10000.0,
        ))
        create_snapshot(db_conn, PortfolioSnapshot(
            date="2026-04-30", cash=11000.0, total_assets=12000.0,
            total_liabilities=0.0, net_worth=12000.0,
        ))
        create_transaction(db_conn, Transaction(
            date="2026-04-15", txn_type="deposit_cash",
            total_amount=1000.0, currency="USD",
        ))
        generate_monthly_report(db_conn, 2026, 4)
        summaries = list_report_summaries(db_conn, report_type="monthly")
        assert len(summaries) == 1
        s = summaries[0]
        assert s.net_worth_change == 2000.0
        assert s.funding_flow == 1000.0
        assert abs(s.approximate_return_pct - 10.0) < 0.001

    def test_list_summaries_query_does_not_select_report_json(self, db_conn):
        # Capture every SQL statement executed by list_report_summaries.
        # report_json must not appear in any of them — that's what keeps
        # the list refresh lightweight.
        from src.storage.report_repo import list_report_summaries
        captured: list[str] = []
        db_conn.set_trace_callback(captured.append)
        try:
            list_report_summaries(db_conn, report_type="monthly")
        finally:
            db_conn.set_trace_callback(None)
        assert captured, "expected list_report_summaries to issue a SQL query"
        for sql in captured:
            assert "report_json" not in sql, f"unexpected report_json in: {sql}"
        assert any("SELECT" in sql.upper() for sql in captured)

    def test_summaries_show_none_for_missing_perf_data(self, db_conn):
        from src.storage.report_repo import list_report_summaries
        # No snapshots → perf metrics are None; funding flow comes from
        # transactions and is still populated.
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=5000.0, currency="USD",
        ))
        generate_monthly_report(db_conn, 2026, 4)
        s = list_report_summaries(db_conn, report_type="monthly")[0]
        assert s.funding_flow == 5000.0
        assert s.net_worth_change is None
        assert s.approximate_return_pct is None

    def test_summary_row_preserves_existing_fields(self, db_conn):
        from src.storage.report_repo import list_report_summaries
        create_transaction(db_conn, Transaction(
            date="2026-04-01", txn_type="deposit_cash",
            total_amount=10000.0, currency="USD",
        ))
        generate_monthly_report(db_conn, 2026, 4)
        s = list_report_summaries(db_conn, report_type="monthly")[0]
        # Existing fields still present.
        assert s.id is not None
        assert s.report_type == "monthly"
        assert s.period_label == "2026-04"
        assert s.title.startswith("Monthly")
        assert s.net_cash_flow == 10000.0
        assert s.operating_net_income == 0.0
        assert s.transaction_count == 1
