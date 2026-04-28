import json
import pytest
from datetime import date

from src.engines import ledger
from src.engines.snapshots import (
    has_meaningful_portfolio_state,
    build_portfolio_snapshot,
    record_daily_portfolio_snapshot,
)
from src.engines.portfolio import get_portfolio_summary
from src.engines.dashboard import get_net_worth_trend
from src.engines.data_management import clear_all_user_data
from src.storage.snapshot_repo import list_snapshots


# ===================================================================
# 1. Empty database
# ===================================================================

class TestEmptyDatabase:
    def test_no_meaningful_state(self, db_conn):
        assert has_meaningful_portfolio_state(db_conn) is False

    def test_record_returns_none(self, db_conn):
        result = record_daily_portfolio_snapshot(db_conn)
        assert result is None

    def test_no_snapshot_written(self, db_conn):
        record_daily_portfolio_snapshot(db_conn)
        assert list_snapshots(db_conn) == []


# ===================================================================
# 2. Cash-only portfolio
# ===================================================================

class TestCashOnlyPortfolio:
    def test_has_meaningful_state(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-04-27", 50000.0)
        assert has_meaningful_portfolio_state(db_conn) is True

    def test_record_writes_snapshot(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-04-27", 50000.0)
        snap = record_daily_portfolio_snapshot(db_conn, date(2026, 4, 27))
        assert snap is not None
        assert snap.id is not None
        assert snap.date == "2026-04-27"

    def test_snapshot_matches_summary(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-04-27", 50000.0)
        summary = get_portfolio_summary(db_conn)
        snap = record_daily_portfolio_snapshot(db_conn, date(2026, 4, 27))
        assert snap.cash == summary["cash"]
        assert snap.net_worth == summary["net_worth"]
        assert snap.total_assets == summary["total_assets"]
        assert snap.total_liabilities == summary["total_liabilities"]


# ===================================================================
# 3. Same-day upsert
# ===================================================================

class TestSameDayUpsert:
    def test_upsert_keeps_one_row(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-04-27", 50000.0)
        record_daily_portfolio_snapshot(db_conn, date(2026, 4, 27))

        ledger.deposit_cash(db_conn, "2026-04-27", 10000.0)
        record_daily_portfolio_snapshot(db_conn, date(2026, 4, 27))

        snapshots = list_snapshots(db_conn)
        assert len(snapshots) == 1

    def test_upsert_updates_values(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-04-27", 50000.0)
        record_daily_portfolio_snapshot(db_conn, date(2026, 4, 27))

        ledger.deposit_cash(db_conn, "2026-04-27", 10000.0)
        snap = record_daily_portfolio_snapshot(db_conn, date(2026, 4, 27))

        assert snap.cash == 60000.0
        assert snap.net_worth == 60000.0


# ===================================================================
# 4. Allocation JSON
# ===================================================================

class TestAllocationJson:
    def test_valid_json(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-04-27", 50000.0)
        snap = record_daily_portfolio_snapshot(db_conn, date(2026, 4, 27))
        parsed = json.loads(snap.allocation_json)
        assert isinstance(parsed, dict)

    def test_includes_allocation_keys(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-04-27", 50000.0)
        snap = record_daily_portfolio_snapshot(db_conn, date(2026, 4, 27))
        parsed = json.loads(snap.allocation_json)
        for key in ("by_asset_type", "by_liquidity", "by_currency", "by_region"):
            assert key in parsed


# ===================================================================
# 5. Different days
# ===================================================================

class TestDifferentDays:
    def test_two_dates_two_rows(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-04-25", 50000.0)
        record_daily_portfolio_snapshot(db_conn, date(2026, 4, 25))

        ledger.deposit_cash(db_conn, "2026-04-26", 10000.0)
        record_daily_portfolio_snapshot(db_conn, date(2026, 4, 26))

        snapshots = list_snapshots(db_conn)
        assert len(snapshots) == 2

    def test_trend_returns_both(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-04-25", 50000.0)
        record_daily_portfolio_snapshot(db_conn, date(2026, 4, 25))

        ledger.deposit_cash(db_conn, "2026-04-26", 10000.0)
        record_daily_portfolio_snapshot(db_conn, date(2026, 4, 26))

        trend = get_net_worth_trend(db_conn, days=90)
        assert len(trend) == 2
        assert trend[0]["date"] == "2026-04-25"
        assert trend[1]["date"] == "2026-04-26"


# ===================================================================
# 6. Clear All Data behavior
# ===================================================================

class TestClearAllData:
    def test_no_snapshot_after_clear(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-04-27", 50000.0)
        record_daily_portfolio_snapshot(db_conn, date(2026, 4, 27))
        assert len(list_snapshots(db_conn)) == 1

        clear_all_user_data(db_conn)
        assert len(list_snapshots(db_conn)) == 0

        result = record_daily_portfolio_snapshot(db_conn, date(2026, 4, 27))
        assert result is None
        assert len(list_snapshots(db_conn)) == 0
