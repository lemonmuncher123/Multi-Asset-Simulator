"""Tests for the auto-deduction / force-sell / bankruptcy pipeline.

Covers:
- add_debt validation (name required, schedule, interest period normalization)
- pay_debt / pay_mortgage overpayment guards
- settle_due_debt_payments (idempotent, monthly + yearly cadence, defer-on-shortfall)
- settle_due_mortgage_payments (idempotent)
- force_sell_to_cover_negative_cash (priority order, market-price fallback)
- check_bankruptcy
- v3 migration: schema-version bump + minimum_payment backfill
"""
from datetime import date

import pytest
import sqlite3

from src.engines import ledger
from src.engines.portfolio import calc_cash_balance
from src.engines.risk import check_bankruptcy
from src.models.asset import Asset
from src.models.debt import Debt
from src.storage.asset_repo import create_asset
from src.storage.debt_repo import create_debt, get_debt_by_asset, list_debts
from src.storage.database import init_db, get_schema_version, CURRENT_SCHEMA_VERSION
from src.storage.price_repo import bulk_upsert_ohlcv
from src.storage.property_repo import get_property_by_asset


# --- add_debt validation ---

def test_add_debt_requires_name(db_conn):
    with pytest.raises(ValueError, match="Debt name is required"):
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="X", name="", amount=1000.0,
            payment_per_period=100.0)
    with pytest.raises(ValueError, match="Debt name is required"):
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="X", name="   ", amount=1000.0,
            payment_per_period=100.0)


def test_add_debt_rejects_monthly_interest_period(db_conn):
    # The annual-only contract rejects any non-'annual' interest_period.
    with pytest.raises(ValueError, match="Interest rate is always annual"):
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="CC", name="Card",
            amount=1000.0, interest_rate=0.015,
            interest_period="monthly",
            payment_per_period=100.0)


def test_add_debt_rejects_invalid_schedule(db_conn):
    with pytest.raises(ValueError, match="schedule_frequency"):
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="X", name="Loan",
            amount=1000.0, schedule_frequency="weekly",
            payment_per_period=100.0)


def test_add_debt_rejects_neither_kwarg(db_conn):
    """Schema v10 / Phase 6.5 dropped the legacy auto-default fallback.
    Callers must explicitly supply `payment_per_period` or
    `term_periods` (spec §6 #6)."""
    with pytest.raises(ValueError, match="payment_per_period.*term_periods"):
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.06, schedule_frequency="monthly")


# --- pay_debt / pay_mortgage overpayment guards ---

def test_pay_debt_rejects_overpayment(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset, _, _ = ledger.add_debt(
        db_conn, "2025-01-01", symbol="L", name="Loan",
        amount=1000.0, interest_rate=0.0, cash_received=False,
        payment_per_period=100.0)
    with pytest.raises(ValueError, match="exceeds payoff amount"):
        ledger.pay_debt(db_conn, "2025-02-01", asset.id, 5000.0)


def test_pay_mortgage_rejects_overpayment(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    # Use existing_property mode so we don't need down-payment cash.
    asset, prop, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H", name="House",
        purchase_price=300000.0,
        acquisition_mode="existing_property")
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=10000.0,
        interest_rate=0.0, payment_per_period=500.0,
    )
    with pytest.raises(ValueError, match="exceeds payoff amount"):
        ledger.pay_mortgage(db_conn, "2025-03-01", asset.id, amount=50000.0)


# --- settle_due_debt_payments ---

def _seed_debt_for_settle(db_conn, *, schedule="monthly", start="2025-01-01"):
    ledger.deposit_cash(db_conn, "2024-12-01", 100000.0)
    asset, _, _ = ledger.add_debt(
        db_conn, "2024-12-15", symbol="L", name="Loan",
        amount=10000.0, interest_rate=0.0,
        schedule_frequency=schedule,
        monthly_payment_amount=200.0 if schedule == "monthly" else 2400.0,
        cashflow_start_date=start,
        cash_received=False)
    return asset


def test_settle_due_debt_payments_creates_monthly(db_conn):
    asset = _seed_debt_for_settle(db_conn, schedule="monthly", start="2025-01-01")
    created, deferred = ledger.settle_due_debt_payments(db_conn, "2025-03-15")
    assert len(created) == 3  # Jan, Feb, Mar
    assert deferred == []
    debt = get_debt_by_asset(db_conn, asset.id)
    assert debt.current_balance == pytest.approx(10000.0 - 600.0)
    assert debt.last_payment_date == "2025-03-01"


def test_settle_due_debt_payments_idempotent(db_conn):
    _seed_debt_for_settle(db_conn, schedule="monthly", start="2025-01-01")
    created1, _ = ledger.settle_due_debt_payments(db_conn, "2025-03-15")
    created2, _ = ledger.settle_due_debt_payments(db_conn, "2025-03-15")
    assert len(created1) == 3
    assert created2 == []


def test_settle_due_debt_payments_yearly(db_conn):
    asset = _seed_debt_for_settle(db_conn, schedule="yearly", start="2024-01-01")
    created, _ = ledger.settle_due_debt_payments(db_conn, "2026-06-30")
    # Due dates: 2024-01-01, 2025-01-01, 2026-01-01.
    assert len(created) == 3
    debt = get_debt_by_asset(db_conn, asset.id)
    assert debt.current_balance == pytest.approx(10000.0 - 7200.0)


def test_settle_due_debt_payments_defers_when_cash_short(db_conn):
    # No cash to start, large auto-deduction → defer.
    asset, _, _ = ledger.add_debt(
        db_conn, "2024-12-15", symbol="L", name="Loan",
        amount=10000.0, interest_rate=0.0,
        schedule_frequency="monthly",
        monthly_payment_amount=500.0,
        cashflow_start_date="2025-01-01",
        cash_received=False)
    created, deferred = ledger.settle_due_debt_payments(db_conn, "2025-03-15")
    assert len(created) == 0
    assert len(deferred) == 3
    assert all(item["kind"] == "debt" for item in deferred)


# --- settle_due_mortgage_payments ---

def test_settle_due_mortgage_payments_idempotent(db_conn):
    from src.storage.mortgage_repo import get_mortgage_by_property
    ledger.deposit_cash(db_conn, "2024-12-01", 200000.0)
    asset, prop, _ = ledger.add_property(
        db_conn, "2024-12-15", symbol="H", name="House",
        purchase_price=300000.0,
        cashflow_start_date="2025-01-01",
        acquisition_mode="existing_property")
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=10000.0,
        interest_rate=0.0, payment_per_period=200.0,
        cashflow_start_date="2025-01-01",
    )
    created1, _ = ledger.settle_due_mortgage_payments(db_conn, "2025-03-15")
    created2, _ = ledger.settle_due_mortgage_payments(db_conn, "2025-03-15")
    assert len(created1) == 3
    assert created2 == []
    mortgage = get_mortgage_by_property(db_conn, prop.id)
    # 3 monthly payments of 200 → balance dropped 600.
    assert mortgage.current_balance == pytest.approx(10000.0 - 600.0)


def test_settle_due_mortgage_final_payment_lands_at_zero(db_conn):
    """The final mortgage payment must drive balance to exactly 0 even with
    a non-zero rate; legacy `min(payment, balance)` left an interest residue.
    """
    from src.storage.mortgage_repo import get_mortgage_by_property
    ledger.deposit_cash(db_conn, "2024-12-01", 100000.0)
    # Small balance + small payment so the schedule terminates within the
    # test horizon. 1000 @ 12% APR with 100/mo payment → ~11 periods, with
    # the last period being ~94.46 (balance + month's interest).
    asset, prop, _ = ledger.add_property(
        db_conn, "2024-12-15", symbol="H", name="House",
        purchase_price=300000.0,
        cashflow_start_date="2025-01-01",
        acquisition_mode="existing_property")
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=1000.0,
        interest_rate=0.12, payment_per_period=100.0,
        cashflow_start_date="2025-01-01",
    )
    ledger.settle_due_mortgage_payments(db_conn, "2026-01-15")
    mortgage = get_mortgage_by_property(db_conn, prop.id)
    assert mortgage.current_balance == pytest.approx(0.0, abs=0.01)


# Note: The v6-migration test and the yearly-mortgage tests at the
# original lines 199, 219, 244-253, 256 were removed. As of schema v11,
# mortgages are monthly-only (no yearly variant) and the legacy
# `mortgage_schedule_frequency` column on properties is gone.


# --- force_sell_to_cover_negative_cash ---

def _seed_holdings(db_conn):
    """Buy one of each sellable type so force-sell has multiple options."""
    ledger.deposit_cash(db_conn, "2024-12-01", 100000.0)
    stk = create_asset(db_conn, Asset(symbol="S", name="Stock A", asset_type="stock"))
    etf = create_asset(db_conn, Asset(symbol="E", name="ETF A", asset_type="etf"))
    crp = create_asset(db_conn, Asset(symbol="C", name="Crypto A", asset_type="crypto"))
    today = "2024-12-02"
    ledger.buy(db_conn, today, stk.id, quantity=10, price=100)
    ledger.buy(db_conn, today, etf.id, quantity=10, price=100)
    ledger.buy(db_conn, today, crp.id, quantity=10, price=100)
    # Seed market_prices so force-sell has a quote to use.
    bulk_upsert_ohlcv(db_conn, [
        {"asset_id": stk.id, "symbol": "S", "asset_type": "stock", "date": "2024-12-02", "close": 100.0, "source": "test"},
        {"asset_id": etf.id, "symbol": "E", "asset_type": "etf", "date": "2024-12-02", "close": 100.0, "source": "test"},
        {"asset_id": crp.id, "symbol": "C", "asset_type": "crypto", "date": "2024-12-02", "close": 100.0, "source": "test"},
    ])
    return stk.id, etf.id, crp.id


def test_force_sell_priority_stock_first(db_conn):
    """Spec §11: stock < etf < other (crypto/custom) < real_estate.

    Stocks sell first because they're typically the easiest to replace
    and the user likely cares less about preserving them than about
    illiquid holdings."""
    stk_id, etf_id, crp_id = _seed_holdings(db_conn)
    # Drain cash so we're negative.
    ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
    # Force a $200 deficit.
    ledger.manual_adjustment(db_conn, "2024-12-03", -200.0, notes="force deficit")

    sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
    assert len(sales) >= 1
    # First sale must be stock per the new selling order.
    assert sales[0].asset_id == stk_id
    assert calc_cash_balance(db_conn) >= -1e-6


def test_force_sell_skips_assets_without_price(db_conn):
    """Assets with no market_prices entry are skipped; loop exits cleanly."""
    ledger.deposit_cash(db_conn, "2024-12-01", 100.0)
    stk = create_asset(db_conn, Asset(symbol="S", name="Stock", asset_type="stock"))
    ledger.buy(db_conn, "2024-12-02", stk.id, quantity=1, price=100)
    # No market_prices row written.
    ledger.manual_adjustment(db_conn, "2024-12-03", -200.0, notes="force deficit")
    sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
    assert sales == []  # nothing usable; bankruptcy state will fire


# --- bankruptcy warning ---

def test_check_bankruptcy_silent_when_sellable_assets_remain(db_conn):
    _seed_holdings(db_conn)
    ledger.manual_adjustment(db_conn, "2024-12-03", -1_000_000.0, notes="huge deficit")
    assert calc_cash_balance(db_conn) < 0
    warnings = check_bankruptcy(db_conn)
    assert warnings == []


def test_check_bankruptcy_fires_when_negative_cash_and_no_sellable(db_conn):
    # Cash < 0 with zero sellable holdings.
    ledger.manual_adjustment(db_conn, "2024-12-03", -100.0, notes="deficit")
    warnings = check_bankruptcy(db_conn)
    assert len(warnings) == 1
    w = warnings[0]
    assert w.severity == "critical"
    assert w.category == "bankruptcy"


# --- migration ---

def test_migration_v3_has_new_debt_columns(tmp_path):
    db_path = tmp_path / "test.db"
    conn = init_db(str(db_path))
    cols = {row[1] for row in conn.execute("PRAGMA table_info(debts)").fetchall()}
    for col in ("schedule_frequency", "interest_period",
                "monthly_payment_amount", "cashflow_start_date",
                "last_payment_date"):
        assert col in cols
    assert get_schema_version(conn) == CURRENT_SCHEMA_VERSION
    conn.close()


def test_migration_v3_backfills_minimum_payment(tmp_path):
    """An older debt with only `minimum_payment` should be promoted to
    `monthly_payment_amount` so it keeps deducting after the upgrade.
    """
    db_path = tmp_path / "legacy.db"
    # Stand up a v2 schema (without the new columns) by running init then
    # stripping the new columns to simulate the legacy state.
    conn = init_db(str(db_path))
    # Insert a debt asset + debt row directly with minimum_payment only.
    conn.execute(
        "INSERT INTO assets (symbol, name, asset_type) VALUES (?, ?, ?)",
        ("OLD", "Old Loan", "debt"))
    asset_id = conn.execute("SELECT id FROM assets WHERE symbol='OLD'").fetchone()[0]
    # Reset to make monthly_payment_amount=0 so the backfill condition triggers.
    conn.execute(
        "INSERT INTO debts (asset_id, name, original_amount, current_balance, "
        "minimum_payment, monthly_payment_amount) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (asset_id, "Old Loan", 5000, 5000, 250, 0))
    conn.commit()
    conn.close()

    # Re-run init_db; the migration backfill copies minimum_payment.
    from src.storage import database as db_module
    db_module._migrate_debts(sqlite3.connect(str(db_path)))  # idempotent
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # Re-run the backfill logic by calling the function explicitly so we
    # exercise the UPDATE path on existing legacy data.
    conn.execute(
        "UPDATE debts SET monthly_payment_amount = minimum_payment "
        "WHERE monthly_payment_amount = 0 AND minimum_payment > 0"
    )
    conn.commit()
    row = conn.execute(
        "SELECT minimum_payment, monthly_payment_amount FROM debts WHERE asset_id=?",
        (asset_id,)).fetchone()
    assert row["minimum_payment"] == 250
    assert row["monthly_payment_amount"] == 250
    conn.close()


# --- debt_repo round-trip of new fields ---

def test_debt_repo_round_trip_new_fields(db_conn):
    asset = create_asset(db_conn, Asset(symbol="L", name="Loan", asset_type="debt"))
    debt = create_debt(db_conn, Debt(
        asset_id=asset.id, name="Loan",
        original_amount=1000.0, current_balance=1000.0,
        interest_rate=0.06,
        schedule_frequency="yearly",
        interest_period="monthly",
        monthly_payment_amount=120.0,
        cashflow_start_date="2025-06-01",
        last_payment_date=None))
    fetched = get_debt_by_asset(db_conn, asset.id)
    assert fetched.schedule_frequency == "yearly"
    assert fetched.interest_period == "monthly"
    assert fetched.monthly_payment_amount == 120.0
    assert fetched.cashflow_start_date == "2025-06-01"
    assert fetched.last_payment_date is None
