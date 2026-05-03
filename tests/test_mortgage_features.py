"""Mirror of basic test_debt_features.py engine tests, adapted for mortgages.

Covers add_mortgage, pay_mortgage, balance math (interest split), preview
value persistence, and the LTV constraint. Mortgages live in their own
table linked to properties via property_id (no Asset row), and are
monthly-only.
"""
from __future__ import annotations

import pytest

from src.engines import ledger
from src.engines.portfolio import calc_cash_balance
from src.storage.mortgage_repo import (
    get_mortgage_by_property, list_mortgages, list_active_mortgages,
)
from src.storage.mortgage_payment_record_repo import (
    list_payment_records_for_mortgage,
)


def _seed_property(conn, *, purchase_price=500000.0):
    """Add an existing property (no cash impact) to attach a mortgage to."""
    asset, prop, _ = ledger.add_property(
        conn, "2025-01-01", symbol="H1", name="House",
        purchase_price=purchase_price,
        acquisition_mode="existing_property",
    )
    return asset, prop


def test_add_mortgage_creates_linked_row(db_conn):
    _, prop = _seed_property(db_conn)
    m = ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=400000.0,
        interest_rate=0.06, payment_per_period=2398.20,
    )
    assert m.id is not None
    assert m.property_id == prop.id
    assert m.original_amount == 400000.0
    assert m.current_balance == 400000.0  # fresh loan today
    assert m.plan_type == "fixed_payment"


def test_add_mortgage_term_periods_derives_payment(db_conn):
    _, prop = _seed_property(db_conn)
    m = ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=200000.0,
        interest_rate=0.06, term_periods=360,
    )
    # 30-year amortization at 6% on $200k → ~$1199.10/mo (monthly only).
    assert m.plan_type == "fixed_term"
    assert m.original_term_periods == 360
    assert m.monthly_payment_amount == pytest.approx(1199.1, abs=1.0)


def test_add_mortgage_rejects_over_ltv(db_conn):
    _, prop = _seed_property(db_conn, purchase_price=200000.0)
    with pytest.raises(ValueError, match="cannot exceed property purchase price"):
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=300000.0,
            interest_rate=0.05, payment_per_period=1000.0,
        )


def test_add_mortgage_rejects_second_mortgage(db_conn):
    _, prop = _seed_property(db_conn)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=100000.0,
        interest_rate=0.05, payment_per_period=500.0,
    )
    with pytest.raises(ValueError, match="already has a mortgage"):
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=50000.0,
            interest_rate=0.05, payment_per_period=300.0,
        )


def test_add_mortgage_requires_one_plan(db_conn):
    _, prop = _seed_property(db_conn)
    with pytest.raises(ValueError, match="exactly one"):
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=100000.0,
            interest_rate=0.05,
        )


def test_pay_mortgage_splits_interest(db_conn):
    """One month's interest is taken off the cash payment first; the
    remainder reduces principal. Mirrors pay_debt's contract."""
    ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
    asset, prop = _seed_property(db_conn)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=100000.0,
        interest_rate=0.12, payment_per_period=1100.0,
    )
    # 100000 * 12% / 12 = $1000 interest. Pay $2000 → $1000 principal reduction.
    ledger.pay_mortgage(db_conn, "2025-02-01", asset.id, amount=2000.0)
    m = get_mortgage_by_property(db_conn, prop.id)
    assert m.current_balance == pytest.approx(99000.0, rel=1e-6)


def test_pay_mortgage_zero_rate_full_principal(db_conn):
    """Zero-rate mortgage: the whole payment reduces principal."""
    ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
    asset, prop = _seed_property(db_conn)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=10000.0,
        interest_rate=0.0, payment_per_period=500.0,
    )
    ledger.pay_mortgage(db_conn, "2025-02-01", asset.id, amount=1000.0)
    m = get_mortgage_by_property(db_conn, prop.id)
    assert m.current_balance == pytest.approx(9000.0)


def test_pay_mortgage_writes_audit_record(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
    asset, prop = _seed_property(db_conn)
    m = ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=50000.0,
        interest_rate=0.0, payment_per_period=500.0,
    )
    ledger.pay_mortgage(db_conn, "2025-02-01", asset.id, amount=500.0)
    records = list_payment_records_for_mortgage(db_conn, m.id)
    assert len(records) == 1
    rec = records[0]
    assert rec.payment_amount == pytest.approx(500.0)
    assert rec.balance_before_payment == pytest.approx(50000.0)
    assert rec.balance_after_payment == pytest.approx(49500.0)
    assert rec.payment_type == "manual"


def test_pay_mortgage_rejects_overpayment(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset, prop = _seed_property(db_conn)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=10000.0,
        interest_rate=0.0, payment_per_period=500.0,
    )
    with pytest.raises(ValueError, match="exceeds payoff amount"):
        ledger.pay_mortgage(db_conn, "2025-02-01", asset.id, amount=50000.0)


def test_pay_mortgage_at_payoff_clears_interest_bearing(db_conn):
    """Manually entering the payoff amount must land balance at 0
    (mirror of the same fix on pay_debt)."""
    ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
    asset, prop = _seed_property(db_conn)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=1000.0,
        interest_rate=0.12, payment_per_period=100.0,
    )
    # payoff = 1000 + 1000*0.12/12 = 1010.
    ledger.pay_mortgage(db_conn, "2025-02-01", asset.id, amount=1010.0)
    m = get_mortgage_by_property(db_conn, prop.id)
    assert m.current_balance == 0.0


def test_preview_values_persisted_at_creation(db_conn):
    _, prop = _seed_property(db_conn)
    m = ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=100000.0,
        interest_rate=0.06, term_periods=120,
    )
    assert m.preview_regular_payment == pytest.approx(m.monthly_payment_amount)
    assert m.preview_period_count > 0
    assert m.preview_total_paid > m.original_amount  # includes interest


def test_list_active_mortgages_excludes_zero_balance(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
    asset, prop = _seed_property(db_conn)
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=1000.0,
        interest_rate=0.0, payment_per_period=500.0,
    )
    assert len(list_active_mortgages(db_conn)) == 1
    ledger.pay_mortgage_in_full(db_conn, "2025-02-01", asset.id)
    # Row stays as historical record; just no longer "active."
    assert len(list_active_mortgages(db_conn)) == 0
    assert len(list_mortgages(db_conn)) == 1
