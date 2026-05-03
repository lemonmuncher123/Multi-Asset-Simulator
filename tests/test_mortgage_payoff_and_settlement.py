"""Mortgage Pay Off in Full + sale-time settlement behavior. Mirrors
relevant sections of test_debt_payoff_and_missed.py for the cloned
mortgage subsystem.
"""
from __future__ import annotations

import pytest

from src.engines import ledger
from src.engines.portfolio import calc_cash_balance
from src.storage.mortgage_repo import get_mortgage_by_property
from src.storage.property_repo import get_property_by_asset
from src.storage.transaction_repo import list_transactions


def _seed(conn, *, purchase=500000.0, mortgage=400000.0, rate=0.06,
          payment=2398.20):
    ledger.deposit_cash(conn, "2025-01-01", 100000.0)
    asset, prop, _ = ledger.add_property(
        conn, "2025-01-01", symbol="H", name="House",
        purchase_price=purchase, acquisition_mode="existing_property",
    )
    ledger.add_mortgage(
        conn, property_id=prop.id, original_amount=mortgage,
        interest_rate=rate, payment_per_period=payment,
    )
    return asset, prop


def test_compute_mortgage_payoff_zero_rate(db_conn):
    asset, prop = _seed(db_conn, mortgage=10000.0, rate=0.0, payment=500.0)
    assert ledger.compute_mortgage_payoff_amount(db_conn, asset.id) == pytest.approx(10000.0)


def test_compute_mortgage_payoff_with_interest(db_conn):
    asset, _ = _seed(db_conn, mortgage=1000.0, rate=0.12, payment=100.0)
    # 1000 + 1000*0.12/12 = 1010
    assert ledger.compute_mortgage_payoff_amount(db_conn, asset.id) == pytest.approx(1010.0)


def test_pay_mortgage_in_full_clears_balance(db_conn):
    asset, prop = _seed(db_conn, mortgage=1000.0, rate=0.12, payment=100.0)
    cash_before = calc_cash_balance(db_conn)
    ledger.pay_mortgage_in_full(db_conn, "2025-02-01", asset.id)
    m = get_mortgage_by_property(db_conn, prop.id)
    assert m.current_balance == 0.0
    assert m.last_payment_date == "2025-02-01"
    # Cash leaves at full payoff (principal + this period's interest).
    assert calc_cash_balance(db_conn) == pytest.approx(cash_before - 1010.0)


def test_pay_mortgage_in_full_writes_marker_note(db_conn):
    asset, _ = _seed(db_conn, mortgage=1000.0, rate=0.12, payment=100.0)
    ledger.pay_mortgage_in_full(db_conn, "2025-02-01", asset.id, notes="user note")
    txns = [t for t in list_transactions(db_conn, asset_id=asset.id)
            if t.txn_type == "pay_mortgage"]
    assert len(txns) == 1
    assert "Pay-off in full" in (txns[0].notes or "")
    assert "principal 1,000.00" in (txns[0].notes or "")
    assert "accrued interest 10.00" in (txns[0].notes or "")
    assert "user note" in (txns[0].notes or "")


def test_pay_mortgage_in_full_rejects_already_cleared(db_conn):
    asset, _ = _seed(db_conn, mortgage=1000.0, rate=0.0, payment=500.0)
    ledger.pay_mortgage_in_full(db_conn, "2025-02-01", asset.id)
    with pytest.raises(ValueError, match="already paid off"):
        ledger.pay_mortgage_in_full(db_conn, "2025-03-01", asset.id)


def test_sell_property_settles_mortgage(db_conn):
    """sell_property routes through pay_mortgage_in_full so the mortgage
    payoff (balance + this period's accrued interest) is correctly
    deducted from the sale proceeds. Behavior change vs schema v10:
    the period's interest is now charged at closing.
    """
    asset, prop = _seed(db_conn, mortgage=200000.0, rate=0.06, payment=1500.0)
    cash_before = calc_cash_balance(db_conn)
    payoff = ledger.compute_mortgage_payoff_amount(db_conn, asset.id)
    # Sell at $300k; expect net = 300000 - payoff - 0 fees.
    ledger.sell_property(db_conn, "2025-03-01", asset.id, sale_price=300000.0)

    # Mortgage settled to 0; row stays as historical record.
    m = get_mortgage_by_property(db_conn, prop.id)
    assert m.current_balance == 0.0

    # Property marked sold; current_value zeroed.
    p = get_property_by_asset(db_conn, asset.id)
    assert p.status == "sold"
    assert p.current_value == 0

    # Cash reflects: net proceeds = sale_price - payoff - fees.
    assert calc_cash_balance(db_conn) == pytest.approx(
        cash_before + 300000.0 - payoff
    )


def test_existing_property_with_loan_future_only_cashflow(db_conn):
    """Adding an existing property with a years-old mortgage:
    - The forward-walk computes today's `current_balance` correctly.
    - `cashflow_start_date` lands in the FUTURE (no historical backfill).
    - Auto-settle does not create any pay_mortgage transactions for past
      months, only for the next scheduled date.
    - Manual Pay Mortgage from the Transactions page reduces the balance.
    """
    from datetime import date as _date
    from src.engines.ledger import (
        first_day_next_month, settle_due_mortgage_payments, pay_mortgage,
    )
    ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
    asset, prop, _ = ledger.add_property(
        db_conn, "2020-01-01", symbol="OLD", name="Old Rental",
        purchase_price=300000.0,
        acquisition_mode="existing_property",
    )
    # Mortgage with a past origination_date (purchase_date) — engine
    # walks the amortization forward to compute today's balance.
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=200000.0,
        interest_rate=0.06, term_periods=360,
        origination_date="2020-01-01",
    )
    m = get_mortgage_by_property(db_conn, prop.id)
    # Balance should be substantially less than original (years of payments).
    assert m.current_balance < m.original_amount
    assert m.current_balance > 150000.0  # rough lower bound — not paid off yet
    # cashflow_start_date is future — engine default fired since the GUI
    # passes None.
    assert m.cashflow_start_date == first_day_next_month()
    assert _date.fromisoformat(m.cashflow_start_date) >= _date.today()

    # Auto-settle through today must NOT backfill any historical payments.
    today_iso = _date.today().isoformat()
    created, deferred = settle_due_mortgage_payments(db_conn, today_iso)
    assert created == []
    assert deferred == []

    # Auto-settle through the next 1st-of-month creates exactly one
    # scheduled pay_mortgage transaction.
    created, _ = settle_due_mortgage_payments(db_conn, m.cashflow_start_date)
    assert len(created) == 1
    assert created[0].txn_type == "pay_mortgage"
    after_auto = get_mortgage_by_property(db_conn, prop.id)
    assert after_auto.current_balance < m.current_balance

    # Manual Pay Mortgage from the Transactions API path reduces balance.
    pre = after_auto.current_balance
    pay_mortgage(db_conn, today_iso, asset.id, amount=5000.0)
    final = get_mortgage_by_property(db_conn, prop.id)
    # Cash payment $5000 - one month interest gets applied to principal.
    assert final.current_balance < pre


def test_sell_property_with_no_mortgage_keeps_full_proceeds(db_conn):
    """Property without a mortgage: net_proceeds = sale_price - fees,
    no implicit pay_mortgage transaction."""
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset, prop, _ = ledger.add_property(
        db_conn, "2025-01-01", symbol="H", name="Cash House",
        purchase_price=300000.0,
        acquisition_mode="existing_property",
    )
    cash_before = calc_cash_balance(db_conn)
    ledger.sell_property(db_conn, "2025-03-01", asset.id, sale_price=350000.0, fees=1000.0)
    pay_mortgage_txns = [
        t for t in list_transactions(db_conn, asset_id=asset.id)
        if t.txn_type == "pay_mortgage"
    ]
    assert len(pay_mortgage_txns) == 0
    assert calc_cash_balance(db_conn) == pytest.approx(cash_before + 349000.0)
