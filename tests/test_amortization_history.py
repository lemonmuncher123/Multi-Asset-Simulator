"""Tests for the v11 loan-history modeling helpers.

`simulate_amortization_balance` and `compute_periods_elapsed` enable
``add_debt`` and ``add_mortgage`` to compute today's balance for a loan
that's been partially paid down before being entered into the simulator
(by walking the amortization forward from origination_date to today).
"""
from __future__ import annotations

import pytest
from datetime import date

from src.engines import ledger
from src.engines.debt_math import (
    simulate_amortization_balance, compute_periods_elapsed,
)
from src.storage.debt_repo import get_debt_by_asset
from src.storage.mortgage_repo import get_mortgage_by_property


# --- simulate_amortization_balance ---

class TestSimulateAmortizationBalance:
    def test_zero_periods_elapsed_returns_principal(self):
        assert simulate_amortization_balance(
            principal=100000.0, annual_rate=0.06, schedule="monthly",
            payment=600.0, periods_elapsed=0,
        ) == pytest.approx(100000.0)

    def test_zero_rate_linear_paydown(self):
        # 5 payments of $200 against $1000 principal at 0% → $0.
        assert simulate_amortization_balance(
            principal=1000.0, annual_rate=0.0, schedule="monthly",
            payment=200.0, periods_elapsed=5,
        ) == pytest.approx(0.0)
        # After 3 payments: 1000 - 600 = 400.
        assert simulate_amortization_balance(
            principal=1000.0, annual_rate=0.0, schedule="monthly",
            payment=200.0, periods_elapsed=3,
        ) == pytest.approx(400.0)

    def test_30_year_mortgage_5_years_in(self):
        # $200k @ 6% / 30yr → ~$1199.10/mo.
        # After 60 payments: balance ≈ $186,108.71 (textbook value).
        balance = simulate_amortization_balance(
            principal=200000.0, annual_rate=0.06, schedule="monthly",
            payment=1199.10, periods_elapsed=60,
        )
        assert balance == pytest.approx(186108.71, abs=10.0)

    def test_saturates_at_zero_when_periods_exceed_term(self):
        # $1000 @ 0%, $200/mo for 100 periods (way past payoff at 5).
        assert simulate_amortization_balance(
            principal=1000.0, annual_rate=0.0, schedule="monthly",
            payment=200.0, periods_elapsed=100,
        ) == 0.0

    def test_yearly_schedule(self):
        # $1000 @ 0%, $200/yr after 3 years = $400.
        assert simulate_amortization_balance(
            principal=1000.0, annual_rate=0.0, schedule="yearly",
            payment=200.0, periods_elapsed=3,
        ) == pytest.approx(400.0)


# --- compute_periods_elapsed ---

class TestComputePeriodsElapsed:
    def test_same_date_zero_periods(self):
        assert compute_periods_elapsed("2025-01-01", "2025-01-01", "monthly") == 0

    def test_end_before_start_zero(self):
        assert compute_periods_elapsed("2025-06-01", "2025-01-01", "monthly") == 0

    def test_three_full_months(self):
        assert compute_periods_elapsed("2025-01-15", "2025-04-15", "monthly") == 3

    def test_partial_month_floored(self):
        # 2025-01-15 → 2025-02-10 is < 1 full month.
        assert compute_periods_elapsed("2025-01-15", "2025-02-10", "monthly") == 0

    def test_year_boundary(self):
        # 2024-11-01 → 2025-02-01 is 3 months.
        assert compute_periods_elapsed("2024-11-01", "2025-02-01", "monthly") == 3

    def test_yearly_schedule_floors_to_whole_years(self):
        assert compute_periods_elapsed("2020-06-01", "2025-05-31", "yearly") == 4
        assert compute_periods_elapsed("2020-06-01", "2025-06-01", "yearly") == 5


# --- add_debt with origination_date ---

class TestAddDebtOriginationDate:
    def test_past_origination_walks_balance_forward(self, db_conn):
        # 3 years ago, $10k loan at 0%, $200/mo. Today balance =
        # 10000 - 36*200 = 2800.
        today = date.today()
        origination = date(today.year - 3, today.month, 1).isoformat()
        asset, debt, txn = ledger.add_debt(
            db_conn, today.isoformat(), symbol="L", name="Old Loan",
            amount=10000.0, interest_rate=0.0,
            payment_per_period=200.0,
            origination_date=origination,
        )
        # Periods elapsed = ~36 months → balance = 10000 - 36*200 = 2800.
        # We also accept 35-37 to absorb day-of-month edge cases.
        assert 2600 <= debt.current_balance <= 3000
        # No cash inflow for an existing loan (the borrowing happened
        # in the past).
        assert txn.total_amount == 0.0

    def test_past_origination_keeps_original_amount(self, db_conn):
        today = date.today()
        origination = date(today.year - 2, 1, 1).isoformat()
        asset, debt, _ = ledger.add_debt(
            db_conn, today.isoformat(), symbol="L", name="Loan2",
            amount=20000.0, interest_rate=0.0,
            payment_per_period=400.0,
            origination_date=origination,
        )
        # original_amount stays at 20000 even though current_balance dropped.
        assert debt.original_amount == 20000.0

    def test_today_origination_is_fresh_loan(self, db_conn):
        # Origination = today → behaves like a fresh add_debt
        # (current_balance == amount, cash inflow per cash_received).
        today_iso = date.today().isoformat()
        asset, debt, txn = ledger.add_debt(
            db_conn, today_iso, symbol="L", name="Fresh",
            amount=5000.0, interest_rate=0.05,
            payment_per_period=200.0,
            origination_date=today_iso,
        )
        assert debt.current_balance == 5000.0
        assert txn.total_amount == 5000.0

    def test_no_origination_is_fresh_loan(self, db_conn):
        # No origination_date → fresh loan.
        asset, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Fresh2",
            amount=5000.0, interest_rate=0.0,
            payment_per_period=200.0, cash_received=False,
        )
        assert debt.current_balance == 5000.0


# --- add_mortgage with origination_date ---

class TestAddMortgageOriginationDate:
    def test_past_origination_walks_mortgage_forward(self, db_conn):
        # 5 years ago, $200k @ 6% / 30yr → today balance ≈ $186k.
        today = date.today()
        origination = date(today.year - 5, today.month, 1).isoformat()
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        asset, prop, _ = ledger.add_property(
            db_conn, today.isoformat(), symbol="H", name="House",
            purchase_price=400000.0,
            acquisition_mode="existing_property",
        )
        m = ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.06, term_periods=360,
            origination_date=origination,
        )
        # Should be substantially less than original — accept a wide
        # range to absorb the day-of-month edge.
        assert m.original_amount == 200000.0
        assert 180000.0 < m.current_balance < 195000.0


# --- validation tests ---

class TestPropertyAndMortgageValidation:
    def test_add_property_rejects_down_payment_over_price(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 1000000.0)
        with pytest.raises(ValueError, match="cannot exceed purchase price"):
            ledger.add_property(
                db_conn, "2025-01-01", symbol="H", name="Bad",
                purchase_price=300000.0, down_payment=500000.0,
            )

    def test_add_mortgage_rejects_over_purchase_price(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        _, prop, _ = ledger.add_property(
            db_conn, "2025-01-01", symbol="H", name="Small House",
            purchase_price=200000.0,
            acquisition_mode="existing_property",
        )
        with pytest.raises(ValueError, match="cannot exceed property purchase price"):
            ledger.add_mortgage(
                db_conn, property_id=prop.id, original_amount=300000.0,
                interest_rate=0.05, payment_per_period=1500.0,
            )
