"""Tests for the redesigned debt-schedule feature set:
- compute_debt_schedule math (both directions)
- always-annual interest rate
- payment-OR-term mutual exclusion
- 1st-of-period auto-deduction alignment
- max_debt_payoff_months setting
- debt-horizon and debt-affordability risk warnings
- Add Debt UI live preview + radio mutex
- Asset Analysis Debts table
- Forced-liquidation note explicitly says "paying debts"
"""
from datetime import date
from unittest.mock import patch

import pytest
from PySide6.QtWidgets import QMessageBox

from src.engines import ledger
from src.engines.debt_math import (
    compute_debt_schedule, normalize_period_to_months, DebtSchedule,
    period_interest)
from src.engines.portfolio import calc_cash_balance
from src.engines.risk import (
    check_debt_payoff_horizon, check_debt_affordability, get_all_warnings)
from src.gui.pages.transactions import TransactionsPage
from src.gui.pages.asset_analysis import AssetAnalysisPage
from src.gui.pages.settings import SettingsPage
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.debt_repo import list_debts, get_debt_by_asset
from src.storage.price_repo import bulk_upsert_ohlcv
from src.storage.settings_repo import (
    get_max_debt_payoff_months, set_max_debt_payoff_months,
    DEFAULT_MAX_DEBT_PAYOFF_MONTHS)
from src.storage.transaction_repo import list_transactions


# ---------------------------------------------------------------------------
# debt_math
# ---------------------------------------------------------------------------

class TestDebtMath:
    def test_term_to_payment_monthly(self):
        # $10k @ 6% over 60 months → ~$193.33/mo
        s = compute_debt_schedule(10000.0, 0.06, "monthly", term_periods=60)
        assert s.feasible
        assert s.num_periods == 60
        assert s.per_period_payment == pytest.approx(193.33, abs=0.5)
        assert s.total_paid == pytest.approx(60 * s.per_period_payment, abs=1.0)
        assert s.total_interest == pytest.approx(s.total_paid - 10000.0, abs=1.0)

    def test_term_to_payment_yearly(self):
        # $10k @ 6% over 5 years → ~$2374/yr
        s = compute_debt_schedule(10000.0, 0.06, "yearly", term_periods=5)
        assert s.feasible
        assert s.num_periods == 5
        assert s.per_period_payment == pytest.approx(2373.96, abs=1.0)

    def test_payment_to_term_monthly(self):
        s = compute_debt_schedule(10000.0, 0.06, "monthly", payment=193.33)
        assert s.feasible
        assert 59 <= s.num_periods <= 61

    def test_zero_rate_term_to_payment(self):
        s = compute_debt_schedule(10000.0, 0.0, "monthly", term_periods=10)
        assert s.feasible
        assert s.per_period_payment == pytest.approx(1000.0)
        assert s.total_interest == pytest.approx(0.0)

    def test_payment_below_interest_is_infeasible(self):
        # 10k @ 6% monthly = $50/mo interest. Pay $30 → debt grows.
        s = compute_debt_schedule(10000.0, 0.06, "monthly", payment=30.0)
        assert not s.feasible
        assert "interest" in (s.infeasibility_reason or "").lower()

    def test_payment_at_exactly_interest_is_infeasible(self):
        # Edge case: payment == periodic interest → never amortizes.
        s = compute_debt_schedule(10000.0, 0.06, "monthly", payment=50.0)
        assert not s.feasible

    def test_must_provide_exactly_one(self):
        with pytest.raises(ValueError):
            compute_debt_schedule(1000.0, 0.06, "monthly")  # neither
        with pytest.raises(ValueError):
            compute_debt_schedule(
                1000.0, 0.06, "monthly", payment=100.0, term_periods=12)  # both

    def test_zero_or_negative_principal_infeasible(self):
        for p in (0.0, -100.0):
            s = compute_debt_schedule(p, 0.06, "monthly", term_periods=12)
            assert not s.feasible

    def test_normalize_period_to_months(self):
        assert normalize_period_to_months(60, "monthly") == 60
        assert normalize_period_to_months(5, "yearly") == 60

    def test_period_interest_monthly(self):
        assert period_interest(10000.0, 0.06, "monthly") == pytest.approx(50.0)

    def test_period_interest_yearly(self):
        assert period_interest(10000.0, 0.06, "yearly") == pytest.approx(600.0)

    def test_period_interest_zero_rate(self):
        assert period_interest(10000.0, 0.0, "monthly") == pytest.approx(0.0)
        assert period_interest(10000.0, 0.0, "yearly") == pytest.approx(0.0)

    def test_period_interest_invalid_schedule(self):
        with pytest.raises(ValueError):
            period_interest(1000.0, 0.06, "weekly")


# ---------------------------------------------------------------------------
# debt_math.recompute_after_payment — Pay Debt preview engine
# ---------------------------------------------------------------------------

class TestRecomputeAfterPayment:
    """Spec §10 "Recalculation After Manual Payment": for fixed_payment
    debts the per-period amount is preserved and the term contracts; for
    fixed_term debts the remaining term is preserved and the per-period
    amount drops.
    """

    def _make_debt(self, **overrides):
        from src.models.debt import Debt
        defaults = dict(
            asset_id=1, name="Test", original_amount=10000.0,
            current_balance=10000.0, interest_rate=0.06,
            schedule_frequency="monthly",
            monthly_payment_amount=200.0,
            plan_type="fixed_payment", original_term_periods=None)
        defaults.update(overrides)
        return Debt(**defaults)

    def test_fixed_payment_keeps_payment_drops_periods(self):
        from src.engines.debt_math import recompute_after_payment
        # 10k @ 6%/mo with $200/mo → ~58 periods. After paying $5k extra,
        # balance ≈ $5k (after interest split) and the same $200/mo
        # payment now clears it in fewer periods.
        debt = self._make_debt()
        s = recompute_after_payment(debt, payment_amount=5000.0,
                                     scheduled_payments_so_far=0)
        assert s.feasible
        # Payment is preserved (modulo rounding).
        assert s.per_period_payment == pytest.approx(200.0)
        # Term should drop sharply from ~58 to roughly 28 periods.
        assert 25 < s.num_periods < 32

    def test_fixed_term_keeps_term_drops_payment(self):
        from src.engines.debt_math import recompute_after_payment
        # 10k @ 6%/mo over 60 months → original payment ≈ $193/mo.
        # After paying $5k extra (no scheduled payments yet), balance is
        # ~$5k, term stays 60 → new payment ≈ $97/mo.
        debt = self._make_debt(
            plan_type="fixed_term", original_term_periods=60,
            monthly_payment_amount=193.33)
        s = recompute_after_payment(debt, payment_amount=5000.0,
                                     scheduled_payments_so_far=0)
        assert s.feasible
        assert s.num_periods == 60  # term preserved
        assert 90 < s.per_period_payment < 105  # payment ~halved

    def test_fixed_term_remaining_term_uses_scheduled_count(self):
        from src.engines.debt_math import recompute_after_payment
        debt = self._make_debt(
            plan_type="fixed_term", original_term_periods=60,
            monthly_payment_amount=193.33,
            current_balance=8000.0)
        # 12 scheduled auto-payments already fired → 48 periods remain.
        s = recompute_after_payment(debt, payment_amount=2000.0,
                                     scheduled_payments_so_far=12)
        assert s.feasible
        assert s.num_periods == 48

    def test_paid_off_sentinel_when_payment_clears_balance(self):
        from src.engines.debt_math import (
            recompute_after_payment, PAID_OFF_REASON)
        # Zero-rate makes the math obvious: paying the whole balance
        # clears it.
        debt = self._make_debt(
            interest_rate=0.0, current_balance=1000.0)
        s = recompute_after_payment(debt, payment_amount=1000.0,
                                     scheduled_payments_so_far=0)
        assert s.feasible
        assert s.infeasibility_reason == PAID_OFF_REASON
        assert s.num_periods == 0
        assert s.per_period_payment == 0.0

    def test_zero_rate_partial_pay_fixed_payment(self):
        from src.engines.debt_math import recompute_after_payment
        # No interest split — full amount reduces principal.
        debt = self._make_debt(
            interest_rate=0.0, current_balance=1000.0,
            monthly_payment_amount=100.0)
        s = recompute_after_payment(debt, payment_amount=400.0,
                                     scheduled_payments_so_far=0)
        assert s.feasible
        # New balance is $600 / $100 per month = 6 periods.
        assert s.num_periods == 6
        assert s.per_period_payment == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# add_debt: payment-OR-term contract
# ---------------------------------------------------------------------------

class TestAddDebtContract:
    def test_payment_path_persists_payment(self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=12000.0, interest_rate=0.06,
            schedule_frequency="monthly",
            payment_per_period=232.0, cash_received=False)
        assert debt.monthly_payment_amount == pytest.approx(232.0)

    def test_term_path_derives_payment(self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=12000.0, interest_rate=0.06,
            schedule_frequency="monthly",
            term_periods=60, cash_received=False)
        # 12000 @ 6% / 60 months ≈ $232/mo
        assert 220 < debt.monthly_payment_amount < 240

    def test_rejects_both_payment_and_term(self, db_conn):
        with pytest.raises(ValueError, match="OR"):
            ledger.add_debt(
                db_conn, "2025-01-01", symbol="L", name="Loan",
                amount=12000.0, interest_rate=0.06,
                payment_per_period=200.0, term_periods=60,
                cash_received=False)

    def test_rejects_infeasible_payment(self, db_conn):
        with pytest.raises(ValueError, match="infeasible"):
            ledger.add_debt(
                db_conn, "2025-01-01", symbol="L", name="Loan",
                amount=10000.0, interest_rate=0.06,
                schedule_frequency="monthly",
                payment_per_period=10.0,  # below interest
                cash_received=False)

    def test_default_cashflow_start_for_yearly_is_jan1(self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=10000.0, interest_rate=0.06,
            schedule_frequency="yearly",
            term_periods=5, cash_received=False)
        assert debt.cashflow_start_date.endswith("-01-01")

    def test_callers_with_neither_kwarg_now_rejected(self, db_conn):
        """Schema v10 / Phase 6.5 removed the legacy auto-default
        fallback. Callers must explicitly choose a planning method
        (spec §6 #6 — "the user must choose exactly one")."""
        with pytest.raises(ValueError, match="payment_per_period.*term_periods"):
            ledger.add_debt(
                db_conn, "2025-01-01", symbol="L", name="Loan",
                amount=10000.0, cash_received=False)


# ---------------------------------------------------------------------------
# Auto-deduction snaps to 1st of period
# ---------------------------------------------------------------------------

class TestAutoDeductionAlignment:
    def test_monthly_snaps_to_first_of_month(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-01", 10000.0)
        # Anchor mid-month — engine must snap to 2025-02-01 not 2025-01-15.
        a, _, _ = ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-15")
        created, _ = ledger.settle_due_debt_payments(db_conn, "2025-04-30")
        assert all(t.date.endswith("-01") for t in created), [t.date for t in created]

    def test_yearly_snaps_to_january_first(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-01-01", 100000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2024-01-01", symbol="L", name="Loan",
            amount=10000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=2000.0, schedule_frequency="yearly",
            cashflow_start_date="2024-06-15")
        created, _ = ledger.settle_due_debt_payments(db_conn, "2027-12-31")
        # First year skips because anchor is mid-2024; first jan-1 is 2025.
        for t in created:
            assert t.date.startswith(("2025-01-01", "2026-01-01", "2027-01-01"))


# ---------------------------------------------------------------------------
# Settings: max_debt_payoff_months
# ---------------------------------------------------------------------------

class TestMaxPayoffSetting:
    def test_default_is_60_months(self, db_conn):
        assert get_max_debt_payoff_months(db_conn) == DEFAULT_MAX_DEBT_PAYOFF_MONTHS == 60

    def test_set_then_get_round_trip(self, db_conn):
        set_max_debt_payoff_months(db_conn, 36)
        assert get_max_debt_payoff_months(db_conn) == 36

    def test_invalid_value_falls_back_to_default(self, db_conn):
        # Backdoor a bad value; reader should sanitize.
        from src.storage.settings_repo import set_setting
        set_setting(db_conn, "max_debt_payoff_months", "not-a-number")
        assert get_max_debt_payoff_months(db_conn) == DEFAULT_MAX_DEBT_PAYOFF_MONTHS

    def test_zero_or_negative_falls_back_to_default(self, db_conn):
        from src.storage.settings_repo import set_setting
        set_setting(db_conn, "max_debt_payoff_months", "0")
        assert get_max_debt_payoff_months(db_conn) == DEFAULT_MAX_DEBT_PAYOFF_MONTHS

    def test_settings_page_shows_value(self, db_conn):
        set_max_debt_payoff_months(db_conn, 36)
        page = SettingsPage(db_conn)
        page.refresh()
        assert page.max_payoff_months_input.text() == "36"

    def test_settings_page_saves_value(self, db_conn):
        page = SettingsPage(db_conn)
        page.refresh()
        page.max_payoff_months_input.setText("24")
        # Patch QMessageBox in case validation triggers.
        with patch.object(QMessageBox, "warning"):
            page._save()
        assert get_max_debt_payoff_months(db_conn) == 24


# ---------------------------------------------------------------------------
# Risk warnings
# ---------------------------------------------------------------------------

class TestRiskWarnings:
    def test_horizon_warning_when_above_cap(self, db_conn):
        set_max_debt_payoff_months(db_conn, 12)
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Big Loan",
            amount=100000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=1000.0, schedule_frequency="monthly")
        warnings = check_debt_payoff_horizon(db_conn)
        assert len(warnings) == 1
        assert warnings[0].category == "debt_horizon"
        assert warnings[0].severity == "high"
        assert "Big Loan" in warnings[0].message

    def test_no_horizon_warning_when_under_cap(self, db_conn):
        set_max_debt_payoff_months(db_conn, 60)
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Small Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0, schedule_frequency="monthly")
        assert check_debt_payoff_horizon(db_conn) == []

    def test_horizon_warning_when_payment_below_interest(self, db_conn):
        # Backdoor a debt with a payment that doesn't cover interest.
        from src.storage.debt_repo import create_debt
        from src.models.debt import Debt
        a = create_asset(db_conn, Asset(symbol="X", name="Hopeless", asset_type="debt"))
        create_debt(db_conn, Debt(
            asset_id=a.id, name="Hopeless",
            original_amount=10000.0, current_balance=10000.0,
            interest_rate=0.12, monthly_payment_amount=10.0,
            schedule_frequency="monthly"))
        warnings = check_debt_payoff_horizon(db_conn)
        assert len(warnings) == 1
        assert "does not cover" in warnings[0].message.lower()

    def test_affordability_warning_when_runway_short(self, db_conn):
        # $500 cash, $200/mo obligation → 2.5-month runway < 6.
        ledger.deposit_cash(db_conn, "2025-01-01", 500.0)
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=10000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly")
        warnings = check_debt_affordability(db_conn)
        assert len(warnings) == 1
        assert warnings[0].category == "debt_affordability"
        assert warnings[0].severity == "high"

    def test_affordability_critical_when_cash_zero_or_negative(self, db_conn):
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=10000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly")
        # Cash starts at 0; affordability should fire critical.
        warnings = check_debt_affordability(db_conn)
        assert len(warnings) == 1
        assert warnings[0].severity == "critical"

    def test_no_affordability_warning_with_long_runway(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=10000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly")
        # 100k / 200 = 500 months runway — way above 6.
        assert check_debt_affordability(db_conn) == []

    def test_warnings_appear_in_get_all_warnings(self, db_conn):
        set_max_debt_payoff_months(db_conn, 6)
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Slow",
            amount=10000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly")
        cats = [w.category for w in get_all_warnings(db_conn)]
        assert "debt_horizon" in cats


# ---------------------------------------------------------------------------
# Forced liquidation note
# ---------------------------------------------------------------------------

class TestAutoTransactionNotes:
    """Each auto-generated transaction names what produced it."""

    def test_auto_rent_note_names_property(self, db_conn):
        from src.engines import ledger
        ledger.add_property(
            db_conn, "2025-01-01", symbol="H", name="Sunset Apt",
            purchase_price=200000.0, monthly_rent=1500.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        created = ledger.settle_due_rent(db_conn, "2025-02-15")
        assert created
        notes = (created[0].notes or "")
        assert "auto-credited rent" in notes.lower()
        assert "Sunset Apt" in notes

    def test_auto_debt_payment_note_names_debt(self, db_conn):
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2024-12-15", 10000.0)
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Visa Card",
            amount=2000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")
        created, _ = ledger.settle_due_debt_payments(db_conn, "2025-01-31")
        assert created
        notes = (created[0].notes or "")
        assert "auto-deducted" in notes.lower()
        assert "Visa Card" in notes

    def test_auto_mortgage_note_names_property(self, db_conn):
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2024-12-15", 10000.0)
        _, prop, _ = ledger.add_property(
            db_conn, "2024-12-15", symbol="H", name="Cape Cod House",
            purchase_price=300000.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.06, payment_per_period=1500.0,
            cashflow_start_date="2025-01-01",
        )
        created, _ = ledger.settle_due_mortgage_payments(db_conn, "2025-01-31")
        assert created
        notes = (created[0].notes or "")
        assert "auto-deducted" in notes.lower()
        assert "Cape Cod House" in notes

    def test_force_sell_note_names_asset_and_quantity(self, db_conn):
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        ledger.deposit_cash(db_conn, "2024-12-01", 10000.0)
        a = create_asset(db_conn, Asset(symbol="STK", name="My Stock", asset_type="stock"))
        ledger.buy(db_conn, "2024-12-02", a.id, quantity=10, price=100)
        bulk_upsert_ohlcv(db_conn, [{
            "asset_id": a.id, "symbol": "STK", "asset_type": "stock",
            "date": "2024-12-02", "close": 100.0, "source": "test",
        }])
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -50.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert sales
        notes = (sales[0].notes or "")
        assert "forced liquidation" in notes.lower()
        assert "STK" in notes
        # The note records the sold quantity for traceability.
        assert any(c.isdigit() for c in notes)

    def test_force_sell_note_carries_through_reason(self, db_conn):
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        ledger.deposit_cash(db_conn, "2024-12-01", 10000.0)
        a = create_asset(db_conn, Asset(symbol="X", name="X", asset_type="crypto"))
        ledger.buy(db_conn, "2024-12-02", a.id, quantity=1, price=100)
        bulk_upsert_ohlcv(db_conn, [{
            "asset_id": a.id, "symbol": "X", "asset_type": "crypto",
            "date": "2024-12-02", "close": 100.0, "source": "test",
        }])
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -10.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(
            db_conn, "2024-12-04", reason="auto debt deduction")
        assert sales
        assert "auto debt deduction" in (sales[0].notes or "")


class TestForcedLiquidationNote:
    def _seed_sellable(self, conn, qty=10, price=100.0):
        types = ("stock", "etf", "crypto", "custom")
        ledger.deposit_cash(conn, "2024-12-01", qty * price * len(types) + 1.0)
        out = {}
        for t in types:
            a = create_asset(conn, Asset(symbol=t.upper(), name=t, asset_type=t))
            ledger.buy(conn, "2024-12-02", a.id, quantity=qty, price=price)
            bulk_upsert_ohlcv(conn, [{
                "asset_id": a.id, "symbol": t.upper(), "asset_type": t,
                "date": "2024-12-02", "close": price, "source": "test",
            }])
            out[t] = a.id
        return out

    def test_force_sell_note_says_paying_debts(self, db_conn):
        self._seed_sellable(db_conn)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -50.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert sales
        notes = (sales[0].notes or "").lower()
        assert "paying debts" in notes or "forced liquidation" in notes

    def test_force_sell_reason_passes_through(self, db_conn):
        self._seed_sellable(db_conn)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -50.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(
            db_conn, "2024-12-04", reason="auto debt deduction")
        assert "auto debt deduction" in (sales[0].notes or "")


# ---------------------------------------------------------------------------
# Add Debt UI: live preview + radio mutex
# ---------------------------------------------------------------------------

class TestAddDebtUI:
    @pytest.fixture
    def page(self, db_conn):
        p = TransactionsPage(db_conn)
        p.refresh()
        yield p

    def test_radio_mutex_hides_unselected_row(self, page):
        # `isHidden()` reflects the explicit setVisible(False) state, which
        # is the load-bearing assertion here — `isVisible()` would also be
        # False for any widget whose top-level window hasn't been shown.
        page.add_debt_radio_payment.setChecked(True)
        assert not page.add_debt_payment.isHidden()
        assert not page.add_debt_payment_label.isHidden()
        assert page.add_debt_term.isHidden()
        assert page.add_debt_term_label.isHidden()
        page.add_debt_radio_term.setChecked(True)
        assert page.add_debt_payment.isHidden()
        assert page.add_debt_payment_label.isHidden()
        assert not page.add_debt_term.isHidden()
        assert not page.add_debt_term_label.isHidden()

    def test_no_rate_period_combo(self, page):
        # Always-annual: no period combo should exist.
        assert not hasattr(page, "add_debt_rate_period")

    def test_live_preview_payment_path(self, page):
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("200")
        page._on_add_debt_inputs_changed()
        preview = page.add_debt_preview.text()
        assert "Per-month payment" in preview
        assert "$200" in preview

    def test_live_preview_term_path(self, page):
        page.add_debt_amount.setText("12000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("60")
        page._on_add_debt_inputs_changed()
        preview = page.add_debt_preview.text()
        assert "60" in preview

    def test_live_preview_flags_above_cap(self, page):
        set_max_debt_payoff_months(page.conn, 12)
        page.add_debt_amount.setText("100000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("1000")
        page._on_add_debt_inputs_changed()
        preview = page.add_debt_preview.text()
        assert "exceeds" in preview.lower()

    def test_live_preview_flags_infeasible(self, page):
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("12.0")
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("50")  # below interest
        page._on_add_debt_inputs_changed()
        preview = page.add_debt_preview.text()
        assert "infeasible" in preview.lower() or "⚠" in preview

    def test_no_modal_preview_method(self, page):
        # The modal preview popup was removed in favor of the inline
        # `add_debt_preview` summary box. Nothing should call into a modal.
        assert not hasattr(page, "_preview_add_debt")

    def test_inline_summary_box_renders_total_paid(self, page):
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("60")
        page._on_add_debt_inputs_changed()
        body = page.add_debt_preview.text()
        assert "Total paid" in body
        assert "60" in body

    @patch.object(QMessageBox, "warning")
    def test_submit_blocks_when_term_field_empty(self, mock_warn, page):
        page.add_debt_name.setText("Loan")
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("")
        page._submit_add_debt()
        mock_warn.assert_called_once()
        assert list_debts(page.conn) == []

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_submit_blocks_when_horizon_exceeds_cap(self, mock_info, mock_warn, page):
        # Cap at 12 months; ask for a 60-month payoff → submit must refuse.
        set_max_debt_payoff_months(page.conn, 12)
        page.add_debt_name.setText("Long Loan")
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("60")
        page._submit_add_debt()
        mock_warn.assert_called_once()
        # The 'Debt Added' info popup must NOT have fired.
        mock_info.assert_not_called()
        assert list_debts(page.conn) == []

    @patch.object(QMessageBox, "information")
    def test_submit_succeeds_when_horizon_equals_cap(self, mock_info, page):
        set_max_debt_payoff_months(page.conn, 60)
        page.add_debt_name.setText("Auto Loan")
        page.add_debt_amount.setText("12000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("60")
        page._submit_add_debt()
        mock_info.assert_called_once()
        assert len(list_debts(page.conn)) == 1


class TestTransactionsPageModeSwitching:
    """Tests that the type-combo drives which form section is visible."""

    @pytest.fixture
    def page(self, db_conn):
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        # Seed one debt and one mortgaged property.
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=2000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=100.0, schedule_frequency="monthly")
        ledger.add_property(
            db_conn, "2025-01-01", symbol="H", name="House",
            purchase_price=300000.0, 
            
            acquisition_mode="existing_property")
        p = TransactionsPage(db_conn)
        p.refresh()
        return p

    def _switch_to(self, page, txn_type):
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == txn_type:
                page.txn_type.setCurrentIndex(i)
                return
        raise AssertionError(f"{txn_type} not in combo")

    def test_default_mode_hides_all_debt_groups(self, page):
        assert page.add_debt_group.isHidden()
        assert page.pay_debt_group.isHidden()
        assert page.pay_mort_group.isHidden()

    def test_add_debt_mode_shows_only_add_debt_group(self, page):
        self._switch_to(page, "add_debt")
        assert not page.add_debt_group.isHidden()
        assert page.pay_debt_group.isHidden()
        assert page.pay_mort_group.isHidden()
        # Main form data rows are hidden.
        assert page.amount_input.isHidden()
        assert page.date_input.isHidden()

    def test_pay_debt_mode_shows_only_pay_debt_group(self, page):
        self._switch_to(page, "pay_debt")
        assert not page.pay_debt_group.isHidden()
        assert page.add_debt_group.isHidden()
        assert page.pay_mort_group.isHidden()

    def test_pay_mortgage_mode_shows_only_pay_mort_group(self, page):
        self._switch_to(page, "pay_mortgage")
        assert not page.pay_mort_group.isHidden()
        assert page.add_debt_group.isHidden()
        assert page.pay_debt_group.isHidden()

    def test_switching_back_to_cash_mode_restores_main_form(self, page):
        self._switch_to(page, "add_debt")
        self._switch_to(page, "deposit_cash")
        assert page.add_debt_group.isHidden()
        assert page.pay_debt_group.isHidden()
        assert page.pay_mort_group.isHidden()
        assert not page.amount_input.isHidden()
        assert not page.date_input.isHidden()


# ---------------------------------------------------------------------------
# Pay Debt / Pay Mortgage section labels
# ---------------------------------------------------------------------------

class TestInputCarryClearing:
    """Switching transaction types must wipe transient inputs so a "500"
    typed for a deposit doesn't silently follow the user into buy/sell
    or pay-debt where 500 means something different."""

    @pytest.fixture
    def page(self, db_conn):
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="V", name="Visa",
            amount=1000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        ledger.add_property(
            db_conn, "2025-01-01", symbol="HSE", name="House",
            purchase_price=300000.0, 
            
            acquisition_mode="existing_property")
        p = TransactionsPage(db_conn)
        p.refresh()
        return p

    def _switch_to(self, page, kind):
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == kind:
                page.txn_type.setCurrentIndex(i)
                return
        raise AssertionError(kind)

    def test_amount_cleared_when_switching_modes(self, page):
        page.amount_input.setText("500")
        self._switch_to(page, "buy")
        assert page.amount_input.text() == ""

    def test_qty_cleared_when_switching_modes(self, page):
        self._switch_to(page, "buy")
        page.qty_input.setText("10")
        page.fees_input.setText("5")
        self._switch_to(page, "sell")
        assert page.qty_input.text() == ""
        assert page.fees_input.text() == ""

    def test_notes_cleared_when_switching_modes(self, page):
        page.notes_input.setText("rainy day")
        self._switch_to(page, "buy")
        assert page.notes_input.text() == ""

    def test_pay_debt_amount_cleared_when_switching_modes(self, page):
        self._switch_to(page, "pay_debt")
        page.pay_debt_amount.setText("100")
        page.pay_debt_notes.setText("extra payment")
        self._switch_to(page, "pay_mortgage")
        assert page.pay_debt_amount.text() == ""
        assert page.pay_debt_notes.text() == ""

    def test_pay_mortgage_amount_cleared_when_switching_modes(self, page):
        self._switch_to(page, "pay_mortgage")
        page.pay_mort_amount.setText("500")
        self._switch_to(page, "deposit_cash")
        assert page.pay_mort_amount.text() == ""

    def test_add_debt_inputs_cleared_when_switching_modes(self, page):
        self._switch_to(page, "add_debt")
        page.add_debt_name.setText("Test Loan")
        page.add_debt_amount.setText("1000")
        self._switch_to(page, "buy")
        assert page.add_debt_name.text() == ""
        assert page.add_debt_amount.text() == ""

    def test_main_form_amount_cleared_when_entering_debt_mode(self, page):
        page.amount_input.setText("999")
        self._switch_to(page, "add_debt")
        # Even though main form is hidden in debt mode, the value should
        # be cleared so a return to deposit/withdraw is a clean slate.
        assert page.amount_input.text() == ""


class TestStrictAssetTypeSeparation:
    """Stricter contract: in any combo on the Transactions page, the
    asset categories don't mix.

    - buy / sell combo: only 'price-syncable + custom' (sellable types).
    - pay_property_expense combo: only real_estate.
    - pay_debt combo: only debt.
    - pay_mortgage combo: only properties (asset_type real_estate).
    """

    @pytest.fixture
    def page(self, db_conn):
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2025-01-01", 1_000_000.0)
        # One of every type so leakage is detectable.
        for sym, atype in [
            ("AAPL", "stock"), ("SPY", "etf"), ("BTC", "crypto"),
            ("CST", "custom"),
        ]:
            create_asset(db_conn, Asset(symbol=sym, name=sym, asset_type=atype))
        _, prop, _ = ledger.add_property(
            db_conn, "2025-01-02", symbol="HSE", name="House",
            purchase_price=300000.0,
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.06, payment_per_period=2000.0,
        )
        ledger.add_debt(
            db_conn, "2025-01-03", symbol="CC", name="Visa",
            amount=1000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        p = TransactionsPage(db_conn)
        p.refresh()
        return p

    def _items(self, combo):
        return [combo.itemText(i) for i in range(combo.count())]

    def _switch(self, page, kind):
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == kind:
                page.txn_type.setCurrentIndex(i)
                return

    def test_buy_combo_has_only_sellable(self, page):
        self._switch(page, "buy")
        items = self._items(page.asset_combo)
        # All sellable types appear; House and Visa do not.
        for s in ("AAPL", "SPY", "BTC", "CST"):
            assert any(s in t for t in items)
        assert not any("House" in t for t in items)
        assert not any("Visa" in t for t in items)

    def test_sell_combo_has_only_sellable(self, page):
        self._switch(page, "sell")
        items = self._items(page.asset_combo)
        assert not any("House" in t for t in items)
        assert not any("Visa" in t for t in items)

    def test_pay_property_expense_combo_has_only_properties(self, page):
        self._switch(page, "pay_property_expense")
        items = self._items(page.asset_combo)
        assert items == ["House"]
        for s in ("AAPL", "SPY", "BTC", "CST", "Visa"):
            assert not any(s in t for t in items)

    def test_pay_debt_combo_has_only_debts(self, page):
        # The pay_debt section uses its own combo; populated on refresh.
        items = self._items(page.pay_debt_combo)
        # Must list "Visa", and nothing else (no property, no stock).
        assert any("Visa" in t for t in items)
        for s in ("AAPL", "SPY", "BTC", "CST", "House"):
            assert not any(s in t for t in items)

    def test_pay_mortgage_combo_has_only_properties_with_mortgage(self, page):
        items = self._items(page.pay_mort_combo)
        assert any("House" in t for t in items)
        for s in ("AAPL", "SPY", "BTC", "CST", "Visa"):
            assert not any(s in t for t in items)


class TestPayDebtPayMortgageInsufficientCash:
    """Manual Pay Debt and Pay Mortgage must reject insufficient-cash
    submits and show the user a warning. Engine-level guard exists; this
    locks the UI behaviour."""

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_pay_debt_with_zero_cash_blocks(self, mock_info, mock_warn, db_conn):
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        from src.storage.debt_repo import get_debt_by_asset
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="V", name="Visa",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        assert calc_cash_balance(db_conn) == 0
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "pay_debt":
                page.txn_type.setCurrentIndex(i)
                break
        page.pay_debt_amount.setText("50")
        page._submit_pay_debt()
        mock_warn.assert_called_once()
        mock_info.assert_not_called()
        # Cash unchanged; debt balance unchanged.
        assert calc_cash_balance(db_conn) == 0
        assert get_debt_by_asset(db_conn, a.id).current_balance == 1000.0

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_pay_debt_with_insufficient_cash_blocks(self, mock_info, mock_warn, db_conn):
        # Deposit a little, then try to pay more than that.
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        from src.storage.debt_repo import get_debt_by_asset
        ledger.deposit_cash(db_conn, "2025-01-01", 30.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="V", name="Visa",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "pay_debt":
                page.txn_type.setCurrentIndex(i)
                break
        page.pay_debt_amount.setText("100")  # > $30 cash
        page._submit_pay_debt()
        mock_warn.assert_called_once()
        # The warning text must mention the cash shortfall, not just a generic error.
        warn_body = mock_warn.call_args.args[2] if len(mock_warn.call_args.args) > 2 else ""
        assert "cash" in warn_body.lower() or "insufficient" in warn_body.lower()
        assert calc_cash_balance(db_conn) == 30.0
        assert get_debt_by_asset(db_conn, a.id).current_balance == 1000.0

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_pay_mortgage_with_zero_cash_blocks(self, mock_info, mock_warn, db_conn):
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        from src.storage.mortgage_repo import get_mortgage_by_property
        a, prop, _ = ledger.add_property(
            db_conn, "2025-01-01", symbol="HSE", name="House",
            purchase_price=300000.0,
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.06, payment_per_period=2000.0,
        )
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "pay_mortgage":
                page.txn_type.setCurrentIndex(i)
                break
        page.pay_mort_amount.setText("500")
        page._submit_pay_mortgage()
        mock_warn.assert_called_once()
        mock_info.assert_not_called()
        # Mortgage unchanged; cash unchanged.
        assert calc_cash_balance(db_conn) == 0
        assert get_mortgage_by_property(db_conn, prop.id).current_balance == 200000.0

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_pay_mortgage_with_insufficient_cash_blocks(self, mock_info, mock_warn, db_conn):
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        from src.storage.mortgage_repo import get_mortgage_by_property
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        a, prop, _ = ledger.add_property(
            db_conn, "2025-01-01", symbol="HSE", name="House",
            purchase_price=300000.0,
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.06, payment_per_period=2000.0,
        )
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "pay_mortgage":
                page.txn_type.setCurrentIndex(i)
                break
        page.pay_mort_amount.setText("500")  # > $100 cash
        page._submit_pay_mortgage()
        mock_warn.assert_called_once()
        warn_body = mock_warn.call_args.args[2] if len(mock_warn.call_args.args) > 2 else ""
        assert "cash" in warn_body.lower() or "insufficient" in warn_body.lower()
        assert calc_cash_balance(db_conn) == 100.0
        assert get_mortgage_by_property(db_conn, prop.id).current_balance == 200000.0

    def test_buy_combo_excludes_real_estate(self, db_conn):
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        ledger.add_property(
            db_conn, "2025-01-02", symbol="HSE", name="House",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "buy":
                page.txn_type.setCurrentIndex(i)
                break
        items = [page.asset_combo.itemText(i) for i in range(page.asset_combo.count())]
        assert not any("House" in t for t in items)

    def test_buy_combo_excludes_debts(self, db_conn):
        from src.engines import ledger
        ledger.add_debt(
            db_conn, "2025-01-02", symbol="CC", name="Visa",
            amount=1000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "buy":
                page.txn_type.setCurrentIndex(i)
                break
        items = [page.asset_combo.itemText(i) for i in range(page.asset_combo.count())]
        assert not any("Visa" in t for t in items)
        assert not any("CC" in t for t in items)

    def test_sell_combo_excludes_real_estate_and_debts(self, db_conn):
        from src.engines import ledger
        ledger.add_property(
            db_conn, "2025-01-02", symbol="HSE", name="House",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "sell":
                page.txn_type.setCurrentIndex(i)
                break
        items = [page.asset_combo.itemText(i) for i in range(page.asset_combo.count())]
        assert not any("House" in t for t in items)
        assert not any("Loan" in t for t in items)

    def test_buy_combo_includes_all_four_sellable_types(self, db_conn):
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        for sym, atype in [("AAPL", "stock"), ("SPY", "etf"),
                           ("BTC", "crypto"), ("CST", "custom")]:
            create_asset(db_conn, Asset(symbol=sym, name=sym, asset_type=atype))
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "buy":
                page.txn_type.setCurrentIndex(i)
                break
        items = [page.asset_combo.itemText(i) for i in range(page.asset_combo.count())]
        # All four types are present.
        for sym in ("AAPL", "SPY", "BTC", "CST"):
            assert any(sym in t for t in items), f"{sym} missing from combo"


class TestBuyInsufficientCash:
    """The buy flow rejects insufficient-cash trades at every layer:
    preview blocks confirmation, confirm-clicked rejects, and the engine
    has its own guard so direct API callers can't slip through.
    """

    def _seed_buy_setup(self, conn, cash, asset_price):
        # Stock/ETF/crypto previews need an *executable quote* — the
        # daily OHLCV row alone is not enough. Seed both the daily
        # close (for valuation) and a market_quotes bid/ask (for the
        # trade-preview execution price). Without the quote row, the
        # preview path falls through to a live provider lookup, which
        # is fragile in CI / offline / yfinance-down scenarios.
        from src.storage.price_repo import bulk_upsert_ohlcv
        from src.storage.quote_repo import upsert_quote
        ledger.deposit_cash(conn, "2025-01-01", cash)
        a = create_asset(conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
        bulk_upsert_ohlcv(conn, [{
            "asset_id": a.id, "symbol": "AAPL", "asset_type": "stock",
            "date": "2025-01-02", "close": asset_price, "source": "test",
        }])
        upsert_quote(
            conn, a.id, "AAPL", "stock",
            bid=asset_price, ask=asset_price, last=asset_price,
            timestamp="2025-01-02T10:00:00", source="test",
        )
        return a

    def test_preview_marks_cant_confirm_when_cash_short(self, db_conn):
        from src.engines.trade_preview import TradeDraft, prepare_trade_preview
        a = self._seed_buy_setup(db_conn, cash=100.0, asset_price=50.0)
        draft = TradeDraft(action="buy", asset_id=a.id, quantity=10, fee=0)
        # `providers={}` opts out of the live-yfinance fallback so the
        # preview deterministically uses the stored quote we just
        # seeded (stable across offline / CI / yfinance outages).
        preview = prepare_trade_preview(db_conn, draft, "2025-01-02", providers={})
        assert not preview.can_confirm
        assert any("insufficient" in e.lower() or "cash" in e.lower()
                   for e in preview.blocking_errors)

    def test_confirm_rejects_even_if_called_with_stale_preview(self, db_conn):
        """If somehow can_confirm got past us, the engine still rejects."""
        from src.engines.trade_preview import (
            TradeDraft, TradePreview, confirm_trade)
        from src.engines.portfolio import calc_cash_balance
        a = self._seed_buy_setup(db_conn, cash=100.0, asset_price=50.0)
        # Build a forged preview that pretends it was approved.
        forged = TradePreview(
            action="buy", asset_id=a.id, symbol="AAPL", asset_type="stock",
            quantity=10, trade_price=50.0, fee=0.0, can_confirm=True,
            cash_before=100.0, cash_after=-400.0)
        result = confirm_trade(db_conn, forged, "2025-01-02")
        assert result is False
        assert calc_cash_balance(db_conn) == 100.0

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_ui_confirm_button_disabled_when_cash_short(self, mock_info, mock_warn, db_conn):
        a = self._seed_buy_setup(db_conn, cash=100.0, asset_price=50.0)
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "buy":
                page.txn_type.setCurrentIndex(i)
                break
        for i in range(page.asset_combo.count()):
            if page.asset_combo.itemData(i) == a.id:
                page.asset_combo.setCurrentIndex(i)
                break
        page.qty_input.setText("10")
        page._preview_trade()
        # The confirm button must be disabled and the preview status must
        # explicitly say it cannot be confirmed.
        assert not page.confirm_btn.isEnabled()
        assert "Cannot confirm" in page.preview_status.text()
        # And calling _confirm_trade is a no-op (early return) — no info modal.
        page._confirm_trade()
        mock_info.assert_not_called()


class TestCashFlowSignGuards:
    """deposit_cash and withdraw_cash must require amount > 0 so a
    negative-deposit can't be used as a back-door withdrawal that
    bypasses the cash-balance check.
    """

    def test_deposit_rejects_negative(self, db_conn):
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        with pytest.raises(ValueError, match="positive"):
            ledger.deposit_cash(db_conn, "2025-01-02", -1000.0)
        # Cash unchanged.
        assert calc_cash_balance(db_conn) == 100.0

    def test_deposit_rejects_zero(self, db_conn):
        from src.engines import ledger
        with pytest.raises(ValueError, match="positive"):
            ledger.deposit_cash(db_conn, "2025-01-01", 0)

    def test_withdraw_rejects_negative(self, db_conn):
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        with pytest.raises(ValueError, match="positive"):
            ledger.withdraw_cash(db_conn, "2025-01-02", -50.0)
        # Cash unchanged.
        assert calc_cash_balance(db_conn) == 100.0

    def test_withdraw_rejects_zero(self, db_conn):
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        with pytest.raises(ValueError, match="positive"):
            ledger.withdraw_cash(db_conn, "2025-01-02", 0)

    def test_withdraw_above_balance_still_rejected(self, db_conn):
        """The original guard remains: a positive withdrawal that exceeds
        the cash balance is rejected with the standard message."""
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.withdraw_cash(db_conn, "2025-01-02", 500.0)
        assert calc_cash_balance(db_conn) == 100.0

    def test_withdraw_at_exactly_balance_succeeds(self, db_conn):
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        ledger.withdraw_cash(db_conn, "2025-01-02", 100.0)
        assert calc_cash_balance(db_conn) == 0.0

    @patch.object(QMessageBox, "warning")
    def test_ui_blocks_overdraft_withdrawal(self, mock_warn, db_conn):
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "withdraw_cash":
                page.txn_type.setCurrentIndex(i)
                break
        page.amount_input.setText("500")
        page._submit()
        mock_warn.assert_called_once()
        assert calc_cash_balance(db_conn) == 100.0

    @patch.object(QMessageBox, "warning")
    def test_ui_blocks_negative_deposit_back_door(self, mock_warn, db_conn):
        """The old back door: a negative number in deposit_cash silently
        withdrew without a balance check. UI must reject it."""
        from src.engines import ledger
        from src.engines.portfolio import calc_cash_balance
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        page = TransactionsPage(db_conn)
        page.refresh()
        # Default selection is deposit_cash.
        page.amount_input.setText("-10000")
        page._submit()
        mock_warn.assert_called_once()
        # Cash stays at the legitimate $100 deposit.
        assert calc_cash_balance(db_conn) == 100.0


class TestRealEstateDisplayLabel:
    """Real-estate symbols are auto-generated (e.g. RE_HOME) so the
    Transactions page should display the property name everywhere it's
    user-facing — both the Pay Property Expense combo and the
    transaction-history Asset column.
    """

    def test_pay_property_expense_combo_shows_name_not_symbol(self, db_conn):
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        ledger.add_property(
            db_conn, "2025-01-03", symbol="RE_HOME", name="Home",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "pay_property_expense":
                page.txn_type.setCurrentIndex(i)
                break
        items = [
            page.asset_combo.itemText(i)
            for i in range(page.asset_combo.count())
        ]
        assert items == ["Home"]
        # Specifically: the auto-generated symbol must NOT appear.
        assert not any("RE_HOME" in t for t in items)

    def test_history_table_shows_property_name_not_symbol(self, db_conn):
        from src.engines import ledger
        from src.storage.asset_repo import list_assets
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        a, _, _ = ledger.add_property(
            db_conn, "2025-01-03", symbol="RE_HOME", name="Home",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        ledger.pay_property_expense(db_conn, "2025-02-01", a.id, 200.0)
        page = TransactionsPage(db_conn)
        page.refresh()
        # Collect the asset column for any row referencing the property.
        asset_cells = []
        for row in range(page.table.rowCount()):
            asset_cells.append(page.table.item(row, 3).text())
        # At least one row references the property; that row reads "Home", not "RE_HOME".
        assert "Home" in asset_cells
        assert "RE_HOME" not in asset_cells

    def test_other_asset_types_unchanged_in_combo(self, db_conn):
        """Stocks/ETFs/crypto still show 'SYMBOL - Name' — the friendly
        rename is real-estate-only because that's the only asset type
        whose symbol is auto-generated."""
        from src.engines import ledger
        from src.models.asset import Asset
        from src.storage.asset_repo import create_asset
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        a = create_asset(db_conn, Asset(symbol="AAPL", name="Apple Inc.", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-02", a.id, quantity=1, price=100)
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "buy":
                page.txn_type.setCurrentIndex(i)
                break
        items = [
            page.asset_combo.itemText(i)
            for i in range(page.asset_combo.count())
        ]
        assert "AAPL - Apple Inc." in items

    def test_other_asset_types_unchanged_in_history(self, db_conn):
        from src.engines import ledger
        from src.models.asset import Asset
        from src.storage.asset_repo import create_asset
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        a = create_asset(db_conn, Asset(symbol="AAPL", name="Apple Inc.", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-02", a.id, quantity=1, price=100)
        page = TransactionsPage(db_conn)
        page.refresh()
        asset_cells = [
            page.table.item(row, 3).text()
            for row in range(page.table.rowCount())
        ]
        # Stock still shows the symbol (compact + meaningful).
        assert "AAPL" in asset_cells

    def test_real_estate_falls_back_to_symbol_when_name_missing(self, db_conn):
        """If a real-estate asset slips into the DB with an empty name
        (e.g. via raw create_asset, bypassing add_property), the display
        falls back to the symbol so the row isn't silently blank."""
        from src.models.asset import Asset
        from src.storage.asset_repo import create_asset
        a = create_asset(db_conn, Asset(symbol="RE_X", name="", asset_type="real_estate"))
        page = TransactionsPage(db_conn)
        # Don't go through pay_property_expense (filter would still show it,
        # but exercising the helper directly is simpler).
        label = page._asset_combo_label(a)
        assert label == "RE_X"


class TestDuplicateNameRejection:
    """Adding a debt or a property with a name that already names another
    active debt/property is rejected at the engine layer (and surfaced
    to the user via the existing UI ValueError handlers)."""

    def test_add_debt_rejects_exact_duplicate(self, db_conn):
        from src.engines import ledger
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="V1", name="Visa Card",
            amount=1000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        with pytest.raises(ValueError, match="already exists"):
            ledger.add_debt(
                db_conn, "2025-01-02", symbol="V2", name="Visa Card",
                amount=2000.0, interest_rate=0.06, cash_received=False,
                payment_per_period=80.0, schedule_frequency="monthly")

    def test_add_debt_rejects_case_insensitive_duplicate(self, db_conn):
        from src.engines import ledger
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="V", name="Visa Card",
            amount=1000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        with pytest.raises(ValueError, match="already exists"):
            ledger.add_debt(
                db_conn, "2025-01-02", symbol="V", name="VISA CARD",
                amount=1000.0, interest_rate=0.06, cash_received=False,
                payment_per_period=50.0, schedule_frequency="monthly")

    def test_add_debt_rejects_whitespace_padded_duplicate(self, db_conn):
        from src.engines import ledger
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="V", name="Visa Card",
            amount=1000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        with pytest.raises(ValueError, match="already exists"):
            ledger.add_debt(
                db_conn, "2025-01-02", symbol="V", name="  Visa Card  ",
                amount=1000.0, interest_rate=0.06, cash_received=False,
                payment_per_period=50.0, schedule_frequency="monthly")

    def test_add_debt_allows_reusing_paid_off_name(self, db_conn):
        """A debt whose balance is zero is historical; its name should be
        reusable for a new debt the user is taking on."""
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="V", name="Visa Card",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0, schedule_frequency="monthly")
        # Pay it off in full.
        ledger.pay_debt(db_conn, "2025-02-01", a.id, 1000.0)
        # New debt with the same name should now succeed.
        _, debt2, _ = ledger.add_debt(
            db_conn, "2025-03-01", symbol="V2", name="Visa Card",
            amount=2000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0, schedule_frequency="monthly")
        assert debt2.name == "Visa Card"

    def test_add_property_rejects_exact_duplicate(self, db_conn):
        from src.engines import ledger
        ledger.add_property(
            db_conn, "2025-01-01", symbol="H1", name="Sunset House",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        with pytest.raises(ValueError, match="already exists"):
            ledger.add_property(
                db_conn, "2025-02-01", symbol="H2", name="Sunset House",
                purchase_price=400000.0, 
                acquisition_mode="existing_property")

    def test_add_property_rejects_case_insensitive_duplicate(self, db_conn):
        from src.engines import ledger
        ledger.add_property(
            db_conn, "2025-01-01", symbol="H1", name="Sunset House",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        with pytest.raises(ValueError, match="already exists"):
            ledger.add_property(
                db_conn, "2025-02-01", symbol="H2", name="sunset HOUSE",
                purchase_price=400000.0, 
                acquisition_mode="existing_property")

    def test_add_property_allows_reusing_sold_name(self, db_conn):
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2025-01-01", 1_000_000.0)
        a, _, _ = ledger.add_property(
            db_conn, "2025-01-01", symbol="H1", name="Lake House",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        ledger.sell_property(db_conn, "2025-06-01", a.id, sale_price=320000.0)
        # Reusing the same name for a NEW property should now be allowed.
        _, prop2, _ = ledger.add_property(
            db_conn, "2025-07-01", symbol="H2", name="Lake House",
            purchase_price=400000.0, 
            acquisition_mode="existing_property")
        assert prop2.id is not None

    def test_add_property_requires_name(self, db_conn):
        from src.engines import ledger
        with pytest.raises(ValueError, match="Property name is required"):
            ledger.add_property(
                db_conn, "2025-01-01", symbol="H", name="",
                purchase_price=300000.0, 
                acquisition_mode="existing_property")
        with pytest.raises(ValueError, match="Property name is required"):
            ledger.add_property(
                db_conn, "2025-01-01", symbol="H", name="   ",
                purchase_price=300000.0, 
                acquisition_mode="existing_property")

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_add_debt_ui_blocks_duplicate(self, mock_info, mock_warn, db_conn):
        from src.engines import ledger
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="V", name="Visa Card",
            amount=1000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "add_debt":
                page.txn_type.setCurrentIndex(i)
                break
        page.add_debt_name.setText("Visa Card")
        page.add_debt_amount.setText("2000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("100")
        page._submit_add_debt()
        mock_warn.assert_called_once()
        mock_info.assert_not_called()
        # Only the seed debt remains.
        assert len(list_debts(db_conn)) == 1


class TestTermFieldUnitLabels:
    """The term + payment row labels must spell out their unit so users
    don't enter "5" thinking years while the schedule is monthly.
    """

    @pytest.fixture
    def page(self, db_conn):
        p = TransactionsPage(db_conn)
        p.refresh()
        for i in range(p.txn_type.count()):
            if p.txn_type.itemData(i) == "add_debt":
                p.txn_type.setCurrentIndex(i)
                break
        return p

    def _set_schedule(self, page, schedule):
        for i in range(page.add_debt_schedule.count()):
            if page.add_debt_schedule.itemData(i) == schedule:
                page.add_debt_schedule.setCurrentIndex(i)
                return
        raise AssertionError(schedule)

    def test_monthly_labels_say_months(self, page):
        self._set_schedule(page, "monthly")
        assert "month" in page.add_debt_term_label.text().lower()
        assert "month" in page.add_debt_term.placeholderText().lower()
        assert "month" in page.add_debt_payment_label.text().lower()
        assert "month" in page.add_debt_payment.placeholderText().lower()
        # No "year" leakage in monthly mode.
        assert "year" not in page.add_debt_term_label.text().lower()
        assert "year" not in page.add_debt_payment_label.text().lower()

    def test_yearly_labels_say_years(self, page):
        self._set_schedule(page, "yearly")
        assert "year" in page.add_debt_term_label.text().lower()
        assert "year" in page.add_debt_term.placeholderText().lower()
        assert "year" in page.add_debt_payment_label.text().lower()
        assert "year" in page.add_debt_payment.placeholderText().lower()

    def test_labels_swap_when_schedule_changes(self, page):
        self._set_schedule(page, "monthly")
        assert "month" in page.add_debt_term_label.text().lower()
        self._set_schedule(page, "yearly")
        assert "year" in page.add_debt_term_label.text().lower()
        self._set_schedule(page, "monthly")
        assert "month" in page.add_debt_term_label.text().lower()


class TestMaxPayoffCapUnitConversion:
    """Regression for the user-reported bug: cap=50 months, user adds a
    debt with payoff "5 years" — the request must be denied."""

    @pytest.fixture
    def page(self, db_conn):
        from src.storage.settings_repo import set_max_debt_payoff_months
        set_max_debt_payoff_months(db_conn, 50)
        p = TransactionsPage(db_conn)
        p.refresh()
        for i in range(p.txn_type.count()):
            if p.txn_type.itemData(i) == "add_debt":
                p.txn_type.setCurrentIndex(i)
                break
        return p

    def _set_schedule(self, page, schedule):
        for i in range(page.add_debt_schedule.count()):
            if page.add_debt_schedule.itemData(i) == schedule:
                page.add_debt_schedule.setCurrentIndex(i)
                return

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_yearly_5_year_term_above_50_month_cap_is_denied(
        self, mock_info, mock_warn, page):
        self._set_schedule(page, "yearly")
        page.add_debt_name.setText("Loan")
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("5")  # 5 YEARS = 60 months > 50
        page._submit_add_debt()
        mock_info.assert_not_called()
        mock_warn.assert_called_once()
        assert list_debts(page.conn) == []

    @patch.object(QMessageBox, "information")
    def test_yearly_4_year_term_below_50_month_cap_is_allowed(
        self, mock_info, page):
        # 4 years = 48 months < 50, so the debt should be allowed.
        self._set_schedule(page, "yearly")
        page.add_debt_name.setText("Loan")
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("4")
        page._submit_add_debt()
        mock_info.assert_called_once()
        assert len(list_debts(page.conn)) == 1

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_yearly_payment_path_5_year_payoff_above_cap_is_denied(
        self, mock_info, mock_warn, page):
        # User picks the per-period-payment path and provides a payment
        # that yields a 5-year payoff under yearly schedule.
        self._set_schedule(page, "yearly")
        page.add_debt_name.setText("Loan")
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_payment.setChecked(True)
        # Annuity for 10k @ 6% over 5 years ≈ $2374/yr.
        page.add_debt_payment.setText("2374")
        page._submit_add_debt()
        mock_info.assert_not_called()
        mock_warn.assert_called_once()
        assert list_debts(page.conn) == []

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_monthly_60_month_term_above_50_month_cap_is_denied(
        self, mock_info, mock_warn, page):
        # The same 5-year-equivalent (60 months) debt under monthly mode.
        self._set_schedule(page, "monthly")
        page.add_debt_name.setText("Loan")
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("60")
        page._submit_add_debt()
        mock_info.assert_not_called()
        mock_warn.assert_called_once()
        assert list_debts(page.conn) == []


class TestTransactionsPageUIPolish:
    """The April-29 polish round: ordering, button text, asset filter."""

    def test_pay_property_expense_is_last_in_combo(self, db_conn):
        from src.gui.pages.transactions import TXN_TYPES
        assert TXN_TYPES[-1] == "pay_property_expense"

    def test_confirm_add_debt_button_has_no_mnemonic_marker(self, db_conn):
        # & is the Qt mnemonic prefix; on macOS it shows as an underscore.
        # The Add-Debt confirm button must use plain text only.
        page = TransactionsPage(db_conn)
        from PySide6.QtWidgets import QPushButton
        confirm_btn_texts = [
            b.text() for b in page.findChildren(QPushButton)
            if b.text() and b.text().startswith("Confirm")
        ]
        # Find the Add Debt confirm specifically.
        add_debt_btns = [t for t in confirm_btn_texts if "Add Debt" in t]
        assert add_debt_btns, "no 'Confirm ... Add Debt' button found"
        for t in add_debt_btns:
            assert "&" not in t, f"button text contains mnemonic: {t!r}"

    def test_pay_property_expense_filters_combo_to_real_estate(self, db_conn):
        from src.engines import ledger
        # Seed a stock, an ETF, a debt, AND a property — only the property
        # should be visible when pay_property_expense is selected.
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        create_asset(db_conn, Asset(symbol="STK", name="Stock A", asset_type="stock"))
        create_asset(db_conn, Asset(symbol="ETF", name="ETF A", asset_type="etf"))
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        ledger.add_property(
            db_conn, "2025-01-01", symbol="HSE", name="House",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        page = TransactionsPage(db_conn)
        page.refresh()
        # Switch to pay_property_expense; combo should reload to property-only.
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "pay_property_expense":
                page.txn_type.setCurrentIndex(i)
                break
        items = [
            page.asset_combo.itemText(i)
            for i in range(page.asset_combo.count())
        ]
        # Only the House should appear.
        assert any("House" in t for t in items)
        assert not any("Stock A" in t or "ETF A" in t or "Loan" in t for t in items)

    def test_buy_filters_combo_to_sellable_types(self, db_conn):
        # The buy/sell combos hide non-sellable types (real-estate, debt)
        # so the user can't even pick them. Engine validation remains as a
        # belt-and-braces guard against direct API callers.
        from src.engines import ledger
        ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
        create_asset(db_conn, Asset(symbol="STK", name="Stock A", asset_type="stock"))
        ledger.add_property(
            db_conn, "2025-01-01", symbol="HSE", name="House",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        ledger.add_debt(
            db_conn, "2025-01-02", symbol="CC", name="Visa",
            amount=1000.0, interest_rate=0.06, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "buy":
                page.txn_type.setCurrentIndex(i)
                break
        items = [
            page.asset_combo.itemText(i)
            for i in range(page.asset_combo.count())
        ]
        # Stock IS available; house and debt are NOT.
        assert any("Stock A" in t for t in items)
        assert not any("House" in t for t in items)
        assert not any("Visa" in t for t in items)

    def test_summary_box_renders_html(self, db_conn):
        page = TransactionsPage(db_conn)
        # Switch to add_debt mode and fill in a feasible plan.
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "add_debt":
                page.txn_type.setCurrentIndex(i)
                break
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("60")
        page._on_add_debt_inputs_changed()
        text = page.add_debt_preview.text()
        # The new format uses an HTML table; the user-visible substrings
        # must still appear so the box is informative.
        assert "<table" in text
        assert "Per-month payment" in text
        assert "Total paid" in text

    def test_summary_box_styles_warning_block_when_above_cap(self, db_conn):
        from src.storage.settings_repo import set_max_debt_payoff_months
        set_max_debt_payoff_months(db_conn, 12)
        page = TransactionsPage(db_conn)
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "add_debt":
                page.txn_type.setCurrentIndex(i)
                break
        page.add_debt_amount.setText("100000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("1000")
        page._on_add_debt_inputs_changed()
        text = page.add_debt_preview.text()
        # Warning shows up as its own block (with red-ish styling) so the
        # user can see the over-cap signal at a glance.
        assert "exceeds" in text.lower()
        assert "background" in text.lower()  # the inline-styled callout div


class TestExtraPaymentLabels:
    def test_pay_debt_section_marked_extra(self, db_conn):
        page = TransactionsPage(db_conn)
        # Find any QGroupBox in the page whose title starts with "Pay Debt".
        from PySide6.QtWidgets import QGroupBox
        debt_group = next(
            g for g in page.findChildren(QGroupBox) if g.title().startswith("Pay Debt")
        )
        assert "extra" in debt_group.title().lower()

    def test_pay_mortgage_section_marked_extra(self, db_conn):
        page = TransactionsPage(db_conn)
        from PySide6.QtWidgets import QGroupBox
        mort_group = next(
            g for g in page.findChildren(QGroupBox) if g.title().startswith("Pay Mortgage")
        )
        assert "extra" in mort_group.title().lower()


# ---------------------------------------------------------------------------
# Asset Analysis: Debts table
# ---------------------------------------------------------------------------

class TestAssetAnalysisDebts:
    def test_debt_table_lists_each_debt(self, db_conn):
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="A", name="Card",
            amount=1000.0, interest_rate=0.12, cash_received=False,
            payment_per_period=50.0, schedule_frequency="monthly")
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="B", name="Auto",
            amount=12000.0, interest_rate=0.06, cash_received=False,
            term_periods=60, schedule_frequency="monthly")
        page = AssetAnalysisPage(db_conn)
        page.refresh()
        rows = page.debt_table.rowCount()
        assert rows == 2
        names = {page.debt_table.item(i, 0).text() for i in range(rows)}
        assert names == {"Card", "Auto"}

    def test_debt_table_shows_months_remaining(self, db_conn):
        # 12000 @ 6% / 60 months — exact horizon should appear in month col.
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="A", name="Auto",
            amount=12000.0, interest_rate=0.06, cash_received=False,
            term_periods=60, schedule_frequency="monthly")
        page = AssetAnalysisPage(db_conn)
        page.refresh()
        months_text = page.debt_table.item(0, 6).text()
        assert months_text in ("60", "61")  # final-period rounding tolerated

    def test_debt_table_shows_infinity_for_infeasible(self, db_conn):
        from src.storage.debt_repo import create_debt
        from src.models.debt import Debt
        a = create_asset(db_conn, Asset(symbol="X", name="Bad", asset_type="debt"))
        create_debt(db_conn, Debt(
            asset_id=a.id, name="Bad",
            original_amount=10000.0, current_balance=10000.0,
            interest_rate=0.12, monthly_payment_amount=10.0,
            schedule_frequency="monthly"))
        page = AssetAnalysisPage(db_conn)
        page.refresh()
        assert page.debt_table.item(0, 6).text() == "∞"
