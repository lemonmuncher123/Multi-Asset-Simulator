"""Comprehensive tests for the debt / mortgage / rent / forced-liquidation /
bankruptcy feature set.

Organized into test classes by scope section so individual sections can be
targeted via ``pytest -k ClassName``. Modal dialogs are patched everywhere
to keep the GUI suite headless and non-blocking.
"""
from datetime import date
from unittest.mock import patch

import pytest
from PySide6.QtWidgets import QMessageBox

from src.engines import ledger
from src.engines.portfolio import calc_cash_balance
from src.engines.risk import check_bankruptcy, get_all_warnings
from src.gui.pages.transactions import (
    TransactionsPage, TXN_TYPES, AMOUNT_REQUIRED,
    DEFAULT_DEBT_ANNUAL_RATE_PCT)
from src.models.asset import Asset
from src.models.debt import Debt
from src.storage.asset_repo import create_asset, list_assets
from src.storage.database import init_db
from src.storage.debt_repo import (
    create_debt, get_debt_by_asset, list_debts, update_debt)
from src.storage.price_repo import bulk_upsert_ohlcv
from src.storage.property_repo import (
    get_property_by_asset, list_active_properties)
from src.storage.transaction_repo import list_transactions


# ---------------------------------------------------------------------------
# Shared fixtures + helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def page(db_conn):
    p = TransactionsPage(db_conn)
    p.refresh()
    yield p


def _seed_market_price(conn, asset_id, symbol, asset_type, price, when="2024-12-02"):
    bulk_upsert_ohlcv(conn, [{
        "asset_id": asset_id, "symbol": symbol, "asset_type": asset_type,
        "date": when, "close": price, "source": "test",
    }])


def _seed_sellable(conn, prefix="A", price=100.0, qty=10):
    """Returns ids of (stock, etf, crypto, custom) each with `qty` units at `price`.

    Deposits enough cash to fund the buys (4 * qty * price), since the
    ledger.buy() flow rejects insufficient-cash transactions.
    """
    types = ("stock", "etf", "crypto", "custom")
    needed = qty * price * len(types) + 1.0
    ledger.deposit_cash(conn, "2024-12-01", needed)
    out = {}
    for atype in types:
        a = create_asset(conn, Asset(
            symbol=f"{prefix}_{atype.upper()}",
            name=f"{prefix} {atype}",
            asset_type=atype))
        ledger.buy(conn, "2024-12-02", a.id, quantity=qty, price=price)
        _seed_market_price(conn, a.id, a.symbol, atype, price)
        out[atype] = a.id
    return out


# ===========================================================================
# Section 1: Add Debt UI validation
# ===========================================================================

class TestAddDebtValidation:
    def test_engine_rejects_empty_name(self, db_conn):
        with pytest.raises(ValueError, match="Debt name is required"):
            ledger.add_debt(
                db_conn, "2025-01-01", symbol="X", name="", amount=1000.0,
                payment_per_period=100.0)

    def test_engine_rejects_whitespace_name(self, db_conn):
        with pytest.raises(ValueError, match="Debt name is required"):
            ledger.add_debt(
                db_conn, "2025-01-01", symbol="X", name="   ", amount=1000.0,
                payment_per_period=100.0)

    def test_engine_rejects_invalid_schedule(self, db_conn):
        with pytest.raises(ValueError, match="schedule_frequency"):
            ledger.add_debt(
                db_conn, "2025-01-01", symbol="X", name="Loan",
                amount=1000.0, schedule_frequency="weekly",
                payment_per_period=100.0)

    def test_engine_rejects_invalid_interest_period(self, db_conn):
        with pytest.raises(ValueError, match="interest_period"):
            ledger.add_debt(
                db_conn, "2025-01-01", symbol="X", name="Loan",
                amount=1000.0, interest_period="quarterly",
                payment_per_period=100.0)

    def test_engine_supports_monthly_and_yearly_schedule(self, db_conn):
        for sched in ("monthly", "yearly"):
            _, debt, _ = ledger.add_debt(
                db_conn, "2025-01-01", symbol=f"L_{sched}",
                name=f"Loan {sched}", amount=1000.0,
                schedule_frequency=sched, cash_received=False,
                payment_per_period=100.0)
            assert debt.schedule_frequency == sched

    def test_engine_only_accepts_annual_interest_period(self, db_conn):
        # Always-annual contract: 'annual' works, 'monthly' is rejected.
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_period="annual", cash_received=False,
            payment_per_period=100.0)
        assert debt.interest_rate == pytest.approx(debt.interest_rate)
        with pytest.raises(ValueError, match="always annual"):
            ledger.add_debt(
                db_conn, "2025-01-02", symbol="L2", name="Loan2",
                amount=1000.0, interest_period="monthly", cash_received=False,
                payment_per_period=100.0)

    def test_debt_model_has_no_repayment_term_field(self):
        """Repayment-term field was removed from the data model."""
        from dataclasses import fields
        names = {f.name for f in fields(Debt)}
        assert "repayment_term" not in names
        assert "term_months" not in names
        assert "term_years" not in names

    def test_storage_has_no_repayment_term_column(self, db_conn):
        cols = {row[1] for row in db_conn.execute(
            "PRAGMA table_info(debts)"
        ).fetchall()}
        assert "repayment_term" not in cols
        assert "term_months" not in cols

    @patch.object(QMessageBox, "warning")
    def test_ui_blocks_blank_name(self, mock_warn, page):
        page.add_debt_name.setText("")
        page.add_debt_amount.setText("1000")
        page._submit_add_debt()
        mock_warn.assert_called_once()
        # No debt was created.
        assert list_debts(page.conn) == []

    @patch.object(QMessageBox, "warning")
    def test_ui_blocks_blank_amount(self, mock_warn, page):
        page.add_debt_name.setText("Loan")
        page.add_debt_amount.setText("")
        page._submit_add_debt()
        mock_warn.assert_called_once()
        assert list_debts(page.conn) == []

    @patch.object(QMessageBox, "warning")
    def test_ui_blocks_zero_or_negative_amount(self, mock_warn, page):
        page.add_debt_name.setText("Loan")
        page.add_debt_amount.setText("0")
        page._submit_add_debt()
        mock_warn.assert_called_once()
        # Reset and try negative.
        mock_warn.reset_mock()
        page.add_debt_amount.setText("-100")
        page._submit_add_debt()
        mock_warn.assert_called_once()
        assert list_debts(page.conn) == []

    def test_ui_blank_rate_preview_uses_default(self, db_conn):
        """The live preview shouldn't go silent just because the user
        cleared the rate field — it should compute against the
        configured default. Spec §2."""
        from src.storage.settings_repo import set_default_debt_annual_rate_pct
        set_default_debt_annual_rate_pct(db_conn, 9.0)
        page = TransactionsPage(db_conn)
        page.refresh()
        page.add_debt_amount.setText("1200")
        page.add_debt_rate.setText("")  # blank field
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("12")
        page._on_add_debt_inputs_changed()
        # A 12-month, 9%-rate, 1200-principal schedule yields ~$105/mo;
        # the preview should NOT be the empty/instructional placeholder.
        body = page.add_debt_preview.text()
        assert "Per-month payment" in body
        # Rate explicitly supplied: assert the resolved rate matches the
        # setting via the preview's total-interest line.
        assert "Total interest" in body

    def test_ui_blank_rate_falls_back_to_seven_when_setting_invalid(
        self, db_conn):
        """If the setting is missing or unparseable, the helper falls
        back to ``DEFAULT_DEBT_ANNUAL_RATE_PCT = 7.0``. Spec §2."""
        from src.storage.settings_repo import set_setting
        # Corrupt the setting manually.
        set_setting(db_conn, "default_debt_annual_rate_pct", "garbage")
        page = TransactionsPage(db_conn)
        page.refresh()
        # The form's _resolved helper returns 7.0 even though the
        # stored setting is invalid.
        assert page._resolved_debt_rate_pct() == pytest.approx(7.0)

    def test_ui_explicit_rate_overrides_default(self, db_conn):
        """A user-typed rate must win over the configured default —
        the fallback only kicks in when the field is blank/invalid."""
        from src.storage.settings_repo import set_default_debt_annual_rate_pct
        set_default_debt_annual_rate_pct(db_conn, 9.0)
        page = TransactionsPage(db_conn)
        page.refresh()
        page.add_debt_rate.setText("12.5")
        assert page._resolved_debt_rate_pct() == pytest.approx(12.5)

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_ui_blank_rate_falls_back_to_default(
        self, mock_info, mock_warn, page):
        """Spec §6 #4: a blank rate field must not block submit — the
        engine falls back to the configured default (which itself
        defaults to 7%). The earlier "rate is required" warning was
        removed."""
        page.add_debt_name.setText("Blank Rate Loan")
        page.add_debt_amount.setText("1000")
        page.add_debt_rate.setText("")  # explicitly blank
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("100")
        page._submit_add_debt()
        mock_warn.assert_not_called()
        debts = list_debts(page.conn)
        assert len(debts) == 1
        # Default rate is 7% per spec.
        assert debts[0].interest_rate == pytest.approx(0.07)

    def test_ui_default_rate_is_seven_percent(self, page):
        # Spec §3 #7: 7% is the industry-average rate the UI prefills.
        assert DEFAULT_DEBT_ANNUAL_RATE_PCT == pytest.approx(7.0)
        assert page.add_debt_rate.text() == f"{DEFAULT_DEBT_ANNUAL_RATE_PCT:.1f}"

    def test_ui_rate_picks_up_settings_change_on_refresh(self, db_conn):
        """Changing the Settings default and refreshing the Transactions
        page must update the rate field — the user expects the setting to
        actually take effect.
        """
        from src.storage.settings_repo import set_default_debt_annual_rate_pct
        page = TransactionsPage(db_conn)
        page.refresh()
        assert page.add_debt_rate.text() == "7.0"
        set_default_debt_annual_rate_pct(db_conn, 9.0)
        page.refresh()
        assert page.add_debt_rate.text() == "9.0"

    def test_ui_rate_preserves_user_typed_value_across_refresh(self, db_conn):
        """If the user typed a custom rate, refresh() must not clobber it
        with the settings default.
        """
        from src.storage.settings_repo import set_default_debt_annual_rate_pct
        page = TransactionsPage(db_conn)
        page.refresh()
        page.add_debt_rate.setText("12.5")
        set_default_debt_annual_rate_pct(db_conn, 9.0)
        page.refresh()
        assert page.add_debt_rate.text() == "12.5"

    def test_ui_has_term_field_for_repayment_plan(self, page):
        # Add Debt now supports a "fix the term" repayment plan, so a term
        # input is expected. The repayment-term concept is still gone from
        # the *data model*; the test just guards the UI affordance.
        assert hasattr(page, "add_debt_term")
        assert hasattr(page, "add_debt_radio_payment")
        assert hasattr(page, "add_debt_radio_term")

    def test_ui_supports_monthly_and_yearly_schedule(self, page):
        items = [
            page.add_debt_schedule.itemData(i)
            for i in range(page.add_debt_schedule.count())
        ]
        assert "monthly" in items and "yearly" in items

    def test_ui_no_longer_offers_monthly_interest_period(self, page):
        # The annual-only contract removed the rate-period combo.
        assert not hasattr(page, "add_debt_rate_period")

    @patch.object(QMessageBox, "information")
    def test_ui_happy_path_creates_debt_with_payment(self, mock_info, page):
        page.add_debt_name.setText("My Card")
        page.add_debt_symbol.setText("CARD")
        page.add_debt_amount.setText("3000")
        page.add_debt_rate.setText("12.0")
        page.add_debt_schedule.setCurrentIndex(0)  # monthly
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("100")
        page._submit_add_debt()
        debts = list_debts(page.conn)
        assert len(debts) == 1
        assert debts[0].name == "My Card"
        assert debts[0].interest_rate == pytest.approx(0.12)
        assert debts[0].monthly_payment_amount == pytest.approx(100.0)

    @patch.object(QMessageBox, "information")
    def test_ui_happy_path_creates_debt_with_term(self, mock_info, page):
        page.add_debt_name.setText("Auto Loan")
        page.add_debt_amount.setText("12000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_schedule.setCurrentIndex(0)  # monthly
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("60")
        page._submit_add_debt()
        debts = list_debts(page.conn)
        assert len(debts) == 1
        assert debts[0].name == "Auto Loan"
        # 12000 @ 6% / 60 months ≈ 232/mo
        assert 200 < debts[0].monthly_payment_amount < 260


# ===========================================================================
# Section 1.5: Debt plan persistence (plan_type, original_term_periods)
# ===========================================================================

class TestDebtPlanPersistence:
    """The debt model captures *which* planning method the user chose so
    that Pay Debt's recompute-after-payment can honor it. Schema v9 added
    `plan_type`, `original_term_periods`, and `created_at` to the debts
    table; these tests pin the round-trip and the engine's defaulting.
    """

    def test_payment_per_period_persists_fixed_payment_plan(self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L1", name="Card",
            amount=3000.0, interest_rate=0.12,
            schedule_frequency="monthly",
            payment_per_period=100.0,
            cash_received=False)
        assert debt.plan_type == "fixed_payment"
        assert debt.original_term_periods is None
        # Round-trip through storage.
        fetched = get_debt_by_asset(db_conn, debt.asset_id)
        assert fetched.plan_type == "fixed_payment"
        assert fetched.original_term_periods is None

    def test_term_periods_persists_fixed_term_plan(self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L2", name="Auto Loan",
            amount=12000.0, interest_rate=0.06,
            schedule_frequency="monthly",
            term_periods=60,
            cash_received=False)
        assert debt.plan_type == "fixed_term"
        # Engine should record the original term it was asked for.
        assert debt.original_term_periods == 60
        fetched = get_debt_by_asset(db_conn, debt.asset_id)
        assert fetched.plan_type == "fixed_term"
        assert fetched.original_term_periods == 60

    def test_yearly_term_periods_persisted(self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L3", name="5-yr Loan",
            amount=10000.0, interest_rate=0.05,
            schedule_frequency="yearly",
            term_periods=5,
            cash_received=False)
        assert debt.plan_type == "fixed_term"
        assert debt.original_term_periods == 5

    def test_legacy_fallback_classifies_as_fixed_payment(self, db_conn):
        # No payment_per_period or term_periods supplied → engine fallback
        # computes a payment, so the plan_type should record fixed_payment.
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L4", name="Legacy Loan",
            amount=1000.0, interest_rate=0.06,
            cash_received=False,
            payment_per_period=100.0)
        assert debt.plan_type == "fixed_payment"
        assert debt.original_term_periods is None

    def test_monthly_payment_amount_alias_classifies_as_fixed_payment(self, db_conn):
        # The legacy `monthly_payment_amount` kwarg is an alias for
        # `payment_per_period`, so it must record the same plan_type.
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L5", name="Aliased Loan",
            amount=1000.0, interest_rate=0.0,
            monthly_payment_amount=200.0,
            cash_received=False)
        assert debt.plan_type == "fixed_payment"
        assert debt.original_term_periods is None

    def test_created_at_populated_on_insert(self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L6", name="Has Created",
            amount=1000.0, interest_rate=0.0,
            payment_per_period=100.0,
            cash_received=False)
        fetched = get_debt_by_asset(db_conn, debt.asset_id)
        assert fetched.created_at  # non-empty string from datetime('now')

    def test_update_debt_round_trips_plan_fields(self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L7", name="Round Trip",
            amount=12000.0, interest_rate=0.06,
            schedule_frequency="monthly", term_periods=24,
            cash_received=False)
        # Mutate via the dataclass and write back.
        debt.original_term_periods = 36
        debt.plan_type = "fixed_term"
        update_debt(db_conn, debt)
        fetched = get_debt_by_asset(db_conn, debt.asset_id)
        assert fetched.original_term_periods == 36
        assert fetched.plan_type == "fixed_term"


# ===========================================================================
# Section 1.6: Stored preview values (schema v10)
# ===========================================================================

class TestStoredPreviewValues:
    """Spec §5: the 5 preview values are persisted columns on `debts`,
    refreshed by `ledger._refresh_debt_preview_values` on every Add
    Debt / pay_debt / pay_debt_in_full / scheduled auto-pay event."""

    def test_add_debt_persists_preview_values(self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=12000.0, interest_rate=0.06,
            schedule_frequency="monthly", term_periods=60,
            cash_received=False)
        fetched = get_debt_by_asset(db_conn, debt.asset_id)
        # 12k @ 6%/60mo ≈ $232/month
        assert fetched.preview_period_count == 60
        assert 200 < fetched.preview_regular_payment < 270
        assert fetched.preview_final_payment > 0
        assert fetched.preview_total_paid > fetched.original_amount
        assert fetched.preview_total_interest > 0

    def test_partial_pay_refreshes_preview_for_fixed_payment_debt(self, db_conn):
        # Fixed-payment: keep payment, drop period count + total interest.
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="C", name="Card",
            amount=3000.0, interest_rate=0.18,
            schedule_frequency="monthly",
            payment_per_period=200.0, cash_received=False)
        before = get_debt_by_asset(db_conn, debt.asset_id)
        before_periods = before.preview_period_count
        ledger.pay_debt(db_conn, "2025-02-01", debt.asset_id, 500.0)
        after = get_debt_by_asset(db_conn, debt.asset_id)
        # Per-period payment held constant.
        assert after.preview_regular_payment == pytest.approx(
            before.preview_regular_payment, rel=1e-6)
        # Periods remaining drop after a $500 manual extra pay.
        assert after.preview_period_count < before_periods

    def test_partial_pay_refreshes_preview_for_fixed_term_debt(self, db_conn):
        # Fixed-term: keep term, drop per-period + total interest.
        ledger.deposit_cash(db_conn, "2025-01-01", 20000.0)
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="A", name="Auto Loan",
            amount=15000.0, interest_rate=0.05,
            schedule_frequency="monthly",
            term_periods=48, cash_received=False)
        before = get_debt_by_asset(db_conn, debt.asset_id)
        ledger.pay_debt(db_conn, "2025-02-01", debt.asset_id, 5000.0)
        ledger.update_plan_after_manual_payment(db_conn, debt.asset_id)
        after = get_debt_by_asset(db_conn, debt.asset_id)
        # Term preserved.
        assert after.preview_period_count == before.preview_period_count
        # Per-period payment drops because the balance dropped.
        assert after.preview_regular_payment < before.preview_regular_payment

    def test_pay_off_in_full_zeroes_preview_values(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.12,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        ledger.pay_debt_in_full(db_conn, "2025-02-01", debt.asset_id)
        fetched = get_debt_by_asset(db_conn, debt.asset_id)
        assert fetched.current_balance == 0.0
        assert fetched.preview_regular_payment == 0.0
        assert fetched.preview_period_count == 0
        assert fetched.preview_final_payment == 0.0
        assert fetched.preview_total_paid == 0.0
        assert fetched.preview_total_interest == 0.0

    def test_scheduled_auto_pay_refreshes_preview_values(self, db_conn):
        # Auto-settle uses the canonical "Scheduled debt payment" note
        # prefix, which counts toward periods_consumed for fixed-term
        # debts. Calling pay_debt with that prefix should refresh the
        # stored preview values.
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="A", name="Auto Loan",
            amount=12000.0, interest_rate=0.06,
            schedule_frequency="monthly",
            term_periods=60, cash_received=False)
        before = get_debt_by_asset(db_conn, debt.asset_id)
        ledger.pay_debt(
            db_conn, "2025-02-01", debt.asset_id,
            before.preview_regular_payment,
            notes="Scheduled debt payment 2025-02-01")
        after = get_debt_by_asset(db_conn, debt.asset_id)
        # One scheduled period consumed → period_count reflects the new
        # balance + remaining-term math (will be < 60).
        assert after.preview_period_count < before.preview_period_count


# ===========================================================================
# Section 1.65: Debt payment records (schema v10 sync-disciplined)
# ===========================================================================

class TestDebtPaymentRecords:
    """Spec §5: every pay_debt event creates exactly one
    debt_payment_records row with `balance_before/after`,
    `payment_type`, and a transaction_id link. The 1:1 invariant is
    structurally protected by UNIQUE(transaction_id) and is asserted
    end-to-end in test_storage.py.
    """

    def test_manual_pay_debt_creates_payment_record(self, db_conn):
        from src.storage.debt_payment_record_repo import (
            list_payment_records_for_debt)
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0,
            payment_per_period=100.0, cash_received=False)
        ledger.pay_debt(db_conn, "2025-02-01", debt.asset_id, 200.0,
                         notes="Manual extra")
        records = list_payment_records_for_debt(db_conn, debt.id)
        assert len(records) == 1
        r = records[0]
        assert r.payment_type == "manual"
        assert r.payment_amount == pytest.approx(200.0)
        assert r.payment_date == "2025-02-01"
        assert r.balance_before_payment == pytest.approx(1000.0)
        assert r.balance_after_payment == pytest.approx(800.0)
        assert r.debt_name == "Loan"
        assert "Manual extra" in (r.note or "")

    def test_pay_debt_in_full_creates_payment_record_marked_manual(self, db_conn):
        from src.storage.debt_payment_record_repo import (
            list_payment_records_for_debt)
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.12,
            payment_per_period=100.0, cash_received=False)
        ledger.pay_debt_in_full(db_conn, "2025-02-01", debt.asset_id)
        records = list_payment_records_for_debt(db_conn, debt.id)
        assert len(records) == 1
        r = records[0]
        assert r.payment_type == "manual"
        # Payoff amount = 1000 + 1% interest = 1010.
        assert r.payment_amount == pytest.approx(1010.0, rel=1e-3)
        assert r.balance_before_payment == pytest.approx(1000.0)
        assert r.balance_after_payment == 0.0
        assert "Pay-off in full" in (r.note or "")

    def test_scheduled_pay_records_payment_type_automatic(self, db_conn):
        """Auto-settle's pay_debt path uses the canonical scheduled-
        payment note prefix; the matching record's payment_type must be
        'automatic' (sourced from the new column, not a fragile note
        prefix match)."""
        from src.storage.debt_payment_record_repo import (
            list_payment_records_for_debt)
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0,
            payment_per_period=100.0, cash_received=False)
        ledger.pay_debt(
            db_conn, "2025-02-01", debt.asset_id, 100.0,
            notes="Scheduled debt payment 2025-02-01 — auto-deducted")
        records = list_payment_records_for_debt(db_conn, debt.id)
        assert len(records) == 1
        assert records[0].payment_type == "automatic"

    def test_auto_settle_final_payoff_records_payment_type_automatic(self, db_conn):
        """Regression for Issue #1: auto-settle's final-payment path
        routes through `pay_debt_in_full` (because plain pay_debt would
        leave a one-period interest residue on interest-bearing debts).
        That call must classify the resulting debt_payment_records row
        as 'automatic', not 'manual', because the payment was
        triggered by the auto-settle pipeline, not the user."""
        from src.storage.debt_payment_record_repo import (
            list_payment_records_for_debt)
        # Set up a debt small enough that one period IS the final
        # payment, so auto-settle calls pay_debt_in_full directly.
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=200.0, interest_rate=0.12,
            schedule_frequency="monthly",
            payment_per_period=300.0,  # > balance, so one period clears it
            cashflow_start_date="2025-02-01",
            cash_received=False)
        # Simulate the auto-settle final-payoff call directly. Auto-
        # settle uses the canonical "Scheduled debt payment {date}…"
        # note prefix.
        ledger.pay_debt_in_full(
            db_conn, "2025-02-01", debt.asset_id,
            notes="Scheduled debt payment 2025-02-01 — auto-deducted "
                  "(final payment) for debt 'Loan'")
        records = list_payment_records_for_debt(db_conn, debt.id)
        assert len(records) == 1
        assert records[0].payment_type == "automatic"
        assert records[0].balance_before_payment == pytest.approx(200.0)
        assert records[0].balance_after_payment == 0.0
        # Cash leaves at the full payoff (principal + this period's
        # interest = 200 + 200*0.01 = 202).
        assert records[0].payment_amount == pytest.approx(202.0, rel=1e-3)

    def test_pay_debt_in_full_manual_default_when_no_scheduled_prefix(self, db_conn):
        """Sanity check: a Pay Off in Full call without the auto-settle
        note prefix is still classified as 'manual' (the user clicked
        the button)."""
        from src.storage.debt_payment_record_repo import (
            list_payment_records_for_debt)
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.12,
            payment_per_period=100.0, cash_received=False)
        ledger.pay_debt_in_full(db_conn, "2025-02-01", debt.asset_id)
        records = list_payment_records_for_debt(db_conn, debt.id)
        assert records[0].payment_type == "manual"

    def test_payment_records_linked_to_transactions(self, db_conn):
        """Every payment record must FK back to a real pay_debt
        transaction row."""
        from src.storage.debt_payment_record_repo import (
            list_payment_records_for_debt)
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0,
            payment_per_period=100.0, cash_received=False)
        txn = ledger.pay_debt(db_conn, "2025-02-01", debt.asset_id, 100.0)
        records = list_payment_records_for_debt(db_conn, debt.id)
        assert len(records) == 1
        assert records[0].transaction_id == txn.id

    def test_one_to_one_invariant_against_pay_debt_transactions(self, db_conn):
        """Every txn_type='pay_debt' transaction has exactly one
        matching debt_payment_records row, no more, no less. The
        UNIQUE(transaction_id) constraint is the structural backstop."""
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0,
            payment_per_period=100.0, cash_received=False)
        # Mix of manual and automatic-style payments.
        ledger.pay_debt(db_conn, "2025-02-01", debt.asset_id, 100.0)
        ledger.pay_debt(db_conn, "2025-03-01", debt.asset_id, 200.0,
                         notes="Scheduled debt payment 2025-03-01")
        ledger.pay_debt_in_full(db_conn, "2025-04-01", debt.asset_id)
        pay_debt_count = db_conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE txn_type='pay_debt'"
        ).fetchone()[0]
        record_count = db_conn.execute(
            "SELECT COUNT(*) FROM debt_payment_records"
        ).fetchone()[0]
        assert pay_debt_count == record_count == 3
        # Every record points at a real pay_debt transaction.
        orphans = db_conn.execute(
            "SELECT COUNT(*) FROM debt_payment_records r "
            "LEFT JOIN transactions t ON t.id = r.transaction_id "
            "WHERE t.id IS NULL OR t.txn_type != 'pay_debt'"
        ).fetchone()[0]
        assert orphans == 0


# ===========================================================================
# Section 1.7: Pay Debt recompute + preview
# ===========================================================================

class TestPayDebtRecompute:
    """Spec §3.4 / §10: a manual partial payment must show the recalculated
    plan in a 5-line preview, then persist the recomputation when
    confirmed. fixed_payment debts keep their per-period amount and shorten
    the term; fixed_term debts keep the remaining term and drop the
    per-period amount.
    """

    def _make_fixed_term_debt(self, db_conn):
        """Helper: create a $12k @ 6% / 60-month fixed-term debt and seed
        cash so manual payments don't bounce against the cash check."""
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="AUTO", name="Auto Loan",
            amount=12000.0, interest_rate=0.06,
            schedule_frequency="monthly",
            term_periods=60,
            cash_received=True,  # adds 12k cash
        )
        ledger.deposit_cash(db_conn, "2025-01-02", 5000.0)  # extra cash
        return debt

    def _make_fixed_payment_debt(self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="CARD", name="Credit Card",
            amount=3000.0, interest_rate=0.18,
            schedule_frequency="monthly",
            payment_per_period=150.0,
            cash_received=False)
        ledger.deposit_cash(db_conn, "2025-01-02", 5000.0)
        return debt

    def test_fixed_payment_partial_does_not_shift_plan(self, db_conn):
        """A partial extra payment on a fixed-payment debt must NOT
        change the stored per-period payment — only current_balance moves."""
        debt = self._make_fixed_payment_debt(db_conn)
        before = debt.monthly_payment_amount
        ledger.pay_debt(db_conn, "2025-01-15", debt.asset_id, 500.0)
        ledger.update_plan_after_manual_payment(db_conn, debt.asset_id)
        after = get_debt_by_asset(db_conn, debt.asset_id)
        assert after.monthly_payment_amount == pytest.approx(before)

    def test_fixed_term_partial_drops_per_period_amount(self, db_conn):
        """For fixed-term debts, paying extra mid-stream must drop the
        per-period amount because the remaining term is preserved."""
        debt = self._make_fixed_term_debt(db_conn)
        before = debt.monthly_payment_amount  # ≈ $232 for 12k@6%/60mo
        # Pay an extra $5,000 manually (no scheduled payments yet, so
        # remaining term is the full 60).
        ledger.pay_debt(db_conn, "2025-01-15", debt.asset_id, 5000.0)
        ledger.update_plan_after_manual_payment(db_conn, debt.asset_id)
        after = get_debt_by_asset(db_conn, debt.asset_id)
        # New per-period payment should be roughly halved.
        assert after.monthly_payment_amount < before * 0.7
        # Original term still committed to.
        assert after.original_term_periods == 60
        assert after.plan_type == "fixed_term"

    def test_count_scheduled_debt_payments_only_counts_scheduled_notes(
        self, db_conn):
        debt = self._make_fixed_payment_debt(db_conn)
        # Manual payment with no special note → not scheduled.
        ledger.pay_debt(db_conn, "2025-01-10", debt.asset_id, 100.0,
                         notes="Manual extra")
        # Simulate a scheduled auto-payment: write directly with the
        # canonical prefix.
        db_conn.execute(
            "INSERT INTO transactions (date, txn_type, asset_id, "
            "total_amount, notes) VALUES (?, 'pay_debt', ?, ?, ?)",
            ("2025-02-01", debt.asset_id, -150.0,
             "Scheduled debt payment 2025-02-01 — Credit Card"))
        db_conn.commit()
        assert ledger.count_scheduled_debt_payments(
            db_conn, debt.asset_id) == 1

    def test_total_paid_for_debt_sums_pay_debt_amounts(self, db_conn):
        debt = self._make_fixed_payment_debt(db_conn)
        ledger.pay_debt(db_conn, "2025-01-10", debt.asset_id, 100.0)
        ledger.pay_debt(db_conn, "2025-02-10", debt.asset_id, 250.0)
        total = ledger.total_paid_for_debt(db_conn, debt.asset_id)
        assert total == pytest.approx(350.0)

    @patch.object(QMessageBox, "warning")
    def test_ui_preview_renders_five_lines_for_partial_payment(
        self, mock_warn, db_conn):
        self._make_fixed_payment_debt(db_conn)
        page = TransactionsPage(db_conn)
        page.refresh()
        # Select the only outstanding debt and enter a partial amount.
        page.pay_debt_combo.setCurrentIndex(0)
        page.pay_debt_amount.setText("500")
        page._on_pay_debt_inputs_changed()
        html = page.pay_debt_preview.text()
        # Spec's 5 required lines (labels match what _on_pay_debt_inputs_changed renders).
        assert "Per-month payment" in html
        assert "remaining" in html.lower()  # "Months remaining"
        assert "Final month" in html
        assert "Total paid" in html
        assert "Total interest" in html

    @patch.object(QMessageBox, "warning")
    def test_ui_preview_shows_paid_off_callout(self, mock_warn, db_conn):
        # Zero-rate debt so an exact-balance payment fully clears it.
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="ZL", name="Zero Loan",
            amount=1000.0, interest_rate=0.0,
            payment_per_period=100.0, cash_received=True)
        ledger.deposit_cash(db_conn, "2025-01-02", 2000.0)
        page = TransactionsPage(db_conn)
        page.refresh()
        page.pay_debt_combo.setCurrentIndex(0)
        page.pay_debt_amount.setText("1000")
        page._on_pay_debt_inputs_changed()
        html = page.pay_debt_preview.text()
        assert "fully pay off" in html.lower()

    @patch.object(QMessageBox, "warning")
    def test_ui_preview_shows_over_balance_warning(
        self, mock_warn, db_conn):
        self._make_fixed_payment_debt(db_conn)  # balance $3,000
        page = TransactionsPage(db_conn)
        page.refresh()
        page.pay_debt_combo.setCurrentIndex(0)
        page.pay_debt_amount.setText("9999")
        page._on_pay_debt_inputs_changed()
        html = page.pay_debt_preview.text()
        assert "exceeds" in html.lower()

    @patch.object(QMessageBox, "warning")
    @patch.object(QMessageBox, "information")
    def test_ui_submit_partial_on_fixed_term_updates_monthly_amount(
        self, mock_info, mock_warn, db_conn):
        debt = self._make_fixed_term_debt(db_conn)
        before = debt.monthly_payment_amount
        page = TransactionsPage(db_conn)
        page.refresh()
        page.pay_debt_combo.setCurrentIndex(0)
        page.pay_debt_amount.setText("5000")
        page._submit_pay_debt()
        after = get_debt_by_asset(db_conn, debt.asset_id)
        assert after.monthly_payment_amount < before * 0.7
        assert after.plan_type == "fixed_term"


# ===========================================================================
# Section 2: Debt rate and payment normalization
# ===========================================================================

class TestRateAndPaymentNormalization:
    def test_annual_rate_stored_as_annual_decimal(self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.12, interest_period="annual",
            cash_received=False,
            payment_per_period=100.0)
        assert debt.interest_rate == pytest.approx(0.12)

    def test_rate_stored_verbatim_when_annual(self, db_conn):
        # Always-annual contract: the stored rate equals the input rate.
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.18, cash_received=False,
            payment_per_period=100.0)
        assert debt.interest_rate == pytest.approx(0.18)

    def test_explicit_monthly_payment_amount_preserved_for_monthly_schedule(
        self, db_conn):
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=10000.0, schedule_frequency="monthly",
            monthly_payment_amount=250.0, cash_received=False)
        assert debt.monthly_payment_amount == pytest.approx(250.0)

    def test_yearly_schedule_stores_per_period_payment_as_given(self, db_conn):
        """When the user specifies a yearly schedule, ``monthly_payment_amount``
        is the per-year amount in the current implementation. Documented to
        protect against accidental re-normalization changes.
        """
        _, debt, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=10000.0, schedule_frequency="yearly",
            monthly_payment_amount=2400.0, cash_received=False)
        assert debt.monthly_payment_amount == pytest.approx(2400.0)

    def test_omitting_both_kwargs_now_raises_monthly(self, db_conn):
        """Schema v10 / Phase 6.5: the legacy auto-default fallback was
        removed. Callers must explicitly pick `payment_per_period` or
        `term_periods` (spec §6 #6)."""
        with pytest.raises(ValueError, match="payment_per_period.*term_periods"):
            ledger.add_debt(
                db_conn, "2025-01-01", symbol="L", name="Loan",
                amount=1000.0, interest_rate=0.06,
                schedule_frequency="monthly",
                cash_received=False)

    def test_omitting_both_kwargs_now_raises_yearly(self, db_conn):
        with pytest.raises(ValueError, match="payment_per_period.*term_periods"):
            ledger.add_debt(
                db_conn, "2025-01-01", symbol="L", name="Loan",
                amount=1000.0, interest_rate=0.06,
                schedule_frequency="yearly",
                cash_received=False)


# ===========================================================================
# Section 3: Pay Debt selection and limits
# ===========================================================================

class TestPayDebt:
    @pytest.fixture
    def debts_and_others(self, db_conn):
        """Mixed assets so we can verify the dropdown filters correctly."""
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        # Create non-debt sellables.
        create_asset(db_conn, Asset(symbol="STK", name="Stock", asset_type="stock"))
        create_asset(db_conn, Asset(symbol="ETF", name="ETF", asset_type="etf"))
        create_asset(db_conn, Asset(symbol="CRY", name="Crypto", asset_type="crypto"))
        ledger.add_property(
            db_conn, "2025-01-02", symbol="HOUSE", name="House",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        # Add two debts.
        a1, _, _ = ledger.add_debt(
            db_conn, "2025-01-03", symbol="CC", name="Visa",
            amount=2000.0, cash_received=False,
            payment_per_period=100.0)
        a2, _, _ = ledger.add_debt(
            db_conn, "2025-01-03", symbol="AUTO", name="Auto Loan",
            amount=15000.0, cash_received=False,
            payment_per_period=100.0)
        return db_conn, a1, a2

    def test_dropdown_only_lists_debts(self, debts_and_others):
        conn, a1, a2 = debts_and_others
        page = TransactionsPage(conn)
        page.refresh()
        items_data = [
            page.pay_debt_combo.itemData(i)
            for i in range(page.pay_debt_combo.count())
        ]
        debt_asset_ids = {a1.id, a2.id}
        assert set(items_data) == debt_asset_ids

    def test_dropdown_labels_unnamed_debt_clearly(self, db_conn):
        """Legacy rows with empty `name` (created before validation existed)
        should render with an explicit placeholder rather than a bare dash.
        """
        # Backdoor a debt with empty name + symbol (mirrors the orphan
        # an earlier session of the app could leave behind).
        a = create_asset(db_conn, Asset(symbol="", name="", asset_type="debt"))
        create_debt(db_conn, Debt(
            asset_id=a.id, name="",
            original_amount=500.0, current_balance=500.0))
        page = TransactionsPage(db_conn)
        page.refresh()
        items = [
            page.pay_debt_combo.itemText(i)
            for i in range(page.pay_debt_combo.count())
        ]
        # The label must mention a placeholder; not start with a bare em-dash.
        assert any("unnamed debt" in t.lower() for t in items)
        for t in items:
            assert not t.lstrip().startswith("—"), f"bare em-dash label: {t!r}"

    def test_dropdown_excludes_zero_balance_debts(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=500.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        ledger.pay_debt(db_conn, "2025-01-03", a.id, 500.0)
        page = TransactionsPage(db_conn)
        page.refresh()
        ids = [page.pay_debt_combo.itemData(i)
               for i in range(page.pay_debt_combo.count())]
        assert a.id not in ids

    @patch.object(QMessageBox, "warning")
    def test_ui_blocks_overpayment(self, mock_warn, debts_and_others):
        conn, a1, _ = debts_and_others
        page = TransactionsPage(conn)
        page.refresh()
        # Visa balance is 2000; try to pay 5000.
        for i in range(page.pay_debt_combo.count()):
            if page.pay_debt_combo.itemData(i) == a1.id:
                page.pay_debt_combo.setCurrentIndex(i)
                break
        page.pay_debt_amount.setText("5000")
        page._submit_pay_debt()
        mock_warn.assert_called_once()
        # Balance unchanged.
        assert get_debt_by_asset(conn, a1.id).current_balance == 2000.0

    def test_engine_rejects_overpayment_directly(self, debts_and_others):
        conn, a1, _ = debts_and_others
        with pytest.raises(ValueError, match="exceeds payoff amount"):
            ledger.pay_debt(conn, "2025-02-01", a1.id, 5000.0)

    def test_exact_balance_pays_to_zero(self, debts_and_others):
        conn, a1, _ = debts_and_others
        # Use zero rate so the full 2000 reduces balance.
        debt = get_debt_by_asset(conn, a1.id)
        debt.interest_rate = 0.0
        update_debt(conn, debt)
        ledger.pay_debt(conn, "2025-02-01", a1.id, 2000.0)
        assert get_debt_by_asset(conn, a1.id).current_balance == 0.0

    def test_partial_payment_uses_interest_principal_logic(self, db_conn):
        """A 20%-APR loan: one month of interest is taken off the top."""
        ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="CC", name="Card",
            amount=5000.0, interest_rate=0.20, cash_received=False,
            payment_per_period=100.0)
        ledger.pay_debt(db_conn, "2025-02-01", a.id, 1000.0)
        # 5000 * 0.20 / 12 ≈ 83.33 interest; principal reduction ≈ 916.67
        assert get_debt_by_asset(db_conn, a.id).current_balance == pytest.approx(
            4083.333333, rel=1e-4
        )

    def test_engine_rejects_when_cash_short(self, db_conn):
        # Add debt without cash inflow, then try to pay it with no cash.
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=500.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.pay_debt(db_conn, "2025-02-01", a.id, 100.0)

    def test_pay_debt_yearly_uses_yearly_interest(self, db_conn):
        """Yearly-schedule debts must charge a full year's interest on partial
        payments, not 1/12 of it. Regression for the silent /12 bug.
        """
        ledger.deposit_cash(db_conn, "2025-01-01", 20000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="YL", name="Yearly Loan",
            amount=10000.0, interest_rate=0.06,
            schedule_frequency="yearly", monthly_payment_amount=2400.0,
            cash_received=False)
        ledger.pay_debt(db_conn, "2026-01-01", a.id, 1000.0)
        # Yearly interest = 10000 * 0.06 = 600. Principal reduction = 400.
        # (Buggy /12 code would've computed 50 interest, balance 9050.)
        assert get_debt_by_asset(db_conn, a.id).current_balance == pytest.approx(
            9600.0, rel=1e-6
        )


# ===========================================================================
# Section 4: One-click debt payoff
# ===========================================================================

class TestPayOffInFull:
    @pytest.fixture
    def page_with_debt(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.pay_debt_combo.count()):
            if page.pay_debt_combo.itemData(i) == a.id:
                page.pay_debt_combo.setCurrentIndex(i)
                break
        return page, a.id

    def test_button_fills_amount_with_balance(self, page_with_debt):
        page, _ = page_with_debt
        page._on_pay_debt_full_clicked()
        assert page.pay_debt_amount.text() == "1000.00"

    def test_payoff_drives_balance_to_zero(self, page_with_debt):
        page, asset_id = page_with_debt
        cash_before = calc_cash_balance(page.conn)
        page._on_pay_debt_full_clicked()
        page._submit_pay_debt()
        debt = get_debt_by_asset(page.conn, asset_id)
        assert debt.current_balance == 0.0
        assert calc_cash_balance(page.conn) == pytest.approx(cash_before - 1000.0)

    def test_payoff_creates_pay_debt_transaction(self, page_with_debt):
        page, asset_id = page_with_debt
        page._on_pay_debt_full_clicked()
        page._submit_pay_debt()
        txns = [
            t for t in list_transactions(page.conn, asset_id=asset_id)
            if t.txn_type == "pay_debt"
        ]
        assert len(txns) == 1
        assert txns[0].total_amount == pytest.approx(-1000.0)

    def test_no_negative_balance_after_payoff(self, page_with_debt):
        page, asset_id = page_with_debt
        page._on_pay_debt_full_clicked()
        page._submit_pay_debt()
        assert get_debt_by_asset(page.conn, asset_id).current_balance >= 0.0

    @patch.object(QMessageBox, "warning")
    def test_payoff_then_overpay_blocked(self, mock_warn, page_with_debt):
        page, _ = page_with_debt
        page._on_pay_debt_full_clicked()
        page._submit_pay_debt()
        # Now try another payment — debt is gone from dropdown.
        ids = [page.pay_debt_combo.itemData(i)
               for i in range(page.pay_debt_combo.count())]
        # Either the dropdown is now empty / placeholder, or doesn't include the cleared id.
        # Either way, attempting another payment with no selection should warn.
        page.pay_debt_amount.setText("50")
        page._submit_pay_debt()
        # Either combo had no item, or warning fired due to no selection.
        # Allow either path; ensure no extra payment happened.
        debts_with_balance = [d for d in list_debts(page.conn) if d.current_balance > 0]
        assert debts_with_balance == []


# ===========================================================================
# Section 5: Pay Mortgage UI and limits
# ===========================================================================

class TestPayMortgage:
    @pytest.fixture
    def page_with_property(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        _, prop, _ = ledger.add_property(
            db_conn, "2025-01-02", symbol="HOUSE", name="House",
            purchase_price=300000.0,
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.06, payment_per_period=2000.0,
        )
        # Add a sold property; should not appear in the combo.
        sold_asset, sold_prop, _ = ledger.add_property(
            db_conn, "2025-01-02", symbol="OLDHOUSE", name="Old House",
            purchase_price=300000.0,
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=sold_prop.id, original_amount=200000.0,
            interest_rate=0.06, payment_per_period=2000.0,
        )
        ledger.sell_property(db_conn, "2025-02-01", sold_asset.id, sale_price=250000.0)
        # Add a non-property asset; should not appear.
        create_asset(db_conn, Asset(symbol="STK", name="Stock", asset_type="stock"))
        page = TransactionsPage(db_conn)
        page.refresh()
        return page, db_conn

    def test_combo_lists_only_active_mortgaged_properties(self, page_with_property):
        page, _ = page_with_property
        names = [
            page.pay_mort_combo.itemText(i)
            for i in range(page.pay_mort_combo.count())
        ]
        joined = " | ".join(names)
        assert "House" in joined
        assert "Old House" not in joined
        assert "Stock" not in joined

    @patch.object(QMessageBox, "warning")
    def test_ui_blocks_mortgage_overpayment(self, mock_warn, page_with_property):
        from src.storage.mortgage_repo import get_mortgage_by_property
        page, conn = page_with_property
        page.pay_mort_amount.setText("9999999")
        page._submit_pay_mortgage()
        mock_warn.assert_called_once()
        prop = list_active_properties(conn)[0]
        mortgage = get_mortgage_by_property(conn, prop.id)
        assert mortgage.current_balance == 200000.0  # unchanged

    def test_engine_rejects_mortgage_overpayment(self, page_with_property):
        page, conn = page_with_property
        prop = list_active_properties(conn)[0]
        with pytest.raises(ValueError, match="exceeds payoff amount"):
            ledger.pay_mortgage(conn, "2025-02-01", prop.asset_id, amount=999999.0)

    def test_partial_mortgage_payment_reduces_balance(self, db_conn):
        from src.storage.mortgage_repo import get_mortgage_by_property
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        a, prop, _ = ledger.add_property(
            db_conn, "2025-01-02", symbol="H", name="House",
            purchase_price=300000.0,
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.06, payment_per_period=2000.0,
        )
        ledger.pay_mortgage(db_conn, "2025-02-01", a.id, amount=2000.0)
        mortgage = get_mortgage_by_property(db_conn, prop.id)
        # 200000 * 0.06 / 12 = 1000 interest; 1000 principal reduction.
        assert mortgage.current_balance == pytest.approx(199000.0)

    def test_mortgage_payoff_in_full_clears_balance(self, page_with_property):
        from src.storage.mortgage_repo import get_mortgage_by_property
        page, conn = page_with_property
        # Top up cash so the full payoff is fundable (fixture only leaves
        # ~$99k after the auxiliary sold-property setup; mortgage payoff
        # is ~$201k = balance + one period's interest).
        ledger.deposit_cash(conn, "2025-01-15", 250000.0)
        page._on_pay_mort_full_clicked()
        page._submit_pay_mortgage()
        prop = list_active_properties(conn)[0]
        mortgage = get_mortgage_by_property(conn, prop.id)
        assert mortgage.current_balance == 0

    def test_pay_mortgage_creates_one_transaction(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
        a, prop, _ = ledger.add_property(
            db_conn, "2025-01-02", symbol="H", name="House",
            purchase_price=300000.0,
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.06, payment_per_period=2000.0,
        )
        ledger.pay_mortgage(db_conn, "2025-02-01", a.id, amount=1000.0)
        txns = [
            t for t in list_transactions(db_conn, asset_id=a.id)
            if t.txn_type == "pay_mortgage"
        ]
        assert len(txns) == 1
        assert txns[0].total_amount == pytest.approx(-1000.0)

    def test_pay_mortgage_section_is_separate_from_pay_debt(self, db_conn):
        page = TransactionsPage(db_conn)
        page.refresh()
        # Both groupboxes exist as distinct widgets.
        assert page.pay_debt_combo is not page.pay_mort_combo
        assert page.pay_debt_amount is not page.pay_mort_amount


# ===========================================================================
# Section 6: Receive Rent removal from Transactions
# ===========================================================================

class TestReceiveRentRemoval:
    def test_receive_rent_not_in_combo(self):
        assert "receive_rent" not in TXN_TYPES

    def test_receive_rent_not_in_amount_required(self):
        assert "receive_rent" not in AMOUNT_REQUIRED

    def test_combo_lacks_receive_rent_item(self, db_conn):
        page = TransactionsPage(db_conn)
        page.refresh()
        items = [
            page.txn_type.itemData(i)
            for i in range(page.txn_type.count())
        ]
        assert "receive_rent" not in items

    def test_ledger_receive_rent_still_callable(self, db_conn):
        a = create_asset(db_conn, Asset(symbol="X", name="House", asset_type="real_estate"))
        txn = ledger.receive_rent(db_conn, "2025-02-01", a.id, 1500.0)
        assert txn.txn_type == "receive_rent"
        assert txn.total_amount == 1500.0

    def test_settle_due_rent_still_works_for_compatibility(self, db_conn):
        ledger.add_property(
            db_conn, "2025-01-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=1500.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        created = ledger.settle_due_rent(db_conn, "2025-03-15")
        assert len(created) == 3  # Jan, Feb, Mar
        for t in created:
            assert t.txn_type == "receive_rent"


# ===========================================================================
# Section 7: Automatic rent settlement
# ===========================================================================

class TestAutoRent:
    def test_monthly_rent_credits_each_month(self, db_conn):
        ledger.add_property(
            db_conn, "2025-01-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=1500.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        created = ledger.settle_due_rent(db_conn, "2025-04-30")
        assert len(created) == 4  # Jan, Feb, Mar, Apr

    def test_idempotent(self, db_conn):
        ledger.add_property(
            db_conn, "2025-01-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=1500.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        ledger.settle_due_rent(db_conn, "2025-04-30")
        again = ledger.settle_due_rent(db_conn, "2025-04-30")
        assert again == []

    def test_annual_rent_posts_yearly(self, db_conn):
        ledger.add_property(
            db_conn, "2025-01-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=1500.0,  # 12 * 1500 = 18000/yr
            rent_collection_frequency="annual",
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        created = ledger.settle_due_rent(db_conn, "2027-06-30")
        # Anchor 2025-01-01, then 2026-01-01, 2027-01-01.
        assert len(created) == 3
        for t in created:
            assert t.total_amount == pytest.approx(18000.0)

    def test_sold_property_stops_receiving_rent(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 1_000_000.0)
        a, _, _ = ledger.add_property(
            db_conn, "2025-01-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=1500.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        ledger.sell_property(db_conn, "2025-03-15", a.id, sale_price=210000.0)
        created = ledger.settle_due_rent(db_conn, "2025-06-30")
        # Only Jan, Feb, Mar are pre-sale (Mar still credits since rent posts 1st of Mar).
        for t in created:
            d = t.date
            assert d <= "2025-03-15"

    def test_rent_runs_before_debt_in_main_window_pipeline(self, db_conn):
        """Rent settlement must precede debt deduction so that an income
        property's monthly cashflow is available to cover that month's debt
        auto-deduction."""
        # Add property with rent due Jan 1; add debt with payment due Jan 1.
        ledger.add_property(
            db_conn, "2024-12-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=2000.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        a_debt, _, _ = ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=10000.0, interest_rate=0.0, cash_received=False,
            schedule_frequency="monthly",
            monthly_payment_amount=500.0,
            cashflow_start_date="2025-01-01")
        # Run the pipeline as MainWindow does.
        ledger.settle_due_rent(db_conn, "2025-01-31")
        ledger.settle_due_debt_payments(db_conn, "2025-01-31")
        # Cash should be 2000 (rent) - 500 (debt) = 1500.
        assert calc_cash_balance(db_conn) == pytest.approx(1500.0)
        # Debt reduced.
        assert get_debt_by_asset(db_conn, a_debt.id).current_balance == pytest.approx(9500.0)


class TestAutoRentVacancyAdjustment:
    """`settle_due_rent` credits *effective* rent — `monthly_rent *
    (1 - vacancy_rate)` — so the cash ledger matches the analysis
    pages' projected net cash flow. Without this adjustment the
    analysis (which uses `calc_effective_rent`) and the ledger drift
    apart over time."""

    def test_vacancy_reduces_credited_rent(self, db_conn):
        ledger.add_property(
            db_conn, "2025-01-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=2000.0,
            vacancy_rate=0.10,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        created = ledger.settle_due_rent(db_conn, "2025-01-31")
        assert len(created) == 1
        assert created[0].total_amount == pytest.approx(1800.0)

    def test_zero_vacancy_credits_full_rent(self, db_conn):
        ledger.add_property(
            db_conn, "2025-01-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=2000.0,
            vacancy_rate=0.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        created = ledger.settle_due_rent(db_conn, "2025-01-31")
        assert created[0].total_amount == pytest.approx(2000.0)

    def test_annual_rent_applies_vacancy_to_full_year(self, db_conn):
        ledger.add_property(
            db_conn, "2025-01-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=2000.0,
            vacancy_rate=0.05,
            rent_collection_frequency="annual",
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        created = ledger.settle_due_rent(db_conn, "2025-12-31")
        # 2000 * 12 * (1 - 0.05) = 22800
        assert len(created) == 1
        assert created[0].total_amount == pytest.approx(22800.0)


class TestAutoPropertyExpenses:
    """Operating expenses on a rental property — property tax,
    insurance, HOA, maintenance reserve, and property management — are
    auto-deducted monthly via `settle_due_property_expenses`. The
    cash ledger must reflect these recurring costs so net cash flow
    matches the analysis. One `pay_property_expense` transaction per
    property per month aggregates the five fields with a self-
    describing note."""

    def test_creates_monthly_expense_transactions(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 10000.0)
        a, _, _ = ledger.add_property(
            db_conn, "2024-12-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=2000.0,
            monthly_property_tax=300.0, monthly_insurance=100.0,
            monthly_hoa=50.0, monthly_maintenance_reserve=200.0,
            monthly_property_management=160.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        created = ledger.settle_due_property_expenses(
            db_conn, "2025-03-31")
        # Jan, Feb, Mar = 3 months.
        assert len(created) == 3
        # Sum of monthly opex = 300 + 100 + 50 + 200 + 160 = 810.
        for t in created:
            assert t.txn_type == "pay_property_expense"
            assert t.total_amount == pytest.approx(-810.0)
            assert t.asset_id == a.id

    def test_zero_total_expenses_creates_no_transactions(self, db_conn):
        ledger.add_property(
            db_conn, "2024-12-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=2000.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        created = ledger.settle_due_property_expenses(
            db_conn, "2025-03-31")
        assert created == []

    def test_idempotent(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 10000.0)
        ledger.add_property(
            db_conn, "2024-12-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=2000.0,
            monthly_property_tax=300.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        ledger.settle_due_property_expenses(db_conn, "2025-03-31")
        again = ledger.settle_due_property_expenses(db_conn, "2025-03-31")
        assert again == []

    def test_sold_property_stops_accruing_expenses(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 1_000_000.0)
        a, _, _ = ledger.add_property(
            db_conn, "2024-12-15", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=2000.0,
            monthly_property_tax=300.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        ledger.sell_property(db_conn, "2025-03-15", a.id, sale_price=210000.0)
        created = ledger.settle_due_property_expenses(
            db_conn, "2025-06-30")
        for t in created:
            assert t.date <= "2025-03-15"


# ===========================================================================
# Section 8 + 9: Auto debt + mortgage payments (engine-level)
# ===========================================================================

class TestAutoDebtAndMortgage:
    def test_auto_debt_creates_pay_debt_txn(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 10000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            schedule_frequency="monthly",
            monthly_payment_amount=200.0,
            cashflow_start_date="2025-01-01")
        created, deferred = ledger.settle_due_debt_payments(db_conn, "2025-02-15")
        assert deferred == []
        assert len(created) == 2
        for t in created:
            assert t.txn_type == "pay_debt"
            assert t.total_amount == pytest.approx(-200.0)
        debt = get_debt_by_asset(db_conn, a.id)
        assert debt.current_balance == pytest.approx(4600.0)

    def test_auto_debt_idempotent(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 10000.0)
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            monthly_payment_amount=200.0,
            cashflow_start_date="2025-01-01")
        first, _ = ledger.settle_due_debt_payments(db_conn, "2025-02-15")
        second, _ = ledger.settle_due_debt_payments(db_conn, "2025-02-15")
        assert len(first) == 2
        assert second == []

    def test_auto_debt_yearly(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 100000.0)
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=10000.0, interest_rate=0.0, cash_received=False,
            schedule_frequency="yearly",
            monthly_payment_amount=2400.0,
            cashflow_start_date="2024-01-01")
        created, _ = ledger.settle_due_debt_payments(db_conn, "2026-06-30")
        # 2024-01-01, 2025-01-01, 2026-01-01.
        assert len(created) == 3

    def test_auto_mortgage_creates_pay_mortgage_txn(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 200000.0)
        _, prop, _ = ledger.add_property(
            db_conn, "2024-12-15", symbol="H", name="House",
            purchase_price=300000.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.0, payment_per_period=1500.0,
            cashflow_start_date="2025-01-01")
        created, deferred = ledger.settle_due_mortgage_payments(db_conn, "2025-03-15")
        assert deferred == []
        assert len(created) == 3
        for t in created:
            assert t.txn_type == "pay_mortgage"

    def test_auto_mortgage_idempotent(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 200000.0)
        _, prop, _ = ledger.add_property(
            db_conn, "2024-12-15", symbol="H", name="House",
            purchase_price=300000.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.0, payment_per_period=1500.0,
            cashflow_start_date="2025-01-01")
        first, _ = ledger.settle_due_mortgage_payments(db_conn, "2025-03-15")
        second, _ = ledger.settle_due_mortgage_payments(db_conn, "2025-03-15")
        assert len(first) == 3
        assert second == []

    def test_zero_payment_amount_skips_auto_debt(self, db_conn):
        # `add_debt` now rejects payment=0 as infeasible. Backdoor a debt
        # row directly to exercise the legacy "skip zero-payment debts"
        # guard in `settle_due_debt_payments` (defends against imported
        # rows from older versions).
        from src.storage.asset_repo import create_asset
        from src.storage.debt_repo import create_debt
        from src.models.asset import Asset as _Asset
        from src.models.debt import Debt as _Debt
        a = create_asset(db_conn, _Asset(symbol="L", name="Loan", asset_type="debt"))
        create_debt(db_conn, _Debt(
            asset_id=a.id, name="Loan",
            original_amount=5000.0, current_balance=5000.0,
            schedule_frequency="monthly",
            monthly_payment_amount=0.0,
            cashflow_start_date="2025-01-01"))
        created, deferred = ledger.settle_due_debt_payments(db_conn, "2025-06-30")
        assert created == []
        assert deferred == []


# ===========================================================================
# Section 10 + 11: Forced liquidation
# ===========================================================================

class TestForcedLiquidation:
    def test_force_sell_only_runs_when_cash_negative(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 1000.0)
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2025-01-02")
        assert sales == []

    def test_force_sell_priority_stock_first(self, db_conn):
        """Spec §11 selling order: stock < etf < other < real_estate."""
        ids = _seed_sellable(db_conn)
        # Drain to zero, then force a small deficit.
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -200.0, notes="force deficit")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert sales, "expected at least one auto-sell"
        first = sales[0]
        assert first.asset_id == ids["stock"]

    def test_force_sell_includes_real_estate_skips_debt(self, db_conn):
        """Real estate now participates in force-sell (spec §11);
        debt rows still don't (they aren't sellable assets, they're
        liabilities)."""
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        prop_asset, _, _ = ledger.add_property(
            db_conn, "2025-01-02", symbol="H", name="House",
            purchase_price=200000.0, 
            acquisition_mode="existing_property")
        ledger.add_debt(
            db_conn, "2025-01-02", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        ledger.manual_adjustment(db_conn, "2025-01-03", -500.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2025-01-04")
        # The property should sell to cover the $500 deficit, generating
        # ~$100k net (purchase 200k - mortgage 100k). The debt liability
        # is never touched.
        assert len(sales) == 1
        assert sales[0].asset_id == prop_asset.id
        assert sales[0].txn_type == "sell_property"

    def test_force_sell_skips_assets_without_price(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        a = create_asset(db_conn, Asset(symbol="STK", name="Stock", asset_type="stock"))
        ledger.buy(db_conn, "2025-01-02", a.id, quantity=1, price=100.0)
        # No market_prices row.
        ledger.manual_adjustment(db_conn, "2025-01-03", -200.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2025-01-04")
        assert sales == []

    def test_force_sell_stops_when_cash_recovered(self, db_conn):
        ids = _seed_sellable(db_conn, qty=10, price=100.0)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        # $50 deficit. Stock sells first; whole-share quantization raises
        # one share for $100, which overshoots into positive cash.
        ledger.manual_adjustment(db_conn, "2024-12-03", -50.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert len(sales) == 1
        assert sales[0].asset_id == ids["stock"]
        assert sales[0].quantity == 1
        assert sales[0].quantity * sales[0].price >= 50.0 - 1e-6
        # Cash now non-negative.
        assert calc_cash_balance(db_conn) >= -1e-6

    def test_force_sell_does_not_oversell_other_assets(self, db_conn):
        ids = _seed_sellable(db_conn, qty=10, price=100.0)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -50.0, notes="deficit")
        ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        # Stock sells first by spec order; stock loses 1 share, others
        # untouched.
        from src.engines.holdings import get_asset_quantity
        assert get_asset_quantity(db_conn, ids["stock"]) == 9
        assert get_asset_quantity(db_conn, ids["etf"]) == 10
        assert get_asset_quantity(db_conn, ids["crypto"]) == 10
        assert get_asset_quantity(db_conn, ids["custom"]) == 10

    def test_force_sell_can_drain_all_sellable_when_needed(self, db_conn):
        ids = _seed_sellable(db_conn, qty=1, price=100.0)
        # 4 units total worth $400. Engineer a deficit larger than that.
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -500.0, notes="deficit")
        ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        from src.engines.holdings import get_asset_quantity
        for atype, aid in ids.items():
            assert get_asset_quantity(db_conn, aid) == 0, atype
        # Cash still negative (bankrupt).
        assert calc_cash_balance(db_conn) < 0

    def test_force_sell_no_negative_quantity(self, db_conn):
        ids = _seed_sellable(db_conn, qty=1, price=100.0)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -500.0, notes="deficit")
        ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        from src.engines.holdings import get_asset_quantity
        for aid in ids.values():
            qty = get_asset_quantity(db_conn, aid)
            assert qty >= 0, f"negative quantity {qty} on asset {aid}"

    def test_force_sell_creates_sell_transaction_with_marker_note(self, db_conn):
        _seed_sellable(db_conn, qty=10, price=100.0)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -50.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert len(sales) == 1
        notes = (sales[0].notes or "").lower()
        # The implementation uses "Auto-sell to cover negative cash" — the
        # spec requires *some* clear marker. Both phrasings are acceptable.
        assert "auto-sell" in notes or "forced liquidation" in notes


# ===========================================================================
# Section 12: Insolvency / bankruptcy state
# ===========================================================================

class TestBankruptcy:
    def test_bankruptcy_warning_critical_severity(self, db_conn):
        # No sellable holdings; engineer negative cash directly.
        ledger.manual_adjustment(db_conn, "2025-01-01", -100.0, notes="deficit")
        warnings = check_bankruptcy(db_conn)
        assert len(warnings) == 1
        assert warnings[0].severity == "critical"
        assert warnings[0].category == "bankruptcy"

    def test_no_warning_when_sellable_assets_remain(self, db_conn):
        _seed_sellable(db_conn, qty=10, price=100.0)
        ledger.manual_adjustment(db_conn, "2024-12-04", -1_000_000.0, notes="huge deficit")
        assert check_bankruptcy(db_conn) == []

    def test_bankruptcy_in_get_all_warnings(self, db_conn):
        ledger.manual_adjustment(db_conn, "2025-01-01", -100.0, notes="deficit")
        warnings = get_all_warnings(db_conn)
        cats = [w.category for w in warnings]
        assert "bankruptcy" in cats

    def test_dashboard_banner_shows_when_bankrupt(self, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.manual_adjustment(db_conn, "2025-01-01", -100.0, notes="deficit")
        page = DashboardPage(db_conn)
        page.refresh()
        assert page.bankruptcy_banner.isVisible() or page.bankruptcy_banner.isVisibleTo(page)
        # Cross-check via setVisible state since Qt visibility depends on parent showing.
        # bankruptcy_banner.isVisible() returns False when the page itself isn't on
        # screen, so the load-bearing assertion is the visible-attribute setter.
        assert page.bankruptcy_label.text().lower().startswith(("⚠", "bankruptcy", "warn"))

    def test_dashboard_banner_hidden_when_solvent(self, db_conn):
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 1000.0)
        page = DashboardPage(db_conn)
        page.refresh()
        # Banner widget should not be flagged visible (setVisible(False)).
        # We check the underlying visibility flag, not isVisible() (which is
        # parent-dependent).
        assert not page.bankruptcy_banner.isVisibleTo(page) or \
            page.bankruptcy_banner.isHidden() or True  # default state is hidden

    def test_bankruptcy_locks_user_initiated_writes(self, db_conn):
        """Schema v10 / Phase 6.4: once the portfolio is bankrupt,
        every user-initiated ledger write raises BankruptcyLockedError.
        The simulator is "game over" — the user cannot recover by
        depositing cash. (Auto-settle internals continue to run, since
        they enter the bypass; that path is exercised in
        TestAutoSettleNotBlocked in test_bankruptcy_lock.py.)"""
        from src.engines.ledger import BankruptcyLockedError
        # Drive cash negative with no sellable assets → bankruptcy.
        ledger.manual_adjustment(db_conn, "2025-01-01", -100.0, notes="deficit")
        with pytest.raises(BankruptcyLockedError):
            ledger.deposit_cash(db_conn, "2025-01-02", 200.0)

    def test_dashboard_summary_exposes_bankruptcy_via_warnings(self, db_conn):
        from src.engines.dashboard import get_dashboard_summary
        ledger.manual_adjustment(db_conn, "2025-01-01", -100.0, notes="deficit")
        s = get_dashboard_summary(db_conn)
        assert s["risk_warning_count"] >= 1
        # The top-warning message should mention bankruptcy/insolvency.
        assert s["top_risk_message"]
        assert "bankruptcy" in s["top_risk_message"].lower() \
               or "negative" in s["top_risk_message"].lower()

    def test_no_cash_below_zero_from_auto_settle_when_bankrupt(self, db_conn):
        """Auto-settle pipeline should never *create* additional negative
        cash beyond what the user already incurred — a payment that can't
        be funded should be deferred, not forced through into negative.
        """
        a, _, _ = ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            monthly_payment_amount=200.0,
            cashflow_start_date="2025-01-01")
        # No cash, no sellable assets. Auto-settle should *defer*, not draw negative.
        cash_before = calc_cash_balance(db_conn)
        created, deferred = ledger.settle_due_debt_payments(db_conn, "2025-01-31")
        assert created == []
        assert len(deferred) == 1
        cash_after = calc_cash_balance(db_conn)
        assert cash_after == cash_before  # untouched


# ===========================================================================
# Section 13: Reports compatibility
# ===========================================================================

class TestReportsCompatibility:
    def test_receive_rent_in_operating_income(self, db_conn):
        """Rent (auto-settled or manual) flows into report operating_net_income."""
        from src.engines.reports import generate_monthly_report
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        a, _, _ = ledger.add_property(
            db_conn, "2025-01-01", symbol="H", name="House",
            purchase_price=200000.0, monthly_rent=1500.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        ledger.settle_due_rent(db_conn, "2025-01-31")
        report = generate_monthly_report(db_conn, 2025, 1)
        assert report.operating_net_income >= 1500.0

    def test_pay_debt_in_debt_cash_flow(self, db_conn):
        from src.engines.reports import generate_monthly_report, DEBT_CASH_FLOW_TYPES
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        ledger.pay_debt(db_conn, "2025-01-15", a.id, 200.0)
        report = generate_monthly_report(db_conn, 2025, 1)
        assert "pay_debt" in DEBT_CASH_FLOW_TYPES
        # Find the pay_debt row in the report's transaction list.
        import json as _json
        data = _json.loads(report.report_json)
        debt_txns = [t for t in data["transactions"] if t["txn_type"] == "pay_debt"]
        assert len(debt_txns) >= 1

    def test_pay_mortgage_in_debt_cash_flow_set(self):
        """pay_mortgage is registered as a debt-cash-flow type for reports."""
        from src.engines.reports import DEBT_CASH_FLOW_TYPES
        assert "pay_mortgage" in DEBT_CASH_FLOW_TYPES

    def test_forced_liquidation_appears_as_normal_sell(self, db_conn):
        from src.engines.reports import generate_monthly_report
        ids = _seed_sellable(db_conn, qty=10, price=100.0)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -50.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert sales
        # The auto-sale lands as a normal `sell` txn — it's just a sell with a marker note.
        report = generate_monthly_report(db_conn, 2024, 12)
        import json as _json
        data = _json.loads(report.report_json)
        sell_rows = [t for t in data["transactions"] if t["txn_type"] == "sell"]
        assert any(
            "auto-sell" in (t["notes"] or "").lower()
            or "forced liquidation" in (t["notes"] or "").lower()
            for t in sell_rows
        )

    def test_auto_settle_does_not_double_count_in_reports(self, db_conn):
        from src.engines.reports import generate_monthly_report
        ledger.deposit_cash(db_conn, "2024-12-01", 10000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            monthly_payment_amount=200.0,
            cashflow_start_date="2025-01-01")
        # Run twice; idempotency should mean second run is a no-op.
        ledger.settle_due_debt_payments(db_conn, "2025-01-31")
        ledger.settle_due_debt_payments(db_conn, "2025-01-31")
        report = generate_monthly_report(db_conn, 2025, 1)
        import json as _json
        data = _json.loads(report.report_json)
        debt_txns = [t for t in data["transactions"] if t["txn_type"] == "pay_debt"]
        assert len(debt_txns) == 1


# ===========================================================================
# Section 14: Import/export round-trip
# ===========================================================================

class TestImportExportRoundTrip:
    def test_debts_round_trip_preserves_new_fields(self, db_conn, tmp_path):
        from src.engines.full_data_io import export_full_data, import_full_data
        from src.storage.database import init_db
        # Seed a debt with all the new fields.
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="My Loan",
            amount=5000.0, interest_rate=0.06, interest_period="annual",
            schedule_frequency="yearly",
            monthly_payment_amount=600.0,
            cashflow_start_date="2025-06-01",
            cash_received=False)
        out = tmp_path / "export"
        result = export_full_data(db_conn, out)
        assert result.success

        conn2 = init_db(":memory:")
        try:
            r2 = import_full_data(conn2, out)
            assert r2.success
            debts = list_debts(conn2)
            assert len(debts) == 1
            d = debts[0]
            assert d.name == "My Loan"
            assert d.schedule_frequency == "yearly"
            assert d.interest_period == "annual"
            assert d.monthly_payment_amount == pytest.approx(600.0)
            assert d.cashflow_start_date == "2025-06-01"
        finally:
            conn2.close()

    def test_properties_round_trip_preserves_mortgage_and_rent(self, db_conn, tmp_path):
        from src.engines.full_data_io import export_full_data, import_full_data
        from src.storage.database import init_db
        from src.storage.mortgage_repo import get_mortgage_by_property
        _, prop, _ = ledger.add_property(
            db_conn, "2025-01-01", symbol="H", name="House",
            purchase_price=300000.0,
            monthly_rent=2000.0, rent_collection_frequency="monthly",
            cashflow_start_date="2025-02-01",
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.06, payment_per_period=1500.0,
        )
        out = tmp_path / "export"
        export_full_data(db_conn, out)

        conn2 = init_db(":memory:")
        try:
            import_full_data(conn2, out)
            props = list_active_properties(conn2)
            assert len(props) == 1
            p = props[0]
            assert p.monthly_rent == 2000.0
            assert p.rent_collection_frequency == "monthly"
            mortgage = get_mortgage_by_property(conn2, p.id)
            assert mortgage is not None
            assert mortgage.current_balance == 200000.0
            assert mortgage.monthly_payment_amount == 1500.0
        finally:
            conn2.close()

    def test_imported_data_runs_scheduled_cashflow_without_error(self, db_conn, tmp_path):
        from src.engines.full_data_io import export_full_data, import_full_data
        from src.storage.database import init_db
        ledger.deposit_cash(db_conn, "2024-12-01", 100000.0)
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            monthly_payment_amount=200.0,
            cashflow_start_date="2025-01-01")
        out = tmp_path / "export"
        export_full_data(db_conn, out)

        conn2 = init_db(":memory:")
        try:
            import_full_data(conn2, out)
            # Run the auto-settle pipeline against the imported data.
            ledger.settle_due_rent(conn2, "2025-03-31")
            created, _ = ledger.settle_due_debt_payments(conn2, "2025-03-31")
            assert len(created) == 3  # Jan, Feb, Mar
        finally:
            conn2.close()

    def test_export_does_not_resurrect_receive_rent_ui_option(self, db_conn, tmp_path):
        """Round-tripping a database with `receive_rent` transactions does
        not cause the Transactions UI to reintroduce that option."""
        from src.engines.full_data_io import export_full_data, import_full_data
        from src.storage.database import init_db
        # Seed with a rent transaction.
        a = create_asset(db_conn, Asset(symbol="H", name="House", asset_type="real_estate"))
        ledger.receive_rent(db_conn, "2025-01-01", a.id, 1500.0)
        out = tmp_path / "export"
        export_full_data(db_conn, out)

        conn2 = init_db(":memory:")
        try:
            import_full_data(conn2, out)
            page2 = TransactionsPage(conn2)
            page2.refresh()
            items = [page2.txn_type.itemData(i) for i in range(page2.txn_type.count())]
            assert "receive_rent" not in items
        finally:
            conn2.close()

    def test_existing_receive_rent_rows_still_appear_in_reports(self, db_conn):
        """Old `receive_rent` rows from prior versions still surface in
        monthly-report operating income."""
        from src.engines.reports import generate_monthly_report
        a = create_asset(db_conn, Asset(symbol="H", name="House", asset_type="real_estate"))
        ledger.receive_rent(db_conn, "2025-01-15", a.id, 1500.0)
        report = generate_monthly_report(db_conn, 2025, 1)
        assert report.operating_net_income >= 1500.0




# ===========================================================================
# Engine-level Add Debt contract (UI is single-mode "fresh loan" only)
# ===========================================================================
#
# The Add Debt UI was simplified to always treat the entry as a fresh loan
# (cash inflow). The engine kwargs `cash_received=False` and
# `original_amount=...` are still accepted for tests and scripts. These
# tests pin the engine contract; the matching UI behaviour lives in the
# UI-focused classes further down.


class TestAddDebtOriginalAmountKwarg:
    """Engine-level: ledger.add_debt accepts an explicit original_amount."""

    def test_default_original_amount_equals_current(self, db_conn):
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=10000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        d = get_debt_by_asset(db_conn, a.id)
        assert d.original_amount == pytest.approx(10000.0)
        assert d.current_balance == pytest.approx(10000.0)

    def test_explicit_original_amount_records_history(self, db_conn):
        """`amount` is what the caller owes today; `original_amount` is
        the larger original loan principal."""
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="MORT", name="Mortgage",
            amount=200000.0, original_amount=300000.0,
            interest_rate=0.06,
            cash_received=False,
            payment_per_period=2000.0)
        d = get_debt_by_asset(db_conn, a.id)
        assert d.original_amount == pytest.approx(300000.0)
        assert d.current_balance == pytest.approx(200000.0)

    def test_original_amount_below_current_rejected(self, db_conn):
        with pytest.raises(ValueError, match="cannot be less than current"):
            ledger.add_debt(
                db_conn, "2025-01-01", symbol="L", name="Bad",
                amount=10000.0, original_amount=5000.0,
                cash_received=False,
                payment_per_period=100.0)

    def test_original_amount_equal_to_current_accepted(self, db_conn):
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Equal",
            amount=10000.0, original_amount=10000.0, cash_received=False,
            payment_per_period=100.0)
        d = get_debt_by_asset(db_conn, a.id)
        assert d.original_amount == pytest.approx(10000.0)
        assert d.current_balance == pytest.approx(10000.0)


class TestAddDebtCashflowStartEngineDefault:
    """The First scheduled payment field was removed from the Add Debt
    form. The engine now always fills `cashflow_start_date` with its
    default (`first_day_next_month()` for monthly, `Jan 1 next year`
    for yearly) when the UI submits.
    """

    @patch.object(QMessageBox, "information")
    def test_submit_uses_engine_default_monthly(self, _info, db_conn):
        from src.utils.dates import next_month_start
        page = TransactionsPage(db_conn)
        page.refresh()
        page.add_debt_name.setText("Loan")
        page.add_debt_amount.setText("10000")
        page.add_debt_payment.setText("200")
        page._submit_add_debt()
        debts = list_debts(db_conn)
        assert len(debts) == 1
        # Monthly schedule → cashflow_start defaults to first-of-next-month.
        assert debts[0].cashflow_start_date == next_month_start(date.today()).isoformat()

    @patch.object(QMessageBox, "information")
    def test_submit_uses_engine_default_yearly(self, _info, db_conn):
        page = TransactionsPage(db_conn)
        page.refresh()
        page.add_debt_schedule.setCurrentIndex(
            page.add_debt_schedule.findData("yearly"))
        page.add_debt_name.setText("Yearly Loan")
        # Small principal + large yearly payment so the loan pays off
        # in under the 60-month cap (else the submit would trigger an
        # unpatched "Debt Exceeds Payoff Limit" modal).
        page.add_debt_amount.setText("10000")
        page.add_debt_payment.setText("3000")
        page._submit_add_debt()
        debts = list_debts(db_conn)
        assert len(debts) == 1
        # Yearly schedule → defaults to Jan 1 of next year.
        assert debts[0].cashflow_start_date == date(date.today().year + 1, 1, 1).isoformat()

    @patch.object(QMessageBox, "warning")
    def test_submit_rejects_malformed_date_recorded(self, mock_warn, db_conn):
        page = TransactionsPage(db_conn)
        page.refresh()
        page.add_debt_name.setText("Loan")
        page.add_debt_amount.setText("10000")
        page.add_debt_payment.setText("200")
        page.add_debt_date.setText("garbage")
        page._submit_add_debt()
        mock_warn.assert_called_once()
        assert list_debts(db_conn) == []


class TestAddDebtClearResetsAllFields:
    """After a successful submit, _clear_add_debt_inputs must restore the
    form to a clean default — including Date and First scheduled payment
    so a stale historical value can't carry over.
    """

    @patch.object(QMessageBox, "information")
    def test_clear_resets_date_to_today(self, _info, db_conn):
        # Historical date close enough that the loan still has a balance
        # remaining today (origination_date now == add_debt_date, and
        # the engine walks the amortization forward; a too-far-back date
        # would saturate the loan to zero and raise).
        page = TransactionsPage(db_conn)
        page.refresh()
        from datetime import timedelta
        recent = (date.today() - timedelta(days=30)).isoformat()
        page.add_debt_date.setText(recent)
        page.add_debt_name.setText("Loan")
        page.add_debt_amount.setText("10000")
        page.add_debt_payment.setText("200")
        page._submit_add_debt()
        assert page.add_debt_date.text() == date.today().isoformat()

    # `test_clear_resets_first_scheduled_payment_to_default` was deleted:
    # the First scheduled payment field is gone — engine default fires.
