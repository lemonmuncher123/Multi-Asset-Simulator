"""Tests for the debt-payoff fix, missed-payments persistence, deferred
shortfall force-sell, Add Debt mode selector, bankruptcy expansion,
and the bankruptcy-event auto-settle rule.

Sections map to the spec requirements. Each section is its own class so
sections can be targeted via ``pytest -k ClassName``. Section 11
exercises the new rule: scheduled debt/mortgage obligations are not
allowed to remain "overdue/missed" — they either pay (after force-sell
if needed) or trigger a bankruptcy event immediately.

All Qt modal dialogs are patched — an unpatched modal hangs the
offscreen suite forever.
"""
from datetime import date
from unittest.mock import patch

import pytest
from PySide6.QtWidgets import QMessageBox

from src.engines import ledger
from src.engines.portfolio import calc_cash_balance
from src.engines.risk import check_bankruptcy, get_all_warnings
from src.gui.pages.dashboard import DashboardPage
from src.gui.pages.transactions import TransactionsPage
from src.models.asset import Asset
from src.storage.asset_repo import create_asset, list_assets
from src.storage.debt_repo import get_debt_by_asset, list_debts
from src.storage.price_repo import bulk_upsert_ohlcv
from src.storage.transaction_repo import list_transactions


# ---------------------------------------------------------------------------
# Shared helpers
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
    """Seed one of each sellable asset type with a market price."""
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
# Section 1: Pay Off in Full with interest (UI path)
# ===========================================================================

class TestPayOffInFullWithInterest:
    @pytest.fixture
    def page_with_interest_debt(self, db_conn):
        # 12% annual, monthly schedule, balance 1000 → payoff = 1010 (one
        # month's interest).
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Visa",
            amount=1000.0, interest_rate=0.12,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        page = TransactionsPage(db_conn)
        page.refresh()
        for i in range(page.pay_debt_combo.count()):
            if page.pay_debt_combo.itemData(i) == a.id:
                page.pay_debt_combo.setCurrentIndex(i)
                break
        return page, a.id

    def test_button_fills_field_with_payoff_amount(self, page_with_interest_debt):
        page, _ = page_with_interest_debt
        page._on_pay_debt_full_clicked()
        # 1000 + (1000 * 0.12 / 12) = 1010.00 — not the 1000 the legacy
        # code would have written.
        assert page.pay_debt_amount.text() == "1010.00"

    def test_button_refreshes_preview_to_paid_off_message(
        self, page_with_interest_debt):
        """Clicking Pay Off in Full must update the preview to show
        the green "this will pay off the debt" callout. The handler
        blocks textEdited (to prevent _on_pay_debt_amount_edited from
        disarming the payoff flag), which also suppresses the preview
        connection — so it manually triggers the preview refresh.
        Without that manual call the preview stays on the placeholder
        "Enter a payment amount..." text.
        """
        page, _ = page_with_interest_debt
        page._on_pay_debt_full_clicked()
        preview_html = page.pay_debt_preview.text()
        assert "fully pay off the debt" in preview_html
        assert "Enter a payment amount" not in preview_html

    @patch.object(QMessageBox, "warning")
    def test_payoff_drives_balance_to_zero(self, mock_warn, page_with_interest_debt):
        page, asset_id = page_with_interest_debt
        cash_before = calc_cash_balance(page.conn)
        page._on_pay_debt_full_clicked()
        page._submit_pay_debt()
        # Balance must be exactly zero — including interest.
        debt = get_debt_by_asset(page.conn, asset_id)
        assert debt.current_balance == 0.0
        # No warning fired (clean submission).
        mock_warn.assert_not_called()
        # Cash decreased by full payoff (principal + interest).
        assert calc_cash_balance(page.conn) == pytest.approx(cash_before - 1010.0)

    def test_payoff_creates_pay_debt_transaction(self, page_with_interest_debt):
        page, asset_id = page_with_interest_debt
        page._on_pay_debt_full_clicked()
        page._submit_pay_debt()
        txns = [
            t for t in list_transactions(page.conn, asset_id=asset_id)
            if t.txn_type == "pay_debt"
        ]
        assert len(txns) == 1
        assert txns[0].total_amount == pytest.approx(-1010.0)

    def test_no_negative_balance_after_payoff(self, page_with_interest_debt):
        page, asset_id = page_with_interest_debt
        page._on_pay_debt_full_clicked()
        page._submit_pay_debt()
        debt = get_debt_by_asset(page.conn, asset_id)
        assert debt.current_balance >= 0.0


# ===========================================================================
# Section 2: Engine payoff helper
# ===========================================================================

class TestEnginePayoffPath:
    def test_compute_payoff_amount_zero_rate(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        assert ledger.compute_payoff_amount(db_conn, a.id) == pytest.approx(1000.0)

    def test_compute_payoff_amount_monthly_interest(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.12,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        # 1000 + 1000*0.12/12 = 1010
        assert ledger.compute_payoff_amount(db_conn, a.id) == pytest.approx(1010.0)

    def test_compute_payoff_amount_yearly_interest(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.06,
            schedule_frequency="yearly",
            payment_per_period=200.0, cash_received=False)
        # 1000 + 1000*0.06 = 1060
        assert ledger.compute_payoff_amount(db_conn, a.id) == pytest.approx(1060.0)

    def test_pay_debt_in_full_clears_interest_bearing_debt(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.12,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        ledger.pay_debt_in_full(db_conn, "2025-02-01", a.id)
        debt = get_debt_by_asset(db_conn, a.id)
        assert debt.current_balance == 0.0

    def test_pay_debt_in_full_preserves_notes(self, db_conn):
        """User-supplied notes must end up in the transaction note. The
        engine prepends a fixed "Pay-off in full (principal + accrued
        interest)" marker so the audit trail captures the payoff
        semantics, then appends the user's text after a separator —
        spec §3 #3."""
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.12,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        txn = ledger.pay_debt_in_full(
            db_conn, "2025-02-01", a.id, notes="early payoff bonus")
        assert "early payoff bonus" in (txn.notes or "")
        assert "Pay-off in full" in (txn.notes or "")
        assert "accrued interest" in (txn.notes or "")

    def test_pay_debt_in_full_note_explicitly_names_principal_and_interest(
        self, db_conn):
        """Spec §3 #3: the transaction note must make clear that the
        payoff includes the current period's accrued interest, not just
        the principal balance. Audit trail surfaces both numbers."""
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="My Loan",
            amount=1000.0, interest_rate=0.12,  # 12% annual → 1%/mo
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        txn = ledger.pay_debt_in_full(db_conn, "2025-02-01", a.id)
        # Note explicitly references principal + accrued interest with
        # numeric breakdowns, and names the debt.
        notes = txn.notes or ""
        assert "Pay-off in full" in notes
        assert "principal" in notes.lower()
        assert "accrued interest" in notes.lower()
        assert "My Loan" in notes
        # Balance lands at exactly zero; cash leaves at the full payoff
        # (principal 1000 + 1% interest = 1010).
        from src.storage.debt_repo import get_debt_by_asset
        assert get_debt_by_asset(db_conn, a.id).current_balance == 0.0
        assert txn.total_amount == pytest.approx(-1010.0, rel=1e-3)

    def test_partial_pay_above_payoff_amount_is_rejected(self, db_conn):
        """Spec §3: a manual partial payment must not exceed the
        allowed payoff amount (principal + accrued interest)."""
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.12,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        # Payoff = 1000 + 1% = 1010. Anything above 1010 must be
        # rejected by the ledger guard.
        with pytest.raises(ValueError, match="exceeds payoff amount"):
            ledger.pay_debt(db_conn, "2025-02-01", a.id, 1500.0)

    def test_pay_debt_at_payoff_amount_clears_interest_bearing_debt(
        self, db_conn):
        """Manually entering the payoff amount via pay_debt must land
        the balance at exactly zero. Previously the engine rejected any
        amount > current_balance even if it equaled the payoff amount,
        forcing the user to click "Pay Off in Full" to do the same thing.
        """
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.12,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        # Payoff = 1000 + (1000 * 0.12 / 12) = 1010. The user types this
        # number directly into the amount field instead of clicking the
        # button — same end state.
        cash_before = calc_cash_balance(db_conn)
        txn = ledger.pay_debt(db_conn, "2025-02-01", a.id, 1010.0)
        assert get_debt_by_asset(db_conn, a.id).current_balance == 0.0
        assert txn.total_amount == pytest.approx(-1010.0)
        assert calc_cash_balance(db_conn) == pytest.approx(cash_before - 1010.0)

    def test_pay_debt_accepts_displayed_payoff_with_rounding_gap(
        self, db_conn):
        """8% APR on $1000 monthly = $6.6666... interest. Payoff is
        $1006.6666..., which fmt_money displays as "$1,006.67". A user
        copying that displayed value types 1006.67 — slightly greater
        than the underlying float. The half-cent tolerance must absorb
        this rounding gap so the input is accepted (it would otherwise
        trip the "exceeds payoff amount" guard at the 7th decimal).
        """
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.08,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        # Confirm the precondition: the actual payoff is below 1006.67.
        actual_payoff = ledger.compute_payoff_amount(db_conn, a.id)
        assert actual_payoff < 1006.67
        assert actual_payoff == pytest.approx(1006.6667, abs=1e-3)
        # User types the displayed payoff. Must succeed without raising.
        ledger.pay_debt(db_conn, "2025-02-01", a.id, 1006.67)
        assert get_debt_by_asset(db_conn, a.id).current_balance == 0.0

    def test_pay_debt_in_full_zero_rate_records_zero_accrued_interest(
        self, db_conn):
        """A 0% debt has no accrued interest — payoff equals balance.
        The note still carries the marker so the audit shape is
        consistent across debts with and without interest."""
        ledger.deposit_cash(db_conn, "2025-01-01", 2000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="Z", name="Zero",
            amount=1000.0, interest_rate=0.0,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        txn = ledger.pay_debt_in_full(db_conn, "2025-02-01", a.id)
        notes = txn.notes or ""
        assert "Pay-off in full" in notes
        # Accrued interest is 0.00 for a zero-rate debt.
        assert "accrued interest 0.00" in notes
        assert txn.total_amount == pytest.approx(-1000.0)

    def test_pay_debt_in_full_charges_full_payoff_amount(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.12,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        cash_before = calc_cash_balance(db_conn)
        txn = ledger.pay_debt_in_full(db_conn, "2025-02-01", a.id)
        # The recorded transaction reflects the full payoff (1010), not
        # just current_balance (1000).
        assert txn.total_amount == pytest.approx(-1010.0)
        assert calc_cash_balance(db_conn) == pytest.approx(cash_before - 1010.0)

    def test_pay_debt_in_full_raises_clear_error_when_cash_short(self, db_conn):
        # Add interest-bearing debt without cash inflow.
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.12,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False)
        with pytest.raises(ValueError, match="Insufficient cash"):
            ledger.pay_debt_in_full(db_conn, "2025-02-01", a.id)
        # Balance unchanged on rejection.
        assert get_debt_by_asset(db_conn, a.id).current_balance == 1000.0

    def test_pay_debt_in_full_rejects_already_cleared(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        ledger.pay_debt(db_conn, "2025-02-01", a.id, 1000.0)
        with pytest.raises(ValueError, match="already paid off"):
            ledger.pay_debt_in_full(db_conn, "2025-03-01", a.id)

    def test_pay_debt_in_full_updates_last_payment_date(self, db_conn):
        """Manual UI payoff should update last_payment_date so auto-settle's
        dedupe (`last_paid >= d`) sees the change.
        """
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="CC", name="Card",
            amount=1000.0, interest_rate=0.18, cash_received=False,
            payment_per_period=100.0)
        ledger.pay_debt_in_full(db_conn, "2025-06-15", a.id)
        assert get_debt_by_asset(db_conn, a.id).last_payment_date == "2025-06-15"


# ===========================================================================
# Section 3: Automatic final payment clears interest-bearing debt
# ===========================================================================

class TestAutomaticFinalPayment:
    def test_settle_clears_interest_bearing_debt_to_zero(self, db_conn):
        # Small balance + non-trivial rate so the schedule terminates and
        # the final period leaves a fractional residue that the legacy
        # `min(payment, balance)` path would have left behind.
        ledger.deposit_cash(db_conn, "2024-12-15", 100000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Mini",
            amount=200.0, interest_rate=0.06,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False,
            cashflow_start_date="2025-01-01")
        created, deferred = ledger.settle_due_debt_payments(
            db_conn, "2025-04-30")
        assert deferred == []
        assert get_debt_by_asset(db_conn, a.id).current_balance == 0.0

    def test_final_payment_amount_uses_schedule_final_payment(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 100000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Mini",
            amount=200.0, interest_rate=0.06,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False,
            cashflow_start_date="2025-01-01")
        created, _ = ledger.settle_due_debt_payments(db_conn, "2025-04-30")
        # The last payment differs from the regular 100. After two normal
        # 100 payments the balance is small + 1 month's interest; that's
        # the load-bearing assertion against the legacy bug where the
        # final payment was capped at balance and left interest residue.
        final_amounts = [t.total_amount for t in created]
        assert final_amounts[-1] != pytest.approx(-100.0)
        # Sum of all payments equals the schedule's total — principal +
        # all interest. Slack of 1¢ for floating-point drift.
        from src.engines.debt_math import compute_debt_schedule
        sched = compute_debt_schedule(
            principal=200.0, annual_rate=0.06,
            schedule="monthly", payment=100.0)
        assert sum(-t.total_amount for t in created) == pytest.approx(
            sched.total_paid, abs=0.01)

    def test_no_extra_payments_if_settle_runs_again(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 100000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Mini",
            amount=200.0, interest_rate=0.06,
            schedule_frequency="monthly",
            payment_per_period=100.0, cash_received=False,
            cashflow_start_date="2025-01-01")
        first, _ = ledger.settle_due_debt_payments(db_conn, "2025-12-31")
        again, _ = ledger.settle_due_debt_payments(db_conn, "2025-12-31")
        assert again == []
        # Debt is paid off; no further pay_debt rows appeared.
        all_pay_debt = [
            t for t in list_transactions(db_conn, asset_id=a.id)
            if t.txn_type == "pay_debt"
        ]
        assert len(all_pay_debt) == len(first)


# ===========================================================================
# Section 4: Monthly and yearly timing
# ===========================================================================

class TestScheduleTiming:
    def test_monthly_mid_month_anchor_pays_next_first(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 10000.0)
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-15")
        created, _ = ledger.settle_due_debt_payments(db_conn, "2025-03-15")
        # Mid-month anchor snaps forward to 2025-02-01 (first 1st *after*
        # the anchor) — not 2025-01-01.
        dates = [t.date for t in created]
        assert dates == ["2025-02-01", "2025-03-01"]

    def test_monthly_first_of_month_anchor_pays_that_date(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 10000.0)
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")
        created, _ = ledger.settle_due_debt_payments(db_conn, "2025-03-15")
        dates = [t.date for t in created]
        assert dates == ["2025-01-01", "2025-02-01", "2025-03-01"]

    def test_yearly_only_on_january_first(self, db_conn):
        ledger.deposit_cash(db_conn, "2023-12-15", 100000.0)
        ledger.add_debt(
            db_conn, "2023-12-15", symbol="L", name="Loan",
            amount=10000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=2000.0, schedule_frequency="yearly",
            cashflow_start_date="2024-01-01")
        created, _ = ledger.settle_due_debt_payments(db_conn, "2026-06-30")
        dates = [t.date for t in created]
        assert dates == ["2024-01-01", "2025-01-01", "2026-01-01"]

    def test_notes_include_schedule_prefix_and_debt_name(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 10000.0)
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="V", name="Visa Card",
            amount=2000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")
        created, _ = ledger.settle_due_debt_payments(db_conn, "2025-01-31")
        assert created
        note = created[0].notes or ""
        assert "Scheduled debt payment" in note
        assert "Visa Card" in note


# ===========================================================================
# Section 5: Add Debt — single-mode (always cash inflow)
# ===========================================================================
#
# The previous Existing-Debt vs New-Borrowing toggle was removed; Add Debt
# now always treats the entry as a fresh loan (cash inflow). The engine
# still accepts cash_received=False / original_amount=... for tests and
# scripts but the UI no longer surfaces those options.


class TestAddDebtUISingleMode:
    @patch.object(QMessageBox, "information")
    def test_add_debt_increases_cash_by_principal(self, mock_info, page):
        """Submitting via the UI always credits cash with the principal."""
        cash_before = calc_cash_balance(page.conn)
        page.add_debt_name.setText("Auto Loan")
        page.add_debt_amount.setText("12000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_term.setChecked(True)
        page.add_debt_term.setText("60")
        page._submit_add_debt()
        debts = list_debts(page.conn)
        assert len(debts) == 1
        add_debt_txns = [
            t for t in list_transactions(page.conn, asset_id=debts[0].asset_id)
            if t.txn_type == "add_debt"
        ]
        assert len(add_debt_txns) == 1
        assert add_debt_txns[0].total_amount == pytest.approx(12000.0)
        assert calc_cash_balance(page.conn) == pytest.approx(cash_before + 12000.0)

    @patch.object(QMessageBox, "information")
    def test_add_debt_preserves_schedule_and_payment(self, mock_info, page):
        page.add_debt_name.setText("Card")
        page.add_debt_amount.setText("3000")
        page.add_debt_rate.setText("12.0")
        page.add_debt_schedule.setCurrentIndex(0)  # monthly
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("150")
        page._submit_add_debt()
        d = list_debts(page.conn)[0]
        assert d.schedule_frequency == "monthly"
        assert d.monthly_payment_amount == pytest.approx(150.0)
        assert d.interest_rate == pytest.approx(0.12)


# ===========================================================================
# Section 6: Deferred shortfall force-sell
# ===========================================================================

class TestDeferredShortfallForceSell:
    def test_force_sell_funds_deferred_payment(self, db_conn):
        # Cash starts positive but smaller than the scheduled debt.
        ledger.deposit_cash(db_conn, "2024-12-01", 600.0)
        a = create_asset(db_conn, Asset(symbol="C", name="Crypto", asset_type="crypto"))
        ledger.buy(db_conn, "2024-12-02", a.id, quantity=5, price=100.0)
        _seed_market_price(db_conn, a.id, "C", "crypto", 100.0)
        # Cash is now 600 - 500 = 100. Scheduled debt of 300/mo can't be paid.
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=10000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=300.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")

        _, deferred = ledger.settle_due_debt_payments(db_conn, "2025-01-31")
        assert len(deferred) == 1

        # Replicate the MainWindow auto-settle pipeline manually.
        shortfall = sum(d["amount"] for d in deferred)
        target = calc_cash_balance(db_conn) + shortfall
        sales = ledger.force_sell_to_raise_cash(
            db_conn, "2025-01-31", target_cash=target,
            reason="auto debt/mortgage deduction")
        assert sales, "expected force-sell to fire to fund the deferred payment"
        # Note must mention forced liquidation and the scheduled-debt reason.
        notes = (sales[0].notes or "").lower()
        assert "forced liquidation" in notes
        assert "scheduled debt" in notes

        created, still = ledger.retry_deferred(db_conn, deferred)
        assert len(created) == 1
        assert still == []
        # Cash didn't go negative.
        assert calc_cash_balance(db_conn) >= -1e-6

    def test_force_sell_runs_before_cash_goes_negative(self, db_conn):
        """The whole point: don't wait for cash<0. Pre-empt the miss."""
        # Deposit enough to fund the buy, then end up with positive but
        # below-target cash on hand.
        ledger.deposit_cash(db_conn, "2024-12-01", 600.0)
        a = create_asset(db_conn, Asset(symbol="C", name="Crypto", asset_type="crypto"))
        ledger.buy(db_conn, "2024-12-02", a.id, quantity=5, price=100.0)
        _seed_market_price(db_conn, a.id, "C", "crypto", 100.0)
        # Cash = 600 - 500 = 100 (positive). Holdings = 5 crypto @ $100.
        assert calc_cash_balance(db_conn) == pytest.approx(100.0)

        # Ask the engine to raise cash to 300. Without the new
        # target_cash arg, the legacy "only fix negative cash" semantics
        # would no-op here (cash is already positive).
        sales = ledger.force_sell_to_raise_cash(
            db_conn, "2024-12-04", target_cash=300.0, reason="test")
        assert sales, "force-sell should fire while cash is still positive"
        assert calc_cash_balance(db_conn) >= 300.0 - 1e-6


# ===========================================================================
# Section 7: Forced sell priority and notes
# ===========================================================================

class TestForceSellPriorityAndNotes:
    def test_stock_sells_first(self, db_conn):
        """Spec §11 selling order: stock < etf < other (crypto/custom)
        < real_estate. Stocks sell first."""
        ids = _seed_sellable(db_conn, qty=10, price=100.0)
        # Drain to zero, then create deficit.
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -50.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert sales
        assert sales[0].asset_id == ids["stock"]

    def _setup_single_type(self, db_conn, symbol, atype):
        # Deposit a little extra so withdraw_cash can flush a non-zero
        # remainder without tripping the >0 guard.
        ledger.deposit_cash(db_conn, "2024-12-01", 1001.0)
        a = create_asset(db_conn, Asset(symbol=symbol, name=symbol, asset_type=atype))
        ledger.buy(db_conn, "2024-12-02", a.id, quantity=10, price=100)
        _seed_market_price(db_conn, a.id, symbol, atype, 100.0)
        # Drain to ~zero, then induce a $50 deficit.
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -50.0, notes="deficit")
        return a

    def test_stock_sells_in_whole_shares(self, db_conn):
        self._setup_single_type(db_conn, "S", "stock")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert sales
        # Need $50; @ $100 a share that's 0.5 shares, must round up to 1.
        assert sales[0].quantity == 1.0

    def test_etf_sells_in_whole_shares(self, db_conn):
        self._setup_single_type(db_conn, "E", "etf")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert sales
        assert sales[0].quantity == 1.0

    def test_crypto_can_sell_fractional(self, db_conn):
        self._setup_single_type(db_conn, "C", "crypto")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert sales
        # Crypto allows up to 8-decimal fractional sells; 0.5 is fine.
        assert 0 < sales[0].quantity < 1.0

    def test_custom_can_sell_fractional(self, db_conn):
        self._setup_single_type(db_conn, "X", "custom")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert sales
        assert 0 < sales[0].quantity < 1.0

    def test_notes_carry_reason(self, db_conn):
        _seed_sellable(db_conn, qty=10, price=100.0)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -50.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(
            db_conn, "2024-12-04", reason="auto debt deduction")
        assert sales
        notes = sales[0].notes or ""
        assert "auto debt deduction" in notes


# Sections 8 & 9 (legacy missed_payments persistence + bankruptcy-from-
# missed-payments) were removed in schema v10. The `missed_payments`
# table no longer exists; unresolved rows from old DBs are migrated to
# `bankruptcy_events` on upgrade. The bankruptcy-trigger paths are now
# tested via `bankruptcy_events` in TestBankruptcyEventRepo and
# TestCheckBankruptcyOnEvents below.


# ===========================================================================
# Section 10: Regression
# ===========================================================================

class TestRegression:
    def test_zero_interest_pay_debt_still_works(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        ledger.pay_debt(db_conn, "2025-02-01", a.id, 1000.0)
        assert get_debt_by_asset(db_conn, a.id).current_balance == 0.0

    def test_pay_debt_overpayment_still_blocked(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        with pytest.raises(ValueError, match="exceeds payoff amount"):
            ledger.pay_debt(db_conn, "2025-02-01", a.id, 5000.0)

    def test_pay_mortgage_overpayment_still_blocked(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        a, prop, _ = ledger.add_property(
            db_conn, "2025-01-02", symbol="H", name="House",
            purchase_price=300000.0,
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=10000.0,
            interest_rate=0.0, payment_per_period=500.0)
        with pytest.raises(ValueError, match="exceeds payoff amount"):
            ledger.pay_mortgage(db_conn, "2025-02-01", a.id, amount=50000.0)

    def test_pay_debt_appears_in_report_debt_cash_flow(self, db_conn):
        from src.engines.reports import generate_monthly_report, DEBT_CASH_FLOW_TYPES
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        ledger.pay_debt(db_conn, "2025-01-15", a.id, 200.0)
        report = generate_monthly_report(db_conn, 2025, 1)
        assert "pay_debt" in DEBT_CASH_FLOW_TYPES
        import json as _json
        data = _json.loads(report.report_json)
        assert any(t["txn_type"] == "pay_debt" for t in data["transactions"])


# ===========================================================================
# Section 11: Bankruptcy event rule (no recoverable "missed payments")
# ===========================================================================

class TestBankruptcyEventRepo:
    """Direct unit tests for src.storage.bankruptcy_event_repo."""

    def test_record_bankruptcy_event_persists_row(self, db_conn):
        from src.storage.bankruptcy_event_repo import (
            record_bankruptcy_event, list_active_bankruptcy_events)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        ev = record_bankruptcy_event(
            db_conn, event_date="2025-02-01", trigger_kind="debt",
            asset_id=a.id, due_date="2025-02-01",
            amount_due=200.0, cash_balance=0.0, shortfall_amount=200.0,
            notes="auto-settle could not fund")
        assert ev is not None
        assert ev.trigger_kind == "debt"
        assert ev.amount_due == pytest.approx(200.0)
        assert ev.status == "active"
        events = list_active_bankruptcy_events(db_conn)
        assert len(events) == 1

    def test_record_bankruptcy_event_idempotent_on_triple(self, db_conn):
        from src.storage.bankruptcy_event_repo import (
            record_bankruptcy_event, list_active_bankruptcy_events)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        for _ in range(3):
            record_bankruptcy_event(
                db_conn, event_date="2025-02-01", trigger_kind="debt",
                asset_id=a.id, due_date="2025-02-01",
                amount_due=200.0, cash_balance=0.0, shortfall_amount=200.0)
        assert len(list_active_bankruptcy_events(db_conn)) == 1

    def test_record_bankruptcy_event_distinct_due_dates_create_separate_rows(self, db_conn):
        from src.storage.bankruptcy_event_repo import (
            record_bankruptcy_event, list_active_bankruptcy_events)
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        record_bankruptcy_event(
            db_conn, event_date="2025-02-01", trigger_kind="debt",
            asset_id=a.id, due_date="2025-02-01", amount_due=200.0)
        record_bankruptcy_event(
            db_conn, event_date="2025-03-01", trigger_kind="debt",
            asset_id=a.id, due_date="2025-03-01", amount_due=200.0)
        assert len(list_active_bankruptcy_events(db_conn)) == 2

    def test_record_bankruptcy_event_rejects_invalid_kind(self, db_conn):
        from src.storage.bankruptcy_event_repo import record_bankruptcy_event
        with pytest.raises(ValueError):
            record_bankruptcy_event(
                db_conn, event_date="2025-02-01",
                trigger_kind="not_a_kind")

    def test_clear_bankruptcy_events_removes_all(self, db_conn):
        from src.storage.bankruptcy_event_repo import (
            record_bankruptcy_event, clear_bankruptcy_events,
            list_active_bankruptcy_events)
        record_bankruptcy_event(
            db_conn, event_date="2025-02-01", trigger_kind="negative_cash",
            cash_balance=-500.0, shortfall_amount=500.0)
        assert clear_bankruptcy_events(db_conn) == 1
        assert list_active_bankruptcy_events(db_conn) == []


class TestForceSellNoteAttribution:
    """Spec §4 #4: when auto-settle force-sells assets to fund a
    deferred debt or mortgage payment, the generated `sell` transaction
    notes must name the specific obligation. When several deferred
    items share one force-sell, the note carries a combined label.
    """

    def _make_window(self, db_conn):
        from src.gui.main_window import MainWindow
        return MainWindow(db_conn, enable_startup_sync=False)

    def _seed_one_stock(self, db_conn, *, qty=1, price=2000.0):
        from src.storage.asset_repo import create_asset
        from src.models.asset import Asset
        from src.storage.price_repo import bulk_upsert_ohlcv
        a = create_asset(db_conn, Asset(symbol="STK", name="Stock A",
                                          asset_type="stock"))
        ledger.deposit_cash(db_conn, "2024-12-01", qty * price + 1.0)
        ledger.buy(db_conn, "2024-12-02", a.id, quantity=qty, price=price)
        bulk_upsert_ohlcv(db_conn, [{
            "asset_id": a.id, "symbol": "STK", "asset_type": "stock",
            "date": "2024-12-02", "close": price, "source": "test",
        }])
        # Drain remaining cash so the force-sell path triggers.
        remainder = calc_cash_balance(db_conn)
        if remainder > 0:
            ledger.withdraw_cash(db_conn, "2024-12-03", remainder)
        return a

    def test_single_debt_label_in_force_sell_note(self, qapp, db_conn):
        # The auto-settle pipeline uses today's date via date.today();
        # anchor cashflow_start_date right at the current month so the
        # walk produces exactly one due period that the stock can cover.
        from datetime import date as date_type
        today = date_type.today()
        anchor = today.replace(day=1).isoformat()
        self._seed_one_stock(db_conn, qty=1, price=500.0)
        ledger.add_debt(
            db_conn, anchor, symbol="L", name="Auto Loan",
            amount=10000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly",
            cashflow_start_date=anchor)
        window = self._make_window(db_conn)
        window._run_auto_settle()
        # Find the forced sale transaction.
        sells = [
            t for t in list_transactions(db_conn)
            if t.txn_type == "sell"
            and "forced liquidation" in (t.notes or "").lower()
        ]
        assert sells, "expected a forced sale to fund the deferred debt"
        notes = sells[0].notes or ""
        assert "Auto Loan" in notes
        # Should NOT carry the bare "auto debt/mortgage deduction"
        # generic phrase when a specific obligation name is available.
        assert "for debt 'Auto Loan'" in notes

    def test_mortgage_name_in_force_sell_note(self, qapp, db_conn):
        """Spec §4: a mortgage shortfall must produce a sell-note that
        names the property and prefixes the obligation kind with
        ``mortgage on``."""
        from datetime import date as date_type
        today = date_type.today()
        anchor = today.replace(day=1).isoformat()
        self._seed_one_stock(db_conn, qty=1, price=2000.0)
        _, prop, _ = ledger.add_property(
            db_conn, anchor, symbol="HSE", name="Lakefront House",
            purchase_price=200000.0,
            cashflow_start_date=anchor,
            acquisition_mode="existing_property")
        # Mortgage with a payment due immediately so auto-settle defers
        # it and triggers the force-sell.
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=150000.0,
            interest_rate=0.0, payment_per_period=1500.0,
            cashflow_start_date=anchor)
        window = self._make_window(db_conn)
        window._run_auto_settle()
        sells = [
            t for t in list_transactions(db_conn)
            if t.txn_type == "sell"
            and "forced liquidation" in (t.notes or "").lower()
        ]
        assert sells, "expected a forced sale to fund the deferred mortgage"
        notes = sells[0].notes or ""
        assert "Lakefront House" in notes
        assert "mortgage on 'Lakefront House'" in notes

    def test_combined_label_when_multiple_obligations_share_force_sell(
        self, qapp, db_conn):
        from datetime import date as date_type
        today = date_type.today()
        anchor = today.replace(day=1).isoformat()
        self._seed_one_stock(db_conn, qty=2, price=600.0)
        # Two debts both due this month with no cash to fund them.
        ledger.add_debt(
            db_conn, anchor, symbol="L1", name="Auto Loan",
            amount=10000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=300.0, schedule_frequency="monthly",
            cashflow_start_date=anchor)
        ledger.add_debt(
            db_conn, anchor, symbol="L2", name="Card Debt",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly",
            cashflow_start_date=anchor)
        window = self._make_window(db_conn)
        window._run_auto_settle()
        sells = [
            t for t in list_transactions(db_conn)
            if t.txn_type == "sell"
            and "forced liquidation" in (t.notes or "").lower()
        ]
        assert sells
        # The single force-sell carries a combined label naming both.
        combined_notes = " ".join(s.notes or "" for s in sells)
        assert "Auto Loan" in combined_notes
        assert "Card Debt" in combined_notes


class TestAutoSettleBankruptcyRule:
    """End-to-end: MainWindow._run_auto_settle records bankruptcy event(s)
    instead of missed_payments rows when an obligation can't be funded.

    Schedules are anchored well in the past so today's auto-settle
    walks them. Each unfundable due date contributes one bankruptcy_event
    (idempotent on the (kind, asset_id, due_date) triple).
    """

    def _make_window(self, db_conn, qapp):
        from src.gui.main_window import MainWindow
        return MainWindow(db_conn, enable_startup_sync=False)

    def test_unfundable_debt_records_bankruptcy_event(self, qapp, db_conn):
        # Schedule a debt due, no cash, no sellable assets to liquidate.
        a, _, _ = ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=50000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")
        window = self._make_window(db_conn, qapp)
        window._run_auto_settle()

        from src.storage.bankruptcy_event_repo import list_active_bankruptcy_events
        events = list_active_bankruptcy_events(db_conn)
        assert len(events) >= 1
        # All events relate to this asset and are debt-kind.
        for ev in events:
            assert ev.trigger_kind == "debt"
            assert ev.asset_id == a.id
            assert ev.amount_due == pytest.approx(200.0)
        # First event is the earliest due date.
        assert events[0].due_date == "2025-01-01"

    def test_unfundable_debt_creates_no_pay_debt_transaction(self, qapp, db_conn):
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=50000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")
        window = self._make_window(db_conn, qapp)
        window._run_auto_settle()
        pay_debt_txns = [
            t for t in list_transactions(db_conn) if t.txn_type == "pay_debt"
        ]
        assert pay_debt_txns == []

    # The legacy "writes no missed_payments row" guard test was removed
    # in schema v10 — the table no longer exists and unresolved rows
    # were migrated to bankruptcy_events on upgrade.

    def test_idempotent_repeated_auto_settle_does_not_duplicate_events(self, qapp, db_conn):
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=50000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")
        from src.storage.bankruptcy_event_repo import list_active_bankruptcy_events
        window = self._make_window(db_conn, qapp)
        window._run_auto_settle()
        first_count = len(list_active_bankruptcy_events(db_conn))
        window._run_auto_settle()
        window._run_auto_settle()
        second_count = len(list_active_bankruptcy_events(db_conn))
        assert first_count == second_count
        assert first_count >= 1

    def test_force_sell_preempts_bankruptcy_when_assets_exist(self, qapp, db_conn):
        """If sellable assets exist, force-sell raises cash and the
        scheduled payment funds — no bankruptcy event recorded.
        """
        # Seed sellable etf with 100 units @ $100 = $10k of value.
        _seed_sellable(db_conn, qty=100, price=100.0)
        # Drain cash to zero so the debt payment forces a sell.
        ledger.withdraw_cash(db_conn, "2024-12-30", calc_cash_balance(db_conn))
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")
        window = self._make_window(db_conn, qapp)
        window._run_auto_settle()
        from src.storage.bankruptcy_event_repo import list_active_bankruptcy_events
        assert list_active_bankruptcy_events(db_conn) == []
        # The pay_debt transaction WAS created (after force-sell raised cash).
        pay_debt_txns = [
            t for t in list_transactions(db_conn) if t.txn_type == "pay_debt"
        ]
        assert len(pay_debt_txns) >= 1


class TestCheckBankruptcyOnEvents:
    """Risk engine reads bankruptcy_events directly."""

    def test_check_bankruptcy_fires_on_active_event(self, db_conn):
        from src.storage.bankruptcy_event_repo import record_bankruptcy_event
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        record_bankruptcy_event(
            db_conn, event_date="2025-02-01", trigger_kind="debt",
            asset_id=a.id, due_date="2025-02-01", amount_due=200.0)
        warnings = check_bankruptcy(db_conn)
        assert len(warnings) == 1
        w = warnings[0]
        assert w.severity == "critical"
        assert w.category == "bankruptcy"
        assert "bankruptcy" in w.message.lower()
        assert "200" in w.message  # the unfunded amount

    def test_check_bankruptcy_silent_with_no_events_and_solvent_cash(self, db_conn):
        # Empty DB, no events. No bankruptcy.
        ledger.deposit_cash(db_conn, "2025-01-01", 1000.0)
        assert check_bankruptcy(db_conn) == []

    def test_check_bankruptcy_message_mentions_simulator_declared(self, db_conn):
        from src.storage.bankruptcy_event_repo import record_bankruptcy_event
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        record_bankruptcy_event(
            db_conn, event_date="2025-02-01", trigger_kind="debt",
            asset_id=a.id, due_date="2025-02-01", amount_due=200.0)
        msg = check_bankruptcy(db_conn)[0].message.lower()
        assert "could not be funded" in msg
        assert "sellable assets were liquidated" in msg

    def test_dashboard_banner_visible_on_active_event(self, db_conn):
        from src.storage.bankruptcy_event_repo import record_bankruptcy_event
        a, _, _ = ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=100.0)
        record_bankruptcy_event(
            db_conn, event_date="2025-02-01", trigger_kind="debt",
            asset_id=a.id, due_date="2025-02-01", amount_due=200.0)
        page = DashboardPage(db_conn)
        page.refresh()
        assert not page.bankruptcy_banner.isHidden()
        assert page.bankruptcy_label.text()


# ===========================================================================
# Section 12: End-to-end coverage of the bankruptcy-event rule
# ===========================================================================
#
# One method per scenario from the product rule. These tests assert the
# full set of expected side effects (transactions created/not created,
# bankruptcy_events present/absent, missed_payments absent, banner state)
# in a single test so each scenario is verified end-to-end without weakening
# any individual claim into "some warning exists".


class TestBankruptcyRuleEndToEnd:
    def _make_window(self, db_conn):
        from src.gui.main_window import MainWindow
        return MainWindow(db_conn, enable_startup_sync=False)

    def _bankruptcy_events(self, db_conn):
        from src.storage.bankruptcy_event_repo import list_active_bankruptcy_events
        return list_active_bankruptcy_events(db_conn)

    def _force_sells(self, db_conn):
        return [
            t for t in list_transactions(db_conn)
            if t.txn_type == "sell"
            and "forced liquidation" in (t.notes or "").lower()
        ]

    # --- Scenario 1: scheduled debt payment with enough cash -----------------

    def test_debt_funded_creates_pay_debt_no_event_no_missed(self, qapp, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-15", 5000.0)
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")
        self._make_window(db_conn)._run_auto_settle()

        pay_debt = [t for t in list_transactions(db_conn) if t.txn_type == "pay_debt"]
        assert len(pay_debt) >= 1
        assert self._bankruptcy_events(db_conn) == []

    # --- Scenario 2: scheduled mortgage payment with enough cash -------------

    def test_mortgage_funded_creates_pay_mortgage_no_event_no_missed(self, qapp, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-01", 200000.0)
        _, prop, _ = ledger.add_property(
            db_conn, "2024-12-15", symbol="H", name="House",
            purchase_price=300000.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.0, payment_per_period=1500.0,
            cashflow_start_date="2025-01-01")
        self._make_window(db_conn)._run_auto_settle()

        pay_mort = [t for t in list_transactions(db_conn) if t.txn_type == "pay_mortgage"]
        assert len(pay_mort) >= 1
        assert self._bankruptcy_events(db_conn) == []

    # --- Scenario 3: debt with insufficient cash but enough sellable assets --

    def test_debt_funded_via_force_sell(self, qapp, db_conn):
        # Seed plenty of sellable equity, then drain cash to zero so the
        # debt payment forces a sell.
        _seed_sellable(db_conn, qty=100, price=100.0)
        ledger.withdraw_cash(db_conn, "2024-12-30", calc_cash_balance(db_conn))
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=5000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")
        self._make_window(db_conn)._run_auto_settle()

        # A force-sell sell transaction was created.
        assert len(self._force_sells(db_conn)) >= 1
        # Retry succeeded — pay_debt landed.
        pay_debt = [t for t in list_transactions(db_conn) if t.txn_type == "pay_debt"]
        assert len(pay_debt) >= 1
        # No bankruptcy event, no missed_payment row.
        assert self._bankruptcy_events(db_conn) == []

    # --- Scenario 4: mortgage with insufficient cash but enough sellable -----

    def test_mortgage_funded_via_force_sell(self, qapp, db_conn):
        _seed_sellable(db_conn, qty=200, price=100.0)
        ledger.withdraw_cash(db_conn, "2024-12-30", calc_cash_balance(db_conn))
        _, prop, _ = ledger.add_property(
            db_conn, "2024-12-15", symbol="H", name="House",
            purchase_price=200000.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=150000.0,
            interest_rate=0.0, payment_per_period=1500.0,
            cashflow_start_date="2025-01-01")
        self._make_window(db_conn)._run_auto_settle()

        assert len(self._force_sells(db_conn)) >= 1
        pay_mort = [t for t in list_transactions(db_conn) if t.txn_type == "pay_mortgage"]
        assert len(pay_mort) >= 1
        assert self._bankruptcy_events(db_conn) == []

    # --- Scenario 5: debt unfundable → bankruptcy event ----------------------

    def test_debt_unfundable_records_bankruptcy_and_banner(self, qapp, db_conn):
        a, _, _ = ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=50000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")
        self._make_window(db_conn)._run_auto_settle()

        # No pay_debt transaction.
        assert [t for t in list_transactions(db_conn) if t.txn_type == "pay_debt"] == []
        # Active bankruptcy event for this debt.
        events = self._bankruptcy_events(db_conn)
        assert len(events) >= 1
        debt_events = [e for e in events if e.trigger_kind == "debt" and e.asset_id == a.id]
        assert debt_events
        # Risk engine surfaces a critical bankruptcy warning.
        warnings = check_bankruptcy(db_conn)
        assert len(warnings) == 1
        assert warnings[0].severity == "critical"
        assert warnings[0].category == "bankruptcy"
        # Dashboard banner visible.
        page = DashboardPage(db_conn)
        page.refresh()
        assert not page.bankruptcy_banner.isHidden()
        assert page.bankruptcy_label.text()
        # New auto-settle path writes no missed_payments row.

    # --- Scenario 6: mortgage unfundable → bankruptcy event ------------------

    def test_mortgage_unfundable_records_bankruptcy_and_banner(self, qapp, db_conn):
        # Underwater property: mortgage equals purchase price, so a
        # force-sell of the property nets $0 cash. The $1500/mo payment
        # still has no source of funds, and bankruptcy fires.
        a, prop, _ = ledger.add_property(
            db_conn, "2024-12-15", symbol="H", name="House",
            purchase_price=200000.0,
            cashflow_start_date="2025-01-01",
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.0, payment_per_period=1500.0,
            cashflow_start_date="2025-01-01")
        self._make_window(db_conn)._run_auto_settle()

        # No pay_mortgage transaction.
        assert [t for t in list_transactions(db_conn) if t.txn_type == "pay_mortgage"] == []
        events = self._bankruptcy_events(db_conn)
        assert len(events) >= 1
        mortgage_events = [e for e in events if e.trigger_kind == "mortgage" and e.asset_id == a.id]
        assert mortgage_events
        warnings = check_bankruptcy(db_conn)
        assert len(warnings) == 1
        assert warnings[0].severity == "critical"
        assert warnings[0].category == "bankruptcy"
        page = DashboardPage(db_conn)
        page.refresh()
        assert not page.bankruptcy_banner.isHidden()
        assert page.bankruptcy_label.text()

    # --- Scenario 7: idempotency on repeated auto-settle ---------------------

    def test_repeated_auto_settle_does_not_duplicate_events(self, qapp, db_conn):
        ledger.add_debt(
            db_conn, "2024-12-15", symbol="L", name="Loan",
            amount=50000.0, interest_rate=0.0, cash_received=False,
            payment_per_period=200.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01")
        window = self._make_window(db_conn)
        window._run_auto_settle()
        first = self._bankruptcy_events(db_conn)
        first_keys = {(e.trigger_kind, e.asset_id, e.due_date) for e in first}
        window._run_auto_settle()
        window._run_auto_settle()
        second = self._bankruptcy_events(db_conn)
        second_keys = {(e.trigger_kind, e.asset_id, e.due_date) for e in second}
        # Same set of (kind, asset, due_date) triples — repeats produce
        # no new active rows.
        assert first_keys == second_keys
        assert len(first) == len(second)
        assert len(first) >= 1

    def test_forced_liquidation_appears_as_normal_sell_in_report(self, db_conn):
        from src.engines.reports import generate_monthly_report
        _seed_sellable(db_conn, qty=10, price=100.0)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -50.0, notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(db_conn, "2024-12-04")
        assert sales
        report = generate_monthly_report(db_conn, 2024, 12)
        import json as _json
        data = _json.loads(report.report_json)
        sell_rows = [t for t in data["transactions"] if t["txn_type"] == "sell"]
        # The auto-sell still surfaces with its marker note.
        assert any(
            "forced liquidation" in (t["notes"] or "").lower()
            for t in sell_rows
        )
