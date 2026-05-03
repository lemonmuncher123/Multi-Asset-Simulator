"""Tests for the bankruptcy global lock (Phase 4 of the Debt Redesign).

Coverage:
- ``risk.is_bankrupt`` predicate
- ``BankruptcyBanner`` widget visibility tied to the predicate
- ``guard_transaction_or_warn`` helper aborts when bankrupt
- Submit handlers in the Transactions page show the warning and skip
  the underlying ledger call when bankrupt
- The top-level banner in MainWindow refreshes after data changes
"""
from unittest.mock import patch

import pytest
from PySide6.QtWidgets import QMessageBox

from src.engines import ledger
from src.engines.portfolio import calc_cash_balance
from src.engines.risk import is_bankrupt
from src.gui.pages.transactions import TransactionsPage
from src.gui.widgets.bankruptcy_banner import (
    BankruptcyBanner, guard_transaction_or_warn)
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.bankruptcy_event_repo import (
    list_active_bankruptcy_events, record_bankruptcy_event)


def _force_bankruptcy(db_conn):
    """Helper: produce a bankruptcy state without going through the
    full auto-settle pipeline. Records a bankruptcy event directly,
    which is one of the trigger paths ``check_bankruptcy`` recognizes.
    """
    record_bankruptcy_event(
        db_conn, event_date="2025-01-01", trigger_kind="debt",
        asset_id=None, due_date="2025-01-01",
        amount_due=1000.0, cash_balance=0.0, shortfall_amount=1000.0,
        notes="test fixture")
    assert list_active_bankruptcy_events(db_conn)
    assert is_bankrupt(db_conn)


# ---------------------------------------------------------------------------
# is_bankrupt predicate
# ---------------------------------------------------------------------------

class TestIsBankruptPredicate:
    def test_false_for_solvent_portfolio(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 1000.0)
        assert not is_bankrupt(db_conn)

    def test_true_when_bankruptcy_event_active(self, db_conn):
        _force_bankruptcy(db_conn)
        assert is_bankrupt(db_conn)


# ---------------------------------------------------------------------------
# BankruptcyBanner widget
# ---------------------------------------------------------------------------

class TestBankruptcyBanner:
    def test_hidden_when_solvent(self, qapp, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        banner = BankruptcyBanner(db_conn)
        banner.refresh()
        assert not banner.isVisible()

    def test_visible_and_message_present_when_bankrupt(self, qapp, db_conn):
        _force_bankruptcy(db_conn)
        banner = BankruptcyBanner(db_conn)
        # Visibility flag flips even without an active window — the
        # widget setVisible(True) is what the test cares about.
        banner.refresh()
        assert banner.isVisibleTo(banner.parentWidget()) or banner.isVisible() \
               or not banner.isHidden()
        assert "Bankruptcy" in banner.label.text()
        assert "transactions are disabled" in banner.label.text().lower()

    def test_refresh_toggles_with_state(self, qapp, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        banner = BankruptcyBanner(db_conn)
        banner.refresh()
        assert banner.isHidden()
        _force_bankruptcy(db_conn)
        banner.refresh()
        assert not banner.isHidden()


# ---------------------------------------------------------------------------
# guard_transaction_or_warn helper
# ---------------------------------------------------------------------------

class TestGuardHelper:
    def test_returns_false_when_solvent(self, db_conn):
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        with patch.object(QMessageBox, "warning") as mock_warn:
            blocked = guard_transaction_or_warn(db_conn, parent=None)
        assert blocked is False
        mock_warn.assert_not_called()

    def test_returns_true_and_shows_dialog_when_bankrupt(self, db_conn):
        _force_bankruptcy(db_conn)
        with patch.object(QMessageBox, "warning") as mock_warn:
            blocked = guard_transaction_or_warn(db_conn, parent=None)
        assert blocked is True
        mock_warn.assert_called_once()


# ---------------------------------------------------------------------------
# Submit handlers abort under bankruptcy
# ---------------------------------------------------------------------------

class TestSubmitHandlersAbort:
    @patch.object(QMessageBox, "warning")
    def test_pay_debt_submit_aborts(self, mock_warn, db_conn):
        # First create a debt + cash so the form has a valid debt.
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=1000.0, interest_rate=0.0,
            payment_per_period=100.0, cash_received=True)
        before_balance = ledger.get_debt_by_asset(  # type: ignore[attr-defined]
            db_conn, asset_id=1).current_balance if False else 1000.0
        # Trigger bankruptcy after the setup so the page constructs OK.
        _force_bankruptcy(db_conn)
        page = TransactionsPage(db_conn)
        page.refresh()
        page.pay_debt_combo.setCurrentIndex(0)
        page.pay_debt_amount.setText("100")
        page._submit_pay_debt()
        # The guard's QMessageBox.warning is called (mock_warn).
        assert mock_warn.called
        # And no pay_debt transaction was written (balance unchanged).
        from src.storage.transaction_repo import list_transactions
        pay_debt_rows = [
            t for t in list_transactions(db_conn) if t.txn_type == "pay_debt"
        ]
        assert pay_debt_rows == []

    @patch.object(QMessageBox, "warning")
    def test_add_debt_submit_aborts(self, mock_warn, db_conn):
        _force_bankruptcy(db_conn)
        page = TransactionsPage(db_conn)
        page.refresh()
        page.add_debt_name.setText("New Loan")
        page.add_debt_amount.setText("1000")
        page.add_debt_rate.setText("6.0")
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("100")
        page._submit_add_debt()
        from src.storage.debt_repo import list_debts
        # No new debt created.
        assert list_debts(db_conn) == []
        assert mock_warn.called

    @patch.object(QMessageBox, "warning")
    def test_deposit_cash_submit_aborts(self, mock_warn, db_conn):
        """Spec §1: deposit_cash is one of the user transaction paths
        that must be locked under bankruptcy."""
        _force_bankruptcy(db_conn)
        page = TransactionsPage(db_conn)
        page.refresh()
        # Switch the txn-type combo to deposit_cash.
        from src.gui.pages.transactions import TXN_TYPES
        page.txn_type.setCurrentIndex(TXN_TYPES.index("deposit_cash"))
        page.amount_input.setText("500")
        before_count = len(_list_transactions(db_conn))
        page._submit()
        assert mock_warn.called
        # No transaction row written.
        assert len(_list_transactions(db_conn)) == before_count

    @patch.object(QMessageBox, "warning")
    def test_withdraw_cash_submit_aborts(self, mock_warn, db_conn):
        """Spec §1: withdraw_cash must be locked too. Even though the
        user might think withdrawing cash isn't "spending", it still
        creates a transaction row and needs to be banned."""
        # Seed cash so a hypothetical withdraw_cash WOULD succeed if not
        # blocked.
        ledger.deposit_cash(db_conn, "2025-01-01", 1000.0)
        _force_bankruptcy(db_conn)
        page = TransactionsPage(db_conn)
        page.refresh()
        from src.gui.pages.transactions import TXN_TYPES
        page.txn_type.setCurrentIndex(TXN_TYPES.index("withdraw_cash"))
        page.amount_input.setText("100")
        before_count = len(_list_transactions(db_conn))
        page._submit()
        assert mock_warn.called
        # No new transaction row.
        assert len(_list_transactions(db_conn)) == before_count

    @patch.object(QMessageBox, "warning")
    def test_pay_property_expense_submit_aborts(self, mock_warn, db_conn):
        """Spec §1: pay_property_expense flows through the same
        ``_submit`` handler as deposit/withdraw and must be guarded."""
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        ledger.add_property(
            db_conn, "2025-01-01", symbol="H", name="House",
            purchase_price=200000.0,
            acquisition_mode="existing_property")
        _force_bankruptcy(db_conn)
        page = TransactionsPage(db_conn)
        page.refresh()
        from src.gui.pages.transactions import TXN_TYPES
        page.txn_type.setCurrentIndex(TXN_TYPES.index("pay_property_expense"))
        # The asset combo will have populated to the property.
        page.amount_input.setText("100")
        before = [
            t for t in _list_transactions(db_conn)
            if t.txn_type == "pay_property_expense"
        ]
        page._submit()
        assert mock_warn.called
        after = [
            t for t in _list_transactions(db_conn)
            if t.txn_type == "pay_property_expense"
        ]
        assert len(after) == len(before)

    @patch.object(QMessageBox, "warning")
    def test_confirm_trade_warns_when_bankrupt_with_no_preview(
        self, mock_warn, db_conn,
    ):
        """If the user is bankrupt and clicks Confirm with no/stale/
        invalid preview, the guard must still fire the bankruptcy
        warning instead of returning silently. Other transaction
        handlers (`_submit`, `_submit_pay_debt`, etc.) place the
        guard at function entry; ``_confirm_trade`` must match for
        consistency. Without this, a bankrupt user gets no signal
        when clicking Confirm in a degenerate state."""
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        page = TransactionsPage(db_conn)
        page.refresh()
        # No preview has been built — `_current_preview` is None.
        assert page._current_preview is None
        _force_bankruptcy(db_conn)

        page._confirm_trade()

        assert mock_warn.called

    @patch.object(QMessageBox, "warning")
    def test_confirm_trade_aborts_when_bankrupt(self, mock_warn, db_conn):
        """Spec §1: Buy/Sell go through ``_confirm_trade`` after a
        preview step. The guard at the top of that handler must still
        block the trade when the portfolio is bankrupt, even if a
        non-bankrupt preview was previously cached on the page."""
        # Prepare a buyable asset and a preview while solvent.
        ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
        from src.storage.asset_repo import create_asset
        a = create_asset(db_conn, Asset(symbol="STK", name="Stock",
                                          asset_type="stock"))
        from src.storage.price_repo import bulk_upsert_ohlcv
        bulk_upsert_ohlcv(db_conn, [{
            "asset_id": a.id, "symbol": "STK", "asset_type": "stock",
            "date": "2025-01-01", "close": 100.0, "source": "test",
        }])
        page = TransactionsPage(db_conn)
        page.refresh()
        from src.engines.trade_preview import (
            prepare_trade_preview, TradeDraft)
        preview = prepare_trade_preview(
            db_conn,
            TradeDraft(action="buy", asset_id=a.id, quantity=1.0,
                        fee=0.0, note=None),
            "2025-01-02")
        page._current_preview = preview
        # Now flip into bankruptcy and confirm the trade.
        _force_bankruptcy(db_conn)
        before_count = len(_list_transactions(db_conn))
        page._confirm_trade()
        assert mock_warn.called
        # No buy transaction was written.
        assert len(_list_transactions(db_conn)) == before_count

    def test_non_transaction_actions_still_work_when_bankrupt(self, db_conn):
        """Spec §1: viewing/refreshing pages and reading data must NOT
        be blocked under bankruptcy — only transaction-creating
        submits."""
        from src.gui.pages.dashboard import DashboardPage
        ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        _force_bankruptcy(db_conn)
        # Dashboard refresh reads risk warnings and must succeed.
        d = DashboardPage(db_conn)
        d.refresh()  # would raise if a guard inadvertently fired
        # Transactions page refresh must also succeed.
        t = TransactionsPage(db_conn)
        t.refresh()
        # And listing transactions/holdings still works.
        _ = _list_transactions(db_conn)

    @patch.object(QMessageBox, "warning")
    def test_pay_mortgage_submit_aborts(self, mock_warn, db_conn):
        """Spec §1: manual mortgage extra payment must be locked."""
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        ledger.add_property(
            db_conn, "2025-01-01", symbol="H", name="House",
            purchase_price=200000.0, 
             
            cashflow_start_date="2025-02-01",
            acquisition_mode="existing_property")
        _force_bankruptcy(db_conn)
        page = TransactionsPage(db_conn)
        page.refresh()
        # Combo is loaded by refresh; pick the only mortgaged property.
        page.pay_mort_combo.setCurrentIndex(0)
        page.pay_mort_amount.setText("200")
        before = [
            t for t in _list_transactions(db_conn) if t.txn_type == "pay_mortgage"
        ]
        page._submit_pay_mortgage()
        assert mock_warn.called
        after = [
            t for t in _list_transactions(db_conn) if t.txn_type == "pay_mortgage"
        ]
        assert len(after) == len(before)


def _list_transactions(conn):
    from src.storage.transaction_repo import list_transactions
    return list_transactions(conn)


# ---------------------------------------------------------------------------
# Preflight bankruptcy: a user submit that triggers auto-settle which
# in turn declares bankruptcy must abort the user's action cleanly
# (Phase 7 — item 5 in the auto-settle timing coverage).
# ---------------------------------------------------------------------------

class TestPreflightBankruptcyAborts:
    @patch.object(QMessageBox, "warning")
    def test_preflight_declared_bankruptcy_aborts_user_submit(
        self, mock_warn, qapp, db_conn):
        """When the user clicks Submit on a previously-solvent
        portfolio, the preflight runs auto-settle for the current
        date. If that pipeline declares bankruptcy (e.g. an obligation
        is now due that the user can't fund), the existing bankruptcy
        guard catches the new state and aborts the user action — no
        transaction is written despite the submit firing.
        """
        from src.engines import ledger
        from src.gui.main_window import MainWindow
        from src.gui.pages.transactions import TXN_TYPES
        from src.storage.bankruptcy_event_repo import (
            list_active_bankruptcy_events)

        # Schedule a debt due that cannot be funded: zero cash, no
        # sellable assets. cash_received=False on add_debt avoids
        # priming cash from the loan.
        ledger.add_debt(
            db_conn, "2025-01-01", symbol="L", name="Loan",
            amount=10000.0, interest_rate=0.0,
            payment_per_period=200.0, schedule_frequency="monthly",
            cashflow_start_date="2025-01-01",
            cash_received=False)

        w = MainWindow(
            db_conn,
            enable_startup_sync=False,
            enable_auto_settle_timer=False)
        # Move "today" past the obligation's anchor so preflight has
        # work to do. Don't run startup_auto_settle so the obligation
        # is still pending when the user clicks Submit.
        w._current_date = lambda: "2025-03-15"
        assert list_active_bankruptcy_events(db_conn) == []

        tx_page = w.page_widgets[w._page_index["Transactions"]]
        tx_page.refresh()

        # User attempts a deposit_cash. Preflight runs first → declares
        # bankruptcy → guard fires the warning dialog and aborts.
        tx_page.txn_type.setCurrentIndex(TXN_TYPES.index("deposit_cash"))
        tx_page.amount_input.setText("100")
        deposits_before = len([
            t for t in _list_transactions(db_conn)
            if t.txn_type == "deposit_cash"
        ])
        tx_page._submit()

        # Bankruptcy event(s) were written by preflight.
        events = list_active_bankruptcy_events(db_conn)
        assert len(events) >= 1
        assert all(
            e.trigger_kind in ("debt", "mortgage", "multiple")
            for e in events
        )
        # Bankruptcy warning dialog was shown.
        assert mock_warn.called
        # The deposit_cash row was NOT written (user action aborted).
        deposits_after = len([
            t for t in _list_transactions(db_conn)
            if t.txn_type == "deposit_cash"
        ])
        assert deposits_after == deposits_before
        w.close()


# ---------------------------------------------------------------------------
# Auto-settle internals still work under bankruptcy
# ---------------------------------------------------------------------------

class TestAutoSettleNotBlocked:
    """The guard lives at the UI submit layer, not at the ledger layer.
    Auto-settle (settle_due_rent / settle_due_debt_payments / force_sell_*)
    must continue to run during bankruptcy so the system can still
    process scheduled obligations and recover cash where possible."""

    def test_ledger_writes_raise_BankruptcyLockedError_when_bankrupt(self, db_conn):
        """Phase 6.4: every public ledger write asserts not-bankrupt at
        entry. The exception type is `BankruptcyLockedError` so callers
        can distinguish it from the generic `ValueError` paths."""
        from src.engines.ledger import BankruptcyLockedError
        _force_bankruptcy(db_conn)
        with pytest.raises(BankruptcyLockedError):
            ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        with pytest.raises(BankruptcyLockedError):
            ledger.withdraw_cash(db_conn, "2025-01-01", 50.0)
        with pytest.raises(BankruptcyLockedError):
            ledger.add_debt(
                db_conn, "2025-01-01", symbol="X", name="X",
                amount=100.0, payment_per_period=10.0)
        with pytest.raises(BankruptcyLockedError):
            ledger.manual_adjustment(db_conn, "2025-01-01", 50.0)

    def test_autosettle_bypass_lets_writes_through(self, db_conn):
        """The `_auto_settle_bypass()` context manager (used by
        settle_due_*, force_sell_*, retry_deferred decorators) lets
        ledger writes proceed during bankruptcy."""
        from src.engines.ledger import _auto_settle_bypass
        _force_bankruptcy(db_conn)
        with _auto_settle_bypass():
            txn = ledger.deposit_cash(db_conn, "2025-01-01", 100.0)
        assert txn is not None
        # The bypass is scoped: outside the with, writes are blocked again.
        from src.engines.ledger import BankruptcyLockedError
        with pytest.raises(BankruptcyLockedError):
            ledger.deposit_cash(db_conn, "2025-01-02", 50.0)

    def test_force_sell_still_runs(self, db_conn):
        # Build a portfolio with one stock and a cash deficit.
        ledger.deposit_cash(db_conn, "2024-12-01", 1000.0)
        a = create_asset(db_conn, Asset(symbol="STK", name="Stock",
                                          asset_type="stock"))
        ledger.buy(db_conn, "2024-12-02", a.id, quantity=10, price=100.0)
        from src.storage.price_repo import bulk_upsert_ohlcv
        bulk_upsert_ohlcv(db_conn, [{
            "asset_id": a.id, "symbol": "STK", "asset_type": "stock",
            "date": "2024-12-02", "close": 100.0, "source": "test",
        }])
        # Buying spent the deposit; cash is at $0, no withdraw needed.
        ledger.manual_adjustment(db_conn, "2024-12-03", -100.0,
                                  notes="deficit")
        _force_bankruptcy(db_conn)
        # Auto-settle entrypoint must still succeed even though
        # is_bankrupt() is True.
        sales = ledger.force_sell_to_cover_negative_cash(
            db_conn, "2024-12-04")
        assert sales, (
            "force-sell should run during bankruptcy — the lock is at "
            "the UI submit layer, not the ledger."
        )
