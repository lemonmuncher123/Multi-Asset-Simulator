"""Coverage for the negative-input ban (May 2026).

Every public ledger.* function that takes a user-supplied numeric is now
guarded against negative or non-positive inputs at the engine layer. The
GUI submit handlers, CSV importer, Full Data Importer, and SQLite CHECK
constraints (schema v12) form four additional layers of defense.

`manual_adjustment.amount` keeps its dual-direction semantics — it is the
documented escape hatch for cash corrections in either direction. That
exception is also tested here so a future refactor doesn't accidentally
ban it.
"""
from __future__ import annotations

import csv
import io
import sqlite3
from pathlib import Path
import tempfile
from unittest.mock import patch

import pytest

from src.engines import ledger
from src.engines.full_data_io import (
    _validate_row_for_table,
    export_full_data,
    import_full_data,
)
from src.engines.import_export import import_transactions_csv
from src.engines.portfolio import calc_cash_balance
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.debt_repo import get_debt_by_asset
from src.storage.mortgage_repo import get_mortgage_by_property


# ----------------------------------------------------------------------
# Engine layer (ledger.*)
# ----------------------------------------------------------------------


def _seed_zero_rate_debt(conn: sqlite3.Connection, balance: float = 1000.0):
    """Convenience: deposit cash + create a zero-rate debt with the given
    balance. Returns the debt object."""
    ledger.deposit_cash(conn, "2026-01-01", 5000.0)
    asset, debt, _ = ledger.add_debt(
        conn, "2026-01-01", symbol="Z", name="Z",
        amount=balance, interest_rate=0.0, schedule_frequency="monthly",
        payment_per_period=50, cash_received=False,
    )
    return debt


def _seed_property_with_mortgage(conn: sqlite3.Connection):
    ledger.deposit_cash(conn, "2026-01-01", 100_000.0)
    asset, prop, _ = ledger.add_property(
        conn, "2026-01-01", symbol="HOME", name="Home",
        purchase_price=200_000, down_payment=20_000,
        acquisition_mode="new_purchase",
    )
    ledger.add_mortgage(
        conn, property_id=prop.id, original_amount=180_000,
        interest_rate=0.0, payment_per_period=1000,
    )
    return asset, prop


class TestPayDebtGuards:
    def test_pay_debt_rejects_negative_amount(self, db_conn):
        debt = _seed_zero_rate_debt(db_conn)
        with pytest.raises(ValueError, match="must be positive"):
            ledger.pay_debt(db_conn, "2026-01-15", debt.asset_id, amount=-100)

    def test_pay_debt_rejects_zero_amount(self, db_conn):
        debt = _seed_zero_rate_debt(db_conn)
        with pytest.raises(ValueError, match="must be positive"):
            ledger.pay_debt(db_conn, "2026-01-15", debt.asset_id, amount=0)

    def test_pay_debt_rejects_principal_portion_above_balance(self, db_conn):
        debt = _seed_zero_rate_debt(db_conn, balance=500.0)
        # Without this guard, a buggy auto-settle path could drive
        # `current_balance` below zero — verified bug #5 from the May
        # 2026 audit.
        with pytest.raises(ValueError, match="exceeds remaining balance"):
            ledger.pay_debt(
                db_conn, "2026-01-15", debt.asset_id,
                amount=10, principal_portion=99999,
            )

    def test_pay_debt_rejects_negative_principal_portion(self, db_conn):
        debt = _seed_zero_rate_debt(db_conn)
        with pytest.raises(ValueError, match="cannot be negative"):
            ledger.pay_debt(
                db_conn, "2026-01-15", debt.asset_id,
                amount=10, principal_portion=-5,
            )


class TestPayMortgageGuards:
    def test_pay_mortgage_rejects_negative_amount(self, db_conn):
        asset, prop = _seed_property_with_mortgage(db_conn)
        with pytest.raises(ValueError, match="must be positive"):
            ledger.pay_mortgage(db_conn, "2026-01-15", asset.id, amount=-500)

    def test_pay_mortgage_rejects_zero_amount(self, db_conn):
        asset, prop = _seed_property_with_mortgage(db_conn)
        with pytest.raises(ValueError, match="must be positive"):
            ledger.pay_mortgage(db_conn, "2026-01-15", asset.id, amount=0)


class TestReceiveRentGuards:
    def test_receive_rent_rejects_negative_amount(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 100_000.0)
        asset, prop, _ = ledger.add_property(
            db_conn, "2026-01-01", symbol="HOME", name="Home",
            purchase_price=200_000, monthly_rent=2000,
            acquisition_mode="existing_property",
        )
        with pytest.raises(ValueError, match="must be positive"):
            ledger.receive_rent(db_conn, "2026-01-15", asset.id, amount=-100)


class TestPayPropertyExpenseGuards:
    def test_pay_property_expense_rejects_negative(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 100_000.0)
        asset, prop, _ = ledger.add_property(
            db_conn, "2026-01-01", symbol="HOME", name="Home",
            purchase_price=200_000,
            acquisition_mode="existing_property",
        )
        with pytest.raises(ValueError, match="must be positive"):
            ledger.pay_property_expense(
                db_conn, "2026-01-15", asset.id, amount=-100,
            )


class TestUpdatePropertyValueGuards:
    def test_rejects_negative_value(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 100_000.0)
        asset, prop, _ = ledger.add_property(
            db_conn, "2026-01-01", symbol="HOME", name="Home",
            purchase_price=200_000,
            acquisition_mode="existing_property",
        )
        with pytest.raises(ValueError, match="cannot be negative"):
            ledger.update_property_value(
                db_conn, "2026-02-01", asset.id, new_value=-50_000,
            )


class TestManualAdjustmentGuards:
    def test_rejects_negative_quantity(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 5000.0)
        asset = create_asset(
            db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        ledger.buy(db_conn, "2026-01-01", asset.id, quantity=10, price=100)
        with pytest.raises(ValueError, match="Quantity must be positive"):
            ledger.manual_adjustment(
                db_conn, "2026-01-15", amount=0,
                asset_id=asset.id, quantity=-5, price=100,
            )

    def test_rejects_zero_quantity(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 5000.0)
        asset = create_asset(
            db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        with pytest.raises(ValueError, match="Quantity must be positive"):
            ledger.manual_adjustment(
                db_conn, "2026-01-15", amount=0,
                asset_id=asset.id, quantity=0, price=100,
            )

    def test_amount_signed_exception_still_works(self, db_conn):
        """`manual_adjustment.amount` is the documented dual-direction
        cash escape hatch — negative amounts MUST still be accepted, even
        though every other ledger.* function now rejects them."""
        ledger.deposit_cash(db_conn, "2026-01-01", 5000.0)
        ledger.manual_adjustment(
            db_conn, "2026-01-15", amount=-100, notes="cash correction",
        )
        assert calc_cash_balance(db_conn) == pytest.approx(4900.0)


class TestAddPropertyGuards:
    @pytest.mark.parametrize("field,value,match", [
        ("monthly_rent", -1, "Monthly rent"),
        ("monthly_property_tax", -1, "property tax"),
        ("monthly_insurance", -1, "insurance"),
        ("monthly_hoa", -1, "HOA"),
        ("monthly_maintenance_reserve", -1, "maintenance reserve"),
        ("monthly_property_management", -1, "management"),
        ("monthly_expense", -1, "Monthly expense"),
        ("current_value", -1, "Current value"),
        ("down_payment", -1, "Down payment"),
        ("vacancy_rate", -0.5, "Vacancy rate"),
        ("vacancy_rate", 1.5, "Vacancy rate"),
    ])
    def test_rejects_negative_field(self, db_conn, field, value, match):
        ledger.deposit_cash(db_conn, "2026-01-01", 1_000_000.0)
        kwargs = {
            "symbol": "HOME",
            "name": "Home",
            "purchase_price": 200_000,
            "acquisition_mode": "existing_property",
            field: value,
        }
        with pytest.raises(ValueError, match=match):
            ledger.add_property(db_conn, "2026-01-01", **kwargs)


class TestAddDebtGuards:
    def test_rejects_negative_interest_rate(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 5000.0)
        with pytest.raises(ValueError, match="Interest rate"):
            ledger.add_debt(
                db_conn, "2026-01-01", symbol="X", name="X",
                amount=1000, interest_rate=-0.01,
                schedule_frequency="monthly", payment_per_period=50,
            )

    def test_rejects_negative_minimum_payment(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 5000.0)
        with pytest.raises(ValueError, match="Minimum payment"):
            ledger.add_debt(
                db_conn, "2026-01-01", symbol="X", name="X",
                amount=1000, interest_rate=0.05, minimum_payment=-50,
                schedule_frequency="monthly", payment_per_period=50,
            )


class TestAddMortgageGuards:
    def test_rejects_negative_interest_rate(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 1_000_000.0)
        asset, prop, _ = ledger.add_property(
            db_conn, "2026-01-01", symbol="HOME", name="Home",
            purchase_price=200_000, acquisition_mode="existing_property",
        )
        with pytest.raises(ValueError, match="Interest rate"):
            ledger.add_mortgage(
                db_conn, property_id=prop.id, original_amount=180_000,
                interest_rate=-0.01, payment_per_period=1000,
            )


# ----------------------------------------------------------------------
# CSV import (`import_export._validate_txn_row`)
# ----------------------------------------------------------------------


class TestCsvImportGuards:
    def _build_csv(self, rows: list[dict]) -> str:
        headers = [
            "date", "txn_type", "asset_symbol", "quantity", "price",
            "total_amount", "currency", "fees", "notes",
        ]
        out = io.StringIO()
        w = csv.DictWriter(out, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h, "") for h in headers})
        return out.getvalue()

    def test_rejects_negative_fees(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 100_000.0)
        asset = create_asset(
            db_conn,
            Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        csv_text = self._build_csv([{
            "date": "2026-01-15", "txn_type": "buy", "asset_symbol": "AAPL",
            "quantity": "10", "price": "100", "total_amount": "-1000",
            "fees": "-5",
        }])
        result = import_transactions_csv(db_conn, csv_text)
        assert result.imported == 0
        assert any("fees" in e.lower() for e in result.errors)

    def test_rejects_receive_rent_without_asset(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 100_000.0)
        csv_text = self._build_csv([{
            "date": "2026-01-15", "txn_type": "receive_rent",
            "total_amount": "1500",
        }])
        result = import_transactions_csv(db_conn, csv_text)
        assert result.imported == 0
        assert any(
            "asset_symbol" in e.lower() for e in result.errors
        )

    def test_rejects_pay_property_expense_without_asset(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 100_000.0)
        csv_text = self._build_csv([{
            "date": "2026-01-15", "txn_type": "pay_property_expense",
            "total_amount": "-100",
        }])
        result = import_transactions_csv(db_conn, csv_text)
        assert result.imported == 0
        assert any(
            "asset_symbol" in e.lower() for e in result.errors
        )


# ----------------------------------------------------------------------
# Full Data Import (`full_data_io._validate_row_for_table`)
# ----------------------------------------------------------------------


class TestFullDataImportRowValidator:
    def test_properties_rejects_negative_monthly_rent(self):
        errors = _validate_row_for_table(
            "properties", {"monthly_rent": "-50"}, row_num=2,
        )
        assert errors
        assert any("monthly_rent" in e for e in errors)

    def test_properties_rejects_vacancy_above_one(self):
        errors = _validate_row_for_table(
            "properties", {"vacancy_rate": "1.5"}, row_num=2,
        )
        assert errors
        assert any("vacancy_rate" in e for e in errors)

    def test_market_prices_rejects_zero_price(self):
        errors = _validate_row_for_table(
            "market_prices", {"price": "0"}, row_num=2,
        )
        assert errors
        assert any("price" in e for e in errors)

    def test_market_prices_accepts_null_volume(self):
        # NULL is permitted by every rule; the rule's first branch
        # short-circuits before parsing.
        errors = _validate_row_for_table(
            "market_prices", {"price": "10", "volume": ""}, row_num=2,
        )
        assert errors == []

    def test_transactions_rejects_negative_fees(self):
        errors = _validate_row_for_table(
            "transactions", {"fees": "-1"}, row_num=2,
        )
        assert errors
        assert any("fees" in e for e in errors)

    def test_transactions_total_amount_signed_unconstrained(self):
        # `total_amount` is signed by convention; the rule table
        # intentionally omits it.
        errors = _validate_row_for_table(
            "transactions", {"total_amount": "-1500", "fees": "0"}, row_num=2,
        )
        assert errors == []


class TestFullDataImportEndToEnd:
    """Round-trip a hand-edited export with a negative monthly_rent and
    confirm `import_full_data` rejects it before touching the DB."""

    def test_import_rejects_negative_monthly_rent_csv(self, db_conn):
        ledger.deposit_cash(db_conn, "2026-01-01", 100_000.0)
        ledger.add_property(
            db_conn, "2026-01-01", symbol="HOME", name="Home",
            purchase_price=200_000,
            acquisition_mode="existing_property",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "export"
            assert export_full_data(db_conn, out).success
            # Hand-edit `properties.csv` to inject a negative rent.
            props_path = out / "properties.csv"
            text = props_path.read_text()
            edited = text.replace(",0.0,", ",-50.0,", 1)
            assert edited != text  # something matched
            props_path.write_text(edited)
            result = import_full_data(db_conn, out)
            assert result.success is False
            assert "Field validation failed" in result.message
            assert any("monthly" in d.lower() for d in result.details)


# ----------------------------------------------------------------------
# Schema v12 CHECK constraints (defense-in-depth)
# ----------------------------------------------------------------------


class TestSchemaCheckConstraints:
    def test_negative_monthly_rent_rejected_by_schema(self, db_conn):
        # Bypass every Python layer — go straight to raw SQL. The
        # schema CHECK should still fire.
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            db_conn.execute(
                "INSERT INTO properties (asset_id, monthly_rent) "
                "VALUES (1, -100)"
            )

    def test_vacancy_rate_above_one_rejected_by_schema(self, db_conn):
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            db_conn.execute(
                "INSERT INTO properties (asset_id, vacancy_rate) "
                "VALUES (1, 1.5)"
            )

    def test_market_prices_zero_price_rejected_by_schema(self, db_conn):
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            db_conn.execute(
                "INSERT INTO market_prices (asset_id, date, price, source) "
                "VALUES (1, '2026-01-01', 0, 'manual')"
            )

    def test_transactions_negative_fees_rejected_by_schema(self, db_conn):
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            db_conn.execute(
                "INSERT INTO transactions (date, txn_type, total_amount, fees) "
                "VALUES ('2026-01-01', 'buy', -1500, -10)"
            )

    def test_transactions_negative_total_amount_allowed(self, db_conn):
        # `total_amount` is signed by convention; the schema must NOT
        # reject negatives here (buys ARE negative).
        asset = create_asset(
            db_conn,
            Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        db_conn.execute(
            "INSERT INTO transactions (date, txn_type, asset_id, "
            "quantity, price, total_amount, fees) "
            "VALUES ('2026-01-01', 'buy', ?, 10, 100, -1000, 0)",
            (asset.id,),
        )
        # No exception means the insert succeeded.
        assert True


# ----------------------------------------------------------------------
# May 2026 audit follow-ups: gaps in the negative-input ban surfaced by
# manual review of the Real Estate / Transactions pages and the
# `transaction_fee_breakdown` table.
# ----------------------------------------------------------------------


class TestRealEstateAddPropertyFormGuards:
    """`RealEstatePage._read_form_values` is the single chokepoint for
    every Add Property submit. Negative loan or rate input used to
    fall through silently — loan was reinterpreted as "no mortgage",
    rate was reinterpreted as 0%. Both are now hard ValueErrors.
    """

    def _make_page(self, db_conn):
        from src.gui.pages.real_estate import RealEstatePage
        page = RealEstatePage(db_conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("House")
        page.date_input.setText("2025-01-15")
        page.price_input.setText("300000")
        page.current_value_input.setText("320000")
        return page

    def test_negative_loan_amount_raises(self, db_conn):
        page = self._make_page(db_conn)
        page.mortgage_input.setText("-50000")
        with pytest.raises(ValueError, match="loan amount cannot be negative"):
            page._read_form_values()

    def test_negative_loan_amount_blocks_submit(self, db_conn):
        from PySide6.QtWidgets import QMessageBox as _QMB
        ledger.deposit_cash(db_conn, "2025-01-01", 50000.0)
        page = self._make_page(db_conn)
        page.mortgage_input.setText("-50000")
        with patch.object(_QMB, "warning", return_value=None) as mock_warning:
            page._submit()
            mock_warning.assert_called_once()
            # No property was created.
            from src.storage.property_repo import list_active_properties
            assert list_active_properties(db_conn) == []

    def test_zero_loan_amount_still_works(self, db_conn):
        # 0 means "no mortgage / cash purchase" — must still be accepted.
        page = self._make_page(db_conn)
        page.mortgage_input.setText("0")
        v = page._read_form_values()
        assert v["mortgage"]["original_loan_amount"] == 0.0

    def test_negative_interest_rate_raises(self, db_conn):
        page = self._make_page(db_conn)
        page.mortgage_input.setText("200000")
        page.rate_input.setText("-5")
        with pytest.raises(ValueError, match="interest rate cannot be negative"):
            page._read_form_values()

    def test_negative_interest_rate_blocks_submit(self, db_conn):
        from PySide6.QtWidgets import QMessageBox as _QMB
        ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
        page = self._make_page(db_conn)
        page.mortgage_input.setText("200000")
        page.rate_input.setText("-5")
        with patch.object(_QMB, "warning", return_value=None) as mock_warning:
            page._submit()
            mock_warning.assert_called_once()
            from src.storage.property_repo import list_active_properties
            assert list_active_properties(db_conn) == []

    def test_zero_interest_rate_still_works(self, db_conn):
        # 0% is a legitimate interest-free loan.
        page = self._make_page(db_conn)
        page.mortgage_input.setText("200000")
        page.rate_input.setText("0")
        v = page._read_form_values()
        assert v["mortgage"]["interest_rate"] == 0.0


class TestAddDebtNegativeRateGuard:
    """Add Debt form: negative interest rate used to silently fall back
    to the configured default (typically 7%). The submit handler now
    surfaces it as a QMessageBox.warning before persisting."""

    def _make_page(self, db_conn):
        from src.gui.pages.transactions import TransactionsPage
        return TransactionsPage(db_conn)

    def test_negative_rate_blocks_submit(self, db_conn):
        from PySide6.QtWidgets import QMessageBox as _QMB
        from src.storage.debt_repo import list_debts
        ledger.deposit_cash(db_conn, "2026-01-01", 50_000.0)
        page = self._make_page(db_conn)
        page.add_debt_name.setText("Loan A")
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("-5")
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("500")
        page.add_debt_date.setText("2026-01-15")
        with patch.object(_QMB, "warning", return_value=None) as mock_warning:
            page._submit_add_debt()
            mock_warning.assert_called_once()
            args = mock_warning.call_args
            # The "Interest rate cannot be negative" message body
            assert "negative" in args.args[2].lower()
            assert list_debts(db_conn) == []

    def test_unparseable_rate_blocks_submit(self, db_conn):
        from PySide6.QtWidgets import QMessageBox as _QMB
        from src.storage.debt_repo import list_debts
        ledger.deposit_cash(db_conn, "2026-01-01", 50_000.0)
        page = self._make_page(db_conn)
        page.add_debt_name.setText("Loan B")
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.setText("abc")
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("500")
        page.add_debt_date.setText("2026-01-15")
        with patch.object(_QMB, "warning", return_value=None) as mock_warning:
            page._submit_add_debt()
            mock_warning.assert_called_once()
            assert list_debts(db_conn) == []

    def test_blank_rate_falls_back_to_default(self, db_conn):
        # Blank stays intentional — falls back to default (7%) per
        # the existing spec §6 #4 behavior, NOT this fix.
        from PySide6.QtWidgets import QMessageBox as _QMB
        from src.storage.debt_repo import list_debts
        ledger.deposit_cash(db_conn, "2026-01-01", 50_000.0)
        page = self._make_page(db_conn)
        page.add_debt_name.setText("Loan C")
        page.add_debt_amount.setText("10000")
        page.add_debt_rate.clear()
        page.add_debt_radio_payment.setChecked(True)
        page.add_debt_payment.setText("500")
        page.add_debt_date.setText("2026-01-15")
        with patch.object(_QMB, "warning", return_value=None), \
             patch.object(_QMB, "information", return_value=None):
            page._submit_add_debt()
            debts = list_debts(db_conn)
            assert len(debts) == 1
            # Default is 7% per `DEFAULT_DEBT_ANNUAL_RATE_PCT`.
            assert debts[0].interest_rate == pytest.approx(0.07)


class TestTradePreviewNegativeFeeGuard:
    """Trade Preview's `_preview_trade` parses Additional Fees from the
    UI. Negatives used to flow into `compute_trading_costs`, which
    silently dropped them via its `if additional_fee > 0` filter — so
    no DB pollution, but the user got no feedback for their typo.
    Now blocked at parse time."""

    def _make_page(self, db_conn):
        from src.gui.pages.transactions import TransactionsPage
        return TransactionsPage(db_conn)

    def test_negative_fee_blocks_preview(self, db_conn):
        from PySide6.QtWidgets import QMessageBox as _QMB
        from src.storage.asset_repo import create_asset
        ledger.deposit_cash(db_conn, "2026-01-01", 5000.0)
        asset = create_asset(
            db_conn,
            Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        # Seed a price so the preview path doesn't bail on missing price.
        from src.storage.price_repo import upsert_price
        upsert_price(
            db_conn, asset_id=asset.id, date="2026-01-15",
            price=100.0, source="manual",
        )
        page = self._make_page(db_conn)
        # Pick "buy"
        for i in range(page.txn_type.count()):
            if page.txn_type.itemData(i) == "buy":
                page.txn_type.setCurrentIndex(i)
                break
        # Pick the AAPL asset
        for i in range(page.asset_combo.count()):
            if page.asset_combo.itemData(i) == asset.id:
                page.asset_combo.setCurrentIndex(i)
                break
        page.qty_input.setText("1")
        page.fees_input.setText("-5")
        page.date_input.setText("2026-01-15")
        with patch.object(_QMB, "warning", return_value=None) as mock_warning:
            page._preview_trade()
            mock_warning.assert_called_once()
            args = mock_warning.call_args
            assert "negative" in args.args[2].lower()


class TestComputeTradingCostsNegativeAdditionalFee:
    """Engine-layer defense in depth: `compute_trading_costs` rejects
    a negative `additional_fee` parameter even when callers fail to
    validate. Mirrors the engine guards on the rest of the codebase."""

    def test_rejects_negative(self, db_conn):
        from src.engines.trading_costs import compute_trading_costs
        with pytest.raises(ValueError, match="cannot be negative"):
            compute_trading_costs(
                db_conn, action="buy", asset_type="stock",
                quantity=10.0, trade_value=1000.0, trade_year=2026,
                additional_fee=-5.0,
            )

    def test_zero_is_ok(self, db_conn):
        from src.engines.trading_costs import compute_trading_costs
        result = compute_trading_costs(
            db_conn, action="buy", asset_type="stock",
            quantity=10.0, trade_value=1000.0, trade_year=2026,
            additional_fee=0.0,
        )
        # Zero fee → no FeeItem of type additional_fee.
        assert all(
            item.fee_type != "additional_fee" for item in result.items
        )

    def test_positive_appears_in_breakdown(self, db_conn):
        from src.engines.trading_costs import compute_trading_costs
        result = compute_trading_costs(
            db_conn, action="buy", asset_type="stock",
            quantity=10.0, trade_value=1000.0, trade_year=2026,
            additional_fee=5.0,
        )
        af = [it for it in result.items if it.fee_type == "additional_fee"]
        assert len(af) == 1
        assert af[0].amount == 5.0


class TestTransactionFeeBreakdownAmountConstraints:
    """`transaction_fee_breakdown.amount` was previously unconstrained at
    schema level and uncovered by `_NEGATIVE_GUARD_RULES`. Both layers
    now reject negative amounts."""

    def test_schema_check_rejects_negative_amount(self, db_conn):
        # Direct SQL — bypass every Python guard. The schema CHECK
        # added in the May 2026 follow-up should still fire.
        # First create a parent transaction so the FK doesn't fail.
        from src.storage.asset_repo import create_asset
        asset = create_asset(
            db_conn,
            Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        cur = db_conn.execute(
            "INSERT INTO transactions (date, txn_type, asset_id, "
            "quantity, price, total_amount, fees) "
            "VALUES ('2026-01-01', 'buy', ?, 10, 100, -1000, 0)",
            (asset.id,),
        )
        txn_id = cur.lastrowid
        db_conn.commit()
        with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
            db_conn.execute(
                "INSERT INTO transaction_fee_breakdown "
                "(transaction_id, fee_type, amount) VALUES (?, 'manual', -1)",
                (txn_id,),
            )

    def test_schema_allows_zero_amount(self, db_conn):
        # 0 is permitted (some legitimate fee items round to 0).
        from src.storage.asset_repo import create_asset
        asset = create_asset(
            db_conn,
            Asset(symbol="AAPL", name="Apple", asset_type="stock"),
        )
        cur = db_conn.execute(
            "INSERT INTO transactions (date, txn_type, asset_id, "
            "quantity, price, total_amount, fees) "
            "VALUES ('2026-01-01', 'buy', ?, 10, 100, -1000, 0)",
            (asset.id,),
        )
        txn_id = cur.lastrowid
        db_conn.execute(
            "INSERT INTO transaction_fee_breakdown "
            "(transaction_id, fee_type, amount) VALUES (?, 'finra_taf', 0)",
            (txn_id,),
        )

    def test_full_data_import_validator_rejects_negative_amount(self):
        errors = _validate_row_for_table(
            "transaction_fee_breakdown", {"amount": "-1.5"}, row_num=2,
        )
        assert errors
        assert any("amount" in e for e in errors)

    def test_full_data_import_validator_allows_zero_amount(self):
        errors = _validate_row_for_table(
            "transaction_fee_breakdown", {"amount": "0"}, row_num=2,
        )
        assert errors == []

    def test_v12_migration_adds_constraint_to_existing_db(self, db_conn):
        # Simulate a DB that was migrated to v12 BEFORE the
        # `transaction_fee_breakdown` rule was added: rebuild the table
        # without the CHECK and verify the migration re-runs and adds it
        # on the next `_migrate_v12_check_constraints` call.
        from src.storage.database import _migrate_v12_check_constraints
        db_conn.execute("PRAGMA foreign_keys=OFF")
        db_conn.execute("DROP TABLE transaction_fee_breakdown")
        db_conn.execute("""
            CREATE TABLE transaction_fee_breakdown (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id  INTEGER NOT NULL,
                fee_type        TEXT NOT NULL,
                amount          REAL NOT NULL,
                rate            REAL,
                notes           TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (transaction_id) REFERENCES transactions(id)
            )
        """)
        db_conn.commit()
        # Confirm the CHECK is gone (sanity check on the test setup).
        sql = db_conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='transaction_fee_breakdown'"
        ).fetchone()[0]
        assert "CHECK" not in sql.upper()
        # Run the migration; it should rebuild with the CHECK in place.
        _migrate_v12_check_constraints(db_conn)
        sql_after = db_conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='transaction_fee_breakdown'"
        ).fetchone()[0]
        assert "CHECK" in sql_after.upper()
