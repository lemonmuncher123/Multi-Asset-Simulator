import pytest
from unittest.mock import patch

from PySide6.QtWidgets import QDialog, QScrollArea, QFormLayout, QGroupBox, QHeaderView, QAbstractScrollArea
from PySide6.QtTest import QTest
from PySide6.QtCore import Qt
from src.storage.database import init_db
from src.engines.ledger import deposit_cash, add_property, sell_property, settle_due_rent, first_day_next_month
from src.engines.portfolio import calc_cash_balance, calc_net_worth, calc_total_assets
from src.engines.real_estate import analyze_all_properties
from src.gui.pages.real_estate import RealEstatePage, SellPropertyDialog
from src.storage.property_repo import get_property_by_asset, list_active_properties, list_properties
from src.storage.transaction_repo import list_transactions


@pytest.fixture
def conn():
    c = init_db(":memory:")
    yield c
    c.close()


@pytest.fixture
def page(conn):
    return RealEstatePage(conn)


def test_instantiates(page):
    assert page is not None


def test_refresh_no_properties(page):
    page.refresh()
    assert page.table.rowCount() == 0
    assert len(page._table_asset_ids) == 0


def test_refresh_with_active_property(conn):
    deposit_cash(conn, "2025-01-01", 200000.0)
    add_property(
        conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, mortgage_balance=400000.0,
    )
    page = RealEstatePage(conn)
    page.refresh()
    assert page.table.rowCount() == 1
    assert len(page._table_asset_ids) == 1


def test_refresh_excludes_sold_property(conn):
    deposit_cash(conn, "2025-01-01", 200000.0)
    asset, _, _ = add_property(
        conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, mortgage_balance=400000.0,
    )
    sell_property(conn, "2025-06-01", asset.id, 550000.0)
    page = RealEstatePage(conn)
    page.refresh()
    assert page.table.rowCount() == 0


def test_management_buttons_exist(page):
    assert page.edit_btn is not None
    assert page.sell_btn is not None
    assert page.delete_btn is not None
    assert page.clear_btn is not None
    assert page.settle_btn is not None


def test_table_asset_ids_match_rows(conn):
    deposit_cash(conn, "2025-01-01", 500000.0)
    a1, _, _ = add_property(
        conn, "2025-02-01", symbol="H1", name="House A",
        purchase_price=300000.0, mortgage_balance=200000.0,
    )
    a2, _, _ = add_property(
        conn, "2025-03-01", symbol="H2", name="House B",
        purchase_price=200000.0, mortgage_balance=100000.0,
    )
    page = RealEstatePage(conn)
    page.refresh()
    assert page.table.rowCount() == 2
    assert len(page._table_asset_ids) == 2
    assert a1.id in page._table_asset_ids
    assert a2.id in page._table_asset_ids


# --- SellPropertyDialog tests ---


class TestSellPropertyDialogFields:
    def test_price_input_is_editable(self):
        dlg = SellPropertyDialog("Test Property")
        assert dlg.price_input.isEnabled()
        assert not dlg.price_input.isReadOnly()

    def test_fees_input_is_editable(self):
        dlg = SellPropertyDialog("Test Property")
        assert dlg.fees_input.isEnabled()
        assert not dlg.fees_input.isReadOnly()


class TestSellPropertyDialogTyping:
    def test_price_field_accepts_typed_input(self):
        dlg = SellPropertyDialog("Test Property")
        dlg.show()
        dlg.price_input.clear()
        dlg.price_input.setFocus()
        QTest.keyClicks(dlg.price_input, "550000")
        assert dlg.price_input.text() == "550000"
        dlg.reject()


class TestSellPropertyDialogValidation:
    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_empty_price_blocks_accept(self, mock_warning):
        dlg = SellPropertyDialog("Test Property")
        dlg.price_input.clear()
        dlg._validate_and_accept()
        assert dlg.result() != QDialog.DialogCode.Accepted
        mock_warning.assert_called_once()

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_zero_price_blocks_accept(self, mock_warning):
        dlg = SellPropertyDialog("Test Property")
        dlg.price_input.setText("0")
        dlg._validate_and_accept()
        assert dlg.result() != QDialog.DialogCode.Accepted

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_empty_date_blocks_accept(self, mock_warning):
        dlg = SellPropertyDialog("Test Property")
        dlg.date_input.clear()
        dlg.price_input.setText("100000")
        dlg._validate_and_accept()
        assert dlg.result() != QDialog.DialogCode.Accepted
        mock_warning.assert_called_once()

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_negative_fees_blocks_accept(self, mock_warning):
        dlg = SellPropertyDialog("Test Property")
        dlg.price_input.setText("100000")
        dlg.fees_input.setText("-500")
        dlg._validate_and_accept()
        assert dlg.result() != QDialog.DialogCode.Accepted

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_valid_input_accepts(self, mock_warning):
        dlg = SellPropertyDialog("Test Property")
        dlg.date_input.setText("2025-06-01")
        dlg.price_input.setText("550000")
        dlg.fees_input.setText("1000")
        dlg._validate_and_accept()
        assert dlg.result() == QDialog.DialogCode.Accepted
        mock_warning.assert_not_called()

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_empty_fees_treated_as_zero_and_accepts(self, mock_warning):
        dlg = SellPropertyDialog("Test Property")
        dlg.date_input.setText("2025-06-01")
        dlg.price_input.setText("550000")
        dlg.fees_input.clear()
        dlg._validate_and_accept()
        assert dlg.result() == QDialog.DialogCode.Accepted
        mock_warning.assert_not_called()


class TestSellPropertyDialogAccessors:
    def test_accessor_methods(self):
        dlg = SellPropertyDialog("Test Property")
        dlg.date_input.setText("2025-06-01")
        dlg.price_input.setText("550000")
        dlg.fees_input.setText("1000")
        dlg.notes_input.setText("Sold at asking")
        assert dlg.sale_date() == "2025-06-01"
        assert dlg.sale_price() == 550000.0
        assert dlg.fees() == 1000.0
        assert dlg.notes() == "Sold at asking"

    def test_accessor_empty_fees_returns_zero(self):
        dlg = SellPropertyDialog("Test Property")
        dlg.fees_input.clear()
        assert dlg.fees() == 0.0

    def test_accessor_empty_notes_returns_none(self):
        dlg = SellPropertyDialog("Test Property")
        dlg.notes_input.clear()
        assert dlg.notes() is None


# --- Management button enable/disable tests ---


class TestManagementButtons:
    def test_buttons_disabled_with_no_selection(self, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        add_property(
            conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0, mortgage_balance=400000.0,
        )
        page = RealEstatePage(conn)
        page.refresh()
        assert page.table.rowCount() == 1
        assert not page.edit_btn.isEnabled()
        assert not page.sell_btn.isEnabled()
        assert not page.delete_btn.isEnabled()

    def test_buttons_enabled_after_row_selection(self, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        asset, _, _ = add_property(
            conn, "2025-02-01", symbol="H1", name="House",
            purchase_price=500000.0, mortgage_balance=400000.0,
        )
        page = RealEstatePage(conn)
        page.refresh()
        page.table.selectRow(0)
        assert page._get_selected_asset_id() == asset.id
        assert page.edit_btn.isEnabled()
        assert page.sell_btn.isEnabled()
        assert page.delete_btn.isEnabled()

    def test_empty_table_buttons_disabled(self, conn):
        page = RealEstatePage(conn)
        page.refresh()
        assert page.table.rowCount() == 0
        assert not page.edit_btn.isEnabled()
        assert not page.sell_btn.isEnabled()
        assert not page.delete_btn.isEnabled()

    def test_clear_and_settle_always_enabled(self, conn):
        page = RealEstatePage(conn)
        page.refresh()
        assert page.clear_btn.isEnabled()
        assert page.settle_btn.isEnabled()


# --- Entry Type and Cashflow Start Date tests ---


class TestAddPropertyFormFields:
    def test_entry_type_combo_exists(self, conn):
        page = RealEstatePage(conn)
        assert page.entry_type_combo is not None
        assert page.entry_type_combo.count() == 2
        assert page.entry_type_combo.itemData(0) == "existing_property"
        assert page.entry_type_combo.itemData(1) == "new_purchase"

    def test_cashflow_start_date_input_exists(self, conn):
        page = RealEstatePage(conn)
        assert page.cashflow_start_input is not None
        assert page.cashflow_start_input.text() != ""

    def test_current_value_input_exists(self, conn):
        page = RealEstatePage(conn)
        assert page.current_value_input is not None

    def test_entry_type_changes_date_label(self, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        assert "Original" in page._date_label.text() or "Purchase Date" in page._date_label.text()
        page.entry_type_combo.setCurrentIndex(1)
        assert "Purchase Date" in page._date_label.text()

    def test_new_purchase_date_defaults_to_today(self, conn):
        from datetime import date
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        assert page.date_input.text() == date.today().isoformat()

    def test_cashflow_start_date_defaults_to_first_of_next_month(self, conn):
        page = RealEstatePage(conn)
        assert page.cashflow_start_input.text() == first_day_next_month()

    def test_existing_property_clears_date_field(self, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.entry_type_combo.setCurrentIndex(0)
        assert page.date_input.text() == ""

    def test_loan_term_input_exists(self, conn):
        page = RealEstatePage(conn)
        assert page.loan_term_input is not None

    def test_down_payment_type_combo_exists(self, conn):
        page = RealEstatePage(conn)
        assert page.dp_type_combo is not None
        assert page.dp_type_combo.count() == 2

    def test_rent_frequency_combo_exists(self, conn):
        page = RealEstatePage(conn)
        assert page.rent_freq_combo is not None
        assert page.rent_freq_combo.count() == 4

    def test_expense_type_combos_exist(self, conn):
        page = RealEstatePage(conn)
        assert page.tax_type_combo is not None
        assert page.insurance_type_combo is not None
        assert page.maint_type_combo is not None
        assert page.mgmt_type_combo is not None

    def test_summary_labels_exist(self, conn):
        page = RealEstatePage(conn)
        assert "equity" in page._summary_labels
        assert "cap_rate" in page._summary_labels
        assert "cash_on_cash_return" in page._summary_labels


# --- Submit integration tests ---


class TestAddPropertySubmitExistingProperty:
    """Existing Property submit (index 0) does not deduct cash, creates active property."""

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_property_no_cash_impact(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 50000.0)
        cash_before = calc_cash_balance(conn)

        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("Old Family Home")
        page.date_input.setText("2009-01-15")
        page.price_input.setText("200000")
        page.mortgage_input.setText("0")
        page.current_value_input.setText("450000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        cash_after = calc_cash_balance(conn)
        assert cash_after == cash_before

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_property_creates_active_property(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("My House")
        page.date_input.setText("2020-01-01")
        page.price_input.setText("500000")
        page.current_value_input.setText("520000")
        page.mortgage_input.setText("400000")
        page.cashflow_start_input.setText("2025-03-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].purchase_price == 500000.0
        assert props[0].status == "active"

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_property_saves_original_purchase_date(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("Old Family Home")
        page.date_input.setText("2009-01-15")
        page.price_input.setText("200000")
        page.current_value_input.setText("350000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].purchase_date == "2009-01-15"
        assert props[0].cashflow_start_date == "2026-05-01"

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_property_saves_current_value(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("Old Family Home")
        page.date_input.setText("2009-01-15")
        page.price_input.setText("200000")
        page.current_value_input.setText("450000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].current_value == 450000.0
        assert props[0].purchase_price == 200000.0

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_property_txn_note_explains_no_cash(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("Old Family Home")
        page.date_input.setText("2009-01-15")
        page.price_input.setText("200000")
        page.current_value_input.setText("350000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        txns = list_transactions(conn, txn_type="add_property")
        assert len(txns) == 1
        assert txns[0].total_amount == 0.0
        assert "Existing property entry" in txns[0].notes

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_property_txn_date_is_today(self, mock_warning, conn):
        from datetime import date as date_cls
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("Old Family Home")
        page.date_input.setText("2009-01-15")
        page.price_input.setText("200000")
        page.current_value_input.setText("350000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        txns = list_transactions(conn, txn_type="add_property")
        assert len(txns) == 1
        assert txns[0].date == date_cls.today().isoformat()
        assert txns[0].date != "2009-01-15"

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_property_past_date_no_historical_rent(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("Old Rental")
        page.date_input.setText("2009-01-15")
        page.price_input.setText("200000")
        page.current_value_input.setText("250000")
        page.rent_input.setText("2000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()

        created = settle_due_rent(conn, "2026-04-30")
        assert len(created) == 0

        created = settle_due_rent(conn, "2026-05-01")
        assert len(created) == 1
        assert created[0].date == "2026-05-01"


class TestAddPropertySubmitNewPurchase:
    """New Purchase submit (index 1) deducts cash and creates active property."""

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_deducts_down_payment(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        cash_before = calc_cash_balance(conn)

        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("600000")
        page.down_payment_input.setText("120000")
        page.mortgage_input.setText("480000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        cash_after = calc_cash_balance(conn)
        assert cash_after == pytest.approx(cash_before - 120000)

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_creates_active_property(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("600000")
        page.down_payment_input.setText("120000")
        page.mortgage_input.setText("480000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        active = list_active_properties(conn)
        assert len(active) == 1
        assert active[0].status == "active"

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_has_active_status(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("600000")
        page.down_payment_input.setText("120000")
        page.mortgage_input.setText("480000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        all_props = list_properties(conn)
        assert len(all_props) == 1
        assert all_props[0].status == "active"

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_txn_shows_cash_deduction(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("600000")
        page.down_payment_input.setText("120000")
        page.mortgage_input.setText("480000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        txns = list_transactions(conn, txn_type="add_property")
        assert len(txns) == 1
        assert txns[0].total_amount == pytest.approx(-120000)

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_rent_settlement_works(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New Rental")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("300000")
        page.down_payment_input.setText("60000")
        page.mortgage_input.setText("240000")
        page.rent_input.setText("2000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        before = settle_due_rent(conn, "2026-06-30")
        assert len(before) == 0

        after = settle_due_rent(conn, "2026-07-01")
        assert len(after) == 1

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_appears_in_active_table(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("600000")
        page.down_payment_input.setText("120000")
        page.mortgage_input.setText("480000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        page.refresh()
        assert page.table.rowCount() == 1
        assert page.planned_table.rowCount() == 0

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_in_analyze_all(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("600000")
        page.down_payment_input.setText("120000")
        page.mortgage_input.setText("480000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        analyses = analyze_all_properties(conn)
        assert len(analyses) == 1
        assert analyses[0].name == "New House"

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_affects_total_assets(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        ta_before = calc_total_assets(conn)

        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("600000")
        page.down_payment_input.setText("120000")
        page.mortgage_input.setText("480000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        ta_after = calc_total_assets(conn)
        assert ta_after == pytest.approx(ta_before - 120000 + 600000)

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_no_down_payment_deducts_price_minus_mortgage(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        cash_before = calc_cash_balance(conn)

        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("500000")
        page.mortgage_input.setText("400000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        cash_after = calc_cash_balance(conn)
        assert cash_after == pytest.approx(cash_before - 100000)


# --- Existing Property detailed behavior ---


class TestExistingPropertyBehavior:
    """Prove Existing Property behavior matches the redesign spec."""

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_without_purchase_price_uses_current_value(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("Inherited House")
        page.date_input.setText("1990-01-01")
        page.price_input.clear()
        page.current_value_input.setText("400000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].current_value == 400000.0

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_without_down_payment_ok(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("House")
        page.date_input.setText("2010-01-01")
        page.price_input.setText("300000")
        page.current_value_input.setText("400000")
        page.down_payment_input.clear()
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_without_interest_rate_ok(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("House")
        page.date_input.setText("2010-01-01")
        page.price_input.setText("300000")
        page.current_value_input.setText("400000")
        page.rate_input.clear()
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_without_loan_term_ok(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("House")
        page.date_input.setText("2010-01-01")
        page.price_input.setText("300000")
        page.current_value_input.setText("400000")
        page.loan_term_input.clear()
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_requires_current_value(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("House")
        page.date_input.setText("2010-01-01")
        page.price_input.setText("300000")
        page.current_value_input.clear()
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_called_once()
        assert list_active_properties(conn) == []

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_uses_user_entered_mortgage_payment(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("House")
        page.date_input.setText("2010-01-01")
        page.price_input.setText("300000")
        page.current_value_input.setText("400000")
        page.mortgage_input.setText("200000")
        page.rate_input.setText("6.5")
        page.loan_term_input.setText("30")
        page.mortgage_pmt_input.setText("1500")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].monthly_mortgage_payment == 1500.0

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_creates_active_asset(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("My House")
        page.date_input.setText("2015-06-01")
        page.price_input.setText("400000")
        page.current_value_input.setText("500000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].status == "active"

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_add_transaction_is_zero(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("House")
        page.date_input.setText("2010-01-01")
        page.price_input.setText("300000")
        page.current_value_input.setText("400000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        txns = list_transactions(conn, txn_type="add_property")
        assert len(txns) == 1
        assert txns[0].total_amount == 0.0

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_appears_in_analyze_all(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("My House")
        page.date_input.setText("2015-06-01")
        page.price_input.setText("400000")
        page.current_value_input.setText("500000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        analyses = analyze_all_properties(conn)
        assert len(analyses) == 1
        assert analyses[0].name == "My House"

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_rent_settlement_after_cashflow_start(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("Rental")
        page.date_input.setText("2010-01-01")
        page.price_input.setText("300000")
        page.current_value_input.setText("400000")
        page.rent_input.setText("2500")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        before = settle_due_rent(conn, "2026-06-30")
        assert len(before) == 0

        after = settle_due_rent(conn, "2026-07-01")
        assert len(after) == 1


# --- GUI entry type switching behavior ---


class TestEntryTypeSwitching:
    def test_switching_to_new_purchase_sets_date_to_today(self, conn):
        from datetime import date
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        assert page.date_input.text() == date.today().isoformat()

    def test_switching_to_existing_clears_date(self, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.entry_type_combo.setCurrentIndex(0)
        assert page.date_input.text() == ""

    def test_new_purchase_shows_hint_about_cash_deduction(self, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        hint = page._entry_type_hint.text().lower()
        assert "deducted" in hint or "cash" in hint

    def test_existing_shows_hint_about_no_cash(self, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        hint = page._entry_type_hint.text().lower()
        assert "no cash" in hint or "already own" in hint
        assert "history only" in hint or "property history" in hint

    def test_new_purchase_submit_button_says_new_purchase(self, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        assert "New Purchase" in page._submit_btn.text()

    def test_existing_submit_button_says_add_property(self, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        assert "Add Property" in page._submit_btn.text()


# --- Vacancy/rate percent input tests ---


class TestPercentInputFields:
    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_vacancy_accepts_user_friendly_percent(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("Rental")
        page.date_input.setText("2015-01-01")
        page.price_input.setText("300000")
        page.current_value_input.setText("350000")
        page.vacancy_input.setText("5")
        page.rent_input.setText("2000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].vacancy_rate == pytest.approx(0.05)

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_rate_accepts_user_friendly_percent(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("House")
        page.date_input.setText("2015-01-01")
        page.price_input.setText("300000")
        page.current_value_input.setText("350000")
        page.mortgage_input.setText("200000")
        page.rate_input.setText("6.5")
        page.loan_term_input.setText("30")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].mortgage_interest_rate == pytest.approx(0.065)


# --- Entry type combo: no planned_purchase exposed ---


class TestEntryTypeComboNoPlannedPurchase:
    """Verify the entry type combo does not expose planned_purchase."""

    def test_combo_does_not_contain_planned_purchase_data(self, conn):
        page = RealEstatePage(conn)
        data_values = [page.entry_type_combo.itemData(i) for i in range(page.entry_type_combo.count())]
        assert "planned_purchase" not in data_values

    def test_combo_does_not_contain_planned_purchase_text(self, conn):
        page = RealEstatePage(conn)
        labels = [page.entry_type_combo.itemText(i) for i in range(page.entry_type_combo.count())]
        for label in labels:
            assert "Planned" not in label

    def test_combo_has_exactly_two_items(self, conn):
        page = RealEstatePage(conn)
        assert page.entry_type_combo.count() == 2
        assert page.entry_type_combo.itemData(0) == "existing_property"
        assert page.entry_type_combo.itemData(1) == "new_purchase"


# --- New Purchase UI text: no scenario/no-impact language ---


class TestNewPurchaseUIText:
    """Verify New Purchase UI text uses purchase-impact wording, no scenario language."""

    def test_new_purchase_hint_no_scenario_language(self, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        hint = page._entry_type_hint.text().lower()
        assert "scenario" not in hint
        assert "do not affect" not in hint
        assert "no cash impact" not in hint
        assert "no impact" not in hint

    def test_new_purchase_hint_mentions_cash_deduction(self, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        hint = page._entry_type_hint.text().lower()
        assert "deducted" in hint or "deduct" in hint

    def test_new_purchase_date_label_says_purchase_date(self, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        assert "Purchase Date" in page._date_label.text()
        assert "Target" not in page._date_label.text()

    def test_new_purchase_submit_button_text(self, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        assert page._submit_btn.text() == "Add New Purchase"


# --- Section title mnemonic fix ---


class TestSectionTitleMnemonicFix:
    """Verify section titles use 'and' not '&' to avoid Qt mnemonic artifacts."""

    def test_value_section_uses_and(self, conn):
        page = RealEstatePage(conn)
        groups = page.findChildren(type(page).__mro__[0].__mro__[0])
        from PySide6.QtWidgets import QGroupBox
        group_boxes = page.findChildren(QGroupBox)
        titles = [g.title() for g in group_boxes]
        assert "Value and Loan" in titles
        assert "Value & Loan" not in titles

    def test_income_section_uses_and(self, conn):
        from PySide6.QtWidgets import QGroupBox
        page = RealEstatePage(conn)
        group_boxes = page.findChildren(QGroupBox)
        titles = [g.title() for g in group_boxes]
        assert "Income and Expenses" in titles
        assert "Income & Expenses" not in titles

    def test_no_ampersand_in_any_section_title(self, conn):
        from PySide6.QtWidgets import QGroupBox
        page = RealEstatePage(conn)
        group_boxes = page.findChildren(QGroupBox)
        for g in group_boxes:
            assert "&" not in g.title(), f"Section title '{g.title()}' contains '&' which causes mnemonic artifacts"


# --- New Purchase: cashflow_start_date default ---


class TestNewPurchaseCashflowStartDefault:
    """Verify New Purchase defaults cashflow_start_date to first day of next month."""

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_default_cashflow_start(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("400000")
        page.down_payment_input.setText("80000")
        page.mortgage_input.setText("320000")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].cashflow_start_date == first_day_next_month()

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_property_default_cashflow_start(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("Old House")
        page.date_input.setText("2015-01-01")
        page.price_input.setText("300000")
        page.current_value_input.setText("400000")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].cashflow_start_date == first_day_next_month()


# --- New Purchase: entry_type stored ---


class TestNewPurchaseEntryTypeStored:
    """Verify New Purchase stores entry_type='new_purchase' on the property record."""

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_entry_type_is_new_purchase(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("500000")
        page.down_payment_input.setText("100000")
        page.mortgage_input.setText("400000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].entry_type == "new_purchase"

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_existing_property_entry_type_is_existing_property(self, mock_warning, conn):
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(0)
        page.name_input.setText("Old House")
        page.date_input.setText("2010-01-01")
        page.price_input.setText("300000")
        page.current_value_input.setText("400000")
        page.cashflow_start_input.setText("2026-05-01")
        page._submit()

        mock_warning.assert_not_called()
        props = list_active_properties(conn)
        assert len(props) == 1
        assert props[0].entry_type == "existing_property"


# --- New Purchase: rent settlement no backfill ---


class TestNewPurchaseNoRentBackfill:
    """Verify New Purchase does not backfill rent from the purchase date to cashflow_start_date."""

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_no_rent_before_cashflow_start_date(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New Rental")
        page.date_input.setText("2026-01-15")
        page.price_input.setText("400000")
        page.down_payment_input.setText("80000")
        page.mortgage_input.setText("320000")
        page.rent_input.setText("2500")
        page.cashflow_start_input.setText("2026-06-01")
        page._submit()

        mock_warning.assert_not_called()
        created = settle_due_rent(conn, "2026-05-31")
        assert len(created) == 0

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_rent_starts_on_cashflow_start_date(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New Rental")
        page.date_input.setText("2026-01-15")
        page.price_input.setText("400000")
        page.down_payment_input.setText("80000")
        page.mortgage_input.setText("320000")
        page.rent_input.setText("2500")
        page.cashflow_start_input.setText("2026-06-01")
        page._submit()

        mock_warning.assert_not_called()
        created = settle_due_rent(conn, "2026-06-01")
        assert len(created) == 1
        assert created[0].date == "2026-06-01"
        assert created[0].total_amount == 2500.0

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_no_backfill_from_purchase_date(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New Rental")
        page.date_input.setText("2026-01-15")
        page.price_input.setText("400000")
        page.down_payment_input.setText("80000")
        page.mortgage_input.setText("320000")
        page.rent_input.setText("2500")
        page.cashflow_start_input.setText("2026-06-01")
        page._submit()

        mock_warning.assert_not_called()
        created = settle_due_rent(conn, "2026-08-01")
        dates = [t.date for t in created]
        for d in dates:
            assert d >= "2026-06-01", f"Rent dated {d} is before cashflow_start_date 2026-06-01"


# --- Legacy planned records ---


class TestLegacyPlannedRecords:
    """Verify legacy planned records are handled correctly in the GUI."""

    def test_legacy_planned_records_load_in_planned_table(self, conn):
        """Old planned records created via the ledger should appear in the legacy table."""
        add_property(
            conn, "2026-06-01", symbol="H1", name="Legacy Dream House",
            purchase_price=600000.0, mortgage_balance=480000.0,
            acquisition_mode="planned_purchase",
        )
        page = RealEstatePage(conn)
        page.refresh()
        assert page.planned_table.rowCount() == 1
        assert page.table.rowCount() == 0

    def test_legacy_planned_label_says_legacy(self, conn):
        add_property(
            conn, "2026-06-01", symbol="H1", name="Legacy Dream House",
            purchase_price=600000.0,
            acquisition_mode="planned_purchase",
        )
        page = RealEstatePage(conn)
        page.refresh()
        assert "Legacy" in page._planned_label.text()

    def test_legacy_planned_hidden_when_no_records(self, conn):
        page = RealEstatePage(conn)
        page.refresh()
        assert page._planned_label.isHidden()
        assert page.planned_table.isHidden()
        assert page.delete_planned_btn.isHidden()

    def test_legacy_planned_not_hidden_when_records_exist(self, conn):
        add_property(
            conn, "2026-06-01", symbol="H1", name="Legacy Dream House",
            purchase_price=600000.0,
            acquisition_mode="planned_purchase",
        )
        page = RealEstatePage(conn)
        page.refresh()
        assert not page._planned_label.isHidden()
        assert not page.planned_table.isHidden()
        assert not page.delete_planned_btn.isHidden()

    def test_legacy_planned_not_in_active_table(self, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        add_property(
            conn, "2026-06-01", symbol="H1", name="Legacy Planned",
            purchase_price=600000.0,
            acquisition_mode="planned_purchase",
        )
        add_property(
            conn, "2025-02-01", symbol="H2", name="Active House",
            purchase_price=400000.0, mortgage_balance=300000.0,
            acquisition_mode="new_purchase",
        )
        page = RealEstatePage(conn)
        page.refresh()
        assert page.table.rowCount() == 1
        assert page.planned_table.rowCount() == 1

    def test_delete_planned_button_says_legacy(self, conn):
        page = RealEstatePage(conn)
        assert "Legacy" in page.delete_planned_btn.text()

    def test_ui_form_cannot_create_planned_purchase(self, conn):
        """The UI form should never send acquisition_mode='planned_purchase' to the ledger."""
        page = RealEstatePage(conn)
        data_values = [page.entry_type_combo.itemData(i) for i in range(page.entry_type_combo.count())]
        assert "planned_purchase" not in data_values


# --- New Purchase summary tests ---


class TestNewPurchaseSummary:
    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_calculates_mortgage_from_rate_and_term(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("500000")
        page.down_payment_input.setText("100000")
        page.rate_input.setText("6.5")
        page.loan_term_input.setText("30")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        active = list_active_properties(conn)
        assert len(active) == 1
        assert active[0].mortgage_balance == pytest.approx(400000)
        assert active[0].monthly_mortgage_payment > 2000

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_manual_mortgage_override(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("500000")
        page.down_payment_input.setText("100000")
        page.rate_input.setText("6.5")
        page.loan_term_input.setText("30")
        page.mortgage_pmt_input.setText("3000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        active = list_active_properties(conn)
        assert len(active) == 1
        assert active[0].monthly_mortgage_payment == pytest.approx(3000)

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_dp_percent_calculates_mortgage_balance(self, mock_warning, conn):
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("500000")
        page.down_payment_input.setText("20")
        page.dp_type_combo.setCurrentIndex(1)
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        active = list_active_properties(conn)
        assert len(active) == 1
        assert active[0].down_payment == pytest.approx(100000)
        assert active[0].mortgage_balance == pytest.approx(400000)

    @patch("src.gui.pages.real_estate.QMessageBox.warning", return_value=None)
    def test_new_purchase_stores_correct_ltv(self, mock_warning, conn):
        from src.engines.property_calculator import calc_ltv
        deposit_cash(conn, "2025-01-01", 200000.0)
        page = RealEstatePage(conn)
        page.entry_type_combo.setCurrentIndex(1)
        page.name_input.setText("New House")
        page.date_input.setText("2026-06-01")
        page.price_input.setText("500000")
        page.down_payment_input.setText("100000")
        page.cashflow_start_input.setText("2026-07-01")
        page._submit()

        mock_warning.assert_not_called()
        active = list_active_properties(conn)
        ltv = calc_ltv(active[0].mortgage_balance, active[0].current_value or active[0].purchase_price)
        assert ltv == pytest.approx(0.8)


# --- Layout / scrollbar fix tests ---


class TestRealEstatePageLayout:
    """Verify layout fixes: no horizontal scrollbar, word-wrapping hint label,
    responsive form fields, and stretch-mode table columns."""

    def _find_scroll_area(self, page):
        return page.findChild(QScrollArea)

    def test_scroll_area_horizontal_scrollbar_always_off(self, conn):
        page = RealEstatePage(conn)
        scroll = self._find_scroll_area(page)
        assert scroll is not None
        assert scroll.horizontalScrollBarPolicy() == Qt.ScrollBarPolicy.ScrollBarAlwaysOff

    def test_entry_type_hint_word_wrap_enabled(self, conn):
        page = RealEstatePage(conn)
        assert page._entry_type_hint.wordWrap() is True

    def test_entry_type_hint_has_height_for_width(self, conn):
        page = RealEstatePage(conn)
        page._entry_type_hint.setText("Some wrapping text for testing")
        assert page._entry_type_hint.hasHeightForWidth() is True

    def test_form_sections_use_all_non_fixed_fields_grow(self, conn):
        page = RealEstatePage(conn)
        group_boxes = page.findChildren(QGroupBox)
        assert len(group_boxes) > 0
        for group in group_boxes:
            form = group.findChild(QFormLayout)
            if form is not None:
                assert form.fieldGrowthPolicy() == QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow

    def test_active_table_property_column_stretches(self, conn):
        page = RealEstatePage(conn)
        header = page.table.horizontalHeader()
        assert header.sectionResizeMode(0) == QHeaderView.ResizeMode.Stretch

    def test_active_table_numeric_columns_resize_to_contents(self, conn):
        page = RealEstatePage(conn)
        header = page.table.horizontalHeader()
        for col in range(1, page.table.columnCount()):
            assert header.sectionResizeMode(col) == QHeaderView.ResizeMode.ResizeToContents

    def test_active_table_stretch_last_section_off(self, conn):
        page = RealEstatePage(conn)
        header = page.table.horizontalHeader()
        assert header.stretchLastSection() is False

    def test_warnings_table_message_column_stretches(self, conn):
        page = RealEstatePage(conn)
        header = page.warn_table.horizontalHeader()
        assert header.sectionResizeMode(1) == QHeaderView.ResizeMode.Stretch

    def test_warnings_table_severity_column_resizes_to_contents(self, conn):
        page = RealEstatePage(conn)
        header = page.warn_table.horizontalHeader()
        assert header.sectionResizeMode(0) == QHeaderView.ResizeMode.ResizeToContents

    def test_planned_table_property_column_stretches(self, conn):
        page = RealEstatePage(conn)
        header = page.planned_table.horizontalHeader()
        assert header.sectionResizeMode(0) == QHeaderView.ResizeMode.Stretch

    def test_planned_table_numeric_columns_resize_to_contents(self, conn):
        page = RealEstatePage(conn)
        header = page.planned_table.horizontalHeader()
        for col in range(1, page.planned_table.columnCount()):
            assert header.sectionResizeMode(col) == QHeaderView.ResizeMode.ResizeToContents

    def test_tables_use_adjust_ignored_to_prevent_layout_loop(self, conn):
        page = RealEstatePage(conn)
        ignored = QAbstractScrollArea.SizeAdjustPolicy.AdjustIgnored
        assert page.table.sizeAdjustPolicy() == ignored
        assert page.planned_table.sizeAdjustPolicy() == ignored
        assert page.warn_table.sizeAdjustPolicy() == ignored

    def test_tables_start_with_compact_fixed_height(self, conn):
        page = RealEstatePage(conn)
        for tbl in [page.table, page.planned_table, page.warn_table]:
            assert tbl.maximumHeight() == tbl.minimumHeight()
            assert tbl.maximumHeight() < 100

    def test_scroll_area_vertical_scrollbar_always_on(self, conn):
        page = RealEstatePage(conn)
        scroll = self._find_scroll_area(page)
        assert scroll is not None
        assert scroll.verticalScrollBarPolicy() == Qt.ScrollBarPolicy.ScrollBarAlwaysOn

    def test_constrained_width_no_horizontal_scrollbar(self, conn, qapp):
        page = RealEstatePage(conn)
        page.resize(400, 800)
        page.show()
        qapp.processEvents()
        scroll = self._find_scroll_area(page)
        assert scroll is not None
        assert not scroll.horizontalScrollBar().isVisible()
        page.hide()
