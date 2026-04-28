import pytest
from unittest.mock import patch

from PySide6.QtWidgets import QMessageBox
from src.gui.pages.transactions import TXN_TYPES, AMOUNT_REQUIRED, TransactionsPage
from src.models.security_master import SecurityMasterRecord
from src.storage.database import init_db
from src.storage.security_master_repo import upsert_security
from src.storage.asset_repo import get_asset_by_symbol, list_assets


# --- Module-level constants ---

def test_txn_types_does_not_include_add_property():
    assert "add_property" not in TXN_TYPES


def test_amount_required_does_not_include_add_property():
    assert "add_property" not in AMOUNT_REQUIRED


def test_txn_types_includes_expected():
    for t in ("deposit_cash", "withdraw_cash", "buy", "sell", "add_debt"):
        assert t in TXN_TYPES


# --- GUI instantiation ---

@pytest.fixture
def page():
    conn = init_db(":memory:")
    p = TransactionsPage(conn)
    yield p
    conn.close()


def test_transactions_page_instantiates(page):
    assert page is not None


def test_combo_does_not_contain_add_property(page):
    combo = page.txn_type
    items = [combo.itemData(i) for i in range(combo.count())]
    assert "add_property" not in items


# --- Search / Add Selected Asset ---

@pytest.fixture
def page_with_universe():
    conn = init_db(":memory:")
    upsert_security(conn, SecurityMasterRecord(
        symbol="QQQ", name="Invesco QQQ Trust", asset_type="etf", exchange="NASDAQ",
    ))
    p = TransactionsPage(conn)
    yield p
    conn.close()


def _run_search(page):
    page.search_input.setText("QQQ")
    page._search_securities()


def test_search_populates_results_table(page_with_universe):
    _run_search(page_with_universe)
    assert page_with_universe.search_results_table.rowCount() == 1
    assert page_with_universe._search_results[0].symbol == "QQQ"


def test_selecting_row_caches_index(page_with_universe):
    _run_search(page_with_universe)
    page_with_universe.search_results_table.selectRow(0)
    assert page_with_universe._selected_search_result_index == 0


def test_new_search_clears_cached_index(page_with_universe):
    _run_search(page_with_universe)
    page_with_universe.search_results_table.selectRow(0)
    assert page_with_universe._selected_search_result_index == 0
    _run_search(page_with_universe)
    assert page_with_universe._selected_search_result_index is None


def test_empty_search_clears_cached_index(page_with_universe):
    _run_search(page_with_universe)
    page_with_universe.search_results_table.selectRow(0)
    page_with_universe.search_input.setText("")
    page_with_universe._search_securities()
    assert page_with_universe._selected_search_result_index is None


@patch.object(QMessageBox, "information")
def test_add_selected_asset_normal_selection(mock_info, page_with_universe):
    page = page_with_universe
    _run_search(page)
    page.search_results_table.selectRow(0)

    page._add_selected_asset()

    asset = get_asset_by_symbol(page.conn, "QQQ")
    assert asset is not None
    assert asset.symbol == "QQQ"

    combo_symbols = [
        page.asset_combo.itemText(i) for i in range(page.asset_combo.count())
    ]
    assert any("QQQ" in s for s in combo_symbols)
    mock_info.assert_called_once()


@patch.object(QMessageBox, "information")
def test_add_selected_asset_fallback_to_cached_index(mock_info, page_with_universe):
    page = page_with_universe
    _run_search(page)
    page.search_results_table.selectRow(0)
    assert page._selected_search_result_index == 0

    page.search_results_table.clearSelection()
    selected = page.search_results_table.selectionModel().selectedRows()
    assert len(selected) == 0

    page._add_selected_asset()

    asset = get_asset_by_symbol(page.conn, "QQQ")
    assert asset is not None
    assert asset.symbol == "QQQ"

    combo_data = [
        page.asset_combo.itemData(i) for i in range(page.asset_combo.count())
    ]
    assert asset.id in combo_data
    mock_info.assert_called_once()


@patch.object(QMessageBox, "information")
def test_add_selected_asset_reselects_row(mock_info, page_with_universe):
    page = page_with_universe
    _run_search(page)
    page.search_results_table.selectRow(0)

    page._add_selected_asset()

    selected = page.search_results_table.selectionModel().selectedRows()
    assert len(selected) == 1
    assert selected[0].row() == 0


def test_add_selected_asset_noop_when_no_selection_or_cache(page_with_universe):
    page = page_with_universe
    _run_search(page)
    assert page._selected_search_result_index is None

    page._add_selected_asset()

    asset = get_asset_by_symbol(page.conn, "QQQ")
    assert asset is None
    assert page.asset_combo.count() == 0
