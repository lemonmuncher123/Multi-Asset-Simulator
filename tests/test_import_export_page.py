import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from PySide6.QtWidgets import QGroupBox, QComboBox, QPushButton, QTabWidget, QMessageBox
from src.storage.database import init_db
from src.gui.pages.import_export import ImportExportPage
from src.models.transaction import Transaction
from src.storage.transaction_repo import create_transaction
from src.engines.reports import generate_monthly_report
from src.storage.report_repo import list_reports


@pytest.fixture
def page():
    conn = init_db(":memory:")
    p = ImportExportPage(conn)
    yield p
    conn.close()


@pytest.fixture
def page_with_reports():
    conn = init_db(":memory:")
    create_transaction(conn, Transaction(
        date="2025-06-01", txn_type="deposit_cash",
        total_amount=50000.0, currency="USD",
    ))
    generate_monthly_report(conn, 2025, 6)
    p = ImportExportPage(conn)
    yield p
    conn.close()


# --- Instantiation ---

def test_page_instantiates(page):
    assert page is not None


# --- Reports group exists ---

def test_reports_group_exists(page):
    assert hasattr(page, "page_tabs")
    tab_titles = [page.page_tabs.tabText(i) for i in range(page.page_tabs.count())]
    assert "Reports" in tab_titles


# --- Report type selector ---

def test_report_type_combo_exists(page):
    assert hasattr(page, "report_type_combo")
    assert isinstance(page.report_type_combo, QComboBox)
    items = [page.report_type_combo.itemText(i) for i in range(page.report_type_combo.count())]
    assert "Monthly" in items
    assert "Annual" in items
    data_items = [page.report_type_combo.itemData(i) for i in range(page.report_type_combo.count())]
    assert "monthly" in data_items
    assert "annual" in data_items


# --- Report list/table ---

def test_report_list_table_exists(page):
    assert hasattr(page, "report_list_table")
    headers = []
    for i in range(page.report_list_table.columnCount()):
        item = page.report_list_table.horizontalHeaderItem(i)
        if item:
            headers.append(item.text())
    assert "Period" in headers
    assert "Net Cash Flow" in headers


# --- Buttons ---

def test_generate_missing_button_exists(page):
    buttons = page.findChildren(QPushButton)
    texts = [b.text() for b in buttons]
    assert "Generate Missing Reports" in texts


def test_export_selected_button_exists(page):
    buttons = page.findChildren(QPushButton)
    texts = [b.text() for b in buttons]
    assert "Export Selected Report" in texts


def test_refresh_reports_button_exists(page):
    buttons = page.findChildren(QPushButton)
    texts = [b.text() for b in buttons]
    assert "Refresh Reports" in texts


def test_generate_selected_period_button_exists(page):
    buttons = page.findChildren(QPushButton)
    texts = [b.text() for b in buttons]
    assert "Generate Selected Period" in texts


# --- Detail tabs ---

def test_report_detail_tabs_exist(page):
    assert hasattr(page, "report_detail_tabs")
    assert isinstance(page.report_detail_tabs, QTabWidget)
    tab_titles = [page.report_detail_tabs.tabText(i) for i in range(page.report_detail_tabs.count())]
    assert "Summary" in tab_titles
    assert "Operations" in tab_titles
    assert "Transactions" in tab_titles
    assert "Journal" in tab_titles
    assert "Current Snapshot" in tab_titles


# --- Selecting a report populates detail tables ---

def test_selecting_report_populates_details(page_with_reports):
    page = page_with_reports
    page._refresh_report_list()
    assert page.report_list_table.rowCount() == 1

    page._on_report_selected(0, 0, -1, -1)
    assert page.report_summary_table.rowCount() > 0
    assert page.report_snapshot_table.rowCount() > 0


# --- refresh() does not crash ---

def test_refresh_does_not_crash(page):
    page.refresh()


def test_refresh_with_reports_shows_data(page_with_reports):
    page = page_with_reports
    page.page_tabs.setCurrentIndex(1)
    page.refresh()
    assert page.report_list_table.rowCount() == 1


# === 1. Desktop helper tests ===


class TestDesktopHelpers:
    """_desktop_dir and _desktop_file resolve to ~/Desktop or home fallback."""

    def test_desktop_dir_returns_desktop_when_it_exists(self, page, tmp_path, monkeypatch):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        assert page._desktop_dir() == str(tmp_path / "Desktop")

    def test_desktop_dir_returns_home_when_no_desktop(self, page, tmp_path, monkeypatch):
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        assert page._desktop_dir() == str(tmp_path)

    def test_desktop_file_under_desktop(self, page, tmp_path, monkeypatch):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        result = page._desktop_file("assets.csv")
        assert result == str(tmp_path / "Desktop" / "assets.csv")

    def test_desktop_file_falls_back_to_home(self, page, tmp_path, monkeypatch):
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        result = page._desktop_file("assets.csv")
        assert result == str(tmp_path / "assets.csv")

    def test_desktop_file_folder_name_under_desktop(self, page, tmp_path, monkeypatch):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        result = page._desktop_file("portfolio_backup")
        assert result == str(tmp_path / "Desktop" / "portfolio_backup")

    def test_desktop_file_folder_name_falls_back_to_home(self, page, tmp_path, monkeypatch):
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        result = page._desktop_file("portfolio_backup")
        assert result == str(tmp_path / "portfolio_backup")


# === 2. Simple CSV dialog defaults ===


class TestSimpleCsvDialogDefaults:
    """All CSV import/export dialogs default to ~/Desktop (or home fallback)."""

    @patch("src.gui.pages.import_export.QFileDialog.getSaveFileName", return_value=("", ""))
    def test_export_assets_default_ends_in_desktop_assets_csv(
        self, mock_save, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._export_assets()
        default_path = mock_save.call_args[0][2]
        assert default_path == str(tmp_path / "Desktop" / "assets.csv")

    @patch("src.gui.pages.import_export.QFileDialog.getSaveFileName", return_value=("", ""))
    def test_export_transactions_default_ends_in_desktop_transactions_csv(
        self, mock_save, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._export_transactions()
        default_path = mock_save.call_args[0][2]
        assert default_path == str(tmp_path / "Desktop" / "transactions.csv")

    @patch("src.gui.pages.import_export.QFileDialog.getSaveFileName", return_value=("", ""))
    def test_export_summary_default_ends_in_desktop_portfolio_summary_csv(
        self, mock_save, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._export_summary()
        default_path = mock_save.call_args[0][2]
        assert default_path == str(tmp_path / "Desktop" / "portfolio_summary.csv")

    @patch("src.gui.pages.import_export.QFileDialog.getOpenFileName", return_value=("", ""))
    def test_import_assets_default_dir_is_desktop(
        self, mock_open, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._import_assets()
        default_dir = mock_open.call_args[0][2]
        assert default_dir == str(tmp_path / "Desktop")

    @patch("src.gui.pages.import_export.QFileDialog.getOpenFileName", return_value=("", ""))
    def test_import_transactions_default_dir_is_desktop(
        self, mock_open, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._import_transactions()
        default_dir = mock_open.call_args[0][2]
        assert default_dir == str(tmp_path / "Desktop")

    @patch("src.gui.pages.import_export.QFileDialog.getOpenFileName", return_value=("", ""))
    def test_import_assets_falls_back_to_home(
        self, mock_open, page, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._import_assets()
        default_dir = mock_open.call_args[0][2]
        assert default_dir == str(tmp_path)

    @patch("src.gui.pages.import_export.QFileDialog.getOpenFileName", return_value=("", ""))
    def test_import_transactions_falls_back_to_home(
        self, mock_open, page, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._import_transactions()
        default_dir = mock_open.call_args[0][2]
        assert default_dir == str(tmp_path)

    @patch("src.gui.pages.import_export.QFileDialog.getOpenFileName", return_value=("", ""))
    def test_open_single_csv_default_dir_is_desktop(
        self, mock_open, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._open_single_csv()
        default_dir = mock_open.call_args[0][2]
        assert default_dir == str(tmp_path / "Desktop")


# === 3. Full import zip/folder selection ===


class TestFullImportZipFolderSelection:
    """_full_import offers zip/folder choice, uses Desktop-default dialogs."""

    @patch("src.gui.pages.import_export.QFileDialog.getOpenFileName", return_value=("", ""))
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Zip file (.zip)", True))
    def test_zip_import_calls_open_file_with_desktop(
        self, mock_item, mock_open, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._full_import()
        mock_open.assert_called_once()
        default_dir = mock_open.call_args[0][2]
        assert default_dir == str(tmp_path / "Desktop")

    @patch("src.gui.pages.import_export.QFileDialog.getExistingDirectory", return_value="")
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Folder", True))
    def test_folder_import_calls_dir_dialog_with_desktop(
        self, mock_item, mock_dir, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._full_import()
        mock_dir.assert_called_once()
        default_dir = mock_dir.call_args[0][2]
        assert default_dir == str(tmp_path / "Desktop")

    @patch("src.gui.pages.import_export.QFileDialog.getOpenFileName")
    @patch("src.gui.pages.import_export.QFileDialog.getExistingDirectory")
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Zip file (.zip)", False))
    def test_cancel_prompt_calls_no_file_dialog(
        self, mock_item, mock_dir, mock_open, page,
    ):
        page._full_import()
        mock_open.assert_not_called()
        mock_dir.assert_not_called()

    @patch("src.gui.pages.import_export.inspect_full_export", return_value=None)
    @patch("src.gui.pages.import_export.QMessageBox.warning")
    @patch("src.gui.pages.import_export.QFileDialog.getOpenFileName", return_value=("/tmp/bad.zip", "Zip Files"))
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Zip file (.zip)", True))
    def test_zip_invalid_manifest_shows_warning(
        self, mock_item, mock_open, mock_warn, mock_inspect, page,
    ):
        page._full_import()
        mock_warn.assert_called_once()
        assert "manifest.csv" in mock_warn.call_args[0][2]

    @patch("src.gui.pages.import_export.inspect_full_export", return_value=None)
    @patch("src.gui.pages.import_export.QMessageBox.warning")
    @patch("src.gui.pages.import_export.QFileDialog.getExistingDirectory", return_value="/tmp/bad_folder")
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Folder", True))
    def test_folder_invalid_manifest_shows_warning(
        self, mock_item, mock_dir, mock_warn, mock_inspect, page,
    ):
        page._full_import()
        mock_dir.assert_called_once()
        mock_warn.assert_called_once()
        assert "manifest.csv" in mock_warn.call_args[0][2]


# === 4. Export Reader zip/folder selection ===


class TestExportReaderZipFolderSelection:
    """_open_full_export offers zip/folder, then calls _load_export_reader."""

    @patch.object(ImportExportPage, "_load_export_reader")
    @patch("src.gui.pages.import_export.QFileDialog.getOpenFileName", return_value=("/tmp/backup.zip", "Zip Files"))
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Zip file (.zip)", True))
    def test_zip_calls_open_file_then_load_reader(
        self, mock_item, mock_open, mock_load, page,
    ):
        page._open_full_export()
        mock_open.assert_called_once()
        mock_load.assert_called_once_with("/tmp/backup.zip")

    @patch.object(ImportExportPage, "_load_export_reader")
    @patch("src.gui.pages.import_export.QFileDialog.getExistingDirectory", return_value="/tmp/export_folder")
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Folder", True))
    def test_folder_calls_dir_dialog_then_load_reader(
        self, mock_item, mock_dir, mock_load, page,
    ):
        page._open_full_export()
        mock_dir.assert_called_once()
        mock_load.assert_called_once_with("/tmp/export_folder")

    @patch.object(ImportExportPage, "_load_export_reader")
    @patch("src.gui.pages.import_export.QFileDialog.getOpenFileName")
    @patch("src.gui.pages.import_export.QFileDialog.getExistingDirectory")
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Zip file (.zip)", False))
    def test_cancel_calls_nothing(
        self, mock_item, mock_dir, mock_open, mock_load, page,
    ):
        page._open_full_export()
        mock_open.assert_not_called()
        mock_dir.assert_not_called()
        mock_load.assert_not_called()

    @patch("src.gui.pages.import_export.QFileDialog.getOpenFileName", return_value=("", ""))
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Zip file (.zip)", True))
    def test_zip_dialog_defaults_to_desktop(
        self, mock_item, mock_open, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._open_full_export()
        default_dir = mock_open.call_args[0][2]
        assert default_dir == str(tmp_path / "Desktop")

    @patch("src.gui.pages.import_export.QFileDialog.getExistingDirectory", return_value="")
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Folder", True))
    def test_folder_dialog_defaults_to_desktop(
        self, mock_item, mock_dir, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._open_full_export()
        default_dir = mock_dir.call_args[0][2]
        assert default_dir == str(tmp_path / "Desktop")


# === 5. Full export zip/folder selection ===


class TestFullExportZipFolderSelection:
    """_full_export offers zip/folder, defaults to Desktop, calls export_full_data."""

    @patch("src.gui.pages.import_export.export_full_data")
    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QFileDialog.getSaveFileName", return_value=("/tmp/test.zip", "Zip Files"))
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Zip file (.zip)", True))
    def test_zip_default_is_desktop_portfolio_backup_zip(
        self, mock_item, mock_save, mock_info, mock_export, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        mock_export.return_value = MagicMock(success=True, message="ok")
        page._full_export()
        default_path = mock_save.call_args[0][2]
        assert default_path == str(tmp_path / "Desktop" / "portfolio_backup.zip")

    @patch("src.gui.pages.import_export.export_full_data")
    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QInputDialog.getText", return_value=("my_backup", True))
    @patch("src.gui.pages.import_export.QFileDialog.getExistingDirectory")
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Folder", True))
    def test_folder_parent_defaults_to_desktop(
        self, mock_item, mock_dir, mock_text, mock_info, mock_export, page, tmp_path, monkeypatch,
    ):
        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        mock_dir.return_value = str(tmp_path / "Desktop")
        mock_export.return_value = MagicMock(success=True, message="ok")
        page._full_export()
        dir_default = mock_dir.call_args[0][2]
        assert dir_default == str(tmp_path / "Desktop")

    @patch("src.gui.pages.import_export.export_full_data")
    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QInputDialog.getText", return_value=("my_backup", True))
    @patch("src.gui.pages.import_export.QFileDialog.getExistingDirectory", return_value="/tmp/parent")
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Folder", True))
    def test_folder_export_path_combines_parent_and_name(
        self, mock_item, mock_dir, mock_text, mock_info, mock_export, page,
    ):
        mock_export.return_value = MagicMock(success=True, message="ok")
        page._full_export()
        mock_export.assert_called_once_with(page.conn, str(Path("/tmp/parent") / "my_backup"))

    @patch("src.gui.pages.import_export.export_full_data")
    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QFileDialog.getSaveFileName", return_value=("/tmp/test.zip", "Zip Files"))
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Zip file (.zip)", True))
    def test_zip_export_calls_export_full_data_with_path(
        self, mock_item, mock_save, mock_info, mock_export, page,
    ):
        mock_export.return_value = MagicMock(success=True, message="ok")
        page._full_export()
        mock_export.assert_called_once_with(page.conn, "/tmp/test.zip")

    @patch("src.gui.pages.import_export.export_full_data")
    @patch("src.gui.pages.import_export.QFileDialog.getSaveFileName")
    @patch("src.gui.pages.import_export.QFileDialog.getExistingDirectory")
    @patch("src.gui.pages.import_export.QInputDialog.getItem", return_value=("Zip file (.zip)", False))
    def test_cancel_calls_no_dialog_or_export(
        self, mock_item, mock_dir, mock_save, mock_export, page,
    ):
        page._full_export()
        mock_save.assert_not_called()
        mock_dir.assert_not_called()
        mock_export.assert_not_called()


# === 6. Report export default path ===


class TestReportExportDefaultPath:
    """_export_selected_report defaults to Desktop with type-appropriate filename."""

    @patch("src.gui.pages.import_export.QMessageBox.critical")
    @patch("src.gui.pages.import_export.QMessageBox.warning")
    @patch("src.gui.pages.import_export.QFileDialog.getSaveFileName", return_value=("", ""))
    def test_monthly_report_default_path(
        self, mock_save, mock_warn, mock_crit, page_with_reports, tmp_path, monkeypatch,
    ):
        page = page_with_reports
        page._refresh_report_list()
        page.report_list_table.setCurrentCell(0, 0)

        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._export_selected_report()

        default_path = mock_save.call_args[0][2]
        assert default_path == str(tmp_path / "Desktop" / "monthly_report_2025_06.xlsx")

    @patch("src.gui.pages.import_export.QMessageBox.critical")
    @patch("src.gui.pages.import_export.QMessageBox.warning")
    @patch("src.gui.pages.import_export.get_report")
    @patch("src.gui.pages.import_export.QFileDialog.getSaveFileName", return_value=("", ""))
    def test_annual_report_default_path(
        self, mock_save, mock_get_report, mock_warn, mock_crit, page, tmp_path, monkeypatch,
    ):
        mock_report = MagicMock()
        mock_report.report_type = "annual"
        mock_report.period_label = "2025"
        mock_report.report_json = '{"summary": {}}'
        mock_get_report.return_value = mock_report
        page._report_summary_cache = [mock_report]
        page.report_list_table.setRowCount(1)
        page.report_list_table.setCurrentCell(0, 0)

        (tmp_path / "Desktop").mkdir()
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        page._export_selected_report()

        default_path = mock_save.call_args[0][2]
        assert default_path == str(tmp_path / "Desktop" / "annual_report_2025.xlsx")


# === 7. Backend compatibility ===
# export_full_data, import_full_data, and inspect_full_export for both zip and
# folder paths are thoroughly covered by tests/test_full_data_io.py (25 tests).
# Run both files together to verify:
#   python3 -m pytest tests/test_import_export_page.py tests/test_full_data_io.py


# --- Export Reader tab exists ---


def test_export_reader_tab_exists(page):
    tab_titles = [page.page_tabs.tabText(i) for i in range(page.page_tabs.count())]
    assert "Export Reader" in tab_titles


# === 8. Lazy refresh tests ===


class TestLazyRefresh:
    """Reports list only refreshes when the Reports tab is active and dirty."""

    def test_refresh_marks_dirty(self, page):
        page._reports_dirty = False
        page.refresh()
        if page.page_tabs.currentIndex() != 1:
            assert page._reports_dirty is True

    def test_refresh_on_reports_tab_clears_dirty(self, page_with_reports):
        page = page_with_reports
        page.page_tabs.setCurrentIndex(1)
        page.refresh()
        assert page._reports_dirty is False

    def test_tab_change_to_reports_triggers_refresh(self, page_with_reports):
        page = page_with_reports
        page._reports_dirty = True
        page.page_tabs.setCurrentIndex(1)
        assert page._reports_dirty is False
        assert page.report_list_table.rowCount() == 1


# === 9. Delete Old Reports button ===


class TestDeleteOldReportsButton:
    """Delete Reports Before Auto Start button exists in the UI."""

    def test_button_exists(self, page):
        buttons = page.findChildren(QPushButton)
        texts = [b.text() for b in buttons]
        assert "Delete Reports Before Auto Start" in texts

    @patch("src.gui.pages.import_export.QMessageBox.warning")
    @patch("src.gui.pages.import_export.get_auto_report_start_date", return_value=None)
    def test_no_cutoff_warns_and_does_nothing(self, mock_start, mock_warn, page):
        page._delete_old_reports()
        mock_warn.assert_called_once()
        assert "no meaningful" in mock_warn.call_args[0][2].lower()

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QMessageBox.question", return_value=QMessageBox.StandardButton.No)
    @patch("src.gui.pages.import_export.get_auto_report_start_date")
    def test_user_declines_confirmation(self, mock_start, mock_question, mock_info, page):
        from datetime import date
        mock_start.return_value = date(2026, 3, 15)
        page._delete_old_reports()
        mock_question.assert_called_once()
        mock_info.assert_not_called()

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.delete_reports_before_date", return_value=5)
    @patch("src.gui.pages.import_export.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes)
    @patch("src.gui.pages.import_export.get_auto_report_start_date")
    def test_user_accepts_deletes_and_shows_count(
        self, mock_start, mock_question, mock_delete, mock_info, page,
    ):
        from datetime import date
        mock_start.return_value = date(2026, 3, 15)
        page._delete_old_reports()
        mock_question.assert_called_once()
        mock_delete.assert_called_once_with(page.conn, "2026-03-15")
        mock_info.assert_called_once()
        assert "5" in mock_info.call_args[0][2]


# === 10. Report info label ===


class TestReportInfoLabel:
    """Report info label shows count info when more reports exist than displayed."""

    def test_info_label_exists(self, page):
        assert hasattr(page, "_report_info_label")


# === 11. Generate Missing Reports confirmation gate ===


class TestGenerateMissingReportsConfirmation:
    """Large backfills are confirmation-gated via QMessageBox.question."""

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QMessageBox.critical")
    @patch("src.gui.pages.import_export.count_due_reports", return_value=0)
    def test_zero_pending_shows_up_to_date(self, mock_count, mock_crit, mock_info, page):
        page._generate_missing_reports()
        mock_info.assert_called_once()
        assert "up to date" in mock_info.call_args[0][2].lower()

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QMessageBox.critical")
    @patch("src.gui.pages.import_export.QMessageBox.question")
    @patch("src.gui.pages.import_export.generate_due_reports", return_value=[])
    @patch("src.gui.pages.import_export.count_due_reports", return_value=3)
    def test_small_count_skips_confirmation(
        self, mock_count, mock_gen, mock_question, mock_crit, mock_info, page,
    ):
        page._generate_missing_reports()
        mock_question.assert_not_called()
        mock_gen.assert_called_once()

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QMessageBox.critical")
    @patch("src.gui.pages.import_export.QMessageBox.question", return_value=QMessageBox.StandardButton.No)
    @patch("src.gui.pages.import_export.generate_due_reports")
    @patch("src.gui.pages.import_export.count_due_reports", return_value=50)
    def test_large_count_asks_confirmation_and_user_declines(
        self, mock_count, mock_gen, mock_question, mock_crit, mock_info, page,
    ):
        page._generate_missing_reports()
        mock_question.assert_called_once()
        assert "50" in mock_question.call_args[0][2]
        mock_gen.assert_not_called()

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QMessageBox.critical")
    @patch("src.gui.pages.import_export.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes)
    @patch("src.gui.pages.import_export.generate_due_reports", return_value=[])
    @patch("src.gui.pages.import_export.count_due_reports", return_value=50)
    def test_large_count_asks_confirmation_and_user_accepts(
        self, mock_count, mock_gen, mock_question, mock_crit, mock_info, page,
    ):
        page._generate_missing_reports()
        mock_question.assert_called_once()
        mock_gen.assert_called_once()


# === 12. New report management buttons ===


class TestNewReportButtons:
    """New delete/management buttons exist in the UI."""

    def test_delete_selected_report_button_exists(self, page):
        buttons = page.findChildren(QPushButton)
        texts = [b.text() for b in buttons]
        assert "Delete Selected Report" in texts

    def test_delete_current_type_reports_button_exists(self, page):
        buttons = page.findChildren(QPushButton)
        texts = [b.text() for b in buttons]
        assert "Delete Current Type Reports" in texts

    def test_delete_reports_by_date_range_button_exists(self, page):
        buttons = page.findChildren(QPushButton)
        texts = [b.text() for b in buttons]
        assert "Delete Reports By Date Range" in texts

    def test_delete_all_reports_button_exists(self, page):
        buttons = page.findChildren(QPushButton)
        texts = [b.text() for b in buttons]
        assert "Delete All Reports" in texts

    def test_report_stats_label_exists(self, page):
        assert hasattr(page, "_report_stats_label")


# === 13. Delete Selected Report flow ===


class TestDeleteSelectedReport:

    @patch("src.gui.pages.import_export.QMessageBox.warning")
    def test_no_selection_warns(self, mock_warn, page):
        page._delete_selected_report()
        mock_warn.assert_called_once()
        assert "select" in mock_warn.call_args[0][2].lower()

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QMessageBox.question", return_value=QMessageBox.StandardButton.No)
    def test_user_declines_confirmation(self, mock_question, mock_info, page_with_reports):
        page = page_with_reports
        page._refresh_report_list()
        page.report_list_table.setCurrentCell(0, 0)
        page._delete_selected_report()
        mock_question.assert_called_once()
        mock_info.assert_not_called()

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.delete_report")
    @patch("src.gui.pages.import_export.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes)
    def test_user_accepts_deletes_report(
        self, mock_question, mock_delete, mock_info, page_with_reports,
    ):
        page = page_with_reports
        page._refresh_report_list()
        page.report_list_table.setCurrentCell(0, 0)
        report_id = page._report_summary_cache[0].id
        page._delete_selected_report()
        mock_delete.assert_called_once_with(page.conn, report_id)
        mock_info.assert_called_once()


# === 14. Delete Current Type Reports flow ===


class TestDeleteCurrentTypeReports:

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.report_count", return_value=0)
    def test_no_reports_shows_info(self, mock_count, mock_info, page):
        page._delete_current_type_reports()
        mock_info.assert_called_once()
        assert "no" in mock_info.call_args[0][2].lower()

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QMessageBox.question", return_value=QMessageBox.StandardButton.No)
    @patch("src.gui.pages.import_export.report_count", return_value=5)
    def test_user_declines(self, mock_count, mock_question, mock_info, page):
        page._delete_current_type_reports()
        mock_question.assert_called_once()
        mock_info.assert_not_called()

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.delete_reports_by_type", return_value=5)
    @patch("src.gui.pages.import_export.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes)
    @patch("src.gui.pages.import_export.report_count", return_value=5)
    def test_user_accepts_deletes(self, mock_count, mock_question, mock_delete, mock_info, page):
        page._delete_current_type_reports()
        mock_delete.assert_called_once_with(page.conn, "monthly")
        mock_info.assert_called_once()
        assert "5" in mock_info.call_args[0][2]


# === 15. Delete Reports By Date Range flow ===


class TestDeleteReportsByDateRange:

    @patch("src.gui.pages.import_export.QInputDialog.getText", return_value=("", False))
    def test_cancel_start_date(self, mock_text, page):
        page._delete_reports_by_date_range()
        mock_text.assert_called_once()

    @patch("src.gui.pages.import_export.QInputDialog.getText")
    def test_cancel_end_date(self, mock_text, page):
        mock_text.side_effect = [("2025-01-01", True), ("", False)]
        page._delete_reports_by_date_range()
        assert mock_text.call_count == 2

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.delete_reports_in_period_range", return_value=3)
    @patch("src.gui.pages.import_export.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes)
    @patch("src.gui.pages.import_export.QInputDialog.getText")
    def test_user_accepts_deletes(self, mock_text, mock_question, mock_delete, mock_info, page):
        mock_text.side_effect = [("2025-01-01", True), ("2025-04-01", True)]
        page._delete_reports_by_date_range()
        mock_delete.assert_called_once_with(page.conn, "2025-01-01", "2025-04-01")
        mock_info.assert_called_once()
        assert "3" in mock_info.call_args[0][2]


# === 16. Delete All Reports flow ===


class TestDeleteAllReports:

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.get_report_stats", return_value={"total": 0, "monthly": 0, "annual": 0})
    def test_no_reports_shows_info(self, mock_stats, mock_info, page):
        page._delete_all_reports()
        mock_info.assert_called_once()
        assert "no reports" in mock_info.call_args[0][2].lower()

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QInputDialog.getText", return_value=("wrong text", True))
    @patch("src.gui.pages.import_export.get_report_stats", return_value={"total": 5, "monthly": 4, "annual": 1})
    def test_wrong_confirmation_text_aborts(self, mock_stats, mock_text, mock_info, page):
        page._delete_all_reports()
        mock_info.assert_not_called()

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.delete_all_reports", return_value=5)
    @patch("src.gui.pages.import_export.QInputDialog.getText", return_value=("DELETE REPORTS", True))
    @patch("src.gui.pages.import_export.get_report_stats", return_value={"total": 5, "monthly": 4, "annual": 1})
    def test_correct_confirmation_deletes(self, mock_stats, mock_text, mock_delete, mock_info, page):
        page._delete_all_reports()
        mock_delete.assert_called_once_with(page.conn)
        mock_info.assert_called_once()
        assert "5" in mock_info.call_args[0][2]

    @patch("src.gui.pages.import_export.QMessageBox.information")
    @patch("src.gui.pages.import_export.QInputDialog.getText", return_value=("DELETE REPORTS", False))
    @patch("src.gui.pages.import_export.get_report_stats", return_value={"total": 5, "monthly": 4, "annual": 1})
    def test_cancel_dialog_aborts(self, mock_stats, mock_text, mock_info, page):
        page._delete_all_reports()
        mock_info.assert_not_called()


# === 17. Clear report detail ===


class TestClearReportDetail:

    def test_clear_report_detail_empties_all_tables(self, page_with_reports):
        page = page_with_reports
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        assert page.report_summary_table.rowCount() > 0

        page._clear_report_detail()
        assert page.report_summary_table.rowCount() == 0
        assert page.report_ops_table.rowCount() == 0
        assert page.report_txns_table.rowCount() == 0
        assert page.report_journal_table.rowCount() == 0
        assert page.report_snapshot_table.rowCount() == 0


# === 18. Report stats label ===


class TestReportStatsLabel:

    def test_stats_label_updates_on_refresh(self, page_with_reports):
        page = page_with_reports
        page._refresh_report_list()
        text = page._report_stats_label.text()
        assert "Total:" in text
        assert "Monthly:" in text
        assert "Annual:" in text


# === Phase 1: New report detail tabs ===


class TestPhase1NewReportDetailTabs:

    def _tab_titles(self, page):
        return [
            page.report_detail_tabs.tabText(i)
            for i in range(page.report_detail_tabs.count())
        ]

    def test_performance_tab_exists(self, page):
        assert "Performance" in self._tab_titles(page)

    def test_cash_flow_breakdown_tab_exists(self, page):
        assert "Cash Flow Breakdown" in self._tab_titles(page)

    def test_trades_tab_exists(self, page):
        assert "Trades" in self._tab_titles(page)

    def test_real_estate_tab_exists(self, page):
        assert "Real Estate" in self._tab_titles(page)

    def test_debt_tab_exists(self, page):
        assert "Debt" in self._tab_titles(page)

    def test_existing_tabs_still_present(self, page):
        titles = self._tab_titles(page)
        for required in (
            "Summary", "Operations", "Transactions",
            "Journal", "Current Snapshot",
        ):
            assert required in titles


# === Phase 1: Render old reports without new sections ===


class TestPhase1RenderOldReport:
    """An older saved report_json without cash_flow_breakdown / performance /
    trades / real_estate / debt must render without crashing. Missing
    sections render as empty tables or N/A rows."""

    def _old_report_data(self):
        return {
            "summary": {
                "report_type": "monthly",
                "period_label": "2024-01",
                "period_start": "2024-01-01",
                "period_end": "2024-02-01",
                "generated_at": "2024-02-01T00:00:00",
                "transaction_count": 0,
                "beginning_cash": 0.0,
                "ending_cash": 0.0,
                "net_cash_flow": 0.0,
                "operating_net_income": 0.0,
                "total_inflow": 0.0,
                "total_outflow": 0.0,
                "total_fees": 0.0,
            },
            "operations": [],
            "transactions": [],
            "journal": [],
            "current_snapshot": {
                "note": "old",
                "cash": 0.0,
                "total_assets": 0.0,
                "total_liabilities": 0.0,
                "net_worth": 0.0,
            },
        }

    def test_render_old_report_does_not_crash(self, page):
        page._render_report_detail(self._old_report_data())

    def test_old_report_perf_table_shows_na_rows(self, page):
        page._render_report_detail(self._old_report_data())
        # Performance table is populated with N/A rows, not crashed.
        assert page.report_perf_table.rowCount() > 0
        values = [
            page.report_perf_table.item(r, 1).text()
            for r in range(page.report_perf_table.rowCount())
        ]
        assert any(v == "N/A" for v in values)

    def test_old_report_cfb_table_shows_na_rows(self, page):
        page._render_report_detail(self._old_report_data())
        assert page.report_cfb_table.rowCount() > 0
        values = [
            page.report_cfb_table.item(r, 2).text()
            for r in range(page.report_cfb_table.rowCount())
        ]
        assert any(v == "N/A" for v in values)

    def test_old_report_trades_re_debt_tables_empty(self, page):
        page._render_report_detail(self._old_report_data())
        assert page.report_trades_table.rowCount() == 0
        assert page.report_re_table.rowCount() == 0
        assert page.report_debt_table.rowCount() == 0


# === Phase 1: Render new report sections ===


@pytest.fixture
def page_with_full_report():
    """A page with one monthly report whose period contains funding,
    trade, real-estate, and debt activity — exercises every new tab."""
    from src.models.asset import Asset
    from src.storage.asset_repo import create_asset
    conn = init_db(":memory:")
    a = create_asset(conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    create_transaction(conn, Transaction(
        date="2025-06-01", txn_type="deposit_cash",
        total_amount=50000.0, currency="USD",
    ))
    create_transaction(conn, Transaction(
        date="2025-06-05", txn_type="buy", asset_id=a.id,
        quantity=10, price=150.0, total_amount=-1500.0, currency="USD", fees=5.0,
    ))
    create_transaction(conn, Transaction(
        date="2025-06-10", txn_type="receive_rent",
        total_amount=2000.0, currency="USD",
    ))
    create_transaction(conn, Transaction(
        date="2025-06-15", txn_type="pay_property_expense",
        total_amount=-300.0, currency="USD",
    ))
    create_transaction(conn, Transaction(
        date="2025-06-20", txn_type="add_debt",
        total_amount=10000.0, currency="USD",
    ))
    create_transaction(conn, Transaction(
        date="2025-06-25", txn_type="pay_mortgage",
        total_amount=-800.0, currency="USD",
    ))
    generate_monthly_report(conn, 2025, 6)
    p = ImportExportPage(conn)
    yield p
    conn.close()


class TestPhase1RenderNewReportSections:

    def test_perf_table_populated(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        assert page.report_perf_table.rowCount() > 0
        labels = {
            page.report_perf_table.item(r, 0).text()
            for r in range(page.report_perf_table.rowCount())
        }
        assert any("Beginning Net Worth" in label for label in labels)
        assert any("Ending Net Worth" in label for label in labels)
        assert any("Funding Flow" in label for label in labels)
        assert any("Approximate Investment Result" in label for label in labels)

    def test_perf_table_funding_flow_value_shown(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        rows = [
            (page.report_perf_table.item(r, 0).text(),
             page.report_perf_table.item(r, 1).text())
            for r in range(page.report_perf_table.rowCount())
        ]
        funding = [v for k, v in rows if "Funding Flow" in k]
        assert funding and "50,000.00" in funding[0]

    def test_cfb_table_populated_with_categories(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        rows = [
            (page.report_cfb_table.item(r, 0).text(),
             page.report_cfb_table.item(r, 1).text(),
             page.report_cfb_table.item(r, 2).text())
            for r in range(page.report_cfb_table.rowCount())
        ]
        categories = {row[0] for row in rows}
        for required in (
            "Funding Flow", "Trade Cash Flow", "Real Estate Cash Flow",
            "Debt Cash Flow", "Fees Total", "Other Cash Flow",
        ):
            assert required in categories

    def test_cfb_table_funding_deposits_value_shown(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        rows = [
            (page.report_cfb_table.item(r, 0).text(),
             page.report_cfb_table.item(r, 1).text(),
             page.report_cfb_table.item(r, 2).text())
            for r in range(page.report_cfb_table.rowCount())
        ]
        deposits = [
            v for cat, sub, v in rows
            if cat == "Funding Flow" and sub == "Deposits"
        ]
        assert deposits and "50,000.00" in deposits[0]

    def test_trades_table_populated(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        assert page.report_trades_table.rowCount() == 1
        assert page.report_trades_table.item(0, 1).text() == "buy"
        assert page.report_trades_table.item(0, 2).text() == "AAPL"

    def test_real_estate_table_populated(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        # rent + expense + mortgage all show up in the RE display section
        # (RE_TYPES, which still includes pay_mortgage).
        assert page.report_re_table.rowCount() >= 2
        types = {
            page.report_re_table.item(r, 1).text()
            for r in range(page.report_re_table.rowCount())
        }
        assert "receive_rent" in types
        assert "pay_property_expense" in types

    def test_debt_table_populated(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        assert page.report_debt_table.rowCount() >= 1
        types = {
            page.report_debt_table.item(r, 1).text()
            for r in range(page.report_debt_table.rowCount())
        }
        assert "add_debt" in types


# === Phase 1: Clear detail wipes new tables too ===


class TestPhase1ClearDetail:

    def test_clear_empties_all_new_tables(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        # All tables populated.
        assert page.report_perf_table.rowCount() > 0
        assert page.report_cfb_table.rowCount() > 0
        assert page.report_trades_table.rowCount() > 0

        page._clear_report_detail()
        assert page.report_perf_table.rowCount() == 0
        assert page.report_cfb_table.rowCount() == 0
        assert page.report_trades_table.rowCount() == 0
        assert page.report_re_table.rowCount() == 0
        assert page.report_debt_table.rowCount() == 0


# === Phase 2: Allocation + Risk Summary tabs ===


class TestPhase2AllocationRiskTabs:

    def _tab_titles(self, page):
        return [
            page.report_detail_tabs.tabText(i)
            for i in range(page.report_detail_tabs.count())
        ]

    def test_allocation_tab_exists(self, page):
        assert "Allocation" in self._tab_titles(page)

    def test_risk_summary_tab_exists(self, page):
        assert "Risk Summary" in self._tab_titles(page)

    def test_existing_tabs_still_present(self, page):
        titles = self._tab_titles(page)
        for required in (
            "Summary", "Performance", "Cash Flow Breakdown",
            "Operations", "Transactions", "Trades", "Real Estate",
            "Debt", "Journal", "Current Snapshot",
        ):
            assert required in titles


class TestPhase2RenderNewSections:
    """Selecting a new report populates allocation and risk-summary tabs."""

    def test_allocation_table_populated(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        assert page.report_alloc_table.rowCount() > 0

    def test_allocation_includes_source_row(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        rows = [
            (page.report_alloc_table.item(r, 0).text(),
             page.report_alloc_table.item(r, 1).text())
            for r in range(page.report_alloc_table.rowCount())
        ]
        sources = [v for k, v in rows if k == "Source"]
        assert sources and sources[0] in ("snapshot", "current")

    def test_allocation_includes_data_quality_note(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        rows = [
            (page.report_alloc_table.item(r, 0).text(),
             page.report_alloc_table.item(r, 1).text())
            for r in range(page.report_alloc_table.rowCount())
        ]
        notes = [v for k, v in rows if k == "Data Quality Note"]
        assert notes and notes[0]

    def test_risk_label_shows_total(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        assert "Total:" in page.report_risk_label.text()
        assert "actionable" in page.report_risk_label.text()


class TestPhase2RenderOldReportWithoutAllocOrRisk:
    """Older saved report_json without `allocation` / `risk_summary` must
    render without crashing."""

    def _old_report_data(self):
        return {
            "summary": {
                "report_type": "monthly",
                "period_label": "2024-01",
                "period_start": "2024-01-01",
                "period_end": "2024-02-01",
                "generated_at": "2024-02-01T00:00:00",
                "transaction_count": 0,
                "beginning_cash": 0.0,
                "ending_cash": 0.0,
                "net_cash_flow": 0.0,
                "operating_net_income": 0.0,
                "total_inflow": 0.0,
                "total_outflow": 0.0,
                "total_fees": 0.0,
            },
            "operations": [],
            "transactions": [],
            "journal": [],
            "current_snapshot": {
                "note": "old",
                "cash": 0.0,
                "total_assets": 0.0,
                "total_liabilities": 0.0,
                "net_worth": 0.0,
            },
        }

    def test_old_report_does_not_crash(self, page):
        page._render_report_detail(self._old_report_data())

    def test_old_report_alloc_table_has_na_rows(self, page):
        page._render_report_detail(self._old_report_data())
        assert page.report_alloc_table.rowCount() > 0
        values = [
            page.report_alloc_table.item(r, 1).text()
            for r in range(page.report_alloc_table.rowCount())
        ]
        # Many values default to "N/A" when allocation section is missing.
        assert any(v == "N/A" for v in values)

    def test_old_report_risk_table_empty_and_label_default(self, page):
        page._render_report_detail(self._old_report_data())
        assert page.report_risk_table.rowCount() == 0
        # Label still emits a default "Total: 0 warnings" string.
        assert "Total:" in page.report_risk_label.text()


class TestPhase2ClearAllocAndRisk:

    def test_clear_empties_alloc_and_risk(self, page_with_full_report):
        page = page_with_full_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        # Populated.
        assert page.report_alloc_table.rowCount() > 0
        assert page.report_risk_label.text() != ""

        page._clear_report_detail()
        assert page.report_alloc_table.rowCount() == 0
        assert page.report_risk_table.rowCount() == 0
        assert page.report_risk_label.text() == ""


# === Phase 3: Lightweight summary fields in the Reports list table ===


class TestPhase3ReportListColumns:
    """The Reports list table exposes the new performance metrics."""

    def _headers(self, page):
        out = []
        for i in range(page.report_list_table.columnCount()):
            item = page.report_list_table.horizontalHeaderItem(i)
            if item:
                out.append(item.text())
        return out

    def test_net_worth_change_column_exists(self, page):
        assert "Net Worth Change" in self._headers(page)

    def test_funding_flow_column_exists(self, page):
        assert "Funding Flow" in self._headers(page)

    def test_approx_return_pct_column_exists(self, page):
        assert "Approx Return %" in self._headers(page)

    def test_existing_columns_still_present(self, page):
        for required in (
            "Period", "Generated",
            "Net Cash Flow", "Operating Net Income",
        ):
            assert required in self._headers(page)


@pytest.fixture
def page_with_perf_report():
    """A page whose single report has both beginning and ending snapshots
    so the new performance metrics are populated end-to-end."""
    from src.storage.snapshot_repo import create_snapshot
    from src.models.portfolio_snapshot import PortfolioSnapshot
    conn = init_db(":memory:")
    create_snapshot(conn, PortfolioSnapshot(
        date="2025-05-31", cash=10000.0, total_assets=10000.0,
        total_liabilities=0.0, net_worth=10000.0,
    ))
    create_snapshot(conn, PortfolioSnapshot(
        date="2025-06-30", cash=11000.0, total_assets=12000.0,
        total_liabilities=0.0, net_worth=12000.0,
    ))
    create_transaction(conn, Transaction(
        date="2025-06-15", txn_type="deposit_cash",
        total_amount=1000.0, currency="USD",
    ))
    generate_monthly_report(conn, 2025, 6)
    p = ImportExportPage(conn)
    yield p
    conn.close()


class TestPhase3ListPopulation:
    """The list table populates the three new columns from the precomputed
    summary fields, without parsing report_json."""

    def _headers(self, page):
        return [
            page.report_list_table.horizontalHeaderItem(i).text()
            for i in range(page.report_list_table.columnCount())
        ]

    def test_funding_flow_cell_shows_money(self, page_with_perf_report):
        page = page_with_perf_report
        page._refresh_report_list()
        col = self._headers(page).index("Funding Flow")
        cell = page.report_list_table.item(0, col).text()
        assert "1,000.00" in cell

    def test_net_worth_change_cell_shows_money(self, page_with_perf_report):
        page = page_with_perf_report
        page._refresh_report_list()
        col = self._headers(page).index("Net Worth Change")
        cell = page.report_list_table.item(0, col).text()
        assert "2,000.00" in cell

    def test_approx_return_pct_cell_shows_percentage(self, page_with_perf_report):
        page = page_with_perf_report
        page._refresh_report_list()
        col = self._headers(page).index("Approx Return %")
        cell = page.report_list_table.item(0, col).text()
        assert "10.00" in cell
        assert "%" in cell


class TestPhase3MissingPerfShowsNA:
    """Reports that lack snapshot-derived metrics show N/A in the list."""

    def _headers(self, page):
        return [
            page.report_list_table.horizontalHeaderItem(i).text()
            for i in range(page.report_list_table.columnCount())
        ]

    def test_no_snapshots_shows_na_in_perf_columns(self, page_with_reports):
        page = page_with_reports
        page._refresh_report_list()
        assert page.report_list_table.rowCount() == 1
        nwc_col = self._headers(page).index("Net Worth Change")
        ret_col = self._headers(page).index("Approx Return %")
        assert page.report_list_table.item(0, nwc_col).text() == "N/A"
        assert page.report_list_table.item(0, ret_col).text() == "N/A"


class TestPhase3SelectionStillLoadsFullJson:
    """`_on_report_selected` is the single place that loads full
    report_json — and it still does so."""

    def test_selection_calls_get_report(self, page_with_perf_report):
        from unittest.mock import patch
        page = page_with_perf_report
        page._refresh_report_list()
        with patch(
            "src.gui.pages.import_export.get_report",
            wraps=__import__(
                "src.gui.pages.import_export", fromlist=["get_report"],
            ).get_report,
        ) as mock_get:
            page._on_report_selected(0, 0, -1, -1)
            assert mock_get.called

    def test_selection_populates_detail_tabs(self, page_with_perf_report):
        page = page_with_perf_report
        page._refresh_report_list()
        page._on_report_selected(0, 0, -1, -1)
        # Summary, Performance, etc. are rendered from full report_json.
        assert page.report_summary_table.rowCount() > 0
        assert page.report_perf_table.rowCount() > 0


class TestPhase3RefreshIsLightweight:
    """Refreshing the list never loads `get_report` (no full report_json
    parsing) — only the lightweight summary query."""

    def test_refresh_does_not_call_get_report(self, page_with_perf_report):
        from unittest.mock import patch
        page = page_with_perf_report
        with patch("src.gui.pages.import_export.get_report") as mock_get:
            page._refresh_report_list()
        mock_get.assert_not_called()

    def test_refresh_does_not_select_report_json(self, page_with_perf_report):
        # SQL-level guarantee: refresh must not issue a query that selects
        # report_json. Instrument the connection's trace callback.
        page = page_with_perf_report
        captured: list[str] = []
        page.conn.set_trace_callback(captured.append)
        try:
            page._refresh_report_list()
        finally:
            page.conn.set_trace_callback(None)
        for sql in captured:
            assert "report_json" not in sql, (
                f"refresh issued a query selecting report_json: {sql}"
            )
