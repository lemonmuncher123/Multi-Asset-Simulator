import pytest

from src.gui.widgets.period_picker import PeriodPickerDialog


def test_picker_monthly_returns_year_and_month(qapp):
    dlg = PeriodPickerDialog(parent=None, cadence="monthly")
    dlg.year_spin.setValue(2026)
    dlg.month_spin.setValue(7)
    assert dlg.values() == (2026, 7)
    dlg.deleteLater()


def test_picker_quarterly_returns_year_and_quarter(qapp):
    dlg = PeriodPickerDialog(parent=None, cadence="quarterly")
    dlg.year_spin.setValue(2026)
    dlg.sub_combo.setCurrentIndex(2)  # Q3
    assert dlg.values() == (2026, 3)
    dlg.deleteLater()


def test_picker_semi_annual_returns_year_and_half(qapp):
    dlg = PeriodPickerDialog(parent=None, cadence="semi_annual")
    dlg.year_spin.setValue(2026)
    dlg.sub_combo.setCurrentIndex(1)  # H2
    assert dlg.values() == (2026, 2)
    dlg.deleteLater()


def test_picker_annual_returns_year_with_zero_sub(qapp):
    dlg = PeriodPickerDialog(parent=None, cadence="annual")
    dlg.year_spin.setValue(2025)
    assert dlg.values() == (2025, 0)
    dlg.deleteLater()


def test_picker_invalid_cadence(qapp):
    with pytest.raises(ValueError):
        PeriodPickerDialog(parent=None, cadence="weekly")  # type: ignore
