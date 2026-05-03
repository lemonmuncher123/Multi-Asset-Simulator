"""Typed picker dialog for the four report cadences. Replaces the
free-text QInputDialog flow for "Generate Selected Period" so users
can't typo a label like '2026 Q1' into a parse error."""
from typing import Literal

from PySide6.QtWidgets import (
    QDialog, QFormLayout, QSpinBox, QComboBox, QDialogButtonBox, QLabel,
)


Cadence = Literal["monthly", "quarterly", "semi_annual", "annual"]


class PeriodPickerDialog(QDialog):
    """Caller invokes ``.exec()`` and reads ``.values()``.

    Returns ``(year, sub)`` where ``sub`` is the month / quarter / half
    index, or ``0`` for annual.
    """

    def __init__(self, parent, cadence: Cadence):
        super().__init__(parent)
        if cadence not in ("monthly", "quarterly", "semi_annual", "annual"):
            raise ValueError(f"unknown cadence {cadence!r}")
        self.cadence = cadence
        titles = {
            "monthly": "Generate Monthly Report",
            "quarterly": "Generate Quarterly Report",
            "semi_annual": "Generate Semi-Annual Report",
            "annual": "Generate Annual Report",
        }
        self.setWindowTitle(titles[cadence])

        form = QFormLayout(self)
        self.year_spin = QSpinBox()
        self.year_spin.setRange(2000, 2099)
        self.year_spin.setValue(2026)
        form.addRow(QLabel("Year:"), self.year_spin)

        self.month_spin: QSpinBox | None = None
        self.sub_combo: QComboBox | None = None
        if cadence == "monthly":
            self.month_spin = QSpinBox()
            self.month_spin.setRange(1, 12)
            self.month_spin.setValue(1)
            form.addRow(QLabel("Month:"), self.month_spin)
        elif cadence == "quarterly":
            self.sub_combo = QComboBox()
            self.sub_combo.addItems(["Q1", "Q2", "Q3", "Q4"])
            form.addRow(QLabel("Quarter:"), self.sub_combo)
        elif cadence == "semi_annual":
            self.sub_combo = QComboBox()
            self.sub_combo.addItems(["H1", "H2"])
            form.addRow(QLabel("Half:"), self.sub_combo)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        form.addRow(buttons)

    def values(self) -> tuple[int, int]:
        year = self.year_spin.value()
        if self.cadence == "monthly":
            return (year, self.month_spin.value())
        if self.cadence in ("quarterly", "semi_annual"):
            return (year, self.sub_combo.currentIndex() + 1)
        return (year, 0)  # annual
