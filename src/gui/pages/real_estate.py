import sqlite3
from datetime import date
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout,
    QLineEdit, QPushButton, QLabel, QMessageBox, QTableWidgetItem, QScrollArea,
    QDialog, QDialogButtonBox, QComboBox, QHeaderView, QAbstractItemView,
    QGroupBox, QRadioButton, QButtonGroup,
)
from PySide6.QtGui import QColor, QDoubleValidator
from PySide6.QtCore import Qt, QTimer, Signal
from src.gui.widgets.common import make_header, make_table, fmt_money, fmt_pct, configure_expanding_table, resize_table_to_contents
from src.gui.widgets.bankruptcy_banner import guard_transaction_or_warn
from src.engines.real_estate import (
    analyze_all_properties,
    calc_net_monthly_cash_flow,
    get_real_estate_warnings,
)
from src.engines.property_calculator import (
    parse_percent, parse_percent_literal, normalize_rent_to_monthly,
    normalize_expense, calc_property_summary,
)
from src.engines.debt_math import (
    compute_debt_schedule, simulate_amortization_balance,
    compute_periods_elapsed,
)
from src.utils.display import format_severity
from src.engines.ledger import add_property, sell_property, settle_due_rent
from src.engines.data_management import delete_property_with_related_data, clear_all_properties
from src.storage.property_repo import get_property_by_asset, update_property, list_active_properties, list_properties
from src.storage.asset_repo import get_asset, update_asset
from src.storage.settings_repo import get_default_debt_annual_rate_pct


class EditPropertyDialog(QDialog):
    def __init__(self, prop, asset, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Edit Property: {asset.name}")
        self.setMinimumWidth(450)

        layout = QVBoxLayout(self)
        form = QFormLayout()
        form.setSpacing(8)

        self.name_input = QLineEdit(asset.name)
        form.addRow("Name:", self.name_input)

        self.address_input = QLineEdit(prop.address or "")
        form.addRow("Address:", self.address_input)

        self.purchase_value_input = QLineEdit(str(prop.purchase_price or ""))
        form.addRow("Purchase Price:", self.purchase_value_input)

        self.current_value_input = QLineEdit(str(prop.current_value or ""))
        form.addRow("Current Value:", self.current_value_input)

        # Mortgage fields are no longer editable here (schema v11): the
        # mortgage lives in its own table and its terms are locked at
        # creation. Use the Pay Mortgage / Pay Off in Full flows on the
        # Transactions page to change the balance.

        self.rent_input = QLineEdit(str(prop.monthly_rent))
        form.addRow("Monthly Rent:", self.rent_input)

        self.tax_input = QLineEdit(str(prop.monthly_property_tax))
        form.addRow("Property Tax:", self.tax_input)

        self.insurance_input = QLineEdit(str(prop.monthly_insurance))
        form.addRow("Insurance:", self.insurance_input)

        self.hoa_input = QLineEdit(str(prop.monthly_hoa))
        form.addRow("HOA:", self.hoa_input)

        self.maint_input = QLineEdit(str(prop.monthly_maintenance_reserve))
        form.addRow("Maintenance:", self.maint_input)

        self.mgmt_input = QLineEdit(str(prop.monthly_property_management))
        form.addRow("Management:", self.mgmt_input)

        # Display vacancy as a percentage (e.g. "5" for 5%) to match the
        # Add form's input convention. Stored value remains a 0..1 fraction.
        vacancy_pct = round((prop.vacancy_rate or 0.0) * 100.0, 4)
        self.vacancy_input = QLineEdit(str(vacancy_pct))
        self.vacancy_input.setPlaceholderText("e.g. 5 for 5%")
        form.addRow("Vacancy Rate (%):", self.vacancy_input)

        self.freq_combo = QComboBox()
        self.freq_combo.addItem("Monthly", "monthly")
        self.freq_combo.addItem("Annual", "annual")
        idx = 0 if prop.rent_collection_frequency == "monthly" else 1
        self.freq_combo.setCurrentIndex(idx)
        form.addRow("Rent Collection Frequency:", self.freq_combo)

        # Cashflow start date is no longer editable — engine default
        # (first_day_next_month) is the only path. To shift an existing
        # property's cashflow_start_date, edit the DB directly.

        self.notes_input = QLineEdit(prop.notes or "")
        form.addRow("Notes:", self.notes_input)

        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)


class SellPropertyDialog(QDialog):
    def __init__(self, asset_name, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Sell Property: {asset_name}")
        self.setModal(True)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)
        self.setMinimumWidth(520)

        layout = QVBoxLayout(self)
        form = QFormLayout()
        form.setSpacing(8)

        self.date_input = QLineEdit(date.today().isoformat())
        form.addRow("Sale Date:", self.date_input)

        price_validator = QDoubleValidator(0.0, 1e12, 2)
        price_validator.setNotation(QDoubleValidator.Notation.StandardNotation)

        fees_validator = QDoubleValidator(0.0, 1e12, 2)
        fees_validator.setNotation(QDoubleValidator.Notation.StandardNotation)

        self.price_input = QLineEdit()
        self.price_input.setPlaceholderText("Sale price")
        self.price_input.setValidator(price_validator)
        form.addRow("Sale Price:", self.price_input)

        self.fees_input = QLineEdit("0")
        self.fees_input.setPlaceholderText("Fees (optional)")
        self.fees_input.setValidator(fees_validator)
        form.addRow("Fees:", self.fees_input)

        self.notes_input = QLineEdit()
        self.notes_input.setPlaceholderText("Notes (optional)")
        form.addRow("Notes:", self.notes_input)

        layout.addLayout(form)

        self.setTabOrder(self.date_input, self.price_input)
        self.setTabOrder(self.price_input, self.fees_input)
        self.setTabOrder(self.fees_input, self.notes_input)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._validate_and_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def showEvent(self, event):
        super().showEvent(event)
        # Receiver-aware form: if `self` is destroyed before the timer fires,
        # Qt drops the call instead of invoking on a deleted Python wrapper.
        QTimer.singleShot(0, self, self._focus_price)

    def _focus_price(self):
        self.raise_()
        self.activateWindow()
        self.price_input.setFocus(Qt.FocusReason.OtherFocusReason)
        self.price_input.selectAll()

    def _validate_and_accept(self):
        sale_date_text = self.date_input.text().strip()
        if not sale_date_text:
            QMessageBox.warning(self, "Validation Error", "Sale date is required.")
            self.date_input.setFocus()
            return

        price_text = self.price_input.text().strip()
        try:
            price_val = float(price_text)
            if price_val <= 0:
                raise ValueError()
        except (ValueError, TypeError):
            QMessageBox.warning(self, "Validation Error", "Sale price must be a number greater than 0.")
            self.price_input.setFocus()
            self.price_input.selectAll()
            return

        fees_text = self.fees_input.text().strip()
        if not fees_text:
            fees_text = "0"
        try:
            fees_val = float(fees_text)
            if fees_val < 0:
                raise ValueError()
        except (ValueError, TypeError):
            QMessageBox.warning(self, "Validation Error", "Fees must be a number >= 0.")
            self.fees_input.setFocus()
            self.fees_input.selectAll()
            return

        self.accept()

    def sale_date(self) -> str:
        return self.date_input.text().strip()

    def sale_price(self) -> float:
        return float(self.price_input.text())

    def fees(self) -> float:
        text = self.fees_input.text().strip()
        return float(text) if text else 0.0

    def notes(self) -> str | None:
        text = self.notes_input.text().strip()
        return text if text else None


def _make_section(title: str) -> tuple[QGroupBox, QFormLayout]:
    group = QGroupBox(title)
    group.setStyleSheet("QGroupBox { font-weight: bold; padding-top: 14px; margin-top: 6px; }")
    form = QFormLayout()
    form.setSpacing(6)
    form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
    group.setLayout(form)
    return group, form


def _make_type_combo_row(
    input_field: QLineEdit,
    type_options: list[tuple[str, str]],
    default_index: int = 0,
) -> tuple[QComboBox, QHBoxLayout]:
    combo = QComboBox()
    for label, data in type_options:
        combo.addItem(label, data)
    combo.setCurrentIndex(default_index)
    combo.setMinimumWidth(100)
    combo.setMaximumWidth(150)

    row = QHBoxLayout()
    row.setSpacing(6)
    row.addWidget(input_field, 1)
    row.addWidget(combo)
    return combo, row


EXPENSE_TYPE_OPTIONS = [
    ("$/month", "monthly"),
    ("$/year", "annual"),
    ("% of value", "pct_value"),
]

MANAGEMENT_TYPE_OPTIONS = [
    ("$/month", "monthly"),
    ("% of rent", "pct_rent"),
]

# Empty-state hint shown inside the Add Property mortgage schedule-summary
# panel until the user has entered enough fields to compute a schedule.
# Reused at the QLabel constructor, the recalc's "no loan amount yet"
# branch, and the post-submit clear so all three paths render the same
# prompt.
_MORTGAGE_PREVIEW_PLACEHOLDER_HTML = (
    '<span style="color:#9aa0a6;">Enter the original loan amount, '
    'rate, and either a per-month payment or term to see the '
    'mortgage schedule.</span>'
)


class RealEstatePage(QWidget):
    data_changed = Signal()

    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn
        self._table_asset_ids: list[int] = []
        self._planned_table_asset_ids: list[int] = []

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(12)

        layout.addWidget(make_header("Real Estate"))

        # === Entry Type ===
        type_row = QHBoxLayout()
        type_row.setSpacing(8)
        type_row.addWidget(QLabel("Entry Type:"))
        self.entry_type_combo = QComboBox()
        self.entry_type_combo.addItem("Existing Property", "existing_property")
        self.entry_type_combo.addItem("New Purchase", "new_purchase")
        self.entry_type_combo.currentIndexChanged.connect(self._on_entry_type_changed)
        type_row.addWidget(self.entry_type_combo)
        type_row.addStretch()
        layout.addLayout(type_row)

        self._entry_type_hint = QLabel("")
        self._entry_type_hint.setWordWrap(True)
        self._entry_type_hint.setStyleSheet("color: #999; font-size: 12px; padding-left: 4px;")
        layout.addWidget(self._entry_type_hint)

        # === Section 1: Basic Info ===
        basic_group, basic_form = _make_section("Basic Info")

        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("Property name")
        basic_form.addRow("Name:", self.name_input)

        self.address_input = QLineEdit()
        self.address_input.setPlaceholderText("Address (optional)")
        basic_form.addRow("Address:", self.address_input)

        self.date_input = QLineEdit()
        self.date_input.setPlaceholderText("YYYY-MM-DD")
        self._date_label = QLabel("Purchase Date:")
        basic_form.addRow(self._date_label, self.date_input)

        layout.addWidget(basic_group)

        # === Section 2: Value & Loan ===
        value_group, value_form = _make_section("Value and Loan")

        self.price_input = QLineEdit()
        self.price_input.setPlaceholderText("Purchase price")
        value_form.addRow("Purchase Price:", self.price_input)

        self.current_value_input = QLineEdit()
        self.current_value_input.setPlaceholderText("Current market value (defaults to purchase price)")
        self._current_value_label = QLabel("Current Value:")
        value_form.addRow(self._current_value_label, self.current_value_input)

        # Down payment is no longer entered by the user — it derives
        # from `purchase_price - original_loan_amount` at submit time
        # (see `_read_form_values`). All-cash purchases (loan = 0 or
        # blank) get `down_payment = purchase_price`. Removing this
        # input eliminates the overdetermined-input footgun where the
        # three fields could disagree.

        # Mortgage subsection (schema v11). The user enters the
        # *original* loan amount (the principal borrowed at origination).
        # When `Loan origination date` is in the past, the engine walks
        # the amortization forward to compute today's current balance.
        # Provide ONE of `mortgage_pmt_input` (per-month payment) or
        # `loan_term_input` (term in years); the engine derives the other.
        self.mortgage_input = QLineEdit()
        self.mortgage_input.setPlaceholderText("0 for cash purchase / no mortgage")
        value_form.addRow("Original Loan Amount:", self.mortgage_input)

        self.rate_input = QLineEdit()
        self.rate_input.setPlaceholderText("e.g. 6.5 for 6.5%")
        value_form.addRow("Interest Rate (%):", self.rate_input)

        # Mutually-exclusive plan toggle — mirror of the Add Debt
        # radio (`_add_debt_choice`). UI-only: the engine still
        # accepts whichever of payment_per_period / term_periods we
        # pass, so the radio just controls which input is visible.
        self._mortgage_choice = QButtonGroup(self)
        self.mortgage_radio_payment = QRadioButton(
            "Fix the monthly payment; the system computes how long it takes."
        )
        self.mortgage_radio_term = QRadioButton(
            "Fix the loan term in years; the system computes the monthly payment."
        )
        self.mortgage_radio_payment.setChecked(True)
        self._mortgage_choice.addButton(self.mortgage_radio_payment)
        self._mortgage_choice.addButton(self.mortgage_radio_term)
        self.mortgage_radio_payment.toggled.connect(self._on_mortgage_choice_changed)
        self.mortgage_radio_term.toggled.connect(self._on_mortgage_choice_changed)
        value_form.addRow("Repayment plan:", self.mortgage_radio_payment)
        value_form.addRow("", self.mortgage_radio_term)

        self.mortgage_pmt_label = QLabel("Mortgage Payment (per month):")
        self.mortgage_pmt_input = QLineEdit()
        self.mortgage_pmt_input.setPlaceholderText("Per-month payment")
        value_form.addRow(self.mortgage_pmt_label, self.mortgage_pmt_input)

        self.loan_term_label = QLabel("Loan Term (years):")
        self.loan_term_input = QLineEdit()
        self.loan_term_input.setPlaceholderText("e.g. 30")
        value_form.addRow(self.loan_term_label, self.loan_term_input)

        # Loan origination date is no longer a separate input — it's
        # always the property's purchase_date (you take out the loan
        # when you buy the property). For existing-property entries
        # the user types a historical purchase_date and the engine
        # walks the amortization forward to today; for new purchases
        # purchase_date is today and the loan starts fresh. Either
        # way the two dates are identical, so collecting both was
        # redundant.

        # Live mortgage schedule preview — mirrors `add_debt_preview` on
        # the Transactions page. Renders the 5-line summary (per-month
        # payment, months total, final payment, total paid, total
        # interest), plus the engine-computed current balance when the
        # purchase_date is in the past (existing-property entries where
        # the engine walks the amortization forward).
        self.mortgage_schedule_preview = QLabel(_MORTGAGE_PREVIEW_PLACEHOLDER_HTML)
        self.mortgage_schedule_preview.setWordWrap(True)
        self.mortgage_schedule_preview.setTextFormat(Qt.TextFormat.RichText)
        self.mortgage_schedule_preview.setStyleSheet(
            "QLabel { padding: 12px 14px; background: #20242b; "
            "border: 1px solid #3a3f48; border-radius: 6px; color: #e8eaed; }"
        )
        value_form.addRow("Schedule summary:", self.mortgage_schedule_preview)

        layout.addWidget(value_group)

        # === Section 3: Income & Expenses ===
        income_group, income_form = _make_section("Income and Expenses")

        self.rent_input = QLineEdit()
        self.rent_input.setPlaceholderText("Rent income amount")
        self.rent_freq_combo = QComboBox()
        self.rent_freq_combo.addItem("Monthly", "monthly")
        self.rent_freq_combo.addItem("Annual", "annual")
        self.rent_freq_combo.setMinimumWidth(100)
        self.rent_freq_combo.setMaximumWidth(150)
        rent_row = QHBoxLayout()
        rent_row.setSpacing(6)
        rent_row.addWidget(self.rent_input, 1)
        rent_row.addWidget(self.rent_freq_combo)
        income_form.addRow("Rent Income:", rent_row)
        # Weekly/Biweekly options were removed; the standalone hint
        # label that explained the mapping is no longer needed.

        self.vacancy_input = QLineEdit()
        self.vacancy_input.setPlaceholderText("e.g. 5 for 5%")
        income_form.addRow("Vacancy Rate (%):", self.vacancy_input)

        # Property Tax
        self.tax_input = QLineEdit()
        self.tax_input.setPlaceholderText("Property tax")
        self.tax_type_combo, tax_row = _make_type_combo_row(
            self.tax_input, EXPENSE_TYPE_OPTIONS,
        )
        income_form.addRow("Property Tax:", tax_row)

        # Insurance
        self.insurance_input = QLineEdit()
        self.insurance_input.setPlaceholderText("Insurance")
        self.insurance_type_combo, ins_row = _make_type_combo_row(
            self.insurance_input, EXPENSE_TYPE_OPTIONS,
        )
        income_form.addRow("Insurance:", ins_row)

        # HOA
        self.hoa_input = QLineEdit()
        self.hoa_input.setPlaceholderText("Monthly HOA")
        income_form.addRow("HOA ($/month):", self.hoa_input)

        # Maintenance
        self.maint_input = QLineEdit()
        self.maint_input.setPlaceholderText("Maintenance reserve")
        self.maint_type_combo, maint_row = _make_type_combo_row(
            self.maint_input, EXPENSE_TYPE_OPTIONS,
        )
        income_form.addRow("Maintenance:", maint_row)

        # Management
        self.mgmt_input = QLineEdit()
        self.mgmt_input.setPlaceholderText("Property management")
        self.mgmt_type_combo, mgmt_row = _make_type_combo_row(
            self.mgmt_input, MANAGEMENT_TYPE_OPTIONS,
        )
        income_form.addRow("Management:", mgmt_row)

        layout.addWidget(income_group)

        # === Section 4: Calculated Summary ===
        summary_group, summary_form = _make_section("Calculated Summary")
        self._summary_labels = {}
        for key, label in [
            ("equity", "Equity:"),
            ("ltv", "Loan-to-Value:"),
            ("effective_monthly_rent", "Effective Rent/mo:"),
            ("monthly_operating_expenses", "Operating Expenses/mo:"),
            ("monthly_cash_flow", "Net Cash Flow/mo:"),
            ("annual_noi", "Annual NOI:"),
            ("cap_rate", "Cap Rate:"),
            ("cash_on_cash_return", "Cash-on-Cash:"),
        ]:
            val_label = QLabel("--")
            val_label.setStyleSheet("font-size: 13px;")
            self._summary_labels[key] = val_label
            summary_form.addRow(label, val_label)

        layout.addWidget(summary_group)

        # Cashflow start date is no longer a user input — see
        # _read_form_values for the rationale. The engine defaults
        # both rent and mortgage cashflow_start to first_day_next_month.

        # === Submit ===
        btn_row = QHBoxLayout()
        self._submit_btn = QPushButton("Add Property")
        self._submit_btn.setStyleSheet("padding: 8px 24px; font-size: 14px;")
        self._submit_btn.clicked.connect(self._submit)
        btn_row.addWidget(self._submit_btn)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        # --- Active Properties table ---
        layout.addWidget(QLabel("Active Properties"))
        self.table = make_table([
            "Property", "Value", "Mortgage", "Equity", "LTV",
            "Eff. Rent", "Expenses", "Net CF/mo",
            "Cap Rate", "CoC Return",
        ])
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for col in range(1, self.table.columnCount()):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)
        header.setStretchLastSection(False)
        configure_expanding_table(self.table)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        layout.addWidget(self.table)

        # --- Management buttons ---
        mgmt_row = QHBoxLayout()

        self.edit_btn = QPushButton("Edit Selected")
        self.edit_btn.setStyleSheet("padding: 8px 16px; font-size: 13px;")
        self.edit_btn.clicked.connect(self._edit_property)
        mgmt_row.addWidget(self.edit_btn)

        self.sell_btn = QPushButton("Sell Selected")
        self.sell_btn.setStyleSheet("padding: 8px 16px; font-size: 13px;")
        self.sell_btn.clicked.connect(self._sell_property)
        mgmt_row.addWidget(self.sell_btn)

        self.delete_btn = QPushButton("Delete Selected")
        self.delete_btn.setStyleSheet("padding: 8px 16px; font-size: 13px; color: #c62828;")
        self.delete_btn.clicked.connect(self._delete_property)
        mgmt_row.addWidget(self.delete_btn)

        self.clear_btn = QPushButton("Clear All Properties")
        self.clear_btn.setStyleSheet("padding: 8px 16px; font-size: 13px; color: #c62828;")
        self.clear_btn.clicked.connect(self._clear_all_properties)
        mgmt_row.addWidget(self.clear_btn)

        self.settle_btn = QPushButton("Settle Due Rent")
        self.settle_btn.setStyleSheet("padding: 8px 16px; font-size: 13px; background-color: #1565c0; color: white;")
        self.settle_btn.clicked.connect(self._settle_rent)
        mgmt_row.addWidget(self.settle_btn)

        mgmt_row.addStretch()
        layout.addLayout(mgmt_row)

        # --- Planned Properties table ---
        self._planned_label = QLabel("Legacy Planned Purchases")
        layout.addWidget(self._planned_label)
        self.planned_table = make_table([
            "Property", "Price", "Down Pmt", "Mortgage",
            "Est. Mortgage/mo", "Est. Rent/mo", "Est. CF/mo",
        ])
        p_header = self.planned_table.horizontalHeader()
        p_header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for col in range(1, self.planned_table.columnCount()):
            p_header.setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)
        p_header.setStretchLastSection(False)
        configure_expanding_table(self.planned_table)
        self.planned_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.planned_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        layout.addWidget(self.planned_table)

        planned_mgmt_row = QHBoxLayout()
        self.delete_planned_btn = QPushButton("Delete Selected Legacy Planned")
        self.delete_planned_btn.setStyleSheet("padding: 8px 16px; font-size: 13px; color: #c62828;")
        self.delete_planned_btn.clicked.connect(self._delete_planned_property)
        planned_mgmt_row.addWidget(self.delete_planned_btn)
        planned_mgmt_row.addStretch()
        layout.addLayout(planned_mgmt_row)

        # --- Warnings ---
        layout.addWidget(QLabel("Real Estate Warnings"))
        self.warn_table = make_table(["Severity", "Message"])
        w_header = self.warn_table.horizontalHeader()
        w_header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        w_header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        configure_expanding_table(self.warn_table)
        layout.addWidget(self.warn_table)

        layout.addStretch()
        scroll.setWidget(content)
        outer.addWidget(scroll)

        self.table.selectionModel().selectionChanged.connect(self._update_management_buttons)
        self._update_management_buttons()
        self._on_entry_type_changed(0)
        # Pre-fill the mortgage rate with the user-configured default
        # BEFORE wiring summary autoupdate — otherwise the setText below
        # would fire a textChanged signal and recompute the (still empty)
        # summary, breaking the "all '--' on launch" contract that other
        # tests depend on. Mirrors Add Debt's pre-fill behavior.
        # `_last_auto_mortgage_rate_text` tracks what we wrote so refresh()
        # can distinguish stale auto-populated values from user overrides.
        self._last_auto_mortgage_rate_text = ""
        self._populate_default_mortgage_rate()
        self._wire_summary_autoupdate()
        # Sync the visible field to the radio choice on launch — pure
        # widget show/hide, no recalc trigger (the form is still empty,
        # so we want the summary labels to keep their initial "--").
        self._sync_mortgage_visibility()

    def _sync_mortgage_visibility(self):
        by_payment = self.mortgage_radio_payment.isChecked()
        self.mortgage_pmt_label.setVisible(by_payment)
        self.mortgage_pmt_input.setVisible(by_payment)
        self.loan_term_label.setVisible(not by_payment)
        self.loan_term_input.setVisible(not by_payment)

    def _on_mortgage_choice_changed(self):
        """Show only the input that matches the chosen radio. Mirror of
        `TransactionsPage._on_add_debt_choice_changed`. UI-only — the
        engine still derives whichever value isn't supplied.
        """
        self._sync_mortgage_visibility()
        # Clear the now-hidden field so a stale value doesn't leak into
        # the submit handler (which prefers payment over term when both
        # are set). The .clear() call fires textChanged → recalc, which
        # refreshes the schedule preview alongside the property summary.
        if self.mortgage_radio_payment.isChecked():
            self.loan_term_input.clear()
        else:
            self.mortgage_pmt_input.clear()

    @staticmethod
    def _resolve_mortgage_payment(
        original: float, rate: float, payment: float,
        loan_term_years: int | None,
    ) -> float:
        """Resolve the per-month mortgage payment for the GUI helpers.

        Single source of truth for both the schedule preview and the
        property summary. Routes through ``compute_debt_schedule`` so
        the math matches the engine's `add_mortgage` path exactly —
        including the 0%-rate fixed-term case where the legacy
        `calc_monthly_mortgage` short-circuits to 0.0 (Bug 4).
        """
        if payment > 0:
            return payment
        if not (original > 0 and loan_term_years):
            return 0.0
        sched = compute_debt_schedule(
            principal=original, annual_rate=rate, schedule="monthly",
            term_periods=loan_term_years * 12,
        )
        return sched.per_period_payment if sched.feasible else 0.0

    @staticmethod
    def _resolve_mortgage_current_balance(
        original: float, rate: float, monthly_payment: float,
        origination_date: str | None,
    ) -> float:
        """Resolve today's `current_balance` for the GUI helpers.

        Mirror of what `add_mortgage` will persist: when the
        origination_date (= property purchase_date) is in the past, walk
        the amortization forward via ``simulate_amortization_balance``;
        otherwise the loan starts fresh today with balance = original.
        """
        if original <= 0:
            return 0.0
        today_iso = date.today().isoformat()
        if not origination_date or origination_date >= today_iso:
            return original
        if monthly_payment <= 0:
            return original
        try:
            periods = compute_periods_elapsed(
                origination_date, today_iso, "monthly",
            )
            return simulate_amortization_balance(
                principal=original, annual_rate=rate, schedule="monthly",
                payment=monthly_payment, periods_elapsed=periods,
            )
        except Exception:
            return original

    def _render_mortgage_schedule_preview(self, mortgage_data: dict) -> None:
        """Render the 5-line mortgage schedule preview. Cloned from the
        Add Debt preview pattern (`_on_add_debt_inputs_changed` in
        transactions.py) and adapted: monthly-only, no payoff cap
        warning, plus an extra "Computed current balance" line when
        the purchase_date is in the past (existing-property entries
        where the engine walks the amortization forward to today).
        """
        original = mortgage_data.get("original_loan_amount") or 0.0
        if original <= 0:
            self.mortgage_schedule_preview.setText(_MORTGAGE_PREVIEW_PLACEHOLDER_HTML)
            return
        rate = mortgage_data.get("interest_rate") or 0.0
        payment = mortgage_data.get("monthly_payment") or 0.0
        loan_term_years = mortgage_data.get("loan_term_years")
        # Resolve plan: prefer payment, else derive from loan_term_years.
        payment_per_period = payment if payment > 0 else None
        term_periods = (
            loan_term_years * 12
            if (payment_per_period is None and loan_term_years) else None
        )
        if payment_per_period is None and term_periods is None:
            self.mortgage_schedule_preview.setText(
                '<span style="color:#9aa0a6;">Add a per-month payment '
                'OR a loan term to compute the schedule.</span>'
            )
            return
        sched = compute_debt_schedule(
            principal=original, annual_rate=rate, schedule="monthly",
            payment=payment_per_period, term_periods=term_periods,
        )
        if not sched.feasible:
            self.mortgage_schedule_preview.setText(
                '<div style="padding:6px 8px; background:#3b1f1f; '
                'border:1px solid #862424; border-radius:4px; color:#ffab91;">'
                f'<b>⚠ Infeasible:</b> {sched.infeasibility_reason}'
                '</div>'
            )
            return
        rows = [
            ("Per-month payment", fmt_money(sched.per_period_payment)),
            ("Number of months", f"{sched.num_periods}"),
            ("Final month's payment", fmt_money(sched.final_payment)),
            ("Total paid", fmt_money(sched.total_paid)),
            ("Total interest", fmt_money(sched.total_interest)),
        ]
        # When origination (== purchase_date) is in the past, also show
        # the post-walk current balance so the user sees what
        # `add_mortgage` will actually persist as `current_balance`.
        origination = mortgage_data.get("origination_date")
        today_iso = date.today().isoformat()
        if origination and origination < today_iso:
            try:
                periods = compute_periods_elapsed(origination, today_iso, "monthly")
                current = simulate_amortization_balance(
                    principal=original, annual_rate=rate, schedule="monthly",
                    payment=sched.per_period_payment, periods_elapsed=periods,
                )
                rows.insert(0, (
                    "Current balance",
                    f"{fmt_money(current)}  (after {periods} months)",
                ))
            except Exception:
                pass
        rows_html = "".join(
            '<tr>'
            f'<td style="padding:3px 22px 3px 0; color:#9aa0a6;">{k}</td>'
            f'<td style="padding:3px 0; color:#e8eaed; font-weight:600;">{v}</td>'
            '</tr>'
            for k, v in rows
        )
        self.mortgage_schedule_preview.setText(
            f'<table style="border-collapse:collapse;">{rows_html}</table>'
        )

    def _wire_summary_autoupdate(self):
        for w in (
            self.price_input, self.current_value_input,
            self.mortgage_input, self.rate_input, self.loan_term_input,
            self.mortgage_pmt_input,
            # Bug 2: changing the purchase_date flips the origination
            # date for the mortgage (= purchase_date) and shifts the
            # walked-forward current balance — must trigger recalc.
            self.date_input,
            self.rent_input, self.vacancy_input,
            self.tax_input, self.insurance_input, self.hoa_input,
            self.maint_input, self.mgmt_input,
        ):
            w.textChanged.connect(self._recalc_summary_silently)
        for c in (
            self.rent_freq_combo, self.tax_type_combo,
            self.insurance_type_combo, self.maint_type_combo, self.mgmt_type_combo,
            # Bug 3: toggling existing_property ↔ new_purchase changes
            # whether the engine walks the loan forward.
            self.entry_type_combo,
        ):
            c.currentIndexChanged.connect(self._recalc_summary_silently)

    def _recalc_summary_silently(self, *_args):
        try:
            v = self._read_form_values_for_summary()
        except Exception:
            for label in self._summary_labels.values():
                label.setText("--")
            return
        # Resolve monthly payment via compute_debt_schedule so the
        # 0%-rate fixed-term case is handled correctly (Bug 4 fix —
        # legacy calc_monthly_mortgage short-circuited to $0/mo).
        m = v["mortgage"]
        monthly_mortgage_payment = self._resolve_mortgage_payment(
            original=m["original_loan_amount"] or 0.0,
            rate=m["interest_rate"] or 0.0,
            payment=m["monthly_payment"] or 0.0,
            loan_term_years=m["loan_term_years"],
        )
        # Resolve current balance via simulate_amortization_balance so
        # equity/LTV match what add_mortgage will actually persist for
        # an existing-property entry with a past purchase_date (Bug 1
        # fix — was using the original loan amount as a shortcut).
        mortgage_balance_for_summary = self._resolve_mortgage_current_balance(
            original=m["original_loan_amount"] or 0.0,
            rate=m["interest_rate"] or 0.0,
            monthly_payment=monthly_mortgage_payment,
            origination_date=m["origination_date"],
        )
        # Also refresh the schedule preview so the user sees the
        # 5-line summary (per-month payment, months total, total paid,
        # total interest, plus current balance for existing-loan entries).
        self._render_mortgage_schedule_preview(m)
        s = calc_property_summary(
            purchase_price=v["purchase_price"],
            current_value=v["current_value"] or v["purchase_price"],
            mortgage_balance=mortgage_balance_for_summary,
            down_payment=v["down_payment"] or 0.0,
            monthly_mortgage_payment=monthly_mortgage_payment,
            monthly_rent=v["monthly_rent"],
            vacancy_rate=v["vacancy_rate"],
            monthly_property_tax=v["monthly_property_tax"],
            monthly_insurance=v["monthly_insurance"],
            monthly_hoa=v["monthly_hoa"],
            monthly_maintenance=v["monthly_maintenance_reserve"],
            monthly_management=v["monthly_property_management"],
        )
        self._summary_labels["equity"].setText(fmt_money(s["equity"]))
        self._summary_labels["ltv"].setText(
            fmt_pct(s["ltv"]) if s["ltv"] is not None else "N/A"
        )
        self._summary_labels["effective_monthly_rent"].setText(fmt_money(s["effective_monthly_rent"]))
        self._summary_labels["monthly_operating_expenses"].setText(fmt_money(s["monthly_operating_expenses"]))
        self._summary_labels["monthly_cash_flow"].setText(fmt_money(s["monthly_cash_flow"]))
        self._summary_labels["annual_noi"].setText(fmt_money(s["annual_noi"]))
        self._summary_labels["cap_rate"].setText(
            fmt_pct(s["cap_rate"]) if s["cap_rate"] is not None else "N/A"
        )
        self._summary_labels["cash_on_cash_return"].setText(
            fmt_pct(s["cash_on_cash_return"]) if s["cash_on_cash_return"] is not None else "N/A"
        )

    def _read_form_values_for_summary(self) -> dict:
        # Permissive sibling of `_read_form_values`: tolerates partial input
        # (no "name required" / "current value required" guards) so the live
        # summary updates as the user types.
        try:
            return self._read_form_values()
        except ValueError:
            pass
        # Best-effort fallback when the strict path raises on missing fields:
        # parse what's there, default the rest, never raise on incomplete forms.
        def _f(field, default=0.0):
            try:
                t = field.text().strip()
                return float(t) if t else default
            except (ValueError, AttributeError):
                return default

        purchase_price = _f(self.price_input)
        current_value_text = self.current_value_input.text().strip()
        current_value = float(current_value_text) if current_value_text else purchase_price

        # Mortgage first, then derive down_payment as price - loan
        # (matches `_read_form_values`).
        mortgage_text = self.mortgage_input.text().strip()
        if mortgage_text:
            mortgage_balance = float(mortgage_text)
        else:
            mortgage_balance = 0.0
        if purchase_price > 0 and mortgage_balance > 0:
            down_payment = max(purchase_price - mortgage_balance, 0.0)
        else:
            down_payment = purchase_price

        rate_raw = _f(self.rate_input)
        # Form input is a documented percent literal (label "%", placeholder
        # "e.g. 5 for 5%"). The heuristic parse_percent silently treats
        # sub-1% inputs as already-fractions, breaking the round-trip on
        # any rate stored as 0.00x. parse_percent_literal always divides
        # by 100, matching the form's stated convention.
        rate = parse_percent_literal(rate_raw) if rate_raw > 0 else 0.0
        loan_term_text = self.loan_term_input.text().strip()
        loan_term_years = int(loan_term_text) if loan_term_text.isdigit() else None

        # Bug 4 fix: route through the engine's compute_debt_schedule
        # via the shared helper so 0%-rate fixed-term loans get the
        # right principal/N payment instead of $0.
        mortgage_pmt_text = self.mortgage_pmt_input.text().strip()
        monthly_mortgage_payment = self._resolve_mortgage_payment(
            original=mortgage_balance,
            rate=rate,
            payment=float(mortgage_pmt_text) if mortgage_pmt_text else 0.0,
            loan_term_years=loan_term_years,
        )

        rent_freq = self.rent_freq_combo.currentData()
        monthly_rent = normalize_rent_to_monthly(_f(self.rent_input), rent_freq)

        vacancy_raw = _f(self.vacancy_input)
        vacancy_rate = parse_percent_literal(vacancy_raw) if vacancy_raw > 0 else 0.0
        vacancy_rate = max(0.0, min(vacancy_rate, 1.0))

        prop_value = current_value or purchase_price or 0.0
        eff_rent = monthly_rent * (1 - vacancy_rate)

        return {
            "purchase_price": purchase_price,
            "current_value": current_value,
            # Always pass the derived value (even 0); the falsy-to-None
            # coercion would make a 100%-financed purchase ($0 down)
            # silently fall back to add_property's "cash_out =
            # purchase_price" branch, blowing up the cash check.
            "down_payment": down_payment,
            "monthly_rent": monthly_rent,
            "vacancy_rate": vacancy_rate,
            "monthly_property_tax": normalize_expense(
                self.tax_type_combo.currentData(), _f(self.tax_input), prop_value,
            ),
            "monthly_insurance": normalize_expense(
                self.insurance_type_combo.currentData(), _f(self.insurance_input), prop_value,
            ),
            "monthly_hoa": _f(self.hoa_input),
            "monthly_maintenance_reserve": normalize_expense(
                self.maint_type_combo.currentData(), _f(self.maint_input), prop_value,
            ),
            "monthly_property_management": normalize_expense(
                self.mgmt_type_combo.currentData(), _f(self.mgmt_input), reference_rent=eff_rent,
            ),
            "mortgage": {
                "original_loan_amount": mortgage_balance,
                "interest_rate": rate,
                "monthly_payment": monthly_mortgage_payment,
                "loan_term_years": loan_term_years,
                # Loan origination is always the purchase date — see the
                # Add Property mortgage subsection comment.
                "origination_date": self.date_input.text().strip() or None,
            },
        }

    def _populate_default_mortgage_rate(self):
        """Set the mortgage rate field to the user-configured default.

        Always overwrites — callers (refresh / clear) decide whether they
        want to call this. Tracks the value we wrote so refresh() can
        distinguish user edits from a stale auto-populated value.
        """
        text = f"{get_default_debt_annual_rate_pct(self.conn):.1f}"
        self.rate_input.setText(text)
        self._last_auto_mortgage_rate_text = text

    def refresh(self):
        self._load_table()
        self._load_planned_table()
        self._load_warnings()
        self._update_management_buttons()
        # Refresh the auto-populated mortgage rate if the user hasn't typed
        # anything custom. Keeps the field in sync with the Settings page.
        current = self.rate_input.text().strip()
        if not current or current == self._last_auto_mortgage_rate_text:
            self._populate_default_mortgage_rate()

    def _load_table(self):
        analyses = analyze_all_properties(self.conn)
        self._table_asset_ids = []
        self.table.setRowCount(len(analyses))
        for i, a in enumerate(analyses):
            self._table_asset_ids.append(a.prop.asset_id)
            self.table.setItem(i, 0, QTableWidgetItem(a.name))
            self.table.setItem(i, 1, QTableWidgetItem(fmt_money(a.prop.current_value or 0)))
            self.table.setItem(i, 2, QTableWidgetItem(fmt_money(a.mortgage_balance)))
            self.table.setItem(i, 3, QTableWidgetItem(fmt_money(a.equity)))
            self.table.setItem(i, 4, QTableWidgetItem(
                fmt_pct(a.ltv) if a.ltv is not None else "N/A"
            ))
            self.table.setItem(i, 5, QTableWidgetItem(fmt_money(a.effective_rent)))
            self.table.setItem(i, 6, QTableWidgetItem(fmt_money(a.monthly_expenses)))
            self.table.setItem(i, 7, QTableWidgetItem(fmt_money(a.net_monthly_cash_flow)))
            self.table.setItem(i, 8, QTableWidgetItem(
                fmt_pct(a.cap_rate) if a.cap_rate is not None else "N/A"
            ))
            self.table.setItem(i, 9, QTableWidgetItem(
                fmt_pct(a.cash_on_cash_return) if a.cash_on_cash_return is not None else "N/A"
            ))
        resize_table_to_contents(self.table)

    def _load_planned_table(self):
        from src.storage.mortgage_repo import get_mortgage_by_property
        all_props = list_properties(self.conn)
        planned = [p for p in all_props if p.status == "planned"]
        self._planned_table_asset_ids = []
        self.planned_table.setRowCount(len(planned))
        for i, prop in enumerate(planned):
            self._planned_table_asset_ids.append(prop.asset_id)
            asset = get_asset(self.conn, prop.asset_id)
            name = asset.name if asset else f"Property {prop.id}"
            mortgage = get_mortgage_by_property(self.conn, prop.id)
            mortgage_balance = mortgage.current_balance if mortgage else 0.0
            monthly_mortgage_payment = (
                mortgage.monthly_payment_amount if mortgage else 0.0
            )
            self.planned_table.setItem(i, 0, QTableWidgetItem(name))
            self.planned_table.setItem(i, 1, QTableWidgetItem(fmt_money(prop.purchase_price or 0)))
            self.planned_table.setItem(i, 2, QTableWidgetItem(fmt_money(prop.down_payment or 0)))
            self.planned_table.setItem(i, 3, QTableWidgetItem(fmt_money(mortgage_balance)))
            self.planned_table.setItem(i, 4, QTableWidgetItem(fmt_money(monthly_mortgage_payment)))
            self.planned_table.setItem(i, 5, QTableWidgetItem(fmt_money(prop.monthly_rent)))
            ncf = calc_net_monthly_cash_flow(prop, self.conn)
            self.planned_table.setItem(i, 6, QTableWidgetItem(fmt_money(ncf)))
        resize_table_to_contents(self.planned_table)

        visible = len(planned) > 0
        self._planned_label.setVisible(visible)
        self.planned_table.setVisible(visible)
        self.delete_planned_btn.setVisible(visible)

    def _load_warnings(self):
        warnings = get_real_estate_warnings(self.conn)
        self.warn_table.setRowCount(len(warnings))
        severity_colors = {
            "critical": "#c62828", "high": "#e65100",
            "medium": "#f9a825", "info": "#1565c0",
        }
        for i, w in enumerate(warnings):
            sev_item = QTableWidgetItem(format_severity(w.severity))
            color = severity_colors.get(w.severity, "#cccccc")
            sev_item.setForeground(QColor(color))
            self.warn_table.setItem(i, 0, sev_item)
            self.warn_table.setItem(i, 1, QTableWidgetItem(w.message))
        resize_table_to_contents(self.warn_table)

    def _get_selected_asset_id(self) -> int | None:
        selected = self.table.selectionModel().selectedRows()
        if not selected:
            return None
        row = selected[0].row()
        if row < len(self._table_asset_ids):
            return self._table_asset_ids[row]
        return None

    def _update_management_buttons(self, *_args):
        has_selection = self._get_selected_asset_id() is not None
        self.edit_btn.setEnabled(has_selection)
        self.sell_btn.setEnabled(has_selection)
        self.delete_btn.setEnabled(has_selection)

    def _on_entry_type_changed(self, index):
        mode = self.entry_type_combo.currentData()
        if mode == "new_purchase":
            self._date_label.setText("Purchase Date:")
            self.date_input.setPlaceholderText("Purchase date (YYYY-MM-DD)")
            self.date_input.setText(date.today().isoformat())
            self._entry_type_hint.setText(
                "Record a new property purchase. Down payment (or purchase price minus mortgage) will be deducted from cash."
            )
            self._submit_btn.setText("Add New Purchase")
            self._current_value_label.setVisible(False)
            self.current_value_input.setVisible(False)
            self.current_value_input.clear()
        else:
            self._date_label.setText("Original Purchase Date:")
            self.date_input.setPlaceholderText("Original purchase date (YYYY-MM-DD)")
            self.date_input.clear()
            self._entry_type_hint.setText(
                "Record a property you already own. No cash deduction. "
                "Original Purchase Date is stored as property history only. "
                "Cashflow Start Date controls future rent settlement."
            )
            self._submit_btn.setText("Add Property")
            self._current_value_label.setVisible(True)
            self.current_value_input.setVisible(True)

    def _read_form_values(self) -> dict:
        name = self.name_input.text().strip()
        if not name:
            raise ValueError("Property name is required.")

        purchase_date = self.date_input.text().strip()
        if not purchase_date:
            raise ValueError("Date is required.")

        mode = self.entry_type_combo.currentData()
        price_text = self.price_input.text().strip()
        current_value_text = self.current_value_input.text().strip()

        if mode == "existing_property":
            purchase_price = float(price_text) if price_text else 0.0
            current_value = float(current_value_text) if current_value_text else None
            if not current_value or current_value <= 0:
                raise ValueError("Current value is required for existing properties.")
        else:
            purchase_price = float(price_text) if price_text else 0.0
            if purchase_price <= 0:
                raise ValueError("Purchase price must be greater than 0.")
            current_value = purchase_price

        def _float_or(field, default=0.0):
            t = field.text().strip()
            return float(t) if t else default

        # Mortgage subsection. Captured as a separate "mortgage" key
        # in the submit handler can call add_mortgage as a follow-up
        # (only if original_loan > 0). Negative input is rejected
        # explicitly — without this guard a negative loan amount silently
        # falls through to the `if mortgage_amount > 0` branch in
        # `_submit` as "no mortgage", masking the user's typo.
        mortgage_text = self.mortgage_input.text().strip()
        if mortgage_text:
            original_loan_amount = float(mortgage_text)
            if original_loan_amount < 0:
                raise ValueError("Original loan amount cannot be negative.")
        else:
            original_loan_amount = 0.0

        # Down payment is derived: it's the slice of the purchase price
        # NOT covered by the loan. With no loan, the property is paid
        # for entirely in cash. The user no longer enters this field
        # directly — it follows mechanically from price and loan.
        if purchase_price > 0 and original_loan_amount > 0:
            down_payment = max(purchase_price - original_loan_amount, 0.0)
        else:
            down_payment = purchase_price

        # Reject negative interest rate explicitly — the existing
        # `if rate_raw > 0` branch silently coerces negative input to
        # 0% (an interest-free loan), which is not what the user typed.
        rate_raw = _float_or(self.rate_input)
        if rate_raw < 0:
            raise ValueError("Mortgage interest rate cannot be negative.")
        mortgage_interest_rate = parse_percent_literal(rate_raw) if rate_raw > 0 else 0.0

        loan_term_text = self.loan_term_input.text().strip()
        loan_term_years = int(loan_term_text) if loan_term_text else None

        # Bug 4 fix: route through the engine's compute_debt_schedule
        # via the shared helper so 0%-rate fixed-term loans get the
        # right principal/N payment instead of $0.
        mortgage_pmt_text = self.mortgage_pmt_input.text().strip()
        monthly_mortgage_payment = self._resolve_mortgage_payment(
            original=original_loan_amount,
            rate=mortgage_interest_rate,
            payment=float(mortgage_pmt_text) if mortgage_pmt_text else 0.0,
            loan_term_years=loan_term_years,
        )

        # Loan origination = property purchase date. The two are
        # inseparable for a mortgage taken at acquisition; collecting
        # them as separate inputs was redundant.
        mortgage_origination = purchase_date or None

        # Rent
        rent_raw = _float_or(self.rent_input)
        rent_freq = self.rent_freq_combo.currentData()
        monthly_rent = normalize_rent_to_monthly(rent_raw, rent_freq)

        # Vacancy
        vacancy_raw = _float_or(self.vacancy_input)
        vacancy_rate = parse_percent_literal(vacancy_raw) if vacancy_raw > 0 else 0.0
        if vacancy_rate < 0 or vacancy_rate > 1:
            raise ValueError("Vacancy rate must be between 0% and 100%.")

        prop_value = (current_value if current_value else purchase_price) or 0.0
        eff_rent = monthly_rent * (1 - vacancy_rate)

        # Expenses with type selectors
        tax_type = self.tax_type_combo.currentData()
        tax_raw = _float_or(self.tax_input)
        monthly_property_tax = normalize_expense(tax_type, tax_raw, prop_value)

        ins_type = self.insurance_type_combo.currentData()
        ins_raw = _float_or(self.insurance_input)
        monthly_insurance = normalize_expense(ins_type, ins_raw, prop_value)

        monthly_hoa = _float_or(self.hoa_input)

        maint_type = self.maint_type_combo.currentData()
        maint_raw = _float_or(self.maint_input)
        monthly_maintenance = normalize_expense(maint_type, maint_raw, prop_value)

        mgmt_type = self.mgmt_type_combo.currentData()
        mgmt_raw = _float_or(self.mgmt_input)
        monthly_management = normalize_expense(mgmt_type, mgmt_raw, reference_rent=eff_rent)

        # Rent settlement schedule is derived from how the user enters rent:
        # annual input → annual settlement; monthly/weekly/biweekly all
        # collapse to monthly settlement after rent is normalized to monthly.
        rent_collection_freq = "annual" if rent_freq == "annual" else "monthly"
        # cashflow_start_date is no longer collected from the user. The
        # engine fills it with first_day_next_month() when None.
        cashflow_start = None

        return {
            "name": name,
            "address": self.address_input.text().strip() or None,
            "purchase_date": purchase_date,
            "purchase_price": purchase_price,
            "current_value": current_value,
            # Always pass the derived value (even 0); the falsy-to-None
            # coercion would make a 100%-financed purchase ($0 down)
            # silently fall back to add_property's "cash_out =
            # purchase_price" branch, blowing up the cash check.
            "down_payment": down_payment,
            "monthly_rent": monthly_rent,
            "monthly_property_tax": monthly_property_tax,
            "monthly_insurance": monthly_insurance,
            "monthly_hoa": monthly_hoa,
            "monthly_maintenance_reserve": monthly_maintenance,
            "monthly_property_management": monthly_management,
            "vacancy_rate": vacancy_rate,
            "rent_collection_frequency": rent_collection_freq,
            "cashflow_start_date": cashflow_start,
            # Mortgage subsection (consumed by _submit only when
            # original_loan_amount > 0).
            "mortgage": {
                "original_loan_amount": original_loan_amount,
                "interest_rate": mortgage_interest_rate,
                "monthly_payment": monthly_mortgage_payment,
                "loan_term_years": loan_term_years,
                "origination_date": mortgage_origination,
            },
        }

    def _submit(self):
        if guard_transaction_or_warn(self.conn, self):
            return
        try:
            v = self._read_form_values()
            acquisition_mode = self.entry_type_combo.currentData()
            symbol = f"RE_{v['name'].upper().replace(' ', '_')[:20]}"

            txn_date = None
            if acquisition_mode == "existing_property":
                txn_date = date.today().isoformat()

            mortgage_data = v["mortgage"]
            mortgage_amount = mortgage_data["original_loan_amount"]

            # Pre-flight LTV check matches the engine's hard validation
            # but surfaces a friendly QMessageBox before any DB writes.
            if (
                mortgage_amount > 0
                and v["purchase_price"] > 0
                and mortgage_amount > v["purchase_price"]
            ):
                QMessageBox.warning(
                    self, "Loan exceeds purchase price",
                    f"Original loan amount ({fmt_money(mortgage_amount)}) "
                    f"cannot exceed purchase price "
                    f"({fmt_money(v['purchase_price'])}).",
                )
                return

            asset, prop, _ = add_property(
                self.conn, date=v["purchase_date"], symbol=symbol, name=v["name"],
                purchase_price=v["purchase_price"], current_value=v["current_value"],
                address=v["address"],
                down_payment=v["down_payment"],
                monthly_rent=v["monthly_rent"],
                monthly_property_tax=v["monthly_property_tax"],
                monthly_insurance=v["monthly_insurance"],
                monthly_hoa=v["monthly_hoa"],
                monthly_maintenance_reserve=v["monthly_maintenance_reserve"],
                monthly_property_management=v["monthly_property_management"],
                vacancy_rate=v["vacancy_rate"],
                rent_collection_frequency=v["rent_collection_frequency"],
                acquisition_mode=acquisition_mode,
                cashflow_start_date=v["cashflow_start_date"],
                transaction_date=txn_date,
            )

            # Attach the mortgage as a follow-up call. Skipped entirely
            # when the user enters 0 for the loan amount (cash purchase /
            # no mortgage), AND when the mortgage section has neither a
            # per-month payment nor a loan term — the engine requires one
            # of those to compute the amortization schedule. This lets a
            # caller skip the mortgage subsection on a property that has
            # one informally without forcing a hard error.
            if mortgage_amount > 0:
                # Resolve the plan choice. If a per-month payment is given,
                # pass it; else if loan_term_years is given, convert to
                # months. The engine requires exactly one.
                payment_per_period = (
                    mortgage_data["monthly_payment"]
                    if mortgage_data["monthly_payment"] > 0 else None
                )
                term_periods = (
                    mortgage_data["loan_term_years"] * 12
                    if (payment_per_period is None
                        and mortgage_data["loan_term_years"]) else None
                )
                if payment_per_period is not None or term_periods is not None:
                    from src.engines.ledger import add_mortgage
                    # Loan origination = property purchase date. For
                    # existing_property entries with a past purchase_date,
                    # the engine walks the amortization forward to today.
                    # For new_purchase / planned_purchase where the date
                    # is today or future, no walk happens and
                    # current_balance == original_amount.
                    add_mortgage(
                        self.conn,
                        property_id=prop.id,
                        original_amount=mortgage_amount,
                        interest_rate=mortgage_data["interest_rate"],
                        payment_per_period=payment_per_period,
                        term_periods=term_periods,
                        cashflow_start_date=v["cashflow_start_date"],
                        origination_date=v["purchase_date"],
                        name=v["name"],
                    )

            self._clear_form()
            self.refresh()
            self.data_changed.emit()

        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _clear_form(self):
        self.name_input.clear()
        self.address_input.clear()
        self.date_input.clear()
        self.price_input.clear()
        self.current_value_input.clear()
        self.mortgage_input.clear()
        self.rate_input.clear()
        # Restore the rate to the configured default so the next entry
        # starts from the user's preferred value, not blank.
        self._populate_default_mortgage_rate()
        self.loan_term_input.clear()
        self.mortgage_pmt_input.clear()
        self.mortgage_schedule_preview.setText(_MORTGAGE_PREVIEW_PLACEHOLDER_HTML)
        self.rent_input.clear()
        self.rent_freq_combo.setCurrentIndex(0)
        self.vacancy_input.clear()
        self.tax_input.clear()
        self.tax_type_combo.setCurrentIndex(0)
        self.insurance_input.clear()
        self.insurance_type_combo.setCurrentIndex(0)
        self.hoa_input.clear()
        self.maint_input.clear()
        self.maint_type_combo.setCurrentIndex(0)
        self.mgmt_input.clear()
        self.mgmt_type_combo.setCurrentIndex(0)
        self.entry_type_combo.setCurrentIndex(0)
        for label in self._summary_labels.values():
            label.setText("--")

    def _edit_property(self):
        # Property edit moves the simulation's underlying numbers
        # (purchase price, current value, monthly rent/expenses, vacancy
        # rate) and emits `data_changed`, which fires the auto-settle
        # pipeline. While bankrupt, every other transaction-creating
        # handler aborts via this guard; edit needs to honor the same
        # rule so the bankruptcy lock is consistent across the page.
        if guard_transaction_or_warn(self.conn, self):
            return
        asset_id = self._get_selected_asset_id()
        if asset_id is None:
            QMessageBox.warning(self, "No Selection", "Select a property to edit.")
            return

        prop = get_property_by_asset(self.conn, asset_id)
        asset = get_asset(self.conn, asset_id)
        if prop is None or asset is None:
            QMessageBox.warning(self, "Error", "Property not found.")
            return

        dlg = EditPropertyDialog(prop, asset, self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        try:
            new_name = dlg.name_input.text().strip()
            if not new_name:
                raise ValueError("Name is required.")

            vacancy_raw = float(dlg.vacancy_input.text() or "0")
            vacancy = parse_percent_literal(vacancy_raw) if vacancy_raw > 0 else 0.0
            if vacancy < 0 or vacancy > 1:
                raise ValueError("Vacancy rate must be between 0% and 100%.")

            asset.name = new_name
            # Embed the asset id so two long names with the same first 20
            # characters can't collapse to the same symbol. `assets.symbol`
            # has no UNIQUE constraint, so collisions would be silent —
            # `get_asset_by_symbol` would shadow one row behind the other.
            asset.symbol = (
                f"RE_{new_name.upper().replace(' ', '_')[:20]}_{asset.id}"
            )

            # Edit Property bypasses ledger.add_property and writes directly
            # to the property row, so the engine-level non-negative guards
            # don't fire here. Reject negative numerics explicitly so the
            # cashflow / allocation / risk engines can rely on the
            # invariant that property amounts are >= 0.
            new_purchase_price = float(dlg.purchase_value_input.text() or "0")
            new_current_value = float(dlg.current_value_input.text() or "0")
            new_rent = float(dlg.rent_input.text() or "0")
            new_tax = float(dlg.tax_input.text() or "0")
            new_insurance = float(dlg.insurance_input.text() or "0")
            new_hoa = float(dlg.hoa_input.text() or "0")
            new_maint = float(dlg.maint_input.text() or "0")
            new_mgmt = float(dlg.mgmt_input.text() or "0")
            for label, val in (
                ("Purchase price", new_purchase_price),
                ("Current value", new_current_value),
                ("Monthly rent", new_rent),
                ("Property tax", new_tax),
                ("Insurance", new_insurance),
                ("HOA", new_hoa),
                ("Maintenance", new_maint),
                ("Management", new_mgmt),
            ):
                if val < 0:
                    raise ValueError(f"{label} cannot be negative.")

            update_asset(self.conn, asset)

            prop.address = dlg.address_input.text().strip() or None
            prop.purchase_price = new_purchase_price
            prop.current_value = new_current_value
            # Mortgage terms locked at creation — see EditPropertyDialog
            # comment. Use Pay Mortgage on the Transactions page instead.
            prop.monthly_rent = new_rent
            prop.monthly_property_tax = new_tax
            prop.monthly_insurance = new_insurance
            prop.monthly_hoa = new_hoa
            prop.monthly_maintenance_reserve = new_maint
            prop.monthly_property_management = new_mgmt
            prop.vacancy_rate = vacancy
            prop.rent_collection_frequency = dlg.freq_combo.currentData()
            # cashflow_start_date is no longer editable from the GUI.
            prop.notes = dlg.notes_input.text().strip() or None
            update_property(self.conn, prop)

            self.refresh()
            self.data_changed.emit()

        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _sell_property(self):
        if guard_transaction_or_warn(self.conn, self):
            return
        asset_id = self._get_selected_asset_id()
        if asset_id is None:
            QMessageBox.warning(self, "No Selection", "Select a property to sell.")
            return

        asset = get_asset(self.conn, asset_id)
        if asset is None:
            QMessageBox.warning(self, "Error", "Asset not found.")
            return

        dlg = SellPropertyDialog(asset.name, self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        try:
            sd = dlg.sale_date()
            price = dlg.sale_price()
            f = dlg.fees()
            n = dlg.notes()

            txn = sell_property(self.conn, sd, asset_id, price, fees=f, notes=n)

            QMessageBox.information(
                self, "Property Sold",
                f"{asset.name} sold for {fmt_money(price)}.\n"
                f"Net cash proceeds: {fmt_money(txn.total_amount)}.",
            )

            self.refresh()
            self.data_changed.emit()

        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _delete_property(self):
        asset_id = self._get_selected_asset_id()
        if asset_id is None:
            QMessageBox.warning(self, "No Selection", "Select a property to delete.")
            return

        asset = get_asset(self.conn, asset_id)
        name = asset.name if asset else f"Asset {asset_id}"

        reply = QMessageBox.question(
            self, "Delete Property",
            f"Delete '{name}' and all related records?\n\n"
            "This will NOT create any cash transaction.\n"
            "All transactions, prices, and data for this property will be permanently removed.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        try:
            deleted = delete_property_with_related_data(self.conn, asset_id)
            QMessageBox.information(
                self, "Deleted",
                f"Deleted property '{name}'.\n"
                f"Removed {deleted.get('transactions', 0)} transactions, "
                f"{deleted.get('prices', 0)} prices.",
            )
            self.refresh()
            self.data_changed.emit()
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _delete_planned_property(self):
        selected = self.planned_table.selectionModel().selectedRows()
        if not selected:
            QMessageBox.warning(self, "No Selection", "Select a legacy planned purchase to delete.")
            return
        row = selected[0].row()
        if row >= len(self._planned_table_asset_ids):
            return
        asset_id = self._planned_table_asset_ids[row]

        asset = get_asset(self.conn, asset_id)
        name = asset.name if asset else f"Asset {asset_id}"

        reply = QMessageBox.question(
            self, "Delete Legacy Planned Purchase",
            f"Delete legacy planned purchase '{name}' and all related records?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        try:
            deleted = delete_property_with_related_data(self.conn, asset_id)
            QMessageBox.information(
                self, "Deleted",
                f"Deleted legacy planned purchase '{name}'.",
            )
            self.refresh()
            self.data_changed.emit()
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _clear_all_properties(self):
        reply = QMessageBox.question(
            self, "Clear All Properties",
            "Delete ALL real estate properties and their related records?\n\n"
            "This will NOT create any cash transactions.\n"
            "All property transactions, prices, and data will be permanently removed.\n\n"
            "Non-real-estate assets and cash transactions will NOT be affected.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        confirm = QMessageBox.warning(
            self, "Confirm Clear All",
            "Are you sure? This cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        try:
            deleted = clear_all_properties(self.conn)
            QMessageBox.information(
                self, "Cleared",
                f"Cleared all properties.\n"
                f"Removed {deleted.get('assets', 0)} assets, "
                f"{deleted.get('transactions', 0)} transactions, "
                f"{deleted.get('properties', 0)} property records.",
            )
            self.refresh()
            self.data_changed.emit()
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _settle_rent(self):
        if guard_transaction_or_warn(self.conn, self):
            return
        through_date = date.today().isoformat()
        try:
            created = settle_due_rent(self.conn, through_date)
            if created:
                QMessageBox.information(
                    self, "Rent Settled",
                    f"Created {len(created)} rent transaction(s) through {through_date}.",
                )
            else:
                QMessageBox.information(
                    self, "Rent Settled",
                    "No rent transactions were due.",
                )
            self.refresh()
            self.data_changed.emit()
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))
