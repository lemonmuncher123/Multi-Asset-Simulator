import sqlite3
from datetime import date
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout,
    QLineEdit, QPushButton, QLabel, QMessageBox, QTableWidgetItem, QScrollArea,
    QDialog, QDialogButtonBox, QComboBox, QHeaderView, QAbstractItemView,
    QGroupBox,
)
from PySide6.QtGui import QColor, QDoubleValidator
from PySide6.QtCore import Qt, QTimer, Signal
from src.gui.widgets.common import make_header, make_table, fmt_money, fmt_pct, configure_expanding_table, resize_table_to_contents
from src.engines.real_estate import (
    analyze_all_properties,
    calc_net_monthly_cash_flow,
    get_real_estate_warnings,
)
from src.engines.property_calculator import (
    parse_percent, normalize_rent_to_monthly, calc_down_payment,
    calc_monthly_mortgage, normalize_expense, calc_property_summary,
)
from src.utils.display import format_severity
from src.engines.ledger import add_property, sell_property, settle_due_rent, first_day_next_month
from src.engines.data_management import delete_property_with_related_data, clear_all_properties
from src.storage.property_repo import get_property_by_asset, update_property, list_active_properties, list_properties
from src.storage.asset_repo import get_asset, update_asset


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

        self.mortgage_input = QLineEdit(str(prop.mortgage_balance))
        form.addRow("Mortgage Balance:", self.mortgage_input)

        self.rate_input = QLineEdit(str(prop.mortgage_interest_rate))
        form.addRow("Interest Rate:", self.rate_input)

        self.mortgage_pmt_input = QLineEdit(str(prop.monthly_mortgage_payment))
        form.addRow("Monthly Mortgage:", self.mortgage_pmt_input)

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

        self.vacancy_input = QLineEdit(str(prop.vacancy_rate))
        form.addRow("Vacancy Rate:", self.vacancy_input)

        self.freq_combo = QComboBox()
        self.freq_combo.addItem("Monthly", "monthly")
        self.freq_combo.addItem("Annual", "annual")
        idx = 0 if prop.rent_collection_frequency == "monthly" else 1
        self.freq_combo.setCurrentIndex(idx)
        form.addRow("Rent Frequency:", self.freq_combo)

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

        # Down payment with type selector
        self.down_payment_input = QLineEdit()
        self.down_payment_input.setPlaceholderText("Down payment amount or %")
        self.dp_type_combo, dp_row = _make_type_combo_row(
            self.down_payment_input,
            [("$ amount", "amount"), ("% of price", "percent")],
        )
        value_form.addRow("Down Payment:", dp_row)

        self.mortgage_input = QLineEdit()
        self.mortgage_input.setPlaceholderText("Mortgage balance (auto-calculated if blank)")
        value_form.addRow("Mortgage Balance:", self.mortgage_input)

        self.rate_input = QLineEdit()
        self.rate_input.setPlaceholderText("e.g. 6.5 for 6.5%")
        value_form.addRow("Interest Rate (%):", self.rate_input)

        self.loan_term_input = QLineEdit()
        self.loan_term_input.setPlaceholderText("e.g. 30")
        value_form.addRow("Loan Term (years):", self.loan_term_input)

        self.mortgage_pmt_input = QLineEdit()
        self.mortgage_pmt_input.setPlaceholderText("Auto-calculated if rate and term provided")
        value_form.addRow("Monthly Mortgage:", self.mortgage_pmt_input)

        layout.addWidget(value_group)

        # === Section 3: Income & Expenses ===
        income_group, income_form = _make_section("Income and Expenses")

        self.rent_input = QLineEdit()
        self.rent_input.setPlaceholderText("Rent income amount")
        self.rent_freq_combo = QComboBox()
        self.rent_freq_combo.addItem("Monthly", "monthly")
        self.rent_freq_combo.addItem("Annual", "annual")
        self.rent_freq_combo.addItem("Weekly", "weekly")
        self.rent_freq_combo.addItem("Biweekly", "biweekly")
        self.rent_freq_combo.setMinimumWidth(100)
        self.rent_freq_combo.setMaximumWidth(150)
        rent_row = QHBoxLayout()
        rent_row.setSpacing(6)
        rent_row.addWidget(self.rent_input, 1)
        rent_row.addWidget(self.rent_freq_combo)
        income_form.addRow("Rent Income:", rent_row)

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

        calc_btn = QPushButton("Calculate")
        calc_btn.setStyleSheet("padding: 4px 16px;")
        calc_btn.clicked.connect(self._update_summary)
        summary_form.addRow("", calc_btn)

        layout.addWidget(summary_group)

        # === Section 5: Advanced ===
        adv_group, adv_form = _make_section("Advanced")

        self.freq_combo = QComboBox()
        self.freq_combo.addItem("Monthly", "monthly")
        self.freq_combo.addItem("Annual", "annual")
        adv_form.addRow("Rent Collection:", self.freq_combo)

        self.cashflow_start_input = QLineEdit()
        self.cashflow_start_input.setPlaceholderText("YYYY-MM-DD (defaults to 1st of next month)")
        self.cashflow_start_input.setText(first_day_next_month())
        adv_form.addRow("Cashflow Start Date:", self.cashflow_start_input)

        layout.addWidget(adv_group)

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

    def refresh(self):
        self._load_table()
        self._load_planned_table()
        self._load_warnings()
        self._update_management_buttons()

    def _load_table(self):
        analyses = analyze_all_properties(self.conn)
        self._table_asset_ids = []
        self.table.setRowCount(len(analyses))
        for i, a in enumerate(analyses):
            self._table_asset_ids.append(a.prop.asset_id)
            self.table.setItem(i, 0, QTableWidgetItem(a.name))
            self.table.setItem(i, 1, QTableWidgetItem(fmt_money(a.prop.current_value or 0)))
            self.table.setItem(i, 2, QTableWidgetItem(fmt_money(a.prop.mortgage_balance)))
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
        all_props = list_properties(self.conn)
        planned = [p for p in all_props if p.status == "planned"]
        self._planned_table_asset_ids = []
        self.planned_table.setRowCount(len(planned))
        for i, prop in enumerate(planned):
            self._planned_table_asset_ids.append(prop.asset_id)
            asset = get_asset(self.conn, prop.asset_id)
            name = asset.name if asset else f"Property {prop.id}"
            self.planned_table.setItem(i, 0, QTableWidgetItem(name))
            self.planned_table.setItem(i, 1, QTableWidgetItem(fmt_money(prop.purchase_price or 0)))
            self.planned_table.setItem(i, 2, QTableWidgetItem(fmt_money(prop.down_payment or 0)))
            self.planned_table.setItem(i, 3, QTableWidgetItem(fmt_money(prop.mortgage_balance)))
            self.planned_table.setItem(i, 4, QTableWidgetItem(fmt_money(prop.monthly_mortgage_payment)))
            self.planned_table.setItem(i, 5, QTableWidgetItem(fmt_money(prop.monthly_rent)))
            ncf = calc_net_monthly_cash_flow(prop)
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

        # Down payment
        dp_type = self.dp_type_combo.currentData()
        dp_raw = _float_or(self.down_payment_input, 0.0)
        if dp_type == "percent":
            dp_pct = parse_percent(dp_raw)
            down_payment = calc_down_payment(purchase_price, "percent", dp_raw)
        else:
            down_payment = dp_raw
            dp_pct = None

        # Mortgage
        mortgage_text = self.mortgage_input.text().strip()
        if mortgage_text:
            mortgage_balance = float(mortgage_text)
        elif down_payment:
            mortgage_balance = purchase_price - down_payment
        else:
            mortgage_balance = 0.0

        rate_raw = _float_or(self.rate_input)
        mortgage_interest_rate = parse_percent(rate_raw) if rate_raw > 0 else 0.0

        loan_term_text = self.loan_term_input.text().strip()
        loan_term_years = int(loan_term_text) if loan_term_text else None

        mortgage_pmt_text = self.mortgage_pmt_input.text().strip()
        if mortgage_pmt_text:
            monthly_mortgage_payment = float(mortgage_pmt_text)
        elif mortgage_balance > 0 and mortgage_interest_rate > 0 and loan_term_years:
            monthly_mortgage_payment = calc_monthly_mortgage(
                mortgage_balance, mortgage_interest_rate, loan_term_years,
            )
        else:
            monthly_mortgage_payment = 0.0

        # Rent
        rent_raw = _float_or(self.rent_input)
        rent_freq = self.rent_freq_combo.currentData()
        monthly_rent = normalize_rent_to_monthly(rent_raw, rent_freq)

        # Vacancy
        vacancy_raw = _float_or(self.vacancy_input)
        vacancy_rate = parse_percent(vacancy_raw) if vacancy_raw > 0 else 0.0
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

        rent_collection_freq = self.freq_combo.currentData()
        cashflow_start = self.cashflow_start_input.text().strip() or None

        return {
            "name": name,
            "address": self.address_input.text().strip() or None,
            "purchase_date": purchase_date,
            "purchase_price": purchase_price,
            "current_value": current_value,
            "down_payment": down_payment if down_payment else None,
            "mortgage_balance": mortgage_balance,
            "mortgage_interest_rate": mortgage_interest_rate,
            "monthly_mortgage_payment": monthly_mortgage_payment,
            "monthly_rent": monthly_rent,
            "monthly_property_tax": monthly_property_tax,
            "monthly_insurance": monthly_insurance,
            "monthly_hoa": monthly_hoa,
            "monthly_maintenance_reserve": monthly_maintenance,
            "monthly_property_management": monthly_management,
            "vacancy_rate": vacancy_rate,
            "rent_collection_frequency": rent_collection_freq,
            "cashflow_start_date": cashflow_start,
            "loan_term_years": loan_term_years,
            "dp_type": dp_type,
            "dp_raw": dp_raw,
            "rent_raw": rent_raw,
            "rent_freq": rent_freq,
            "tax_type": tax_type,
            "tax_raw": tax_raw,
            "ins_type": ins_type,
            "ins_raw": ins_raw,
            "maint_type": maint_type,
            "maint_raw": maint_raw,
            "mgmt_type": mgmt_type,
            "mgmt_raw": mgmt_raw,
        }

    def _update_summary(self):
        try:
            v = self._read_form_values()
            s = calc_property_summary(
                purchase_price=v["purchase_price"],
                current_value=v["current_value"] or v["purchase_price"],
                mortgage_balance=v["mortgage_balance"],
                down_payment=v["down_payment"] or 0.0,
                monthly_mortgage_payment=v["monthly_mortgage_payment"],
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
        except Exception as e:
            QMessageBox.warning(self, "Calculation Error", str(e))

    def _submit(self):
        try:
            v = self._read_form_values()
            acquisition_mode = self.entry_type_combo.currentData()
            symbol = f"RE_{v['name'].upper().replace(' ', '_')[:20]}"

            txn_date = None
            if acquisition_mode == "existing_property":
                txn_date = date.today().isoformat()

            add_property(
                self.conn, date=v["purchase_date"], symbol=symbol, name=v["name"],
                purchase_price=v["purchase_price"], current_value=v["current_value"],
                address=v["address"],
                down_payment=v["down_payment"],
                mortgage_balance=v["mortgage_balance"],
                mortgage_interest_rate=v["mortgage_interest_rate"],
                monthly_mortgage_payment=v["monthly_mortgage_payment"],
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
        self.down_payment_input.clear()
        self.dp_type_combo.setCurrentIndex(0)
        self.mortgage_input.clear()
        self.rate_input.clear()
        self.loan_term_input.clear()
        self.mortgage_pmt_input.clear()
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
        self.freq_combo.setCurrentIndex(0)
        self.entry_type_combo.setCurrentIndex(0)
        self.cashflow_start_input.setText(first_day_next_month())
        for label in self._summary_labels.values():
            label.setText("--")

    def _edit_property(self):
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

            vacancy = float(dlg.vacancy_input.text() or "0")
            if vacancy < 0 or vacancy > 1:
                raise ValueError("Vacancy rate must be between 0 and 1.")

            asset.name = new_name
            asset.symbol = f"RE_{new_name.upper().replace(' ', '_')[:20]}"
            update_asset(self.conn, asset)

            prop.address = dlg.address_input.text().strip() or None
            prop.purchase_price = float(dlg.purchase_value_input.text() or "0")
            prop.current_value = float(dlg.current_value_input.text() or "0")
            prop.mortgage_balance = float(dlg.mortgage_input.text() or "0")
            prop.mortgage_interest_rate = float(dlg.rate_input.text() or "0")
            prop.monthly_mortgage_payment = float(dlg.mortgage_pmt_input.text() or "0")
            prop.monthly_rent = float(dlg.rent_input.text() or "0")
            prop.monthly_property_tax = float(dlg.tax_input.text() or "0")
            prop.monthly_insurance = float(dlg.insurance_input.text() or "0")
            prop.monthly_hoa = float(dlg.hoa_input.text() or "0")
            prop.monthly_maintenance_reserve = float(dlg.maint_input.text() or "0")
            prop.monthly_property_management = float(dlg.mgmt_input.text() or "0")
            prop.vacancy_rate = vacancy
            prop.rent_collection_frequency = dlg.freq_combo.currentData()
            prop.notes = dlg.notes_input.text().strip() or None
            update_property(self.conn, prop)

            self.refresh()
            self.data_changed.emit()

        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _sell_property(self):
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
