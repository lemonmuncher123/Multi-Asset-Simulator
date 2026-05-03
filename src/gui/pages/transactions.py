import sqlite3
from datetime import date
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, QComboBox,
    QLineEdit, QPushButton, QLabel, QMessageBox, QTableWidgetItem, QScrollArea,
    QGroupBox, QHeaderView, QSizePolicy, QRadioButton, QButtonGroup,
)
from PySide6.QtGui import QColor
from PySide6.QtCore import Qt, Signal
from src.gui.widgets.common import make_header, make_table, fmt_money, fmt_pct, fmt_qty, configure_expanding_table, resize_table_to_contents
from src.models.asset import Asset
from src.models.asset_types import SELLABLE_ASSET_TYPES
from src.storage.asset_repo import create_asset, list_assets
from src.storage.transaction_repo import list_transactions
from src.storage.debt_repo import list_debts, get_debt_by_asset
from src.storage.property_repo import list_active_properties
from src.storage.settings_repo import (
    get_max_debt_payoff_months, get_default_debt_annual_rate_pct,
    DEFAULT_DEBT_ANNUAL_RATE_PCT,
)
from src.engines import ledger
from src.engines.debt_math import (
    compute_debt_schedule, normalize_period_to_months,
    recompute_after_payment, PAID_OFF_REASON,
)
from src.gui.widgets.bankruptcy_banner import guard_transaction_or_warn
from src.engines.trade_preview import (
    TradeDraft, TradePreview, prepare_trade_preview, confirm_trade,
)
from src.engines.security_universe_engine import (
    initialize_universe, search_universe, ensure_asset_from_security,
)
from src.utils.display import (
    format_asset_type, format_transaction_type, format_price_source,
    get_transaction_type_options,
)


TXN_TYPES = [
    "deposit_cash",
    "withdraw_cash",
    "buy",
    "sell",
    "add_debt",
    "pay_debt",
    "pay_mortgage",
    "pay_property_expense",
]

ASSET_REQUIRED = {"buy", "sell", "pay_property_expense"}
QTY_PRICE_REQUIRED = {"buy", "sell"}
PREVIEWABLE = {"buy", "sell"}
AMOUNT_REQUIRED = {
    "deposit_cash", "withdraw_cash", "pay_property_expense",
}
# Types whose form lives in a dedicated QGroupBox below the main form,
# not in the main form itself. Selecting one of these collapses the main
# form to just the type combo and reveals the matching groupbox.
DEBT_OP_TYPES = {"add_debt", "pay_debt", "pay_mortgage"}

_STYLE_PRIMARY = "padding: 8px 24px; font-size: 14px; background-color: #2e7d32; color: white;"
_STYLE_SECONDARY = "padding: 8px 24px; font-size: 14px;"

# Empty-state hint shown inside the Add Debt schedule-summary panel until
# the user has entered enough fields to compute a schedule. Reused in the
# QLabel constructor, the recalc's "not enough input" branch, and the
# post-submit clear so all three paths render the same prompt.
_ADD_DEBT_PREVIEW_PLACEHOLDER_HTML = (
    '<span style="color:#9aa0a6;">Fill in principal, rate, and '
    'either payment or term to see the schedule.</span>'
)


class TransactionsPage(QWidget):
    data_changed = Signal()

    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn
        self._current_preview = None
        self._search_results = []
        self._selected_search_result_index: int | None = None
        # Set when the user clicks "Pay Off in Full" so the next submit
        # routes through pay_debt_in_full (which charges balance + this
        # period's interest and clears the balance to 0). Cleared the
        # moment the user edits the amount field.
        self._pay_debt_payoff_armed = False
        # Mirror flag for the Pay Mortgage form.
        self._pay_mortgage_payoff_armed = False
        # Tracks the value we last auto-populated into the Add Debt rate
        # field from settings. We update the field from settings whenever
        # it's empty OR still matches this value (i.e. the user hasn't
        # typed over it). Lets the Settings page change propagate back to
        # the form on next refresh.
        self._last_auto_debt_rate_text: str | None = None
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(12)
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        layout.addWidget(make_header("Transactions"))

        # --- Form ---
        form_frame = QWidget()
        form_frame.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Maximum)
        form = QFormLayout(form_frame)
        form.setSpacing(8)

        self._main_form = form
        self.txn_type = QComboBox()
        for label, val in get_transaction_type_options(TXN_TYPES):
            self.txn_type.addItem(label, val)
        self.txn_type.currentIndexChanged.connect(self._on_type_changed)
        form.addRow("Type:", self.txn_type)

        self.date_label = QLabel("Date:")
        self.date_input = QLineEdit(date.today().isoformat())
        form.addRow(self.date_label, self.date_input)

        self.asset_label = QLabel("Asset:")
        self.asset_combo = QComboBox()
        self.asset_combo.setEditable(False)
        form.addRow(self.asset_label, self.asset_combo)

        self.amount_label = QLabel("Amount:")
        self.amount_input = QLineEdit()
        self.amount_input.setPlaceholderText("Amount (cash)")
        form.addRow(self.amount_label, self.amount_input)

        self.qty_label = QLabel("Quantity:")
        self.qty_input = QLineEdit()
        self.qty_input.setPlaceholderText("Quantity")
        form.addRow(self.qty_label, self.qty_input)

        self.price_label = QLabel("Price:")
        self.price_input = QLineEdit()
        self.price_input.setPlaceholderText("Price per unit (optional for preview)")
        form.addRow(self.price_label, self.price_input)

        self.fees_label = QLabel("Additional Fees:")
        self.fees_input = QLineEdit()
        self.fees_input.setPlaceholderText("0")
        form.addRow(self.fees_label, self.fees_input)

        self.notes_label = QLabel("Notes:")
        self.notes_input = QLineEdit()
        self.notes_input.setPlaceholderText("Notes (optional)")
        form.addRow(self.notes_label, self.notes_input)

        self.btn_row_widget = QWidget()
        btn_row = QHBoxLayout(self.btn_row_widget)
        btn_row.setContentsMargins(0, 0, 0, 0)

        self.preview_btn = QPushButton("Preview Trade")
        self.preview_btn.setStyleSheet(_STYLE_PRIMARY)
        self.preview_btn.clicked.connect(self._preview_trade)
        btn_row.addWidget(self.preview_btn)

        self.confirm_btn = QPushButton("Confirm Trade")
        self.confirm_btn.setStyleSheet(_STYLE_SECONDARY)
        self.confirm_btn.clicked.connect(self._confirm_trade)
        self.confirm_btn.setEnabled(False)
        self.confirm_btn.setToolTip("Preview the trade before confirming.")
        btn_row.addWidget(self.confirm_btn)

        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setStyleSheet("padding: 8px 24px; font-size: 14px;")
        self.cancel_btn.clicked.connect(self._cancel_preview)
        self.cancel_btn.setEnabled(False)
        btn_row.addWidget(self.cancel_btn)

        self.submit_btn = QPushButton("Submit Cash/Other Transaction")
        self.submit_btn.setStyleSheet("padding: 8px 24px; font-size: 14px;")
        self.submit_btn.clicked.connect(self._submit)
        btn_row.addWidget(self.submit_btn)

        btn_row.addStretch()
        self._btn_row_label = QLabel("")
        form.addRow(self._btn_row_label, self.btn_row_widget)

        layout.addWidget(form_frame)

        # --- Add Debt ---
        self.add_debt_group = QGroupBox("Add Debt")
        self.add_debt_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Maximum)
        debt_form = QFormLayout(self.add_debt_group)
        debt_form.setSpacing(8)
        self._add_debt_form = debt_form

        self.add_debt_date = QLineEdit(date.today().isoformat())
        debt_form.addRow("Date recorded:", self.add_debt_date)

        self.add_debt_name = QLineEdit()
        self.add_debt_name.setPlaceholderText("Debt name (required, e.g. \"Visa Card\")")
        debt_form.addRow("Name:", self.add_debt_name)

        self.add_debt_symbol = QLineEdit()
        self.add_debt_symbol.setPlaceholderText("Short label (optional, e.g. VISA)")
        debt_form.addRow("Symbol:", self.add_debt_symbol)

        # Add Debt always records a fresh loan (cash inflow). The engine
        # still accepts cash_received=False / original_amount=... for tests
        # and scripts; the UI no longer exposes those because the dual-mode
        # form was more complexity than the workflow warranted.
        self.add_debt_amount = QLineEdit()
        self.add_debt_amount.setPlaceholderText("Loan principal")
        self.add_debt_amount.textChanged.connect(self._on_add_debt_inputs_changed)
        debt_form.addRow("Principal amount:", self.add_debt_amount)

        self.add_debt_schedule = QComboBox()
        self.add_debt_schedule.addItem("Monthly", "monthly")
        self.add_debt_schedule.addItem("Yearly", "yearly")
        # Re-label the term + payment rows when the schedule changes so the
        # input unit is unambiguous, then refresh the live preview.
        self.add_debt_schedule.currentIndexChanged.connect(self._on_add_debt_schedule_changed)
        debt_form.addRow("Auto-deduction:", self.add_debt_schedule)

        # First scheduled payment date is no longer collected from the
        # user — the engine defaults to first-of-next-period. To shift
        # it, edit the debts row directly.

        # Interest rate is always annual. Field starts blank; refresh()
        # populates the user-configured default from settings.
        self.add_debt_rate = QLineEdit()
        self.add_debt_rate.setPlaceholderText("Annual interest rate (%)")
        self.add_debt_rate.textChanged.connect(self._on_add_debt_inputs_changed)
        debt_form.addRow("Annual interest rate (%):", self.add_debt_rate)

        # Mutually-exclusive: pick either fixed payment OR fixed term.
        self._add_debt_choice = QButtonGroup(self)
        self.add_debt_radio_payment = QRadioButton(
            "Fix the payment per period; the system computes how long it takes."
        )
        self.add_debt_radio_term = QRadioButton(
            "Fix the time to pay off; the system computes the per-period payment."
        )
        self.add_debt_radio_payment.setChecked(True)
        self._add_debt_choice.addButton(self.add_debt_radio_payment)
        self._add_debt_choice.addButton(self.add_debt_radio_term)
        self.add_debt_radio_payment.toggled.connect(self._on_add_debt_choice_changed)
        self.add_debt_radio_term.toggled.connect(self._on_add_debt_choice_changed)
        debt_form.addRow("Repayment plan:", self.add_debt_radio_payment)
        debt_form.addRow("", self.add_debt_radio_term)

        self.add_debt_payment_label = QLabel("Payment per period:")
        self.add_debt_payment = QLineEdit()
        self.add_debt_payment.setPlaceholderText("Per-period payment")
        self.add_debt_payment.textChanged.connect(self._on_add_debt_inputs_changed)
        debt_form.addRow(self.add_debt_payment_label, self.add_debt_payment)

        self.add_debt_term_label = QLabel("Term (in periods):")
        self.add_debt_term = QLineEdit()
        self.add_debt_term.setPlaceholderText("Number of periods (months or years)")
        self.add_debt_term.textChanged.connect(self._on_add_debt_inputs_changed)
        debt_form.addRow(self.add_debt_term_label, self.add_debt_term)

        # Loan origination date is no longer a separate input — it
        # always equals the "Date recorded" field above. For a fresh
        # loan, both are today and current_balance == principal. For
        # an existing loan, set Date recorded to the original loan
        # start; the engine walks the amortization forward to today.
        # Mirrors how the property purchase_date drives mortgage
        # origination on the Real Estate page.

        self.add_debt_notes = QLineEdit()
        self.add_debt_notes.setPlaceholderText("Notes (optional)")
        debt_form.addRow("Notes:", self.add_debt_notes)

        # Live preview of the computed schedule (refreshes on every keystroke).
        # This is the only summary surface — there is no modal preview popup.
        # Rendered as styled HTML inside a QLabel; the page uses Qt's rich-text
        # auto-detection so we can colour rows and embed the over-cap callout.
        self.add_debt_preview = QLabel(_ADD_DEBT_PREVIEW_PLACEHOLDER_HTML)
        self.add_debt_preview.setWordWrap(True)
        self.add_debt_preview.setTextFormat(Qt.TextFormat.RichText)
        self.add_debt_preview.setStyleSheet(
            "QLabel { padding: 12px 14px; background: #20242b; "
            "border: 1px solid #3a3f48; border-radius: 6px; color: #e8eaed; }"
        )
        debt_form.addRow("Schedule summary:", self.add_debt_preview)

        add_debt_btn_row = QHBoxLayout()
        # No `&` — Qt would render it as a mnemonic underscore on macOS.
        add_debt_btn = QPushButton("Confirm Add Debt")
        add_debt_btn.setStyleSheet(_STYLE_PRIMARY)
        add_debt_btn.clicked.connect(self._submit_add_debt)
        add_debt_btn_row.addWidget(add_debt_btn)
        add_debt_btn_row.addStretch()
        debt_form.addRow("", add_debt_btn_row)

        # Sync field-visibility to the radio choice and the unit labels
        # to the schedule choice on launch.
        self._on_add_debt_schedule_changed()
        self._on_add_debt_choice_changed()

        layout.addWidget(self.add_debt_group)

        # --- Pay Debt (extra payments) ---
        self.pay_debt_group = QGroupBox("Pay Debt — extra payment beyond auto-deduction")
        self.pay_debt_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Maximum)
        pay_debt_form = QFormLayout(self.pay_debt_group)
        pay_debt_form.setSpacing(8)

        self.pay_debt_date = QLineEdit(date.today().isoformat())
        pay_debt_form.addRow("Date:", self.pay_debt_date)

        self.pay_debt_combo = QComboBox()
        self.pay_debt_combo.currentIndexChanged.connect(self._on_pay_debt_combo_changed)
        # Switching debts must re-render the preview against the new selection.
        self.pay_debt_combo.currentIndexChanged.connect(self._on_pay_debt_inputs_changed)
        pay_debt_form.addRow("Debt:", self.pay_debt_combo)

        self.pay_debt_balance_label = QLabel("Remaining balance: —")
        self.pay_debt_balance_label.setStyleSheet("color: #666; padding: 2px 0;")
        pay_debt_form.addRow("", self.pay_debt_balance_label)

        amt_row = QHBoxLayout()
        self.pay_debt_amount = QLineEdit()
        self.pay_debt_amount.setPlaceholderText("Payment amount")
        # User typing in the amount field cancels any "Pay Off in Full"
        # arming so we don't accidentally apply the payoff path on a
        # smaller-than-payoff amount.
        self.pay_debt_amount.textEdited.connect(self._on_pay_debt_amount_edited)
        # Refresh the live preview whenever the amount changes.
        self.pay_debt_amount.textEdited.connect(self._on_pay_debt_inputs_changed)
        amt_row.addWidget(self.pay_debt_amount)
        self.pay_debt_full_btn = QPushButton("Pay Off in Full")
        self.pay_debt_full_btn.setToolTip(
            "Pay off the principal balance plus the current payment "
            "period's accrued interest. Cash leaves at the full payoff "
            "amount; debt balance lands at exactly zero."
        )
        self.pay_debt_full_btn.clicked.connect(self._on_pay_debt_full_clicked)
        amt_row.addWidget(self.pay_debt_full_btn)
        pay_debt_form.addRow("Amount:", amt_row)

        # Live recomputation preview — mirrors Add Debt's 5-line summary,
        # but driven by `recompute_after_payment` so the user sees the
        # post-payment regular payment, period count, final payment,
        # total paid and total interest before clicking Submit.
        self.pay_debt_preview = QLabel("")
        self.pay_debt_preview.setWordWrap(True)
        self.pay_debt_preview.setTextFormat(Qt.TextFormat.RichText)
        self.pay_debt_preview.setStyleSheet(
            "QLabel { padding: 12px 14px; background: #20242b; "
            "border: 1px solid #3a3f48; border-radius: 6px; color: #e8eaed; }"
        )
        pay_debt_form.addRow("Schedule summary:", self.pay_debt_preview)

        self.pay_debt_notes = QLineEdit()
        self.pay_debt_notes.setPlaceholderText("Notes (optional)")
        pay_debt_form.addRow("Notes:", self.pay_debt_notes)

        pay_debt_btn = QPushButton("Submit Payment")
        pay_debt_btn.setStyleSheet(_STYLE_PRIMARY)
        pay_debt_btn.clicked.connect(self._submit_pay_debt)
        pay_debt_form.addRow("", pay_debt_btn)

        layout.addWidget(self.pay_debt_group)

        # --- Pay Mortgage (extra payments) ---
        self.pay_mort_group = QGroupBox("Pay Mortgage — extra payment beyond auto-deduction")
        self.pay_mort_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Maximum)
        pay_mort_form = QFormLayout(self.pay_mort_group)
        pay_mort_form.setSpacing(8)

        self.pay_mort_date = QLineEdit(date.today().isoformat())
        pay_mort_form.addRow("Date:", self.pay_mort_date)

        self.pay_mort_combo = QComboBox()
        self.pay_mort_combo.currentIndexChanged.connect(self._on_pay_mort_combo_changed)
        # Refresh the Pay Mortgage preview whenever the property selection
        # changes (mirrors Pay Debt — see _on_pay_mort_inputs_changed).
        self.pay_mort_combo.currentIndexChanged.connect(self._on_pay_mort_inputs_changed)
        pay_mort_form.addRow("Property:", self.pay_mort_combo)

        self.pay_mort_balance_label = QLabel("Remaining mortgage: —")
        self.pay_mort_balance_label.setStyleSheet("color: #666; padding: 2px 0;")
        pay_mort_form.addRow("", self.pay_mort_balance_label)

        m_amt_row = QHBoxLayout()
        self.pay_mort_amount = QLineEdit()
        self.pay_mort_amount.setPlaceholderText("Payment amount")
        # Manual edits to the amount field cancel the payoff intent (mirror Pay Debt).
        self.pay_mort_amount.textEdited.connect(self._on_pay_mort_amount_edited)
        # And refresh the live preview.
        self.pay_mort_amount.textEdited.connect(self._on_pay_mort_inputs_changed)
        m_amt_row.addWidget(self.pay_mort_amount)
        self.pay_mort_full_btn = QPushButton("Pay Off in Full")
        self.pay_mort_full_btn.clicked.connect(self._on_pay_mort_full_clicked)
        m_amt_row.addWidget(self.pay_mort_full_btn)
        pay_mort_form.addRow("Amount:", m_amt_row)

        # 5-line schedule preview — matches the styling used by the
        # Add Debt, Pay Debt, and Add Property mortgage previews so all
        # four "Schedule summary:" panels look identical.
        self.pay_mort_preview = QLabel("")
        self.pay_mort_preview.setWordWrap(True)
        self.pay_mort_preview.setTextFormat(Qt.TextFormat.RichText)
        self.pay_mort_preview.setStyleSheet(
            "QLabel { padding: 12px 14px; background: #20242b; "
            "border: 1px solid #3a3f48; border-radius: 6px; color: #e8eaed; }"
        )
        pay_mort_form.addRow("Schedule summary:", self.pay_mort_preview)

        self.pay_mort_notes = QLineEdit()
        self.pay_mort_notes.setPlaceholderText("Notes (optional)")
        pay_mort_form.addRow("Notes:", self.pay_mort_notes)

        pay_mort_btn = QPushButton("Submit Mortgage Payment")
        pay_mort_btn.setStyleSheet(_STYLE_PRIMARY)
        pay_mort_btn.clicked.connect(self._submit_pay_mortgage)
        pay_mort_form.addRow("", pay_mort_btn)

        layout.addWidget(self.pay_mort_group)

        # --- Security Search ---
        search_group = QGroupBox("Search Securities Universe")
        search_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Maximum)
        search_layout = QVBoxLayout(search_group)

        search_row = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search by symbol or name (e.g. AAPL, Apple, SPY)...")
        self.search_input.returnPressed.connect(self._search_securities)
        search_row.addWidget(self.search_input)

        self.search_type_filter = QComboBox()
        self.search_type_filter.addItem("All", "")
        self.search_type_filter.addItem("Stock", "stock")
        self.search_type_filter.addItem("ETF", "etf")
        self.search_type_filter.setFixedWidth(100)
        search_row.addWidget(self.search_type_filter)

        search_btn = QPushButton("Search")
        search_btn.clicked.connect(self._search_securities)
        search_row.addWidget(search_btn)

        self.add_asset_btn = QPushButton("Add Selected Asset")
        self.add_asset_btn.setStyleSheet("padding: 6px 16px; font-size: 13px; background-color: #1565c0; color: white;")
        self.add_asset_btn.clicked.connect(self._add_selected_asset)
        self.add_asset_btn.setEnabled(False)
        search_row.addWidget(self.add_asset_btn)

        search_layout.addLayout(search_row)

        self.search_results_table = make_table(["Symbol", "Name", "Type", "Exchange", "Sector/Category"])
        configure_expanding_table(self.search_results_table)
        self.search_results_table.setSelectionBehavior(self.search_results_table.SelectionBehavior.SelectRows)
        self.search_results_table.selectionModel().selectionChanged.connect(self._on_search_selection_changed)
        hdr = self.search_results_table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        hdr.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        self.search_results_table.setFixedHeight(60)
        search_layout.addWidget(self.search_results_table)

        self.search_status = QLabel("")
        self.search_status.setStyleSheet("font-size: 13px; color: #999;")
        search_layout.addWidget(self.search_status)

        layout.addWidget(search_group)

        # --- Preview panel ---
        self.preview_group = QGroupBox("Trade Preview")
        self.preview_group.setVisible(False)
        self.preview_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Maximum)
        preview_layout = QVBoxLayout(self.preview_group)

        self.preview_status = QLabel("")
        self.preview_status.setStyleSheet("font-size: 14px; font-weight: bold; padding: 4px;")
        preview_layout.addWidget(self.preview_status)

        self.preview_details = make_table([
            "Field", "Value",
        ])
        configure_expanding_table(self.preview_details)
        hdr_pd = self.preview_details.horizontalHeader()
        hdr_pd.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hdr_pd.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        preview_layout.addWidget(self.preview_details)

        self.preview_alloc_label = QLabel("Allocation Changes")
        self.preview_alloc_label.setStyleSheet("font-weight: bold; padding-top: 8px;")
        preview_layout.addWidget(self.preview_alloc_label)

        self.preview_alloc_table = make_table([
            "Asset Type", "Before %", "After %",
        ])
        configure_expanding_table(self.preview_alloc_table)
        hdr_pa = self.preview_alloc_table.horizontalHeader()
        hdr_pa.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        hdr_pa.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        hdr_pa.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        preview_layout.addWidget(self.preview_alloc_table)

        self.preview_risk_label = QLabel("Risk Changes")
        self.preview_risk_label.setStyleSheet("font-weight: bold; padding-top: 8px;")
        preview_layout.addWidget(self.preview_risk_label)

        self.preview_risk_table = make_table(["Type", "Detail"])
        configure_expanding_table(self.preview_risk_table)
        hdr_pr = self.preview_risk_table.horizontalHeader()
        hdr_pr.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hdr_pr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        preview_layout.addWidget(self.preview_risk_table)

        layout.addWidget(self.preview_group)

        # --- History table ---
        history_group = QGroupBox("Transaction History")
        history_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Maximum)
        history_layout = QVBoxLayout(history_group)
        self.table = make_table([
            "ID", "Date", "Type", "Asset", "Qty", "Price", "Total", "Fees", "Notes",
        ])
        configure_expanding_table(self.table)
        hdr_h = self.table.horizontalHeader()
        for col in range(8):
            hdr_h.setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)
        hdr_h.setSectionResizeMode(8, QHeaderView.ResizeMode.Stretch)
        history_layout.addWidget(self.table)
        layout.addWidget(history_group)

        layout.addStretch()

        scroll.setWidget(content)
        outer.addWidget(scroll)

        self._on_type_changed()

        self.date_input.textChanged.connect(self._invalidate_preview)
        self.asset_combo.currentIndexChanged.connect(self._invalidate_preview)
        self.qty_input.textChanged.connect(self._invalidate_preview)
        self.amount_input.textChanged.connect(self._invalidate_preview)
        self.fees_input.textChanged.connect(self._invalidate_preview)
        self.notes_input.textChanged.connect(self._invalidate_preview)

    @staticmethod
    def _set_field_visible(label, widget, visible: bool):
        label.setVisible(visible)
        widget.setVisible(visible)

    def _set_trade_action_state(self, state: str):
        if state == "needs_preview":
            self.preview_btn.setStyleSheet(_STYLE_PRIMARY)
            self.preview_btn.setText("Preview Trade")
            self.confirm_btn.setStyleSheet(_STYLE_SECONDARY)
            self.confirm_btn.setEnabled(False)
            self.confirm_btn.setToolTip("Preview the trade before confirming.")
            self.cancel_btn.setEnabled(False)
        elif state == "preview_ready":
            self.preview_btn.setStyleSheet(_STYLE_SECONDARY)
            self.preview_btn.setText("Update Preview")
            self.confirm_btn.setStyleSheet(_STYLE_PRIMARY)
            self.confirm_btn.setEnabled(True)
            self.confirm_btn.setToolTip("Confirm this previewed trade.")
            self.cancel_btn.setEnabled(True)
        elif state == "preview_failed":
            self.preview_btn.setStyleSheet(_STYLE_PRIMARY)
            self.preview_btn.setText("Preview Trade")
            self.confirm_btn.setStyleSheet(_STYLE_SECONDARY)
            self.confirm_btn.setEnabled(False)
            self.confirm_btn.setToolTip("Preview the trade before confirming.")
            self.cancel_btn.setEnabled(True)

    def _invalidate_preview(self):
        if self._current_preview is not None:
            self._current_preview = None
            self.preview_group.setVisible(False)
            self._set_trade_action_state("needs_preview")

    def _on_type_changed(self, _index=None):
        txn_type = self.txn_type.currentData()
        if txn_type is None:
            return
        is_debt_op = txn_type in DEBT_OP_TYPES

        # Wipe stale numeric inputs from the previous mode so a "500"
        # typed for a deposit can't carry into a buy / pay-debt screen.
        self._clear_all_transient_inputs()

        # Toggle the matching debt groupbox; hide the other two.
        # Stored as instance attrs in __init__ so they're addressable here.
        if hasattr(self, "add_debt_group"):
            self.add_debt_group.setVisible(txn_type == "add_debt")
        if hasattr(self, "pay_debt_group"):
            self.pay_debt_group.setVisible(txn_type == "pay_debt")
        if hasattr(self, "pay_mort_group"):
            self.pay_mort_group.setVisible(txn_type == "pay_mortgage")

        if is_debt_op:
            # Collapse the main form to just the type combo. The selected
            # groupbox owns the rest of the input surface.
            self._set_field_visible(self.date_label, self.date_input, False)
            self._set_field_visible(self.asset_label, self.asset_combo, False)
            self._set_field_visible(self.amount_label, self.amount_input, False)
            self._set_field_visible(self.qty_label, self.qty_input, False)
            self._set_field_visible(self.price_label, self.price_input, False)
            self._set_field_visible(self.fees_label, self.fees_input, False)
            self._set_field_visible(self.notes_label, self.notes_input, False)
            self._set_field_visible(self._btn_row_label, self.btn_row_widget, False)
            self._cancel_preview()
            return

        # Non-debt types: show the main form, hide all three debt groupboxes.
        self._set_field_visible(self.date_label, self.date_input, True)
        self._set_field_visible(self.notes_label, self.notes_input, True)
        self._set_field_visible(self._btn_row_label, self.btn_row_widget, True)

        # Repopulate the asset combo so pay_property_expense gets only
        # real-estate options when that mode is selected.
        self._load_assets()

        needs_asset = txn_type in ASSET_REQUIRED
        needs_amount = txn_type in AMOUNT_REQUIRED
        needs_qty = txn_type in QTY_PRICE_REQUIRED
        is_previewable = txn_type in PREVIEWABLE

        show_amount = needs_amount or is_previewable
        self._set_field_visible(self.asset_label, self.asset_combo, needs_asset)
        self._set_field_visible(self.amount_label, self.amount_input, show_amount)
        if is_previewable:
            self.amount_label.setText("Trade Amount:")
            self.amount_input.setPlaceholderText("Target asset value (optional)")
        else:
            self.amount_label.setText("Amount:")
            self.amount_input.setPlaceholderText("Amount (cash)")
        self._set_field_visible(self.qty_label, self.qty_input, needs_qty)
        self._set_field_visible(self.price_label, self.price_input, needs_qty and not is_previewable)
        self._set_field_visible(self.fees_label, self.fees_input, needs_qty)
        self.preview_btn.setVisible(is_previewable)
        self.confirm_btn.setVisible(is_previewable)
        self.cancel_btn.setVisible(is_previewable)
        self.submit_btn.setVisible(not is_previewable)

        self._cancel_preview()

    @staticmethod
    def _asset_combo_label(asset) -> str:
        """How an asset shows in dropdowns.

        Real-estate symbols are auto-generated from the user's typed name
        (e.g. "Home" → "RE_HOME"), so showing the symbol is uninformative
        clutter. Other asset types have meaningful symbols (AAPL, BTC) and
        keep the "SYMBOL - Name" form.
        """
        if asset.asset_type == "real_estate":
            return (asset.name or "").strip() or asset.symbol
        return f"{asset.symbol} - {asset.name}"

    @staticmethod
    def _asset_history_label(asset) -> str:
        """How an asset shows in the transaction-history Asset column."""
        if asset.asset_type == "real_estate":
            return (asset.name or "").strip() or asset.symbol
        return asset.symbol

    def _load_assets(self):
        """Populate the main asset combo for the *current* transaction type.

        - ``buy`` / ``sell`` show **only** sellable types (stock, ETF,
          crypto, custom). Real-estate is bought/sold via the Real Estate
          page; debts move via Add/Pay Debt. Letting them appear here was
          a UX trap: the engine rejected the trade after the click but
          the user could pick the asset in the first place.
        - ``pay_property_expense`` shows only real-estate.
        - Other types (cash flows, etc.) don't use this combo.
        """
        self.asset_combo.clear()
        txn_type = self.txn_type.currentData() if self.txn_type.count() else None
        assets = list_assets(self.conn)
        if txn_type in ("buy", "sell"):
            assets = [a for a in assets if a.asset_type in SELLABLE_ASSET_TYPES]
        elif txn_type == "pay_property_expense":
            assets = [a for a in assets if a.asset_type == "real_estate"]
        for a in assets:
            self.asset_combo.addItem(self._asset_combo_label(a), a.id)

    def _load_debts(self):
        self.pay_debt_combo.clear()
        debts = [d for d in list_debts(self.conn) if d.current_balance > 0]
        if not debts:
            self.pay_debt_combo.addItem("(no outstanding debts)", None)
        else:
            for d in debts:
                # Fall back to a placeholder so legacy/imported rows with a
                # missing name don't render as a bare em-dash.
                pretty = (d.name or "").strip() or f"(unnamed debt #{d.id})"
                label = f"{pretty} — balance {fmt_money(d.current_balance)}"
                self.pay_debt_combo.addItem(label, d.asset_id)
        self._on_pay_debt_combo_changed()
        # The currentIndexChanged signal may have already fired during
        # addItem to repopulate the preview, but a direct explicit call
        # keeps the placeholder text visible for the empty/no-amount case
        # even when the index didn't actually change.
        self._on_pay_debt_inputs_changed()

    def _load_mortgaged_properties(self):
        from src.storage.mortgage_repo import get_mortgage_by_property
        self.pay_mort_combo.clear()
        # Filter to properties whose linked mortgage has remaining balance.
        candidates = []
        for p in list_active_properties(self.conn):
            m = get_mortgage_by_property(self.conn, p.id)
            if m is not None and m.current_balance > 0:
                candidates.append((p, m))
        if not candidates:
            self.pay_mort_combo.addItem("(no properties with outstanding mortgage)", None)
        else:
            asset_lookup = {a.id: a for a in list_assets(self.conn)}
            for p, m in candidates:
                asset = asset_lookup.get(p.asset_id)
                pname = (asset.name if asset else "").strip() or f"(unnamed property #{p.id})"
                label = f"{pname} — mortgage {fmt_money(m.current_balance)}"
                self.pay_mort_combo.addItem(label, p.asset_id)
        self._on_pay_mort_combo_changed()

    def _current_pay_debt_balance(self) -> float | None:
        aid = self.pay_debt_combo.currentData()
        if aid is None:
            return None
        for d in list_debts(self.conn):
            if d.asset_id == aid:
                return d.current_balance
        return None

    def _current_pay_mort_balance(self) -> float | None:
        from src.storage.mortgage_repo import get_mortgage_by_property
        aid = self.pay_mort_combo.currentData()
        if aid is None:
            return None
        for p in list_active_properties(self.conn):
            if p.asset_id == aid:
                m = get_mortgage_by_property(self.conn, p.id)
                return m.current_balance if m is not None else 0.0
        return None

    def _on_pay_debt_combo_changed(self, _index=None):
        balance = self._current_pay_debt_balance()
        if balance is None:
            self.pay_debt_balance_label.setText("Remaining balance: —")
            self.pay_debt_full_btn.setEnabled(False)
        else:
            self.pay_debt_balance_label.setText(
                f"Remaining balance: {fmt_money(balance)}"
            )
            self.pay_debt_full_btn.setEnabled(True)
        # Switching debts disarms a previously-armed payoff: the displayed
        # amount belonged to the old debt and is no longer the right
        # payoff figure. The preview will re-render via the second
        # currentIndexChanged connection.
        self._pay_debt_payoff_armed = False

    def _on_pay_mort_combo_changed(self, _index=None):
        balance = self._current_pay_mort_balance()
        if balance is None:
            self.pay_mort_balance_label.setText("Remaining mortgage: —")
            self.pay_mort_full_btn.setEnabled(False)
        else:
            self.pay_mort_balance_label.setText(
                f"Remaining mortgage: {fmt_money(balance)}"
            )
            self.pay_mort_full_btn.setEnabled(True)
        # Switching properties disarms a previously-armed payoff (the
        # displayed amount belonged to the old mortgage).
        self._pay_mortgage_payoff_armed = False

    def _on_pay_mort_amount_edited(self, _text=None):
        # Manual edits cancel a Pay Off in Full intent so submit
        # behaves as a partial pay.
        self._pay_mortgage_payoff_armed = False

    def _on_pay_mort_inputs_changed(self, *_):
        """Render the post-payment mortgage plan preview. Mirror of
        _on_pay_debt_inputs_changed."""
        from src.storage.mortgage_repo import get_mortgage_by_property
        aid = self.pay_mort_combo.currentData()
        if aid is None:
            self.pay_mort_preview.setText(
                '<span style="color:#9aa0a6;">Select a property to see '
                'the post-payment plan.</span>'
            )
            return
        prop_obj = next(
            (p for p in list_active_properties(self.conn) if p.asset_id == aid),
            None,
        )
        mortgage = (
            get_mortgage_by_property(self.conn, prop_obj.id)
            if prop_obj is not None else None
        )
        if mortgage is None:
            self.pay_mort_preview.setText("")
            return
        amount_text = self.pay_mort_amount.text().strip()
        if not amount_text:
            self.pay_mort_preview.setText(
                '<span style="color:#9aa0a6;">Enter a payment amount to see '
                'the recalculated plan.</span>'
            )
            return
        try:
            amount = float(amount_text)
        except ValueError:
            self.pay_mort_preview.setText(
                '<span style="color:#9aa0a6;">Payment amount must be a number.</span>'
            )
            return
        if amount <= 0:
            self.pay_mort_preview.setText(
                '<div style="padding:6px 8px; background:#3b1f1f; '
                'border:1px solid #862424; border-radius:4px; color:#ffab91;">'
                '<b>⚠</b> Payment amount must be greater than zero.'
                '</div>'
            )
            return
        payoff = ledger.compute_mortgage_payoff_amount(self.conn, aid)
        if amount > payoff + 0.005:
            self.pay_mort_preview.setText(
                '<div style="padding:6px 8px; background:#3b1f1f; '
                'border:1px solid #862424; border-radius:4px; color:#ffab91;">'
                f'<b>⚠ Payment exceeds payoff amount</b> '
                f'({fmt_money(amount)} &gt; {fmt_money(payoff)}). '
                f'Reduce the amount or use "Pay Off in Full".'
                '</div>'
            )
            return
        scheduled = ledger.count_scheduled_mortgage_payments(self.conn, aid)
        sched = recompute_after_payment(mortgage, amount, scheduled)
        if sched.infeasibility_reason == PAID_OFF_REASON:
            self.pay_mort_preview.setText(
                '<div style="padding:6px 8px; background:#1f3b22; '
                'border:1px solid #2f7a3a; border-radius:4px; color:#b9f6ca;">'
                '<b>✓ This payment will fully pay off the mortgage</b> '
                '(principal balance plus this period&rsquo;s accrued interest).'
                '</div>'
            )
            return
        if not sched.feasible:
            self.pay_mort_preview.setText(
                '<div style="padding:6px 8px; background:#3b1f1f; '
                'border:1px solid #862424; border-radius:4px; color:#ffab91;">'
                f'<b>⚠ Recalculation infeasible:</b> {sched.infeasibility_reason}'
                '</div>'
            )
            return
        past_paid = ledger.total_paid_for_mortgage(self.conn, aid)
        total_paid = past_paid + amount + sched.total_paid
        total_interest = max(0.0, total_paid - float(mortgage.original_amount or 0.0))
        rows = [
            ("Per-month payment", fmt_money(sched.per_period_payment)),
            ("Months remaining", str(sched.num_periods)),
            ("Final month's payment", fmt_money(sched.final_payment)),
            ("Total paid", fmt_money(total_paid)),
            ("Total interest", fmt_money(total_interest)),
        ]
        rows_html = "".join(
            '<tr>'
            f'<td style="padding:3px 22px 3px 0; color:#9aa0a6;">{k}</td>'
            f'<td style="padding:3px 0; color:#e8eaed; font-weight:600;">{v}</td>'
            '</tr>'
            for k, v in rows
        )
        self.pay_mort_preview.setText(
            f'<table style="border-collapse:collapse;">{rows_html}</table>'
        )

    def _on_pay_debt_full_clicked(self):
        aid = self.pay_debt_combo.currentData()
        if aid is None:
            return
        # Show the *true* payoff amount (balance + this period's interest)
        # in the field so the user sees what cash will leave. Manual edits
        # to the field disarm the payoff path — see _on_pay_debt_amount_edited.
        payoff = ledger.compute_payoff_amount(self.conn, aid)
        if payoff <= 0:
            return
        self._pay_debt_payoff_armed = True
        # Block textEdited so _on_pay_debt_amount_edited can't disarm the
        # payoff flag we just set. This also suppresses the preview-refresh
        # connection, so we trigger it manually below.
        self.pay_debt_amount.blockSignals(True)
        self.pay_debt_amount.setText(f"{payoff:.2f}")
        self.pay_debt_amount.blockSignals(False)
        self._on_pay_debt_inputs_changed()

    def _on_pay_debt_amount_edited(self, _text=None):
        # Any keystroke in the amount field cancels the payoff intent —
        # the user has overridden the suggested amount, so submit should
        # behave as a regular partial pay_debt.
        self._pay_debt_payoff_armed = False

    def _on_pay_debt_inputs_changed(self, *_):
        """Render the post-payment debt plan preview.

        The preview shows the spec's 5 required lines (regular payment,
        periods left, final payment, total paid, total interest) using
        `debt_math.recompute_after_payment` so the calculation lives in
        one place. Empty/invalid amount → instructional placeholder. Amount
        > balance → red warning that disables the submit path. Payment
        fully clears the debt → an explicit "this will pay off the debt"
        callout instead of the table.
        """
        aid = self.pay_debt_combo.currentData()
        if aid is None:
            self.pay_debt_preview.setText(
                '<span style="color:#9aa0a6;">Select a debt to see the '
                'post-payment plan.</span>'
            )
            return
        debt = get_debt_by_asset(self.conn, aid)
        if debt is None:
            self.pay_debt_preview.setText("")
            return
        amount_text = self.pay_debt_amount.text().strip()
        if not amount_text:
            self.pay_debt_preview.setText(
                '<span style="color:#9aa0a6;">Enter a payment amount to see '
                'the recalculated plan.</span>'
            )
            return
        try:
            amount = float(amount_text)
        except ValueError:
            self.pay_debt_preview.setText(
                '<span style="color:#9aa0a6;">Payment amount must be a number.</span>'
            )
            return
        if amount <= 0:
            self.pay_debt_preview.setText(
                '<div style="padding:6px 8px; background:#3b1f1f; '
                'border:1px solid #862424; border-radius:4px; color:#ffab91;">'
                '<b>⚠</b> Payment amount must be greater than zero.'
                '</div>'
            )
            return
        # Upper bound is the payoff amount (balance + this period's
        # interest). Anything within that is a valid pay_debt — the
        # interest-split math lands the balance at zero when the user
        # pays exactly the payoff. Half-cent tolerance absorbs the gap
        # between the displayed payoff (rounded to 2 decimals) and the
        # underlying float — without it, typing the displayed value
        # would trip the "exceeds" warning when the float is slightly
        # less (e.g. interest = 6.6666... displayed as 6.67).
        payoff = ledger.compute_payoff_amount(self.conn, aid)
        if amount > payoff + 0.005:
            self.pay_debt_preview.setText(
                '<div style="padding:6px 8px; background:#3b1f1f; '
                'border:1px solid #862424; border-radius:4px; color:#ffab91;">'
                f'<b>⚠ Payment exceeds payoff amount</b> '
                f'({fmt_money(amount)} &gt; {fmt_money(payoff)}). '
                f'Reduce the amount or use "Pay Off in Full".'
                '</div>'
            )
            return
        scheduled = ledger.count_scheduled_debt_payments(self.conn, aid)
        sched = recompute_after_payment(debt, amount, scheduled)
        if sched.infeasibility_reason == PAID_OFF_REASON:
            # Spec §3 #3: emphasize that "paying off" = principal + the
            # current period's accrued interest. Use Pay Off in Full to
            # charge exactly that amount.
            self.pay_debt_preview.setText(
                '<div style="padding:6px 8px; background:#1f3b22; '
                'border:1px solid #2f7a3a; border-radius:4px; color:#b9f6ca;">'
                '<b>✓ This payment will fully pay off the debt</b> '
                '(principal balance plus this period&rsquo;s accrued interest).'
                '</div>'
            )
            return
        if not sched.feasible:
            self.pay_debt_preview.setText(
                '<div style="padding:6px 8px; background:#3b1f1f; '
                'border:1px solid #862424; border-radius:4px; color:#ffab91;">'
                f'<b>⚠ Recalculation infeasible:</b> {sched.infeasibility_reason}'
                '</div>'
            )
            return
        per_label = "month" if sched.schedule == "monthly" else "year"
        # Spec §3.4.10: "total paid" = past payments + this payment +
        # future scheduled payments. Total interest is total paid minus
        # the original principal.
        past_paid = ledger.total_paid_for_debt(self.conn, aid)
        total_paid = past_paid + amount + sched.total_paid
        total_interest = max(0.0, total_paid - float(debt.original_amount or 0.0))
        rows = [
            (f"Per-{per_label} payment", fmt_money(sched.per_period_payment)),
            (f"{per_label.capitalize()}s remaining", str(sched.num_periods)),
            (f"Final {per_label}'s payment", fmt_money(sched.final_payment)),
            ("Total paid", fmt_money(total_paid)),
            ("Total interest", fmt_money(total_interest)),
        ]
        rows_html = "".join(
            '<tr>'
            f'<td style="padding:3px 22px 3px 0; color:#9aa0a6;">{k}</td>'
            f'<td style="padding:3px 0; color:#e8eaed; font-weight:600;">{v}</td>'
            '</tr>'
            for k, v in rows
        )
        self.pay_debt_preview.setText(
            f'<table style="border-collapse:collapse;">{rows_html}</table>'
        )

    def _on_pay_mort_full_clicked(self):
        aid = self.pay_mort_combo.currentData()
        if aid is None:
            return
        payoff = ledger.compute_mortgage_payoff_amount(self.conn, aid)
        if payoff <= 0:
            return
        self._pay_mortgage_payoff_armed = True
        # Block textEdited so _on_pay_mort_amount_edited can't disarm
        # the payoff flag we just set; manually trigger the preview
        # refresh after (mirror of _on_pay_debt_full_clicked).
        self.pay_mort_amount.blockSignals(True)
        self.pay_mort_amount.setText(f"{payoff:.2f}")
        self.pay_mort_amount.blockSignals(False)
        self._on_pay_mort_inputs_changed()

    def _on_add_debt_schedule_changed(self, _index=None):
        """Re-label the term and payment rows so their unit is unambiguous.

        Without this, a user picking "Monthly" and entering "5" in the
        term field could mean "5 years" in their head while the system
        reads "5 months" — leading to a debt that silently slips under
        a months-denominated payoff cap.

        Also retargets the "First scheduled payment" default — first of
        next month for monthly; next Jan 1 for yearly — but only when the
        user hasn't typed something custom into the field.
        """
        schedule = self.add_debt_schedule.currentData() or "monthly"
        if schedule == "yearly":
            self.add_debt_payment_label.setText("Payment per year:")
            self.add_debt_payment.setPlaceholderText("Per-year payment")
            self.add_debt_term_label.setText("Term (in years):")
            self.add_debt_term.setPlaceholderText("Number of years")
        else:
            self.add_debt_payment_label.setText("Payment per month:")
            self.add_debt_payment.setPlaceholderText("Per-month payment")
            self.add_debt_term_label.setText("Term (in months):")
            self.add_debt_term.setPlaceholderText("Number of months")
        self._on_add_debt_inputs_changed()

    def _on_add_debt_choice_changed(self):
        """Show only the input that matches the chosen radio.

        The unselected row's label *and* widget are hidden so the form
        only displays the field that's actually in use. Clearing the
        hidden field keeps the live preview in sync.
        """
        by_payment = self.add_debt_radio_payment.isChecked()
        # Hide both label and widget for the unselected option.
        self.add_debt_payment_label.setVisible(by_payment)
        self.add_debt_payment.setVisible(by_payment)
        self.add_debt_term_label.setVisible(not by_payment)
        self.add_debt_term.setVisible(not by_payment)
        if by_payment:
            self.add_debt_term.clear()
        else:
            self.add_debt_payment.clear()
        self._on_add_debt_inputs_changed()

    def _on_add_debt_inputs_changed(self, *_):
        """Refresh the live preview when any input changes.

        Renders a small HTML table inside `add_debt_preview` so each row
        reads as label + value, with the over-cap warning styled as a
        dedicated red callout.
        """
        sched = self._compute_preview_schedule()
        if sched is None:
            self.add_debt_preview.setText(_ADD_DEBT_PREVIEW_PLACEHOLDER_HTML)
            return
        if not sched.feasible:
            self.add_debt_preview.setText(
                '<div style="padding:6px 8px; background:#3b1f1f; '
                'border:1px solid #862424; border-radius:4px; '
                'color:#ffab91;">'
                f'<b>⚠ Infeasible:</b> {sched.infeasibility_reason}'
                '</div>'
            )
            return
        months = normalize_period_to_months(sched.num_periods, sched.schedule)
        cap = get_max_debt_payoff_months(self.conn)
        per_label = "month" if sched.schedule == "monthly" else "year"

        rows = [
            (f"Per-{per_label} payment", fmt_money(sched.per_period_payment)),
            (f"Number of {per_label}s", f"{sched.num_periods} ({months} months total)"),
            (f"Final {per_label}'s payment", fmt_money(sched.final_payment)),
            ("Total paid", fmt_money(sched.total_paid)),
            ("Total interest", fmt_money(sched.total_interest)),
        ]
        rows_html = "".join(
            '<tr>'
            f'<td style="padding:3px 22px 3px 0; color:#9aa0a6;">{k}</td>'
            f'<td style="padding:3px 0; color:#e8eaed; font-weight:600;">{v}</td>'
            '</tr>'
            for k, v in rows
        )
        warn_html = ""
        if months > cap:
            warn_html = (
                '<div style="margin-top:10px; padding:6px 10px; '
                'background:#3b1f1f; border:1px solid #862424; '
                'border-radius:4px; color:#ffab91;">'
                f'⚠ <b>Payoff horizon ({months} months)</b> exceeds the max '
                f'set in Settings ({cap} months). Submit will be blocked '
                f'until you raise the limit or shorten the schedule.'
                '</div>'
            )
        self.add_debt_preview.setText(
            f'<table style="border-collapse:collapse;">{rows_html}</table>'
            f'{warn_html}'
        )

    def _resolved_debt_rate_pct(self) -> float:
        """Effective annual interest rate (in percent) for the Add Debt
        form. Returns the user-typed value when it parses to a
        non-negative number; otherwise falls back to the configured
        default (which itself ultimately falls back to
        ``DEFAULT_DEBT_ANNUAL_RATE_PCT = 7.0`` when the setting is
        missing/invalid). Used uniformly by the live preview, submit
        validation, and the call into ``ledger.add_debt`` so the three
        paths can never disagree on the rate.
        """
        text = self.add_debt_rate.text().strip()
        if text:
            try:
                v = float(text)
            except ValueError:
                v = None
            else:
                if v >= 0:
                    return v
        return get_default_debt_annual_rate_pct(self.conn)

    def _compute_preview_schedule(self):
        """Best-effort schedule preview from the current form. Returns
        None if the inputs aren't sufficient to even attempt a schedule."""
        try:
            amount = float(self.add_debt_amount.text().strip())
        except (ValueError, AttributeError):
            return None
        if amount <= 0:
            return None
        rate = self._resolved_debt_rate_pct() / 100.0
        schedule = self.add_debt_schedule.currentData()
        if self.add_debt_radio_payment.isChecked():
            try:
                payment = float(self.add_debt_payment.text().strip())
            except (ValueError, AttributeError):
                return None
            return compute_debt_schedule(
                principal=amount, annual_rate=rate, schedule=schedule,
                payment=payment,
            )
        try:
            term = int(float(self.add_debt_term.text().strip()))
        except (ValueError, AttributeError):
            return None
        return compute_debt_schedule(
            principal=amount, annual_rate=rate, schedule=schedule,
            term_periods=term,
        )

    def _submit_add_debt(self):
        if guard_transaction_or_warn(self.conn, self):
            return
        try:
            name = self.add_debt_name.text().strip()
            if not name:
                QMessageBox.warning(self, "Input Error", "Debt name is required.")
                return
            symbol = self.add_debt_symbol.text().strip() or name[:8].upper()
            amount_text = self.add_debt_amount.text().strip()
            if not amount_text:
                QMessageBox.warning(self, "Input Error", "Enter the principal amount.")
                return
            amount = float(amount_text)
            if amount <= 0:
                QMessageBox.warning(self, "Input Error", "Principal must be positive.")
                return

            schedule = self.add_debt_schedule.currentData()
            # Reject negative rate explicitly. `_resolved_debt_rate_pct`
            # silently substitutes the configured default for any v < 0,
            # so without this check the user's "-5" would silently become
            # 7% (the default) instead of either being respected or
            # surfaced as an error. A blank rate field still falls back
            # to the default below — spec §6 #4 — that path is intentional.
            rate_text = self.add_debt_rate.text().strip()
            if rate_text:
                try:
                    rate_value = float(rate_text)
                except ValueError:
                    QMessageBox.warning(
                        self, "Input Error",
                        f"Interest rate must be a number (got '{rate_text}').",
                    )
                    return
                if rate_value < 0:
                    QMessageBox.warning(
                        self, "Input Error",
                        "Interest rate cannot be negative.",
                    )
                    return
            rate = self._resolved_debt_rate_pct() / 100.0

            payment = None
            term = None
            if self.add_debt_radio_payment.isChecked():
                payment_text = self.add_debt_payment.text().strip()
                if not payment_text:
                    QMessageBox.warning(self, "Input Error", "Enter the per-period payment.")
                    return
                payment = float(payment_text)
                if payment <= 0:
                    QMessageBox.warning(self, "Input Error", "Payment must be positive.")
                    return
            else:
                term_text = self.add_debt_term.text().strip()
                if not term_text:
                    QMessageBox.warning(self, "Input Error", "Enter the term (number of periods).")
                    return
                term = int(float(term_text))
                if term <= 0:
                    QMessageBox.warning(self, "Input Error", "Term must be positive.")
                    return

            # Enforce the max-payoff cap before persisting. Compute the
            # schedule once and reject if it'd exceed the user's limit.
            sched = compute_debt_schedule(
                principal=amount, annual_rate=rate, schedule=schedule,
                payment=payment, term_periods=term,
            )
            if not sched.feasible:
                QMessageBox.warning(
                    self, "Infeasible Debt",
                    sched.infeasibility_reason or "Debt is not payable at the rate/payment given.",
                )
                return
            cap_months = get_max_debt_payoff_months(self.conn)
            projected_months = normalize_period_to_months(
                sched.num_periods, schedule,
            )
            if projected_months > cap_months:
                QMessageBox.warning(
                    self, "Debt Exceeds Payoff Limit",
                    f"This debt is projected to take {projected_months} "
                    f"months to pay off, but the limit set in Settings is "
                    f"{cap_months} months. Increase the per-period payment "
                    f"(or the term, if shorter) — or raise the limit in "
                    f"Settings — to add this debt.",
                )
                return

            dt = self.add_debt_date.text().strip()
            try:
                date.fromisoformat(dt)
            except ValueError:
                QMessageBox.warning(
                    self, "Input Error",
                    f"Date recorded must be in YYYY-MM-DD format (got '{dt}').",
                )
                return

            # cashflow_start_date is no longer collected — engine
            # default (first_day_next_month / Jan 1 next year) wins.

            notes = self.add_debt_notes.text().strip() or None

            # Loan origination = "Date recorded" (dt) — analogous to
            # how mortgage origination = property purchase_date.
            ledger.add_debt(
                self.conn, dt, symbol=symbol, name=name, amount=amount,
                interest_rate=rate, schedule_frequency=schedule,
                payment_per_period=payment, term_periods=term,
                notes=notes, cash_received=True,
                origination_date=dt,
            )
            QMessageBox.information(
                self, "Debt Added",
                f"Added debt '{name}' with {schedule} auto-deduction.",
            )
            self._clear_add_debt_inputs()
            self.refresh()
            self.data_changed.emit()
        except ValueError as e:
            QMessageBox.warning(self, "Input Error", str(e))
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _submit_pay_debt(self):
        if guard_transaction_or_warn(self.conn, self):
            return
        try:
            aid = self.pay_debt_combo.currentData()
            if aid is None:
                QMessageBox.warning(self, "Input Error", "Select a debt to pay.")
                return
            amount_text = self.pay_debt_amount.text().strip()
            if not amount_text:
                QMessageBox.warning(self, "Input Error", "Enter a payment amount.")
                return
            amount = float(amount_text)
            dt = self.pay_debt_date.text().strip()
            notes = self.pay_debt_notes.text().strip() or None

            if self._pay_debt_payoff_armed:
                # "Pay Off in Full" path — clears the balance, including
                # this period's interest. Amount may legitimately exceed
                # current_balance (it's balance + interest), so we don't
                # apply the partial-payment overpayment guard here.
                ledger.pay_debt_in_full(self.conn, dt, aid, notes=notes)
                self._pay_debt_payoff_armed = False
            else:
                # Upper bound is the payoff amount (balance + this period's
                # interest), not just current_balance — pay_debt's
                # interest-split math handles the boundary case correctly.
                # Half-cent tolerance matches the engine and absorbs the
                # display-rounding gap (see _on_pay_debt_inputs_changed).
                payoff = ledger.compute_payoff_amount(self.conn, aid)
                if amount > payoff + 0.005:
                    QMessageBox.warning(
                        self, "Payment Exceeds Payoff Amount",
                        f"Payment of {fmt_money(amount)} exceeds the payoff "
                        f"amount of {fmt_money(payoff)} (balance plus this "
                        f"period's accrued interest).",
                    )
                    return
                ledger.pay_debt(self.conn, dt, aid, amount, notes=notes)
                # Fixed-term debts get their per-period payment recomputed
                # from the post-payment balance and remaining term. No-op
                # for fixed-payment debts (their per-period amount stays
                # constant by definition).
                ledger.update_plan_after_manual_payment(self.conn, aid)

            self.pay_debt_amount.clear()
            self.pay_debt_notes.clear()
            self.refresh()
            self.data_changed.emit()
        except ValueError as e:
            QMessageBox.warning(self, "Input Error", str(e))
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _submit_pay_mortgage(self):
        if guard_transaction_or_warn(self.conn, self):
            return
        try:
            aid = self.pay_mort_combo.currentData()
            if aid is None:
                QMessageBox.warning(self, "Input Error", "Select a property to pay the mortgage on.")
                return
            amount_text = self.pay_mort_amount.text().strip()
            if not amount_text:
                QMessageBox.warning(self, "Input Error", "Enter a payment amount.")
                return
            amount = float(amount_text)
            dt = self.pay_mort_date.text().strip()
            notes = self.pay_mort_notes.text().strip() or None

            if self._pay_mortgage_payoff_armed:
                # Pay Off in Full path: charges balance + this period's
                # interest; clears mortgage to 0.
                ledger.pay_mortgage_in_full(self.conn, dt, aid, notes=notes)
                self._pay_mortgage_payoff_armed = False
            else:
                # Half-cent tolerance matches the engine and absorbs the
                # display-rounding gap (mirror of Pay Debt).
                payoff = ledger.compute_mortgage_payoff_amount(self.conn, aid)
                if amount > payoff + 0.005:
                    QMessageBox.warning(
                        self, "Payment Exceeds Payoff Amount",
                        f"Payment of {fmt_money(amount)} exceeds the payoff "
                        f"amount of {fmt_money(payoff)} (balance plus this "
                        f"period's accrued interest).",
                    )
                    return
                ledger.pay_mortgage(self.conn, dt, aid, amount, notes=notes)
                # fixed_term mortgages: drop per-period payment after a
                # manual extra payment (debt parity).
                ledger.update_mortgage_plan_after_manual_payment(self.conn, aid)

            self.pay_mort_amount.clear()
            self.pay_mort_notes.clear()
            self.refresh()
            self.data_changed.emit()
        except ValueError as e:
            QMessageBox.warning(self, "Input Error", str(e))
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _clear_add_debt_inputs(self):
        self.add_debt_name.clear()
        self.add_debt_symbol.clear()
        self.add_debt_amount.clear()
        self.add_debt_payment.clear()
        self.add_debt_term.clear()
        self.add_debt_notes.clear()
        self.add_debt_preview.setText(_ADD_DEBT_PREVIEW_PLACEHOLDER_HTML)
        # Reset Date recorded to today so a stale historical date from
        # the last entry can't carry over into the next.
        self.add_debt_date.setText(date.today().isoformat())
        # Restore the rate to the user-configured default so the next debt
        # entry starts from the user's preferred value, not the last one
        # they typed.
        self._populate_default_debt_rate()

    def _populate_default_debt_rate(self):
        """Set the Add Debt rate field to the user-configured default.

        Always overwrites — callers (refresh / clear) decide whether they
        want to call this. Tracks the value we wrote so refresh() can
        distinguish user edits from a stale auto-populated value.
        """
        text = f"{get_default_debt_annual_rate_pct(self.conn):.1f}"
        self.add_debt_rate.setText(text)
        self._last_auto_debt_rate_text = text

    def refresh(self):
        # Discard any preview held over from a prior visit; prices may have
        # moved and the user shouldn't confirm against stale data.
        self._cancel_preview()
        self._load_assets()
        self._load_debts()
        self._load_mortgaged_properties()
        self._load_history()
        # Pre-populate the Add Debt rate field with the user-configured
        # default. Update the field if it's empty OR still matches the
        # last auto-populated value (i.e. user hasn't typed a custom rate).
        # This lets a Settings change reach the form on next visit.
        current = self.add_debt_rate.text().strip()
        if not current or current == self._last_auto_debt_rate_text:
            self._populate_default_debt_rate()
        initialize_universe(self.conn)

    def _load_history(self):
        txns = list_transactions(self.conn)
        asset_by_id = {a.id: a for a in list_assets(self.conn)}
        self.table.setRowCount(len(txns))
        for i, t in enumerate(reversed(txns)):
            if t.asset_id and t.asset_id in asset_by_id:
                asset_name = self._asset_history_label(asset_by_id[t.asset_id])
            else:
                asset_name = ""
            self.table.setItem(i, 0, QTableWidgetItem(str(t.id)))
            self.table.setItem(i, 1, QTableWidgetItem(t.date))
            self.table.setItem(i, 2, QTableWidgetItem(format_transaction_type(t.txn_type)))
            self.table.setItem(i, 3, QTableWidgetItem(asset_name))
            self.table.setItem(i, 4, QTableWidgetItem(str(t.quantity or "")))
            self.table.setItem(i, 5, QTableWidgetItem(fmt_money(t.price) if t.price else ""))
            self.table.setItem(i, 6, QTableWidgetItem(fmt_money(t.total_amount)))
            self.table.setItem(i, 7, QTableWidgetItem(fmt_money(t.fees) if t.fees else ""))
            self.table.setItem(i, 8, QTableWidgetItem(t.notes or ""))
        resize_table_to_contents(self.table, min_visible_rows=6)

    def _preview_trade(self):
        try:
            txn_type = self.txn_type.currentData()
            if txn_type not in PREVIEWABLE:
                return

            aid = self.asset_combo.currentData()
            if aid is None:
                QMessageBox.warning(self, "Error", "Select an asset.")
                return

            qty_text = self.qty_input.text().strip()
            amount_text = self.amount_input.text().strip()
            has_qty = bool(qty_text)
            has_amount = bool(amount_text)

            if has_qty and has_amount:
                QMessageBox.warning(self, "Input Error", "Enter either quantity or trade amount, not both.")
                return
            if not has_qty and not has_amount:
                QMessageBox.warning(self, "Input Error", "Enter either quantity or trade amount.")
                return

            if has_qty:
                qty = float(qty_text)
                target_amount = None
            else:
                qty = 0
                target_amount = float(amount_text)

            manual_price = None

            fee = float(self.fees_input.text().strip() or 0)
            # Reject negative additional fees at the preview boundary.
            # `compute_trading_costs` silently drops `additional_fee <= 0`
            # via its `if additional_fee > 0` filter, so without this
            # guard the user's negative input is accepted by the form
            # but never appears anywhere downstream — surface it instead.
            if fee < 0:
                raise ValueError("Additional fees cannot be negative.")
            note = self.notes_input.text().strip() or None

            draft = TradeDraft(
                action=txn_type,
                asset_id=aid,
                quantity=qty,
                manual_price=manual_price,
                fee=fee,
                note=note,
                target_amount=target_amount,
            )

            dt = self.date_input.text().strip()
            preview = prepare_trade_preview(self.conn, draft, dt)

            self._current_preview = preview
            self._show_preview(preview)

        except ValueError as e:
            QMessageBox.warning(self, "Input Error", str(e))
        except Exception as e:
            QMessageBox.warning(self, "Preview Error", str(e))

    def _show_preview(self, p: TradePreview):
        self.preview_group.setVisible(True)

        if p.can_confirm:
            self._set_trade_action_state("preview_ready")
            if p.quantity_source == "amount":
                status = (
                    f"Ready to confirm: {format_transaction_type(p.action)} {p.quantity} "
                    f"{p.symbol} @ {fmt_money(p.trade_price)} from {fmt_money(p.target_amount)} target"
                )
            else:
                status = (
                    f"Ready to confirm: {format_transaction_type(p.action)} {p.quantity} "
                    f"{p.symbol} @ {fmt_money(p.trade_price)}"
                )
            self.preview_status.setText(status)
            self.preview_status.setStyleSheet(
                "font-size: 14px; font-weight: bold; padding: 4px; color: #2e7d32;"
            )
        else:
            self._set_trade_action_state("preview_failed")
            errors = " | ".join(p.blocking_errors)
            self.preview_status.setText(f"Cannot confirm: {errors}")
            self.preview_status.setStyleSheet(
                "font-size: 14px; font-weight: bold; padding: 4px; color: #c62828;"
            )

        rows = [
            ("Action", format_transaction_type(p.action)),
            ("Symbol", p.symbol),
            ("Asset Type", format_asset_type(p.asset_type)),
        ]
        if p.quantity_source == "amount":
            rows.append(("Input Mode", "Trade Amount"))
            rows.append(("Requested Trade Amount", fmt_money(p.target_amount)))
            rows.append(("Derived Quantity", fmt_qty(p.quantity)))
            rows.append(("Unused Amount", fmt_money(p.uninvested_amount)))
        else:
            rows.append(("Input Mode", "Quantity"))
        rows.extend([
            ("Quantity", fmt_qty(p.quantity)),
            ("Trade Price", fmt_money(p.trade_price)),
            ("Price Source", format_price_source(p.price_source)),
        ])
        if p.execution_side:
            rows.append(("Execution Side", p.execution_side.title()))
        if p.bid_price is not None:
            rows.append(("Bid Price", fmt_money(p.bid_price)))
        if p.ask_price is not None:
            rows.append(("Ask Price", fmt_money(p.ask_price)))
        if p.quote_time:
            rows.append(("Quote Time", p.quote_time))
        elif p.price_date:
            rows.append(("Price Date", p.price_date))
        rows.append(("Estimated Trade Value", fmt_money(p.estimated_trade_value)))
        fee_type_labels = {
            "broker_commission": "Broker Commission",
            "broker_commission_rate": "Broker Commission Rate",
            "sec_section31": "SEC Section 31 Fee",
            "finra_taf": "FINRA TAF",
            "additional_fee": "Additional Fees",
        }
        if p.fee_breakdown:
            for item in p.fee_breakdown:
                label = fee_type_labels.get(item.fee_type, item.fee_type)
                rows.append((f"  {label}", fmt_money(item.amount)))
        rows.extend([
            ("Total Fees", fmt_money(p.fee)),
            ("Cash Before", fmt_money(p.cash_before)),
            ("Cash After", fmt_money(p.cash_after)),
            ("Total Assets Before", fmt_money(p.total_assets_before)),
            ("Total Assets After", fmt_money(p.total_assets_after)),
            ("Net Worth Before", fmt_money(p.net_worth_before)),
            ("Net Worth After", fmt_money(p.net_worth_after)),
        ])
        self.preview_details.setRowCount(len(rows))
        for i, (field, value) in enumerate(rows):
            self.preview_details.setItem(i, 0, QTableWidgetItem(field))
            self.preview_details.setItem(i, 1, QTableWidgetItem(value))

        all_types = set(p.allocation_before.keys()) | set(p.allocation_after.keys())
        alloc_rows = sorted(all_types)
        self.preview_alloc_table.setRowCount(len(alloc_rows))
        for i, atype in enumerate(alloc_rows):
            before_pct = p.allocation_before.get(atype, {}).get("pct", 0)
            after_pct = p.allocation_after.get(atype, {}).get("pct", 0)
            self.preview_alloc_table.setItem(i, 0, QTableWidgetItem(format_asset_type(atype)))
            self.preview_alloc_table.setItem(i, 1, QTableWidgetItem(fmt_pct(before_pct)))
            self.preview_alloc_table.setItem(i, 2, QTableWidgetItem(fmt_pct(after_pct)))

        if p.simulation_failed:
            risk_rows = [
                "WARN: Risk simulation failed; showing pre-trade warnings only. "
                "Check the application log for details."
            ]
        elif p.risk_changes_summary:
            risk_rows = p.risk_changes_summary
        else:
            risk_rows = ["No risk changes"]
        self.preview_risk_table.setRowCount(len(risk_rows))
        for i, entry in enumerate(risk_rows):
            if entry.startswith("NEW:"):
                type_text = "New"
                detail = entry[4:].strip()
                color = "#c62828"
            elif entry.startswith("RESOLVED:"):
                type_text = "Resolved"
                detail = entry[9:].strip()
                color = "#2e7d32"
            elif entry.startswith("WARN:"):
                type_text = "Warning"
                detail = entry[5:].strip()
                color = "#e65100"
            else:
                type_text = "Info"
                detail = entry
                color = "#999999"
            type_item = QTableWidgetItem(type_text)
            type_item.setForeground(QColor(color))
            self.preview_risk_table.setItem(i, 0, type_item)
            self.preview_risk_table.setItem(i, 1, QTableWidgetItem(detail))

        resize_table_to_contents(self.preview_details)
        resize_table_to_contents(self.preview_alloc_table)
        resize_table_to_contents(self.preview_risk_table)

    def _confirm_trade(self):
        # Bankruptcy gate first — match the entry-point pattern used by
        # every other submit handler on this page. If we returned early
        # on a missing/stale preview, a bankrupt user would get no
        # modal warning when clicking Confirm in a degenerate state.
        if guard_transaction_or_warn(self.conn, self):
            return
        if self._current_preview is None or not self._current_preview.can_confirm:
            return

        dt = self.date_input.text().strip()
        result = confirm_trade(self.conn, self._current_preview, dt)

        if result:
            QMessageBox.information(self, "Trade Confirmed",
                f"{format_transaction_type(self._current_preview.action)} {self._current_preview.quantity} "
                f"{self._current_preview.symbol} @ {fmt_money(self._current_preview.trade_price)} confirmed.")
            self._cancel_preview()
            self._clear_inputs()
            self.refresh()
            self.data_changed.emit()
        else:
            # Re-run the preview against current state so the message
            # reflects whatever blocked the trade (typically cash dropped
            # between preview and confirm because of an auto-deduction).
            try:
                draft = TradeDraft(
                    action=self._current_preview.action,
                    asset_id=self._current_preview.asset_id,
                    quantity=self._current_preview.quantity,
                    fee=self._current_preview.fee,
                    note=self._current_preview.note,
                )
                fresh = prepare_trade_preview(self.conn, draft, dt)
                detail = " | ".join(fresh.blocking_errors) or "Trade could not be confirmed."
            except Exception:
                detail = "Trade could not be confirmed."
            QMessageBox.warning(self, "Trade Rejected", detail)

    def _cancel_preview(self):
        self._current_preview = None
        self.preview_group.setVisible(False)
        self._set_trade_action_state("needs_preview")

    def _submit(self):
        # Bankruptcy gate: deposit_cash, withdraw_cash, and
        # pay_property_expense all hit the ledger from this single
        # handler. Spec §6 #25 — every user-initiated transaction is
        # banned during bankruptcy.
        if guard_transaction_or_warn(self.conn, self):
            return
        try:
            txn_type = self.txn_type.currentData()

            if txn_type in PREVIEWABLE:
                QMessageBox.warning(
                    self,
                    "Preview Required",
                    "Buy/Sell trades must be previewed and confirmed using live/latest price data.",
                )
                return

            dt = self.date_input.text().strip()
            notes = self.notes_input.text().strip() or None

            if txn_type == "deposit_cash":
                amt = float(self.amount_input.text())
                ledger.deposit_cash(self.conn, dt, amt, notes=notes)

            elif txn_type == "withdraw_cash":
                amt = float(self.amount_input.text())
                ledger.withdraw_cash(self.conn, dt, amt, notes=notes)

            elif txn_type == "pay_property_expense":
                aid = self.asset_combo.currentData()
                amt = float(self.amount_input.text())
                ledger.pay_property_expense(self.conn, dt, aid, amt, notes=notes)

            self._clear_inputs()
            self.refresh()
            self.data_changed.emit()

        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _clear_inputs(self):
        self.amount_input.clear()
        self.qty_input.clear()
        self.price_input.clear()
        self.fees_input.clear()
        self.notes_input.clear()

    def _clear_all_transient_inputs(self):
        """Wipe every numeric/text field in every mode's form.

        Called on transaction-type switch so that a "500" typed for a
        deposit isn't silently carried into a subsequent buy/sell or
        pay-debt screen, where 500 means something different.
        """
        # Main form (deposit/withdraw/buy/sell/pay_property_expense)
        self._clear_inputs()
        # Add Debt section
        if hasattr(self, "add_debt_name"):
            self._clear_add_debt_inputs()
        # Pay Debt section
        if hasattr(self, "pay_debt_amount"):
            self.pay_debt_amount.clear()
            self.pay_debt_notes.clear()
        # Pay Mortgage section
        if hasattr(self, "pay_mort_amount"):
            self.pay_mort_amount.clear()
            self.pay_mort_notes.clear()

    def _search_securities(self):
        query = self.search_input.text().strip()
        self._selected_search_result_index = None
        if not query:
            self.search_results_table.setRowCount(0)
            self.search_results_table.setFixedHeight(60)
            self.search_status.setText("")
            return

        asset_type = self.search_type_filter.currentData() or None
        self._search_results = search_universe(self.conn, query, asset_type=asset_type, limit=50)
        self.search_results_table.setRowCount(len(self._search_results))
        for i, rec in enumerate(self._search_results):
            self.search_results_table.setItem(i, 0, QTableWidgetItem(rec.symbol))
            self.search_results_table.setItem(i, 1, QTableWidgetItem(rec.name))
            self.search_results_table.setItem(i, 2, QTableWidgetItem(format_asset_type(rec.asset_type)))
            self.search_results_table.setItem(i, 3, QTableWidgetItem(rec.exchange or ""))
            cat = rec.etf_category or rec.sector or ""
            self.search_results_table.setItem(i, 4, QTableWidgetItem(cat))

        resize_table_to_contents(self.search_results_table, min_visible_rows=2)
        self.search_status.setText(f"{len(self._search_results)} result(s) found")
        self.add_asset_btn.setEnabled(False)

    def _on_search_selection_changed(self):
        selected = self.search_results_table.selectionModel().selectedRows()
        if selected:
            self._selected_search_result_index = selected[0].row()
        self.add_asset_btn.setEnabled(len(selected) > 0)

    def _add_selected_asset(self):
        selected = self.search_results_table.selectionModel().selectedRows()
        if selected:
            row_idx = selected[0].row()
        elif self._selected_search_result_index is not None:
            row_idx = self._selected_search_result_index
        else:
            return

        if row_idx >= len(self._search_results):
            return

        security = self._search_results[row_idx]
        asset = ensure_asset_from_security(self.conn, security)

        self._load_assets()

        for i in range(self.asset_combo.count()):
            if self.asset_combo.itemData(i) == asset.id:
                self.asset_combo.setCurrentIndex(i)
                break

        if row_idx < self.search_results_table.rowCount():
            self.search_results_table.selectRow(row_idx)

        QMessageBox.information(
            self, "Asset Added",
            f"{asset.symbol} - {asset.name} is now available in the asset list.",
        )

        self.data_changed.emit()
