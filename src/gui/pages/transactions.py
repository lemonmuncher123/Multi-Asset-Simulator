import sqlite3
from datetime import date
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, QComboBox,
    QLineEdit, QPushButton, QLabel, QMessageBox, QTableWidgetItem, QScrollArea,
    QGroupBox, QHeaderView, QSizePolicy,
)
from PySide6.QtGui import QColor
from PySide6.QtCore import Qt, Signal
from src.gui.widgets.common import make_header, make_table, fmt_money, fmt_pct, configure_expanding_table, resize_table_to_contents
from src.models.asset import Asset
from src.storage.asset_repo import create_asset, list_assets
from src.storage.transaction_repo import list_transactions
from src.engines import ledger
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
    "receive_rent",
    "pay_property_expense",
]

ASSET_REQUIRED = {"buy", "sell", "receive_rent", "pay_property_expense", "pay_debt"}
QTY_PRICE_REQUIRED = {"buy", "sell"}
PREVIEWABLE = {"buy", "sell"}
AMOUNT_REQUIRED = {
    "deposit_cash", "withdraw_cash", "add_debt",
    "pay_debt", "receive_rent", "pay_property_expense",
}

_STYLE_PRIMARY = "padding: 8px 24px; font-size: 14px; background-color: #2e7d32; color: white;"
_STYLE_SECONDARY = "padding: 8px 24px; font-size: 14px;"


class TransactionsPage(QWidget):
    data_changed = Signal()

    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn
        self._current_preview = None
        self._search_results = []
        self._selected_search_result_index: int | None = None

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

        self.txn_type = QComboBox()
        for label, val in get_transaction_type_options(TXN_TYPES):
            self.txn_type.addItem(label, val)
        self.txn_type.currentIndexChanged.connect(self._on_type_changed)
        form.addRow("Type:", self.txn_type)

        self.date_input = QLineEdit(date.today().isoformat())
        form.addRow("Date:", self.date_input)

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

        self.symbol_label = QLabel("Symbol:")
        self.symbol_input = QLineEdit()
        self.symbol_input.setPlaceholderText("Symbol (for new asset)")
        form.addRow(self.symbol_label, self.symbol_input)

        self.name_label = QLabel("Name:")
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("Name (for new asset)")
        form.addRow(self.name_label, self.name_input)

        self.notes_input = QLineEdit()
        self.notes_input.setPlaceholderText("Notes (optional)")
        form.addRow("Notes:", self.notes_input)

        btn_row = QHBoxLayout()

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
        form.addRow("", btn_row)

        layout.addWidget(form_frame)

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
        needs_asset = txn_type in ASSET_REQUIRED
        needs_amount = txn_type in AMOUNT_REQUIRED
        needs_qty = txn_type in QTY_PRICE_REQUIRED
        needs_new = txn_type == "add_debt"
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
        self._set_field_visible(self.symbol_label, self.symbol_input, needs_new)
        self._set_field_visible(self.name_label, self.name_input, needs_new)
        self.preview_btn.setVisible(is_previewable)
        self.confirm_btn.setVisible(is_previewable)
        self.cancel_btn.setVisible(is_previewable)
        self.submit_btn.setVisible(not is_previewable)

        self._cancel_preview()

    def _load_assets(self):
        self.asset_combo.clear()
        assets = list_assets(self.conn)
        for a in assets:
            self.asset_combo.addItem(f"{a.symbol} - {a.name}", a.id)

    def refresh(self):
        # Discard any preview held over from a prior visit; prices may have
        # moved and the user shouldn't confirm against stale data.
        self._cancel_preview()
        self._load_assets()
        self._load_history()
        initialize_universe(self.conn)

    def _load_history(self):
        txns = list_transactions(self.conn)
        symbol_by_id = {a.id: a.symbol for a in list_assets(self.conn)}
        self.table.setRowCount(len(txns))
        for i, t in enumerate(reversed(txns)):
            asset_name = symbol_by_id.get(t.asset_id, "") if t.asset_id else ""
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
            rows.append(("Derived Quantity", str(int(p.quantity))))
            rows.append(("Unused Amount", fmt_money(p.uninvested_amount)))
        else:
            rows.append(("Input Mode", "Quantity"))
        rows.extend([
            ("Quantity", str(p.quantity)),
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
            ("Portfolio Value Before", fmt_money(p.portfolio_value_before)),
            ("Portfolio Value After", fmt_money(p.portfolio_value_after)),
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
            QMessageBox.warning(self, "Error", "Trade could not be confirmed.")

    def _cancel_preview(self):
        self._current_preview = None
        self.preview_group.setVisible(False)
        self._set_trade_action_state("needs_preview")

    def _submit(self):
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

            elif txn_type == "add_debt":
                sym = self.symbol_input.text().strip()
                name = self.name_input.text().strip()
                amt = float(self.amount_input.text())
                ledger.add_debt(self.conn, dt, symbol=sym, name=name, amount=amt, notes=notes)

            elif txn_type == "pay_debt":
                aid = self.asset_combo.currentData()
                amt = float(self.amount_input.text())
                ledger.pay_debt(self.conn, dt, aid, amt, notes=notes)

            elif txn_type == "receive_rent":
                aid = self.asset_combo.currentData()
                amt = float(self.amount_input.text())
                ledger.receive_rent(self.conn, dt, aid, amt, notes=notes)

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
        self.symbol_input.clear()
        self.name_input.clear()
        self.notes_input.clear()

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
