import json
import sqlite3
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, QComboBox,
    QLineEdit, QTextEdit, QPushButton, QLabel, QMessageBox,
    QTableWidgetItem, QScrollArea, QSpinBox, QGroupBox,
)
from PySide6.QtGui import QColor
from PySide6.QtCore import Qt, Signal
from src.gui.widgets.common import make_header, make_table, fmt_money, fmt_pct, configure_expanding_table, resize_table_to_contents
from src.utils.display import format_transaction_type
from src.storage.transaction_repo import list_transactions
from src.storage.asset_repo import get_asset
from src.storage.journal_repo import (
    list_journal_entries,
    get_journal_entry,
    get_journal_by_transaction,
)
from src.engines.journal import (
    capture_portfolio_snapshot,
    create_journal_for_transaction,
    add_post_trade_review,
    get_before_after,
    calc_structure_changes,
    calc_training_score,
    get_lessons_learned,
    set_snapshot_before,
)


class JournalPage(QWidget):
    data_changed = Signal()

    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn
        self._pending_snapshot_before = None

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(12)

        layout.addWidget(make_header("Decision Journal"))

        # --- New entry form ---
        form_group = QGroupBox("New Journal Entry")
        form_layout = QFormLayout(form_group)
        form_layout.setSpacing(8)

        self.txn_combo = QComboBox()
        form_layout.addRow("Transaction:", self.txn_combo)

        snap_row = QHBoxLayout()
        self.snap_btn = QPushButton("Capture Before Snapshot")
        self.snap_btn.setStyleSheet("padding: 6px 16px;")
        self.snap_btn.clicked.connect(self._capture_before)
        snap_row.addWidget(self.snap_btn)
        self.snap_label = QLabel("")
        snap_row.addWidget(self.snap_label)
        snap_row.addStretch()
        form_layout.addRow("Pre-Trade:", snap_row)

        self.thesis_input = QTextEdit()
        self.thesis_input.setMaximumHeight(60)
        self.thesis_input.setPlaceholderText("Why are you making this trade?")
        form_layout.addRow("Thesis:", self.thesis_input)

        self.role_input = QLineEdit()
        self.role_input.setPlaceholderText("e.g. growth, income, hedge, speculation")
        form_layout.addRow("Intended Role:", self.role_input)

        self.risk_input = QTextEdit()
        self.risk_input.setMaximumHeight(60)
        self.risk_input.setPlaceholderText("What could go wrong?")
        form_layout.addRow("Risk Reasoning:", self.risk_input)

        self.exit_input = QLineEdit()
        self.exit_input.setPlaceholderText("When will you review or exit?")
        form_layout.addRow("Exit/Review Plan:", self.exit_input)

        self.confidence_spin = QSpinBox()
        self.confidence_spin.setRange(0, 5)
        self.confidence_spin.setValue(0)
        self.confidence_spin.setSpecialValueText("Not set")
        form_layout.addRow("Confidence (1-5):", self.confidence_spin)

        self.holding_input = QLineEdit()
        self.holding_input.setPlaceholderText("e.g. 6 months, 1 year, indefinite")
        form_layout.addRow("Holding Period:", self.holding_input)

        self.pre_notes_input = QTextEdit()
        self.pre_notes_input.setMaximumHeight(50)
        self.pre_notes_input.setPlaceholderText("Any additional pre-trade notes")
        form_layout.addRow("Pre-Trade Notes:", self.pre_notes_input)

        btn_row = QHBoxLayout()
        submit_btn = QPushButton("Save Journal Entry")
        submit_btn.setStyleSheet("padding: 8px 24px; font-size: 14px;")
        submit_btn.clicked.connect(self._submit)
        btn_row.addWidget(submit_btn)
        btn_row.addStretch()
        form_layout.addRow("", btn_row)

        layout.addWidget(form_group)

        # --- Post-trade review form ---
        review_group = QGroupBox("Post-Trade Review")
        review_layout = QFormLayout(review_group)
        review_layout.setSpacing(8)

        self.review_entry_combo = QComboBox()
        review_layout.addRow("Journal Entry:", self.review_entry_combo)

        self.review_input = QTextEdit()
        self.review_input.setMaximumHeight(60)
        self.review_input.setPlaceholderText("How did the trade play out?")
        review_layout.addRow("Review:", self.review_input)

        self.mistake_input = QLineEdit()
        self.mistake_input.setPlaceholderText("e.g. FOMO, no thesis, overleveraged")
        review_layout.addRow("Mistake Tags:", self.mistake_input)

        self.lesson_input = QTextEdit()
        self.lesson_input.setMaximumHeight(60)
        self.lesson_input.setPlaceholderText("What did you learn?")
        review_layout.addRow("Lesson Learned:", self.lesson_input)

        review_btn_row = QHBoxLayout()
        review_btn = QPushButton("Save Review")
        review_btn.setStyleSheet("padding: 8px 24px; font-size: 14px;")
        review_btn.clicked.connect(self._submit_review)
        review_btn_row.addWidget(review_btn)
        review_btn_row.addStretch()
        review_layout.addRow("", review_btn_row)

        layout.addWidget(review_group)

        # --- Journal entries table ---
        layout.addWidget(QLabel("Journal Entries"))
        self.table = make_table([
            "Date", "Title", "Confidence", "Thesis", "Score", "Review",
        ])
        configure_expanding_table(self.table)
        self.table.currentCellChanged.connect(self._on_entry_selected)
        layout.addWidget(self.table)

        # --- Before/After + Score detail ---
        detail_row = QHBoxLayout()

        ba_group = QGroupBox("Before / After Structure")
        ba_layout = QVBoxLayout(ba_group)
        self.ba_table = make_table(["Metric", "Before", "After", "Direction"])
        configure_expanding_table(self.ba_table)
        ba_layout.addWidget(self.ba_table)
        detail_row.addWidget(ba_group)

        score_group = QGroupBox("Training Score")
        score_layout = QVBoxLayout(score_group)
        self.score_table = make_table(["Component", "Score"])
        configure_expanding_table(self.score_table)
        score_layout.addWidget(self.score_table)
        detail_row.addWidget(score_group)

        layout.addLayout(detail_row)

        # --- Score details ---
        layout.addWidget(QLabel("Score Details"))
        self.detail_table = make_table(["Detail"])
        configure_expanding_table(self.detail_table)
        layout.addWidget(self.detail_table)

        # --- Lessons learned ---
        layout.addWidget(QLabel("Lessons Learned"))
        self.lessons_table = make_table(["Date", "Trade", "Lesson"])
        configure_expanding_table(self.lessons_table)
        layout.addWidget(self.lessons_table)

        layout.addStretch()
        scroll.setWidget(content)
        outer.addWidget(scroll)

    def refresh(self):
        # Drop any in-progress capture from a prior visit so it can't bleed
        # into a different journal entry.
        self._pending_snapshot_before = None
        self.snap_label.setText("")
        self.snap_label.setStyleSheet("")
        self._load_txn_combo()
        self._load_review_combo()
        self._load_table()
        self._load_lessons()
        self._clear_details()

    def _load_txn_combo(self):
        self.txn_combo.clear()
        self.txn_combo.addItem("(Select a transaction)", None)
        txns = list_transactions(self.conn)
        for t in reversed(txns):
            asset = get_asset(self.conn, t.asset_id) if t.asset_id else None
            symbol = asset.symbol if asset else ""
            existing = get_journal_by_transaction(self.conn, t.id)
            marker = " [journaled]" if existing else ""
            label = f"{t.date} | {format_transaction_type(t.txn_type)} {symbol} | {fmt_money(t.total_amount)}{marker}"
            self.txn_combo.addItem(label, t.id)

    def _load_review_combo(self):
        self.review_entry_combo.clear()
        self.review_entry_combo.addItem("(Select a journal entry)", None)
        entries = list_journal_entries(self.conn)
        for e in entries:
            reviewed = " [reviewed]" if e.post_trade_review else ""
            self.review_entry_combo.addItem(
                f"{e.date} | {e.title}{reviewed}", e.id
            )

    def _load_table(self):
        entries = list_journal_entries(self.conn)
        self.table.setRowCount(len(entries))
        for i, e in enumerate(entries):
            self.table.setItem(i, 0, QTableWidgetItem(e.date))
            self.table.setItem(i, 1, QTableWidgetItem(e.title))
            conf = str(e.confidence_level) if e.confidence_level else "-"
            self.table.setItem(i, 2, QTableWidgetItem(conf))
            thesis_preview = (e.thesis or "")[:60]
            self.table.setItem(i, 3, QTableWidgetItem(thesis_preview))

            score = calc_training_score(e)
            score_item = QTableWidgetItem(f"{score.overall_score:.0f}")
            if score.overall_score >= 80:
                score_item.setForeground(QColor("#2e7d32"))
            elif score.overall_score >= 60:
                score_item.setForeground(QColor("#f57f17"))
            else:
                score_item.setForeground(QColor("#c62828"))
            self.table.setItem(i, 4, score_item)

            review_status = "Yes" if e.post_trade_review else "-"
            self.table.setItem(i, 5, QTableWidgetItem(review_status))
        resize_table_to_contents(self.table)

    def _on_entry_selected(self, row, col, prev_row, prev_col):
        if row < 0:
            return
        entries = list_journal_entries(self.conn)
        if row >= len(entries):
            return
        entry = entries[row]
        self._show_details(entry)

    def _show_details(self, entry: object):
        changes = calc_structure_changes(entry)
        self.ba_table.setRowCount(len(changes))
        for i, c in enumerate(changes):
            self.ba_table.setItem(i, 0, QTableWidgetItem(c.metric))
            if isinstance(c.before, float):
                if c.metric.endswith("allocation") or c.metric.endswith("%") or "Ratio" in c.metric or "Concentration" in c.metric:
                    self.ba_table.setItem(i, 1, QTableWidgetItem(fmt_pct(c.before)))
                    self.ba_table.setItem(i, 2, QTableWidgetItem(fmt_pct(c.after)))
                else:
                    self.ba_table.setItem(i, 1, QTableWidgetItem(fmt_money(c.before)))
                    self.ba_table.setItem(i, 2, QTableWidgetItem(fmt_money(c.after)))
            else:
                self.ba_table.setItem(i, 1, QTableWidgetItem(str(c.before)))
                self.ba_table.setItem(i, 2, QTableWidgetItem(str(c.after)))
            dir_item = QTableWidgetItem(c.direction)
            if c.direction == "increased":
                dir_item.setForeground(QColor("#e65100"))
            else:
                dir_item.setForeground(QColor("#2e7d32"))
            self.ba_table.setItem(i, 3, dir_item)

        score = calc_training_score(entry)
        components = [
            ("Diversification", score.diversification_score),
            ("Liquidity", score.liquidity_score),
            ("Concentration", score.concentration_score),
            ("Leverage", score.leverage_score),
            ("Journal Quality", score.journal_quality_score),
            ("Overall", score.overall_score),
        ]
        self.score_table.setRowCount(len(components))
        for i, (name, val) in enumerate(components):
            self.score_table.setItem(i, 0, QTableWidgetItem(name))
            val_item = QTableWidgetItem(f"{val:.0f}")
            if val >= 80:
                val_item.setForeground(QColor("#2e7d32"))
            elif val >= 60:
                val_item.setForeground(QColor("#f57f17"))
            else:
                val_item.setForeground(QColor("#c62828"))
            self.score_table.setItem(i, 1, val_item)

        self.detail_table.setRowCount(len(score.details))
        for i, d in enumerate(score.details):
            self.detail_table.setItem(i, 0, QTableWidgetItem(d))
        resize_table_to_contents(self.ba_table)
        resize_table_to_contents(self.score_table)
        resize_table_to_contents(self.detail_table)

    def _clear_details(self):
        self.ba_table.setRowCount(0)
        self.score_table.setRowCount(0)
        self.detail_table.setRowCount(0)
        resize_table_to_contents(self.ba_table)
        resize_table_to_contents(self.score_table)
        resize_table_to_contents(self.detail_table)

    def _load_lessons(self):
        lessons = get_lessons_learned(self.conn)
        self.lessons_table.setRowCount(len(lessons))
        for i, l in enumerate(lessons):
            self.lessons_table.setItem(i, 0, QTableWidgetItem(l["date"]))
            self.lessons_table.setItem(i, 1, QTableWidgetItem(l["title"]))
            self.lessons_table.setItem(i, 2, QTableWidgetItem(l["lesson"]))
        resize_table_to_contents(self.lessons_table)

    def _capture_before(self):
        self._pending_snapshot_before = capture_portfolio_snapshot(self.conn)
        self.snap_label.setText("Snapshot captured")
        self.snap_label.setStyleSheet("color: #2e7d32; font-weight: bold;")

    def _submit(self):
        try:
            txn_id = self.txn_combo.currentData()
            if txn_id is None:
                raise ValueError("Select a transaction.")

            existing = get_journal_by_transaction(self.conn, txn_id)
            if existing:
                raise ValueError("This transaction already has a journal entry.")

            confidence = self.confidence_spin.value()
            if confidence == 0:
                confidence = None

            entry = create_journal_for_transaction(
                self.conn,
                transaction_id=txn_id,
                thesis=self.thesis_input.toPlainText().strip() or None,
                intended_role=self.role_input.text().strip() or None,
                risk_reasoning=self.risk_input.toPlainText().strip() or None,
                exit_plan=self.exit_input.text().strip() or None,
                confidence_level=confidence,
                expected_holding_period=self.holding_input.text().strip() or None,
                pre_trade_notes=self.pre_notes_input.toPlainText().strip() or None,
            )

            if self._pending_snapshot_before:
                set_snapshot_before(self.conn, entry, self._pending_snapshot_before)
                self._pending_snapshot_before = None

            self.thesis_input.clear()
            self.role_input.clear()
            self.risk_input.clear()
            self.exit_input.clear()
            self.confidence_spin.setValue(0)
            self.holding_input.clear()
            self.pre_notes_input.clear()
            self.snap_label.setText("")

            self.refresh()
            self.data_changed.emit()

        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _submit_review(self):
        try:
            entry_id = self.review_entry_combo.currentData()
            if entry_id is None:
                raise ValueError("Select a journal entry.")

            add_post_trade_review(
                self.conn,
                entry_id=entry_id,
                post_trade_review=self.review_input.toPlainText().strip() or None,
                mistake_tags=self.mistake_input.text().strip() or None,
                lesson_learned=self.lesson_input.toPlainText().strip() or None,
            )

            self.review_input.clear()
            self.mistake_input.clear()
            self.lesson_input.clear()

            self.refresh()
            self.data_changed.emit()

        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))
