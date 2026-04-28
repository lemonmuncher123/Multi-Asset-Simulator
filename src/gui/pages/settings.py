import json
import sqlite3
from datetime import date
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, QLineEdit, QPushButton,
    QLabel, QMessageBox, QScrollArea, QGroupBox, QCheckBox, QComboBox,
    QDialog, QDialogButtonBox, QGridLayout,
)
from PySide6.QtCore import Qt
from src.gui.widgets.common import make_header
from src.gui.pages.data_management import DataManagementPanel
from src.storage.settings_repo import get_setting, set_setting, parse_threshold, THRESHOLD_DEFAULTS
from src.engines.trading_costs import FINRA_TAF_PRESETS, get_finra_taf_rates, _parse_finra_custom_json


DEFAULTS = {
    "base_currency": "USD",
    "low_cash_threshold": "0.05",
    "concentration_threshold": "0.25",
    "crypto_threshold": "0.20",
    "debt_threshold": "0.50",
}

THRESHOLD_KEYS = set(THRESHOLD_DEFAULTS.keys())

TRADING_COST_KEYS = {
    "broker_commission_per_trade",
    "broker_commission_rate_bps",
    "sec_section31_rate_per_million",
}


class SettingsPage(QWidget):
    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn
        self.inputs: dict[str, QLineEdit] = {}

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.Shape.NoFrame)
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(16)

        layout.addWidget(make_header("Settings"))

        form = QFormLayout()
        form.setSpacing(10)

        labels = {
            "base_currency": "Base Currency (display only)",
            "low_cash_threshold": "Low Cash Threshold (%)",
            "concentration_threshold": "Concentration Threshold (%)",
            "crypto_threshold": "Crypto Exposure Threshold (%)",
            "debt_threshold": "Debt Ratio Threshold (%)",
        }

        for key, label in labels.items():
            inp = QLineEdit()
            inp.setMaximumWidth(200)
            self.inputs[key] = inp
            form.addRow(f"{label}:", inp)

        layout.addLayout(form)

        save_btn = QPushButton("Save Settings")
        save_btn.setStyleSheet("padding: 8px 24px; font-size: 14px;")
        save_btn.setMaximumWidth(200)
        save_btn.clicked.connect(self._save)
        layout.addWidget(save_btn)

        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #2e7d32;")
        layout.addWidget(self.status_label)

        # --- Trading Costs Section ---
        tc_group = QGroupBox("Trading Costs")
        tc_layout = QFormLayout(tc_group)
        tc_layout.setSpacing(10)

        self.tc_commission_per_trade = QLineEdit()
        self.tc_commission_per_trade.setMaximumWidth(200)
        self.tc_commission_per_trade.setPlaceholderText("0")
        tc_layout.addRow("Broker Commission Per Trade ($):", self.tc_commission_per_trade)

        self.tc_commission_rate_bps = QLineEdit()
        self.tc_commission_rate_bps.setMaximumWidth(200)
        self.tc_commission_rate_bps.setPlaceholderText("0")
        tc_layout.addRow("Broker Commission Rate (bps):", self.tc_commission_rate_bps)

        self.tc_auto_regulatory = QCheckBox("Auto Apply Regulatory Fees (sell-side stock/ETF)")
        tc_layout.addRow("", self.tc_auto_regulatory)

        self.tc_sec_rate = QLineEdit()
        self.tc_sec_rate.setMaximumWidth(200)
        self.tc_sec_rate.setPlaceholderText("0")
        tc_layout.addRow("SEC Section 31 Rate (per $1M sold):", self.tc_sec_rate)

        current_year = date.today().year
        preset = FINRA_TAF_PRESETS.get(current_year, FINRA_TAF_PRESETS[max(FINRA_TAF_PRESETS)])
        self.finra_info_label = QLabel(
            f"FINRA TAF {current_year} preset: per_share={preset['per_share']}, "
            f"max_per_trade={preset['max_per_trade']}"
        )
        self.finra_info_label.setStyleSheet("color: #666; font-size: 12px;")
        tc_layout.addRow("FINRA TAF:", self.finra_info_label)

        customize_finra_btn = QPushButton("Customize FINRA TAF...")
        customize_finra_btn.setMaximumWidth(200)
        customize_finra_btn.clicked.connect(self._open_finra_dialog)
        tc_layout.addRow("", customize_finra_btn)

        tc_save_btn = QPushButton("Save Trading Costs")
        tc_save_btn.setStyleSheet("padding: 8px 24px; font-size: 14px;")
        tc_save_btn.setMaximumWidth(200)
        tc_save_btn.clicked.connect(self._save_trading_costs)
        tc_layout.addRow("", tc_save_btn)

        self.tc_status_label = QLabel("")
        self.tc_status_label.setStyleSheet("color: #2e7d32;")
        tc_layout.addRow("", self.tc_status_label)

        layout.addWidget(tc_group)

        self.data_panel = DataManagementPanel(conn)
        layout.addWidget(self.data_panel)

        layout.addStretch()
        scroll.setWidget(content)
        outer.addWidget(scroll)

    def refresh(self):
        for key, default in DEFAULTS.items():
            value = get_setting(self.conn, key, default)
            if key in THRESHOLD_KEYS:
                ratio = parse_threshold(value, THRESHOLD_DEFAULTS[key])
                pct = ratio * 100
                value = f"{pct:g}"
            self.inputs[key].setText(value)
        self.status_label.setText("")

        self.tc_commission_per_trade.setText(
            get_setting(self.conn, "broker_commission_per_trade", "0") or "0"
        )
        self.tc_commission_rate_bps.setText(
            get_setting(self.conn, "broker_commission_rate_bps", "0") or "0"
        )
        self.tc_auto_regulatory.setChecked(
            get_setting(self.conn, "auto_apply_regulatory_fees", "0") == "1"
        )
        self.tc_sec_rate.setText(
            get_setting(self.conn, "sec_section31_rate_per_million", "0") or "0"
        )
        self._refresh_finra_label()
        self.tc_status_label.setText("")

        self.data_panel.refresh()

    def _refresh_finra_label(self):
        current_year = date.today().year
        custom_raw = get_setting(self.conn, "finra_taf_custom_json", None)
        custom = _parse_finra_custom_json(custom_raw)
        rates = get_finra_taf_rates(current_year, custom)
        source = "custom" if current_year in custom else "preset"
        self.finra_info_label.setText(
            f"FINRA TAF {current_year} ({source}): per_share={rates['per_share']}, "
            f"max_per_trade={rates['max_per_trade']}"
        )

    def _save(self):
        try:
            for key, inp in self.inputs.items():
                raw = inp.text().strip()
                if key in THRESHOLD_KEYS:
                    cleaned = raw.rstrip("%").strip()
                    pct = float(cleaned)
                    ratio = pct / 100.0
                    raw = f"{ratio:.4f}".rstrip("0").rstrip(".")
                set_setting(self.conn, key, raw)
            self.status_label.setText("Settings saved.")
        except ValueError:
            QMessageBox.warning(self, "Error", "Threshold values must be numbers.")
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _save_trading_costs(self):
        try:
            val = float(self.tc_commission_per_trade.text().strip() or "0")
            if val < 0:
                raise ValueError("Broker commission per trade must be non-negative.")
            set_setting(self.conn, "broker_commission_per_trade", str(val))

            val = float(self.tc_commission_rate_bps.text().strip() or "0")
            if val < 0:
                raise ValueError("Broker commission rate must be non-negative.")
            set_setting(self.conn, "broker_commission_rate_bps", str(val))

            set_setting(
                self.conn, "auto_apply_regulatory_fees",
                "1" if self.tc_auto_regulatory.isChecked() else "0",
            )

            val = float(self.tc_sec_rate.text().strip() or "0")
            if val < 0:
                raise ValueError("SEC Section 31 rate must be non-negative.")
            set_setting(self.conn, "sec_section31_rate_per_million", str(val))

            self.tc_status_label.setText("Trading costs saved.")
        except ValueError as e:
            QMessageBox.warning(self, "Error", str(e))
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))

    def _open_finra_dialog(self):
        dlg = FinraTafDialog(self.conn, self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._refresh_finra_label()


class FinraTafDialog(QDialog):
    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn
        self.setWindowTitle("Customize FINRA TAF")
        self.setMinimumWidth(500)

        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("Select a year to view and customize FINRA TAF rates."))

        year_row = QHBoxLayout()
        year_row.addWidget(QLabel("Year:"))
        self.year_combo = QComboBox()
        current_year = date.today().year
        for y in range(min(FINRA_TAF_PRESETS), max(FINRA_TAF_PRESETS) + 3):
            self.year_combo.addItem(str(y), y)
        idx = self.year_combo.findData(current_year)
        if idx >= 0:
            self.year_combo.setCurrentIndex(idx)
        self.year_combo.currentIndexChanged.connect(self._on_year_changed)
        year_row.addWidget(self.year_combo)
        year_row.addStretch()
        layout.addLayout(year_row)

        grid = QGridLayout()
        grid.addWidget(QLabel(""), 0, 0)
        grid.addWidget(QLabel("Per Share"), 0, 1)
        grid.addWidget(QLabel("Max Per Trade"), 0, 2)

        grid.addWidget(QLabel("Preset:"), 1, 0)
        self.preset_per_share = QLabel("")
        self.preset_max_per_trade = QLabel("")
        grid.addWidget(self.preset_per_share, 1, 1)
        grid.addWidget(self.preset_max_per_trade, 1, 2)

        grid.addWidget(QLabel("Custom:"), 2, 0)
        self.custom_per_share = QLineEdit()
        self.custom_per_share.setPlaceholderText("(use preset)")
        self.custom_per_share.setMaximumWidth(150)
        self.custom_max_per_trade = QLineEdit()
        self.custom_max_per_trade.setPlaceholderText("(use preset)")
        self.custom_max_per_trade.setMaximumWidth(150)
        grid.addWidget(self.custom_per_share, 2, 1)
        grid.addWidget(self.custom_max_per_trade, 2, 2)

        layout.addLayout(grid)

        btn_row = QHBoxLayout()
        apply_btn = QPushButton("Apply Custom")
        apply_btn.clicked.connect(self._apply_custom)
        btn_row.addWidget(apply_btn)

        reset_year_btn = QPushButton("Reset This Year To Preset")
        reset_year_btn.clicked.connect(self._reset_year)
        btn_row.addWidget(reset_year_btn)

        reset_all_btn = QPushButton("Reset All FINRA Custom Values")
        reset_all_btn.clicked.connect(self._reset_all)
        btn_row.addWidget(reset_all_btn)

        layout.addLayout(btn_row)

        self.info_label = QLabel("")
        self.info_label.setStyleSheet("color: #666; font-size: 12px;")
        layout.addWidget(self.info_label)

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        button_box.rejected.connect(self.accept)
        layout.addWidget(button_box)

        self._load_custom()
        self._on_year_changed()

    def _load_custom(self):
        raw = get_setting(self.conn, "finra_taf_custom_json", None)
        self._custom = _parse_finra_custom_json(raw)

    def _selected_year(self) -> int:
        return self.year_combo.currentData()

    def _on_year_changed(self):
        year = self._selected_year()
        preset = FINRA_TAF_PRESETS.get(year)
        if preset:
            self.preset_per_share.setText(str(preset["per_share"]))
            self.preset_max_per_trade.setText(str(preset["max_per_trade"]))
        else:
            fallback_year = max(FINRA_TAF_PRESETS) if year > max(FINRA_TAF_PRESETS) else min(FINRA_TAF_PRESETS)
            fb = FINRA_TAF_PRESETS[fallback_year]
            self.preset_per_share.setText(f"{fb['per_share']} (from {fallback_year})")
            self.preset_max_per_trade.setText(f"{fb['max_per_trade']} (from {fallback_year})")

        if year in self._custom:
            c = self._custom[year]
            self.custom_per_share.setText(str(c["per_share"]))
            self.custom_max_per_trade.setText(str(c["max_per_trade"]))
            self.info_label.setText(f"Custom override active for {year}.")
        else:
            self.custom_per_share.clear()
            self.custom_max_per_trade.clear()
            self.info_label.setText(f"Using preset for {year}.")

    def _apply_custom(self):
        year = self._selected_year()
        ps_text = self.custom_per_share.text().strip()
        mpt_text = self.custom_max_per_trade.text().strip()
        if not ps_text or not mpt_text:
            QMessageBox.warning(self, "Error", "Enter both per-share and max-per-trade values.")
            return
        try:
            ps = float(ps_text)
            mpt = float(mpt_text)
        except ValueError:
            QMessageBox.warning(self, "Error", "Values must be numbers.")
            return
        if ps < 0 or mpt < 0:
            QMessageBox.warning(self, "Error", "Values must be non-negative.")
            return

        self._custom[year] = {"per_share": ps, "max_per_trade": mpt}
        self._save_custom()
        self.info_label.setText(f"Custom override applied for {year}.")

    def _reset_year(self):
        year = self._selected_year()
        if year in self._custom:
            del self._custom[year]
            self._save_custom()
        self.custom_per_share.clear()
        self.custom_max_per_trade.clear()
        self.info_label.setText(f"Reset to preset for {year}.")

    def _reset_all(self):
        self._custom.clear()
        self._save_custom()
        self.custom_per_share.clear()
        self.custom_max_per_trade.clear()
        self.info_label.setText("All custom FINRA values cleared.")

    def _save_custom(self):
        if self._custom:
            serializable = {str(k): v for k, v in self._custom.items()}
            set_setting(self.conn, "finra_taf_custom_json", json.dumps(serializable))
        else:
            set_setting(self.conn, "finra_taf_custom_json", "{}")
