"""Tests for trading cost engine, fee calculation, trade preview integration,
confirm_trade fee persistence, full export/import, and no-tax-implementation guard."""

import json
import os
import tempfile
import pytest
from pathlib import Path

from src.storage.database import init_db
from src.storage.settings_repo import get_setting, set_setting
from src.storage.fee_breakdown_repo import list_fee_breakdowns
from src.storage.transaction_repo import list_transactions
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.engines.ledger import deposit_cash, buy
from src.engines.portfolio import calc_cash_balance
from src.engines.trading_costs import (
    compute_trading_costs,
    get_finra_taf_rates,
    FINRA_TAF_PRESETS,
    _parse_finra_custom_json,
    REGULATORY_FEE_ASSET_TYPES,
)
from src.engines.trade_preview import (
    TradeDraft, TradePreview, prepare_trade_preview, confirm_trade,
)
from src.engines.full_data_io import (
    EXPORT_TABLES, IMPORT_ORDER, export_full_data, import_full_data,
)
from src.data_sources.price_provider import PriceProvider, PriceRecord, QuoteRecord


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def db_conn():
    conn = init_db(":memory:")
    yield conn
    conn.close()


class FixedQuoteProvider(PriceProvider):
    """bid=99, ask=101, last=100 for every symbol."""

    def source_name(self):
        return "fixed_quote"

    def fetch_daily_prices(self, symbol, start_date, end_date):
        return [
            PriceRecord(symbol=symbol, date="2026-03-15", open=98, high=102,
                        low=97, close=100, adjusted_close=100, volume=500_000,
                        source=self.source_name()),
        ]

    def fetch_latest_quote(self, symbol):
        return QuoteRecord(symbol=symbol, bid=99.0, ask=101.0, last=100.0,
                           timestamp="2026-03-15T14:30:00", source=self.source_name())


def _providers():
    p = FixedQuoteProvider()
    return {"stock": p, "etf": p, "crypto": p}


def _setup(db_conn, cash=100_000.0, symbol="TEST", asset_type="stock",
           qty=20, price=100.0):
    deposit_cash(db_conn, "2026-01-01", cash)
    asset = create_asset(db_conn, Asset(symbol=symbol, name=f"Test {symbol}",
                                        asset_type=asset_type))
    if qty > 0:
        buy(db_conn, "2026-01-02", asset.id, qty, price)
    return asset


def _enable_regulatory(db_conn, sec_rate=0.0):
    set_setting(db_conn, "auto_apply_regulatory_fees", "1")
    if sec_rate:
        set_setting(db_conn, "sec_section31_rate_per_million", str(sec_rate))


# ===================================================================
# 1. Trading cost engine — FINRA preset lookup
# ===================================================================

class TestFinraPresetLookup:

    def test_2026_preset_values(self):
        rates = get_finra_taf_rates(2026)
        assert rates["per_share"] == 0.000195
        assert rates["max_per_trade"] == 9.79

    def test_years_after_2029_use_2029_preset(self):
        rates_2030 = get_finra_taf_rates(2030)
        rates_2029 = FINRA_TAF_PRESETS[2029]
        assert rates_2030 == rates_2029

    def test_years_before_2024_use_2024_preset(self):
        rates_2020 = get_finra_taf_rates(2020)
        rates_2024 = FINRA_TAF_PRESETS[2024]
        assert rates_2020 == rates_2024

    def test_each_preset_year_returns_own_values(self):
        for year, expected in FINRA_TAF_PRESETS.items():
            rates = get_finra_taf_rates(year)
            assert rates == expected, f"Mismatch for year {year}"


# ===================================================================
# 1b. FINRA custom JSON override
# ===================================================================

class TestFinraCustomOverride:

    def test_custom_overrides_preset_for_that_year(self):
        custom = {2026: {"per_share": 0.0005, "max_per_trade": 15.0}}
        rates = get_finra_taf_rates(2026, custom)
        assert rates["per_share"] == 0.0005
        assert rates["max_per_trade"] == 15.0

    def test_non_custom_year_still_uses_preset(self):
        custom = {2026: {"per_share": 0.0005, "max_per_trade": 15.0}}
        rates_2027 = get_finra_taf_rates(2027, custom)
        assert rates_2027 == FINRA_TAF_PRESETS[2027]

    def test_invalid_json_falls_back_to_empty(self):
        result = _parse_finra_custom_json("not json at all")
        assert result == {}

    def test_none_input_falls_back_to_empty(self):
        result = _parse_finra_custom_json(None)
        assert result == {}

    def test_empty_string_falls_back_to_empty(self):
        result = _parse_finra_custom_json("")
        assert result == {}

    def test_valid_json_parsed(self):
        raw = json.dumps({"2026": {"per_share": 0.0002, "max_per_trade": 10.0}})
        result = _parse_finra_custom_json(raw)
        assert 2026 in result
        assert result[2026]["per_share"] == 0.0002
        assert result[2026]["max_per_trade"] == 10.0

    def test_partial_invalid_entries_skipped(self):
        raw = json.dumps({
            "2026": {"per_share": 0.0002, "max_per_trade": 10.0},
            "bad": "not a dict",
            "2027": {"per_share": "abc"},
        })
        result = _parse_finra_custom_json(raw)
        assert 2026 in result
        assert len(result) == 1


# ===================================================================
# 1c. Reset / custom helper behavior
# ===================================================================

class TestFinraCustomResetBehavior:

    def test_remove_single_year(self):
        custom = {
            2026: {"per_share": 0.0005, "max_per_trade": 15.0},
            2027: {"per_share": 0.0006, "max_per_trade": 16.0},
        }
        del custom[2026]
        assert 2026 not in custom
        assert 2027 in custom
        rates = get_finra_taf_rates(2026, custom)
        assert rates == FINRA_TAF_PRESETS[2026]

    def test_clear_all_reverts_to_presets(self):
        custom = {
            2026: {"per_share": 0.0005, "max_per_trade": 15.0},
            2027: {"per_share": 0.0006, "max_per_trade": 16.0},
        }
        custom.clear()
        for year in (2026, 2027):
            rates = get_finra_taf_rates(year, custom)
            assert rates == FINRA_TAF_PRESETS[year]

    def test_settings_roundtrip_custom_json(self, db_conn):
        custom_data = {"2026": {"per_share": 0.0002, "max_per_trade": 10.0}}
        set_setting(db_conn, "finra_taf_custom_json", json.dumps(custom_data))
        raw = get_setting(db_conn, "finra_taf_custom_json")
        parsed = _parse_finra_custom_json(raw)
        assert 2026 in parsed
        assert parsed[2026]["per_share"] == 0.0002

    def test_reset_year_in_settings(self, db_conn):
        custom_data = {
            "2026": {"per_share": 0.0002, "max_per_trade": 10.0},
            "2027": {"per_share": 0.0003, "max_per_trade": 11.0},
        }
        set_setting(db_conn, "finra_taf_custom_json", json.dumps(custom_data))
        raw = get_setting(db_conn, "finra_taf_custom_json")
        parsed = json.loads(raw)
        del parsed["2026"]
        set_setting(db_conn, "finra_taf_custom_json", json.dumps(parsed))
        reloaded = _parse_finra_custom_json(get_setting(db_conn, "finra_taf_custom_json"))
        assert 2026 not in reloaded
        assert 2027 in reloaded

    def test_reset_all_in_settings(self, db_conn):
        custom_data = {"2026": {"per_share": 0.0002, "max_per_trade": 10.0}}
        set_setting(db_conn, "finra_taf_custom_json", json.dumps(custom_data))
        set_setting(db_conn, "finra_taf_custom_json", "{}")
        reloaded = _parse_finra_custom_json(get_setting(db_conn, "finra_taf_custom_json"))
        assert reloaded == {}


# ===================================================================
# 1d. SEC Section 31 default rate
# ===================================================================

class TestSecDefault:

    def test_sec_default_is_zero(self, db_conn):
        raw = get_setting(db_conn, "sec_section31_rate_per_million", "0")
        assert float(raw) == 0.0

    def test_sec_fee_zero_when_default(self, db_conn):
        _enable_regulatory(db_conn)
        result = compute_trading_costs(db_conn, "sell", "stock", 100, 10000.0, 2026)
        sec_items = [i for i in result.items if i.fee_type == "sec_section31"]
        assert len(sec_items) == 0


# ===================================================================
# 2. Fee calculation
# ===================================================================

class TestFeeCalculation:

    def test_buy_stock_no_sec_or_finra(self, db_conn):
        _enable_regulatory(db_conn, sec_rate=22.90)
        result = compute_trading_costs(db_conn, "buy", "stock", 100, 10000.0, 2026)
        fee_types = {i.fee_type for i in result.items}
        assert "sec_section31" not in fee_types
        assert "finra_taf" not in fee_types

    def test_sell_stock_applies_finra_taf(self, db_conn):
        _enable_regulatory(db_conn)
        result = compute_trading_costs(db_conn, "sell", "stock", 100, 10000.0, 2026)
        finra_items = [i for i in result.items if i.fee_type == "finra_taf"]
        assert len(finra_items) == 1
        assert finra_items[0].amount > 0

    def test_sell_etf_applies_finra_taf(self, db_conn):
        _enable_regulatory(db_conn)
        result = compute_trading_costs(db_conn, "sell", "etf", 200, 20000.0, 2026)
        finra_items = [i for i in result.items if i.fee_type == "finra_taf"]
        assert len(finra_items) == 1
        assert finra_items[0].amount > 0

    def test_crypto_buy_no_sec_or_finra(self, db_conn):
        _enable_regulatory(db_conn, sec_rate=22.90)
        result = compute_trading_costs(db_conn, "buy", "crypto", 1, 50000.0, 2026)
        fee_types = {i.fee_type for i in result.items}
        assert "sec_section31" not in fee_types
        assert "finra_taf" not in fee_types

    def test_crypto_sell_no_sec_or_finra(self, db_conn):
        _enable_regulatory(db_conn, sec_rate=22.90)
        result = compute_trading_costs(db_conn, "sell", "crypto", 1, 50000.0, 2026)
        fee_types = {i.fee_type for i in result.items}
        assert "sec_section31" not in fee_types
        assert "finra_taf" not in fee_types

    def test_sec_fee_zero_unless_configured(self, db_conn):
        _enable_regulatory(db_conn)
        result = compute_trading_costs(db_conn, "sell", "stock", 100, 10000.0, 2026)
        sec_items = [i for i in result.items if i.fee_type == "sec_section31"]
        assert len(sec_items) == 0

    def test_sec_fee_applied_when_configured(self, db_conn):
        _enable_regulatory(db_conn, sec_rate=22.90)
        result = compute_trading_costs(db_conn, "sell", "stock", 100, 10000.0, 2026)
        sec_items = [i for i in result.items if i.fee_type == "sec_section31"]
        assert len(sec_items) == 1
        expected = 10000.0 * 22.90 / 1_000_000.0
        assert sec_items[0].amount == pytest.approx(round(expected, 2))

    def test_manual_additional_fee_included(self, db_conn):
        result = compute_trading_costs(db_conn, "buy", "stock", 10, 1000.0, 2026,
                                       additional_fee=15.0)
        add_items = [i for i in result.items if i.fee_type == "additional_fee"]
        assert len(add_items) == 1
        assert add_items[0].amount == 15.0
        assert result.total >= 15.0

    def test_broker_fixed_commission(self, db_conn):
        set_setting(db_conn, "broker_commission_per_trade", "4.95")
        result = compute_trading_costs(db_conn, "buy", "stock", 10, 1000.0, 2026)
        comm_items = [i for i in result.items if i.fee_type == "broker_commission"]
        assert len(comm_items) == 1
        assert comm_items[0].amount == 4.95

    def test_broker_bps_commission(self, db_conn):
        set_setting(db_conn, "broker_commission_rate_bps", "10")
        result = compute_trading_costs(db_conn, "buy", "stock", 100, 10000.0, 2026)
        rate_items = [i for i in result.items if i.fee_type == "broker_commission_rate"]
        assert len(rate_items) == 1
        expected = 10000.0 * 10 / 10000.0
        assert rate_items[0].amount == pytest.approx(expected)

    def test_broker_both_commissions(self, db_conn):
        set_setting(db_conn, "broker_commission_per_trade", "4.95")
        set_setting(db_conn, "broker_commission_rate_bps", "5")
        result = compute_trading_costs(db_conn, "buy", "stock", 100, 10000.0, 2026)
        fee_types = [i.fee_type for i in result.items]
        assert "broker_commission" in fee_types
        assert "broker_commission_rate" in fee_types
        expected_total = 4.95 + (10000.0 * 5 / 10000.0)
        assert result.total == pytest.approx(expected_total)

    def test_finra_max_per_trade_cap_respected(self, db_conn):
        _enable_regulatory(db_conn)
        # 2026: per_share=0.000195, max=9.79
        # 100_000 shares * 0.000195 = 19.50 → should be capped to 9.79
        result = compute_trading_costs(db_conn, "sell", "stock", 100_000,
                                       10_000_000.0, 2026)
        finra_items = [i for i in result.items if i.fee_type == "finra_taf"]
        assert len(finra_items) == 1
        assert finra_items[0].amount == pytest.approx(9.79)

    def test_finra_below_cap_uses_per_share(self, db_conn):
        _enable_regulatory(db_conn)
        # 100 shares * 0.000195 = 0.0195 → rounds to 0.02
        result = compute_trading_costs(db_conn, "sell", "stock", 100, 10000.0, 2026)
        finra_items = [i for i in result.items if i.fee_type == "finra_taf"]
        assert len(finra_items) == 1
        expected = round(min(100 * 0.000195, 9.79), 2)
        assert finra_items[0].amount == pytest.approx(expected)

    def test_total_fee_sums_all_items(self, db_conn):
        set_setting(db_conn, "broker_commission_per_trade", "4.95")
        _enable_regulatory(db_conn, sec_rate=22.90)
        result = compute_trading_costs(db_conn, "sell", "stock", 1000, 100000.0,
                                       2026, additional_fee=5.0)
        manual_sum = round(sum(i.amount for i in result.items), 2)
        assert result.total == pytest.approx(manual_sum)

    def test_no_regulatory_fees_when_auto_off(self, db_conn):
        set_setting(db_conn, "auto_apply_regulatory_fees", "0")
        set_setting(db_conn, "sec_section31_rate_per_million", "22.90")
        result = compute_trading_costs(db_conn, "sell", "stock", 100, 10000.0, 2026)
        fee_types = {i.fee_type for i in result.items}
        assert "sec_section31" not in fee_types
        assert "finra_taf" not in fee_types


# ===================================================================
# 3. Trade preview integration
# ===================================================================

class TestTradePreviewFees:

    def test_preview_fee_is_total_fee(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "4.95")
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=10, fee=5.0)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        assert preview.fee == pytest.approx(4.95 + 5.0)

    def test_preview_additional_fee_is_manual_input(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=10, fee=7.50)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        assert preview.additional_fee == 7.50

    def test_preview_fee_breakdown_has_expected_rows(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "4.95")
        _enable_regulatory(db_conn, sec_rate=22.90)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=10, fee=3.0)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        fee_types = {item.fee_type for item in preview.fee_breakdown}
        assert "broker_commission" in fee_types
        assert "sec_section31" in fee_types
        assert "finra_taf" in fee_types
        assert "additional_fee" in fee_types

    def test_buy_cash_after_subtracts_total_fee(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "10.0")
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=10, fee=5.0)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        total_fee = preview.fee
        expected = preview.cash_before - (10 * 101.0 + total_fee)
        assert preview.cash_after == pytest.approx(expected)

    def test_sell_cash_after_adds_value_minus_total_fee(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "10.0")
        _enable_regulatory(db_conn)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5, fee=2.0)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        total_fee = preview.fee
        expected = preview.cash_before + (5 * 99.0 - total_fee)
        assert preview.cash_after == pytest.approx(expected)

    def test_insufficient_cash_uses_total_fee(self, db_conn):
        deposit_cash(db_conn, "2026-01-01", 1100.0)
        asset = create_asset(db_conn, Asset(symbol="EXP", name="Expensive",
                                            asset_type="stock"))
        set_setting(db_conn, "broker_commission_per_trade", "50.0")
        # ask=101, qty=10 → trade_value=1010, total cost = 1010+50 = 1060
        # cash=1100 → should succeed (1100-1060=40)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=10)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        assert preview.can_confirm is True

    def test_insufficient_cash_blocks_with_total_fee(self, db_conn):
        deposit_cash(db_conn, "2026-01-01", 1050.0)
        asset = create_asset(db_conn, Asset(symbol="EXP", name="Expensive",
                                            asset_type="stock"))
        set_setting(db_conn, "broker_commission_per_trade", "50.0")
        # ask=101, qty=10 → trade_value=1010, total cost = 1010+50 = 1060
        # cash=1050 → should block (1050-1060=-10)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=10)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        assert preview.can_confirm is False
        assert any("insufficient cash" in e.lower() for e in preview.blocking_errors)

    def test_trade_by_amount_fees_dont_change_derived_quantity(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "100.0")
        _enable_regulatory(db_conn, sec_rate=22.90)
        # ask=101, target=1000, floor(1000/101)=9
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=0,
                           target_amount=1000.0, fee=50.0)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        assert preview.quantity == 9
        assert preview.quantity_source == "amount"

    def test_trade_by_amount_sell_fees_dont_change_derived_quantity(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "100.0")
        _enable_regulatory(db_conn, sec_rate=22.90)
        # bid=99, target=500, floor(500/99)=5
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=0,
                           target_amount=500.0, fee=50.0)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        assert preview.quantity == 5
        assert preview.quantity_source == "amount"

    def test_preview_no_fees_when_nothing_configured(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=10)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        assert preview.fee == 0.0
        assert preview.additional_fee == 0.0
        assert preview.fee_breakdown == []


# ===================================================================
# 4. Confirm trade — fee persistence
# ===================================================================

class TestConfirmTradeFees:

    def test_confirm_saves_transaction_with_total_fee(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "4.95")
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5, fee=3.0)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        assert preview.can_confirm is True
        expected_total_fee = preview.fee

        confirm_trade(db_conn, preview, "2026-03-15")

        txns = list_transactions(db_conn)
        new_buy = [t for t in txns if t.txn_type == "buy" and t.quantity == 5
                   and t.date == "2026-03-15"]
        assert len(new_buy) == 1
        assert new_buy[0].fees == pytest.approx(expected_total_fee)

    def test_confirm_writes_fee_breakdown_rows(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "4.95")
        _enable_regulatory(db_conn, sec_rate=22.90)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5, fee=2.0)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        assert preview.can_confirm is True
        assert len(preview.fee_breakdown) > 0

        confirm_trade(db_conn, preview, "2026-03-15")

        txns = list_transactions(db_conn)
        sell_txns = [t for t in txns if t.txn_type == "sell"]
        assert len(sell_txns) == 1
        txn_id = sell_txns[0].id

        rows = list_fee_breakdowns(db_conn, txn_id)
        assert len(rows) == len(preview.fee_breakdown)
        saved_types = {r.fee_type for r in rows}
        expected_types = {item.fee_type for item in preview.fee_breakdown}
        assert saved_types == expected_types

    def test_confirm_fee_breakdown_linked_to_transaction(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "10.0")
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=3)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        confirm_trade(db_conn, preview, "2026-03-15")

        txns = list_transactions(db_conn)
        new_buy = [t for t in txns if t.quantity == 3 and t.date == "2026-03-15"]
        assert len(new_buy) == 1
        txn_id = new_buy[0].id

        rows = list_fee_breakdowns(db_conn, txn_id)
        assert len(rows) == 1
        assert rows[0].fee_type == "broker_commission"
        assert rows[0].amount == 10.0
        assert rows[0].transaction_id == txn_id

    def test_non_confirmable_saves_nothing(self, db_conn):
        deposit_cash(db_conn, "2026-01-01", 10.0)
        asset = create_asset(db_conn, Asset(symbol="X", name="X", asset_type="stock"))
        set_setting(db_conn, "broker_commission_per_trade", "4.95")
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=1000)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        assert preview.can_confirm is False

        count_before = len(list_transactions(db_conn))
        result = confirm_trade(db_conn, preview, "2026-03-15")
        assert result is False
        assert len(list_transactions(db_conn)) == count_before

        all_rows = db_conn.execute(
            "SELECT COUNT(*) FROM transaction_fee_breakdown"
        ).fetchone()[0]
        assert all_rows == 0

    def test_confirm_no_breakdown_rows_when_no_fees(self, db_conn):
        asset = _setup(db_conn)
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=2)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        assert preview.fee == 0.0
        assert preview.fee_breakdown == []

        confirm_trade(db_conn, preview, "2026-03-15")

        total_rows = db_conn.execute(
            "SELECT COUNT(*) FROM transaction_fee_breakdown"
        ).fetchone()[0]
        assert total_rows == 0


# ===================================================================
# 5. Full export/import — fee breakdown
# ===================================================================

class TestFullDataIoFeeBreakdown:

    def test_fee_breakdown_in_export_tables(self):
        assert "transaction_fee_breakdown" in EXPORT_TABLES

    def test_fee_breakdown_in_import_order(self):
        assert "transaction_fee_breakdown" in IMPORT_ORDER

    def test_fee_breakdown_import_after_transactions(self):
        txn_idx = IMPORT_ORDER.index("transactions")
        fb_idx = IMPORT_ORDER.index("transaction_fee_breakdown")
        assert fb_idx > txn_idx

    def test_export_creates_fee_breakdown_csv(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "4.95")
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        confirm_trade(db_conn, preview, "2026-03-15")

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "export"
            result = export_full_data(db_conn, out)
            assert result.success
            assert (out / "transaction_fee_breakdown.csv").exists()

    def test_import_restores_fee_breakdown_rows(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "4.95")
        _enable_regulatory(db_conn, sec_rate=22.90)
        draft = TradeDraft(action="sell", asset_id=asset.id, quantity=5, fee=2.0)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        confirm_trade(db_conn, preview, "2026-03-15")

        original_count = db_conn.execute(
            "SELECT COUNT(*) FROM transaction_fee_breakdown"
        ).fetchone()[0]
        assert original_count > 0

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "export"
            export_full_data(db_conn, out)

            conn2 = init_db(":memory:")
            result = import_full_data(conn2, out)
            assert result.success, result.message

            imported_count = conn2.execute(
                "SELECT COUNT(*) FROM transaction_fee_breakdown"
            ).fetchone()[0]
            assert imported_count == original_count
            conn2.close()

    def test_import_fk_check_passes(self, db_conn):
        asset = _setup(db_conn)
        set_setting(db_conn, "broker_commission_per_trade", "4.95")
        draft = TradeDraft(action="buy", asset_id=asset.id, quantity=5)
        preview = prepare_trade_preview(db_conn, draft, "2026-03-15", _providers())
        confirm_trade(db_conn, preview, "2026-03-15")

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "export"
            export_full_data(db_conn, out)

            conn2 = init_db(":memory:")
            result = import_full_data(conn2, out)
            assert result.success

            conn2.execute("PRAGMA foreign_keys=ON")
            fk_issues = conn2.execute("PRAGMA foreign_key_check").fetchall()
            assert len(fk_issues) == 0
            conn2.close()


# ===================================================================
# 6. Settings defaults
# ===================================================================

class TestSettingsDefaults:

    def test_sec_default_zero_in_fresh_db(self, db_conn):
        raw = get_setting(db_conn, "sec_section31_rate_per_million", "0")
        assert float(raw) == 0.0

    def test_broker_commission_default_zero(self, db_conn):
        raw = get_setting(db_conn, "broker_commission_per_trade", "0")
        assert float(raw) == 0.0

    def test_broker_rate_default_zero(self, db_conn):
        raw = get_setting(db_conn, "broker_commission_rate_bps", "0")
        assert float(raw) == 0.0

    def test_auto_regulatory_default_off(self, db_conn):
        raw = get_setting(db_conn, "auto_apply_regulatory_fees", "0")
        assert raw == "0"

    def test_saving_fee_settings_persists(self, db_conn):
        set_setting(db_conn, "broker_commission_per_trade", "9.99")
        set_setting(db_conn, "broker_commission_rate_bps", "3")
        set_setting(db_conn, "auto_apply_regulatory_fees", "1")
        set_setting(db_conn, "sec_section31_rate_per_million", "22.90")

        assert get_setting(db_conn, "broker_commission_per_trade") == "9.99"
        assert get_setting(db_conn, "broker_commission_rate_bps") == "3"
        assert get_setting(db_conn, "auto_apply_regulatory_fees") == "1"
        assert get_setting(db_conn, "sec_section31_rate_per_million") == "22.90"


# ===================================================================
# 7. No tax implementation guard
# ===================================================================

class TestNoTaxImplementation:

    def _scan_files_for_tax_code(self):
        """Grep src/ for new tax calculation code. Ignores comments and docs."""
        src_root = Path(__file__).parent.parent / "src"
        tax_keywords = [
            "tax_rate", "tax_amount", "realized_gain_tax",
            "capital_gains_tax", "tax_settings", "tax_table",
            "calc_tax", "compute_tax", "estimate_tax",
            "tax_report", "tax_liability",
        ]
        hits = []
        for py_file in src_root.rglob("*.py"):
            content = py_file.read_text()
            for kw in tax_keywords:
                if kw in content:
                    hits.append(f"{py_file.name}: {kw}")
        return hits

    def test_no_tax_calculation_code_in_src(self):
        hits = self._scan_files_for_tax_code()
        assert hits == [], f"Found tax-related code: {hits}"

    def test_no_tax_table_in_schema(self):
        schema_path = Path(__file__).parent.parent / "src" / "storage" / "schema.sql"
        content = schema_path.read_text().lower()
        assert "tax" not in content or "create table" not in content.split("tax")[0][-100:], \
            "Found tax-related table in schema"

    def test_no_tax_settings_in_db(self, db_conn):
        rows = db_conn.execute(
            "SELECT key FROM settings WHERE key LIKE '%tax%'"
        ).fetchall()
        assert len(rows) == 0, f"Found tax settings: {[r['key'] for r in rows]}"
