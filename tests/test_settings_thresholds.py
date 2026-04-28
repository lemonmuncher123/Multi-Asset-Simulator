import pytest
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.settings_repo import (
    parse_threshold,
    get_threshold,
    set_setting,
    THRESHOLD_DEFAULTS,
)
from src.engines import ledger
from src.engines.risk import (
    check_concentration,
    check_crypto_exposure,
    check_low_cash,
    check_leverage,
)


# --- parse_threshold ---

class TestParseThreshold:
    def test_legacy_ratio_005(self):
        assert parse_threshold("0.05", 0.05) == 0.05

    def test_legacy_ratio_025(self):
        assert parse_threshold("0.25", 0.25) == 0.25

    def test_legacy_ratio_050(self):
        assert parse_threshold("0.50", 0.50) == 0.50

    def test_percentage_5(self):
        assert parse_threshold("5", 0.05) == 0.05

    def test_percentage_25(self):
        assert parse_threshold("25", 0.25) == 0.25

    def test_percentage_50(self):
        assert parse_threshold("50", 0.50) == 0.50

    def test_percentage_with_percent_sign(self):
        assert parse_threshold("5%", 0.05) == 0.05

    def test_percentage_with_spaces(self):
        assert parse_threshold("  25 % ", 0.25) == 0.25

    def test_invalid_returns_default(self):
        assert parse_threshold("abc", 0.10) == 0.10

    def test_empty_returns_default(self):
        assert parse_threshold("", 0.10) == 0.10

    def test_negative_returns_default(self):
        assert parse_threshold("-5", 0.10) == 0.10

    def test_zero_stays_zero(self):
        assert parse_threshold("0", 0.05) == 0.0


# --- get_threshold with db ---

def test_get_threshold_defaults_when_no_setting(db_conn):
    for key, expected in THRESHOLD_DEFAULTS.items():
        assert get_threshold(db_conn, key) == expected


def test_get_threshold_reads_legacy_ratio(db_conn):
    set_setting(db_conn, "low_cash_threshold", "0.10")
    assert get_threshold(db_conn, "low_cash_threshold") == 0.10


def test_get_threshold_reads_percentage(db_conn):
    set_setting(db_conn, "low_cash_threshold", "10")
    assert get_threshold(db_conn, "low_cash_threshold") == 0.10


def test_get_threshold_invalid_key(db_conn):
    with pytest.raises(ValueError):
        get_threshold(db_conn, "nonexistent_key")


# --- Risk checks respect custom settings ---

def test_low_cash_custom_threshold(db_conn):
    set_setting(db_conn, "low_cash_threshold", "10")
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    stock = create_asset(db_conn, Asset(symbol="VTI", name="Vanguard", asset_type="etf"))
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=920, price=100.0)

    warnings = check_low_cash(db_conn)
    assert len(warnings) == 1
    assert warnings[0].threshold == 0.10


def test_low_cash_no_warning_above_custom_threshold(db_conn):
    set_setting(db_conn, "low_cash_threshold", "3")
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    stock = create_asset(db_conn, Asset(symbol="VTI", name="Vanguard", asset_type="etf"))
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=960, price=100.0)

    warnings = check_low_cash(db_conn)
    assert len(warnings) == 0


def test_concentration_custom_threshold(db_conn):
    set_setting(db_conn, "concentration_threshold", "15")
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    stock = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=200, price=100.0)
    from src.storage.price_repo import upsert_price
    upsert_price(db_conn, stock.id, "2025-01-15", 100.0)

    warnings = check_concentration(db_conn)
    assert len(warnings) == 1
    assert warnings[0].severity == "medium"
    assert warnings[0].threshold == 0.15


def test_crypto_custom_threshold(db_conn):
    set_setting(db_conn, "crypto_threshold", "30")
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    btc = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    ledger.buy(db_conn, "2025-01-15", btc.id, quantity=0.5, price=50000.0)

    warnings = check_crypto_exposure(db_conn)
    assert len(warnings) == 0


def test_crypto_triggers_at_custom_threshold(db_conn):
    set_setting(db_conn, "crypto_threshold", "10")
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    btc = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    ledger.buy(db_conn, "2025-01-15", btc.id, quantity=0.3, price=50000.0)

    warnings = check_crypto_exposure(db_conn)
    assert len(warnings) == 1
    assert warnings[0].threshold == 0.10


def test_leverage_custom_threshold(db_conn):
    set_setting(db_conn, "debt_threshold", "40")
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, mortgage_balance=250000.0,
        down_payment=50000.0,
    )
    warnings = check_leverage(db_conn)
    assert len(warnings) == 1
    assert warnings[0].threshold == 0.40
