from src.utils.display import (
    format_asset_type, format_transaction_type,
    format_price_source, format_severity,
    format_category, format_sync_status,
    get_transaction_type_options,
)


def test_format_asset_type_stock():
    assert format_asset_type("stock") == "Stock"


def test_format_asset_type_etf():
    assert format_asset_type("etf") == "ETF"


def test_format_asset_type_crypto():
    assert format_asset_type("crypto") == "Crypto"


def test_format_asset_type_real_estate():
    assert format_asset_type("real_estate") == "Real Estate"


def test_format_asset_type_all_known():
    expected = {
        "stock": "Stock", "etf": "ETF", "crypto": "Crypto",
        "real_estate": "Real Estate",
        "cash": "Cash", "debt": "Debt", "custom": "Custom",
    }
    for val, label in expected.items():
        assert format_asset_type(val) == label


def test_format_asset_type_unknown():
    assert format_asset_type("some_new_type") == "Some New Type"


def test_format_transaction_type_all_known():
    expected = {
        "deposit_cash": "Deposit Cash", "withdraw_cash": "Withdraw Cash",
        "buy": "Buy", "sell": "Sell",
        "add_property": "Add Property",
        "update_property_value": "Update Property Value",
        "receive_rent": "Receive Rent",
        "pay_property_expense": "Pay Property Expense",
        "pay_mortgage": "Pay Mortgage",
        "add_debt": "Add Debt", "pay_debt": "Pay Debt",
        "sell_property": "Sell Property",
        "manual_adjustment": "Manual Adjustment",
    }
    for val, label in expected.items():
        assert format_transaction_type(val) == label


def test_format_transaction_type_sell_property():
    assert format_transaction_type("sell_property") == "Sell Property"


def test_format_transaction_type_unknown():
    assert format_transaction_type("future_type") == "Future Type"


def test_format_price_source_all_known():
    expected = {
        "synced": "Synced", "manual": "Manual",
        "last_available": "Last Available", "missing": "Missing",
    }
    for val, label in expected.items():
        assert format_price_source(val) == label


def test_format_price_source_unknown():
    assert format_price_source("cached_value") == "Cached Value"


def test_format_severity_all_known():
    expected = {
        "info": "Info", "low": "Low", "medium": "Medium",
        "high": "High", "critical": "Critical",
    }
    for val, label in expected.items():
        assert format_severity(val) == label


def test_format_severity_unknown():
    assert format_severity("extreme") == "Extreme"


def test_format_category():
    assert format_category("concentration") == "Concentration"
    assert format_category("liquidity_risk") == "Liquidity Risk"
    assert format_category("leverage") == "Leverage"


def test_format_sync_status_all_known():
    expected = {
        "success": "Success", "failed": "Failed",
        "partial": "Partial", "running": "Running",
    }
    for val, label in expected.items():
        assert format_sync_status(val) == label


def test_format_sync_status_unknown():
    assert format_sync_status("pending") == "Pending"


def test_get_transaction_type_options_all():
    options = get_transaction_type_options()
    assert len(options) == 13
    assert ("Buy", "buy") in options
    assert ("Deposit Cash", "deposit_cash") in options


def test_get_transaction_type_options_subset():
    options = get_transaction_type_options(["buy", "sell"])
    assert options == [("Buy", "buy"), ("Sell", "sell")]


# --- Money / percent / period helpers (used by reports rendering) ---


def test_money_or_na_none():
    from src.utils.display import money_or_na
    assert money_or_na(None) == "N/A"


def test_money_or_na_zero():
    from src.utils.display import money_or_na
    assert money_or_na(0) == "$0.00"


def test_money_or_na_thousands():
    from src.utils.display import money_or_na
    assert money_or_na(1234.5) == "$1,234.50"


def test_money_or_na_negative():
    from src.utils.display import money_or_na
    assert money_or_na(-1234.5) == "$-1,234.50"


def test_percent_or_na_already_percent():
    from src.utils.display import percent_or_na
    assert percent_or_na(None) == "N/A"
    assert percent_or_na(12.5) == "12.50%"
    assert percent_or_na(0) == "0.00%"


def test_fraction_as_percent_or_na():
    from src.utils.display import fraction_as_percent_or_na
    assert fraction_as_percent_or_na(None) == "N/A"
    assert fraction_as_percent_or_na(0.10) == "10.00%"
    assert fraction_as_percent_or_na(0) == "0.00%"
    assert fraction_as_percent_or_na(0.005) == "0.50%"


def test_format_period_inclusive_monthly():
    from src.utils.display import format_period_inclusive
    assert format_period_inclusive("2026-03-01", "2026-04-01") == \
        "2026-03-01 to 2026-03-31"


def test_format_period_inclusive_quarterly():
    from src.utils.display import format_period_inclusive
    assert format_period_inclusive("2026-04-01", "2026-07-01") == \
        "2026-04-01 to 2026-06-30"


def test_format_period_inclusive_annual():
    from src.utils.display import format_period_inclusive
    assert format_period_inclusive("2026-01-01", "2027-01-01") == \
        "2026-01-01 to 2026-12-31"


def test_format_period_inclusive_handles_empty():
    """Defensive: should not crash when called with empty strings."""
    from src.utils.display import format_period_inclusive
    assert format_period_inclusive("", "") == " to "


