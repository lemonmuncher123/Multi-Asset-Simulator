_ASSET_TYPE_LABELS = {
    "stock": "Stock",
    "etf": "ETF",
    "crypto": "Crypto",
    "real_estate": "Real Estate",
    "cash": "Cash",
    "debt": "Debt",
    "custom": "Custom",
}

_TXN_TYPE_LABELS = {
    "deposit_cash": "Deposit Cash",
    "withdraw_cash": "Withdraw Cash",
    "buy": "Buy",
    "sell": "Sell",
    "add_property": "Add Property",
    "update_property_value": "Update Property Value",
    "receive_rent": "Receive Rent",
    "pay_property_expense": "Pay Property Expense",
    "pay_mortgage": "Pay Mortgage",
    "add_debt": "Add Debt",
    "pay_debt": "Pay Debt",
    "sell_property": "Sell Property",
    "manual_adjustment": "Manual Adjustment",
}

_PRICE_SOURCE_LABELS = {
    "synced": "Synced",
    "manual": "Manual",
    "last_available": "Last Available",
    "missing": "Missing",
    "quote_ask": "Market Ask",
    "quote_bid": "Market Bid",
}

_SEVERITY_LABELS = {
    "info": "Info",
    "low": "Low",
    "medium": "Medium",
    "high": "High",
    "critical": "Critical",
}

_SYNC_STATUS_LABELS = {
    "success": "Success",
    "failed": "Failed",
    "partial": "Partial",
    "running": "Running",
}


def _fallback_label(value: str) -> str:
    return value.replace("_", " ").title()


def format_asset_type(value: str) -> str:
    return _ASSET_TYPE_LABELS.get(value, _fallback_label(value))


def format_transaction_type(value: str) -> str:
    return _TXN_TYPE_LABELS.get(value, _fallback_label(value))


def format_price_source(value: str) -> str:
    return _PRICE_SOURCE_LABELS.get(value, _fallback_label(value))


def format_severity(value: str) -> str:
    return _SEVERITY_LABELS.get(value, _fallback_label(value))


def format_category(value: str) -> str:
    return _fallback_label(value)


def format_sync_status(value: str) -> str:
    return _SYNC_STATUS_LABELS.get(value, _fallback_label(value))


def get_transaction_type_options(keys: list[str] | None = None) -> list[tuple[str, str]]:
    if keys is None:
        keys = list(_TXN_TYPE_LABELS.keys())
    return [(format_transaction_type(k), k) for k in keys]


