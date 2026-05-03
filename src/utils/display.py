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


def money_or_na(val) -> str:
    """Render a money amount as $X,XXX.XX, or 'N/A' if None."""
    if val is None:
        return "N/A"
    return f"${val:,.2f}"


def percent_or_na(val) -> str:
    """For values stored as already-percentages (e.g. approximate_return_pct).
    Returns '12.50%' for val=12.5, or 'N/A' if None."""
    if val is None:
        return "N/A"
    return f"{val:.2f}%"


def fraction_as_percent_or_na(val) -> str:
    """For values stored as fractions (0.10 = 10%) — used by allocation.
    Returns '10.00%' for val=0.10, or 'N/A' if None."""
    if val is None:
        return "N/A"
    return f"{val * 100:.2f}%"


def format_period_inclusive(period_start_iso: str, period_end_exclusive_iso: str) -> str:
    """Render a period as an inclusive range. Storage is exclusive-end
    ('2026-04-01' to '2026-05-01' for April); users read the second date
    as inclusive ('Apr 1 to May 1?'). This subtracts one day for display
    and keeps storage unchanged."""
    from datetime import date, timedelta
    if not period_start_iso or not period_end_exclusive_iso:
        return f"{period_start_iso} to {period_end_exclusive_iso}"
    end_inclusive = date.fromisoformat(period_end_exclusive_iso) - timedelta(days=1)
    return f"{period_start_iso} to {end_inclusive.isoformat()}"


