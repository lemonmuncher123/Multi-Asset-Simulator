import json
import sqlite3
from dataclasses import dataclass

from src.storage.settings_repo import get_setting

FINRA_TAF_PRESETS: dict[int, dict[str, float]] = {
    2024: {"per_share": 0.000166, "max_per_trade": 8.30},
    2025: {"per_share": 0.000166, "max_per_trade": 8.30},
    2026: {"per_share": 0.000195, "max_per_trade": 9.79},
    2027: {"per_share": 0.000232, "max_per_trade": 11.61},
    2028: {"per_share": 0.000240, "max_per_trade": 12.05},
    2029: {"per_share": 0.000249, "max_per_trade": 12.50},
}

_FIRST_PRESET_YEAR = min(FINRA_TAF_PRESETS)
_LAST_PRESET_YEAR = max(FINRA_TAF_PRESETS)

REGULATORY_FEE_ASSET_TYPES = {"stock", "etf"}


@dataclass
class FeeItem:
    fee_type: str
    amount: float
    rate: float | None = None
    notes: str | None = None


@dataclass
class TradingCostResult:
    items: list[FeeItem]
    total: float


def _parse_finra_custom_json(raw: str | None) -> dict[int, dict[str, float]]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    result: dict[int, dict[str, float]] = {}
    for year_str, vals in parsed.items():
        try:
            year = int(year_str)
        except (ValueError, TypeError):
            continue
        if not isinstance(vals, dict):
            continue
        per_share = vals.get("per_share")
        max_per_trade = vals.get("max_per_trade")
        if per_share is not None and max_per_trade is not None:
            try:
                result[year] = {
                    "per_share": float(per_share),
                    "max_per_trade": float(max_per_trade),
                }
            except (ValueError, TypeError):
                continue
    return result


def get_finra_taf_rates(year: int, custom_overrides: dict[int, dict[str, float]] | None = None) -> dict[str, float]:
    if custom_overrides and year in custom_overrides:
        return custom_overrides[year]
    if year < _FIRST_PRESET_YEAR:
        return FINRA_TAF_PRESETS[_FIRST_PRESET_YEAR]
    if year > _LAST_PRESET_YEAR:
        return FINRA_TAF_PRESETS[_LAST_PRESET_YEAR]
    return FINRA_TAF_PRESETS[year]


def compute_trading_costs(
    conn: sqlite3.Connection,
    action: str,
    asset_type: str,
    quantity: float,
    trade_value: float,
    trade_year: int,
    additional_fee: float = 0.0,
) -> TradingCostResult:
    # Engine-layer defense in depth for the negative-input ban: a
    # negative `additional_fee` would silently fall through the
    # `if additional_fee > 0` filter below and leave no trace in the
    # FeeItem list. Reject it so callers (CSV importer, scripted
    # paths, future GUI surfaces) cannot smuggle one through.
    if additional_fee < 0:
        raise ValueError(
            f"Additional fee cannot be negative (got {additional_fee!r})."
        )
    items: list[FeeItem] = []

    commission_per_trade = float(get_setting(conn, "broker_commission_per_trade", "0") or "0")
    commission_rate_bps = float(get_setting(conn, "broker_commission_rate_bps", "0") or "0")
    auto_regulatory = get_setting(conn, "auto_apply_regulatory_fees", "0")
    sec_rate_per_million = float(get_setting(conn, "sec_section31_rate_per_million", "0") or "0")
    finra_custom_raw = get_setting(conn, "finra_taf_custom_json", None)

    if commission_per_trade > 0:
        items.append(FeeItem(
            fee_type="broker_commission",
            amount=commission_per_trade,
            rate=None,
            notes="Fixed per trade",
        ))

    if commission_rate_bps > 0:
        rate_decimal = commission_rate_bps / 10000.0
        amount = trade_value * rate_decimal
        items.append(FeeItem(
            fee_type="broker_commission_rate",
            amount=round(amount, 2),
            rate=commission_rate_bps,
            notes=f"{commission_rate_bps} bps",
        ))

    is_sell_side = action == "sell"
    is_regulatory_eligible = asset_type in REGULATORY_FEE_ASSET_TYPES
    apply_regulatory = auto_regulatory == "1" and is_sell_side and is_regulatory_eligible

    if apply_regulatory and sec_rate_per_million > 0:
        sec_amount = trade_value * sec_rate_per_million / 1_000_000.0
        items.append(FeeItem(
            fee_type="sec_section31",
            amount=round(sec_amount, 2),
            rate=sec_rate_per_million,
            notes=f"${sec_rate_per_million} per $1M sold",
        ))

    if apply_regulatory:
        custom_overrides = _parse_finra_custom_json(finra_custom_raw)
        taf_rates = get_finra_taf_rates(trade_year, custom_overrides)
        taf_amount = min(
            quantity * taf_rates["per_share"],
            taf_rates["max_per_trade"],
        )
        items.append(FeeItem(
            fee_type="finra_taf",
            amount=round(taf_amount, 2),
            rate=taf_rates["per_share"],
            notes=f"per_share={taf_rates['per_share']}, max={taf_rates['max_per_trade']}",
        ))

    if additional_fee > 0:
        items.append(FeeItem(
            fee_type="additional_fee",
            amount=additional_fee,
            notes="Manual additional fee",
        ))

    total = round(sum(item.amount for item in items), 2)
    return TradingCostResult(items=items, total=total)
