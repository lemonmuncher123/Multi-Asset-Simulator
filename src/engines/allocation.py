import sqlite3
from dataclasses import dataclass

from src.engines.portfolio import (
    calc_cash_balance,
    calc_positions,
    calc_total_property_value,
    calc_total_mortgage,
    calc_total_liabilities,
    calc_total_assets,
    get_portfolio_summary,
)
from src.storage.property_repo import list_active_properties
from src.storage.asset_repo import get_asset


def _safe_pct(part: float, whole: float) -> float:
    if whole == 0:
        return 0.0
    return part / whole


@dataclass
class _AllocCtx:
    """Cached aggregates for a single allocation pass.

    Used by `get_full_allocation` to avoid re-running `calc_total_assets`
    and `calc_positions` 5+ times per dashboard refresh. Callers that only
    need one allocation breakdown still go through the public functions
    which build a fresh context per call.
    """
    cash: float
    positions: list
    property_val: float
    total_assets: float
    mortgage: float
    liabilities: float
    props: list
    asset_lookup: dict  # asset_id -> Asset (used by liquidity/currency/region)


def _build_ctx(conn: sqlite3.Connection) -> _AllocCtx:
    summary = get_portfolio_summary(conn)
    props = list_active_properties(conn)
    needed_ids: set[int] = {p.asset_id for p in props}
    needed_ids.update(p.asset_id for p in summary["positions"])
    asset_lookup: dict = {aid: get_asset(conn, aid) for aid in needed_ids if aid is not None}
    return _AllocCtx(
        cash=summary["cash"],
        positions=summary["positions"],
        property_val=summary["property_value"],
        total_assets=summary["total_assets"],
        mortgage=summary["mortgage"],
        liabilities=summary["total_liabilities"],
        props=props,
        asset_lookup=asset_lookup,
    )


def _by_asset_type(ctx: _AllocCtx) -> dict[str, dict]:
    buckets: dict[str, float] = {}
    if ctx.cash != 0:
        buckets["cash"] = ctx.cash
    for p in ctx.positions:
        buckets[p.asset_type] = buckets.get(p.asset_type, 0) + p.effective_value()
    if ctx.property_val > 0:
        buckets["real_estate"] = ctx.property_val
    return {
        atype: {"value": value, "pct": _safe_pct(value, ctx.total_assets)}
        for atype, value in sorted(buckets.items())
    }


def _by_asset(ctx: _AllocCtx) -> list[dict]:
    items: list[dict] = []
    if ctx.cash != 0:
        items.append({
            "name": "Cash",
            "asset_type": "cash",
            "value": ctx.cash,
            "pct": _safe_pct(ctx.cash, ctx.total_assets),
        })
    for p in ctx.positions:
        val = p.effective_value()
        items.append({
            "name": f"{p.symbol} - {p.name}",
            "asset_type": p.asset_type,
            "value": val,
            "pct": _safe_pct(val, ctx.total_assets),
        })
    for prop in ctx.props:
        asset = ctx.asset_lookup.get(prop.asset_id)
        val = prop.current_value or 0
        display_name = (asset.name if asset else None) or (asset.symbol if asset else f"Property {prop.id}")
        items.append({
            "name": display_name,
            "asset_type": "real_estate",
            "value": val,
            "pct": _safe_pct(val, ctx.total_assets),
        })
    items.sort(key=lambda x: x["value"], reverse=True)
    return items


def _liquidity_split(ctx: _AllocCtx) -> tuple[float, float]:
    liquid = ctx.cash
    illiquid = 0.0
    for p in ctx.positions:
        val = p.effective_value()
        if p.asset_type in ("stock", "etf", "crypto"):
            liquid += val
        else:
            asset = ctx.asset_lookup.get(p.asset_id)
            if asset and asset.liquidity == "illiquid":
                illiquid += val
            else:
                liquid += val
    illiquid += ctx.property_val
    return liquid, illiquid


def _by_liquidity(ctx: _AllocCtx) -> dict[str, dict]:
    liquid, illiquid = _liquidity_split(ctx)
    return {
        "liquid": {"value": liquid, "pct": _safe_pct(liquid, ctx.total_assets)},
        "illiquid": {"value": illiquid, "pct": _safe_pct(illiquid, ctx.total_assets)},
    }


def _by_currency(ctx: _AllocCtx) -> dict[str, dict]:
    buckets: dict[str, float] = {"USD": ctx.cash}
    for p in ctx.positions:
        buckets[p.currency] = buckets.get(p.currency, 0) + p.effective_value()
    for prop in ctx.props:
        asset = ctx.asset_lookup.get(prop.asset_id)
        cur = asset.currency if asset else "USD"
        val = prop.current_value or 0
        buckets[cur] = buckets.get(cur, 0) + val
    return {
        cur: {"value": val, "pct": _safe_pct(val, ctx.total_assets)}
        for cur, val in sorted(buckets.items())
    }


def _by_region(ctx: _AllocCtx) -> dict[str, dict]:
    buckets: dict[str, float] = {"US": ctx.cash}
    for p in ctx.positions:
        asset = ctx.asset_lookup.get(p.asset_id)
        region = asset.region if asset else "US"
        buckets[region] = buckets.get(region, 0) + p.effective_value()
    for prop in ctx.props:
        asset = ctx.asset_lookup.get(prop.asset_id)
        region = asset.region if asset else "US"
        val = prop.current_value or 0
        buckets[region] = buckets.get(region, 0) + val
    return {
        region: {"value": val, "pct": _safe_pct(val, ctx.total_assets)}
        for region, val in sorted(buckets.items())
    }


# --- Public API (back-compat with prior single-call usage) ---

def calc_allocation_by_asset_type(conn: sqlite3.Connection) -> dict[str, dict]:
    return _by_asset_type(_build_ctx(conn))


def calc_allocation_by_asset(conn: sqlite3.Connection) -> list[dict]:
    return _by_asset(_build_ctx(conn))


def calc_allocation_by_liquidity(conn: sqlite3.Connection) -> dict[str, dict]:
    return _by_liquidity(_build_ctx(conn))


def calc_allocation_by_currency(conn: sqlite3.Connection) -> dict[str, dict]:
    return _by_currency(_build_ctx(conn))


def calc_allocation_by_region(conn: sqlite3.Connection) -> dict[str, dict]:
    return _by_region(_build_ctx(conn))


def calc_cash_pct(conn: sqlite3.Connection) -> float:
    return _safe_pct(calc_cash_balance(conn), calc_total_assets(conn))


def calc_crypto_pct(conn: sqlite3.Connection) -> float:
    total = calc_total_assets(conn)
    positions = calc_positions(conn)
    crypto_val = sum(p.effective_value() for p in positions if p.asset_type == "crypto")
    return _safe_pct(crypto_val, total)


def calc_real_estate_equity_pct(conn: sqlite3.Connection) -> float:
    total = calc_total_assets(conn)
    equity = calc_total_property_value(conn) - calc_total_mortgage(conn)
    return _safe_pct(equity, total)


def calc_debt_ratio(conn: sqlite3.Connection) -> float:
    total = calc_total_assets(conn)
    liabilities = calc_total_liabilities(conn)
    return _safe_pct(liabilities, total)


def calc_liquid_assets(conn: sqlite3.Connection) -> float:
    return _liquidity_split(_build_ctx(conn))[0]


def calc_illiquid_assets(conn: sqlite3.Connection) -> float:
    return _liquidity_split(_build_ctx(conn))[1]


def calc_asset_pie_breakdown(conn: sqlite3.Connection) -> list[dict]:
    items = _by_asset(_build_ctx(conn))
    positive = [i for i in items if i["value"] > 0 and i["asset_type"] != "debt"]
    total = sum(i["value"] for i in positive)
    for i in positive:
        i["pct"] = _safe_pct(i["value"], total)
    positive.sort(key=lambda x: x["value"], reverse=True)
    return positive


def get_full_allocation(conn: sqlite3.Connection) -> dict:
    """Compute every allocation breakdown in one pass.

    Builds the shared aggregate context once, so the underlying
    `calc_total_assets`/`calc_positions` queries only run a single time
    per call instead of 5+ times.
    """
    ctx = _build_ctx(conn)
    liquid, illiquid = _liquidity_split(ctx)
    crypto_val = sum(p.effective_value() for p in ctx.positions if p.asset_type == "crypto")

    return {
        "by_asset_type": _by_asset_type(ctx),
        "by_asset": _by_asset(ctx),
        "by_liquidity": {
            "liquid": {"value": liquid, "pct": _safe_pct(liquid, ctx.total_assets)},
            "illiquid": {"value": illiquid, "pct": _safe_pct(illiquid, ctx.total_assets)},
        },
        "by_currency": _by_currency(ctx),
        "by_region": _by_region(ctx),
        "cash_pct": _safe_pct(ctx.cash, ctx.total_assets),
        "crypto_pct": _safe_pct(crypto_val, ctx.total_assets),
        "real_estate_equity_pct": _safe_pct(ctx.property_val - ctx.mortgage, ctx.total_assets),
        "debt_ratio": _safe_pct(ctx.liabilities, ctx.total_assets),
        "liquid_assets": liquid,
        "illiquid_assets": illiquid,
    }
