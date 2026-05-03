"""Plan-then-execute force-sell engine.

When the user can't fund a scheduled debt or mortgage payment from cash
on hand, the simulator force-liquidates assets to cover the gap. The
spec (Debt Redesign §11) asks for an explicit *plan* — built and
validated as a single object — before any transaction is written. If
the plan can't cover the required payment, the caller declares
bankruptcy without leaving the portfolio in a partial-sale state.

Selling order is fixed by the spec: ``stock < etf < other < real_estate``
("other" = crypto + custom). Within each bucket the cheapest assets
sell first (per-unit price for tradeable instruments; per-property
value for real estate). Prices are refreshed before the plan is built
so a stale cached quote can't lead to under-coverage.

The execute step routes each item through ``ledger.sell`` (tradeable)
or ``ledger.sell_property`` (real estate). Transaction notes name the
debt that triggered the liquidation, so the user can audit *why* an
asset was sold.

Public surface:
- ``ForceSellPlanItem``, ``ForceSellPlan`` — dataclasses
- ``build_force_sell_plan(conn, *, target_cash, required_payment, debt_id, debt_name)``
- ``execute_force_sell_plan(conn, plan, today)``

The functions in ``ledger.py`` (``force_sell_to_raise_cash``,
``force_sell_to_cover_negative_cash``) are now thin wrappers around
this module.
"""
from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass, field

from src.engines.holdings import get_asset_quantity, _EPSILON
from src.engines.portfolio import calc_cash_balance
from src.models.asset_types import SELLABLE_ASSET_TYPES, SYNCABLE_ASSET_TYPES
from src.storage.asset_repo import list_assets
from src.storage.price_repo import get_latest_price
from src.storage.property_repo import get_property_by_asset

_log = logging.getLogger(__name__)

# Spec §11: stock < etf < other < real_estate. "Other" = crypto + custom.
_BUCKET_ORDER = ("stock", "etf", "other", "real_estate")
_BUCKET_FOR_TYPE = {
    "stock": "stock",
    "etf": "etf",
    "crypto": "other",
    "custom": "other",
    "real_estate": "real_estate",
}


@dataclass
class ForceSellPlanItem:
    """One asset that the plan intends to sell.

    `quantity_or_unit_sold` is in shares for stock/ETF/crypto/custom and
    always 1 for real_estate (whole-property sales only). Pricing is
    captured at plan-time so execution doesn't depend on a price that
    moved between plan and write.
    """
    asset_id: int
    asset_name: str
    asset_type: str
    sell_price_used: float
    quantity_or_unit_sold: float
    estimated_cash_generated: float


@dataclass
class ForceSellPlan:
    debt_id: int | None
    debt_name: str | None
    required_payment_amount: float
    cash_available_before_selling: float
    cash_shortage: float
    assets_to_sell: list[ForceSellPlanItem] = field(default_factory=list)
    estimated_cash_after_selling: float = 0.0
    can_cover_payment: bool = False
    bankruptcy_triggered: bool = False


def _try_sync_prices(conn: sqlite3.Connection, asset_ids: set[int]) -> None:
    """Best-effort price refresh for all tradeable assets in the set.

    Failures (no provider, no network, yfinance not installed) are
    swallowed — the plan falls back to the latest cached price for any
    asset that didn't sync. This keeps the auto-settle pipeline robust
    in offline/test environments while still benefiting from a fresh
    price when one is available.
    """
    try:
        from src.engines.pricing_engine import sync_asset_price
    except Exception:
        return
    for asset in list_assets(conn):
        if asset.id not in asset_ids:
            continue
        if asset.asset_type not in SYNCABLE_ASSET_TYPES:
            continue
        try:
            sync_asset_price(conn, asset)
        except Exception as exc:
            _log.debug(
                "Pre-sell price sync skipped for %s: %s", asset.symbol, exc,
            )


def _real_estate_value(conn: sqlite3.Connection, asset_id: int) -> tuple[float, float]:
    """Return ``(estimated_sale_price, estimated_cash_generated)`` for a
    property. Cash generated is sale_price minus the mortgage payoff
    (principal + one period's accrued interest) — matching what
    ``ledger.sell_property`` actually charges via
    ``pay_mortgage_in_full``. Using just the principal balance would
    overstate cash by exactly one period's interest, letting the plan
    declare ``can_cover_payment=True`` for obligations the actual sale
    cannot fund.
    """
    prop = get_property_by_asset(conn, asset_id)
    if prop is None or prop.status != "active":
        return 0.0, 0.0
    # Spec §4.7: use the most recent updated value if set; otherwise the
    # original purchase price.
    sale_price = float(
        prop.current_value if prop.current_value and prop.current_value > 0
        else (prop.purchase_price or 0.0)
    )
    if sale_price <= 0:
        return 0.0, 0.0
    from src.engines.ledger import _payoff_amount_for_mortgage
    from src.storage.mortgage_repo import get_mortgage_by_property
    m = get_mortgage_by_property(conn, prop.id)
    payoff = _payoff_amount_for_mortgage(m)
    cash = max(0.0, sale_price - payoff)
    return sale_price, cash


def build_force_sell_plan(
    conn: sqlite3.Connection,
    *,
    target_cash: float,
    required_payment: float,
    debt_id: int | None = None,
    debt_name: str | None = None,
) -> ForceSellPlan:
    """Plan the minimum sequence of sales to lift cash to ``target_cash``.

    The plan walks the spec's category order (stock → ETF → other →
    real_estate), and within each category sells the cheapest assets
    first. Tradeable assets are quantized: stock/ETF round up to whole
    shares; crypto/custom allow 8-decimal fractional. Real estate is
    sold whole — partial properties are not modeled.

    ``required_payment`` is the obligation we're trying to fund;
    ``target_cash`` is the cash threshold we need to cross to fund it
    (typically equal to ``current_cash + required_payment`` for
    obligation-driven force-sells, or 0 for negative-cash mop-ups).
    The plan reports ``can_cover_payment`` and ``bankruptcy_triggered``
    based on whether the projected cash after selling everything queued
    will reach ``target_cash``.

    Builds a plan only — does not write any transaction.
    """
    cash_available = calc_cash_balance(conn)
    cash_shortage = max(0.0, target_cash - cash_available)
    plan = ForceSellPlan(
        debt_id=debt_id, debt_name=debt_name,
        required_payment_amount=float(required_payment),
        cash_available_before_selling=cash_available,
        cash_shortage=cash_shortage,
    )

    if cash_shortage <= _EPSILON:
        plan.estimated_cash_after_selling = cash_available
        plan.can_cover_payment = cash_available >= required_payment - _EPSILON
        return plan

    assets = list_assets(conn)
    sellable_asset_ids = {
        a.id for a in assets
        if a.asset_type in SELLABLE_ASSET_TYPES and a.id is not None
    }
    _try_sync_prices(conn, sellable_asset_ids)

    # Bucket candidates: (per_unit_price, quantity, asset, total_value, cash_gen).
    # Cash_gen accounts for mortgage payoff on real estate; for sellables,
    # it's qty * price.
    buckets: dict[str, list] = {b: [] for b in _BUCKET_ORDER}
    for asset in assets:
        bucket = _BUCKET_FOR_TYPE.get(asset.asset_type)
        if bucket is None:
            continue
        if asset.asset_type == "real_estate":
            sale_price, cash_gen = _real_estate_value(conn, asset.id)
            if sale_price <= 0 or cash_gen <= 0:
                continue
            buckets[bucket].append({
                "asset": asset, "qty": 1.0,
                "price": sale_price, "cash_gen": cash_gen,
            })
        else:
            qty = get_asset_quantity(conn, asset.id)
            if qty <= _EPSILON:
                continue
            price = get_latest_price(conn, asset.id)
            if price is None or price <= 0:
                _log.debug(
                    "Force-sell plan: skipping %s (no market price)",
                    asset.symbol,
                )
                continue
            buckets[bucket].append({
                "asset": asset, "qty": float(qty),
                "price": float(price), "cash_gen": float(qty) * float(price),
            })

    # Sort each bucket cheapest-first by per-unit price.
    for b in _BUCKET_ORDER:
        buckets[b].sort(key=lambda c: c["price"])

    cumulative_cash = cash_available
    for bucket in _BUCKET_ORDER:
        for cand in buckets[bucket]:
            if cumulative_cash >= target_cash - _EPSILON:
                break
            asset = cand["asset"]
            price = cand["price"]
            still_needed = target_cash - cumulative_cash
            atype = asset.asset_type
            if atype == "real_estate":
                # Whole-property sale — take the full proceeds even if
                # they overshoot the shortage. Partial property sales
                # aren't modeled by ``sell_property``.
                qty_to_sell = 1.0
                cash_gen = cand["cash_gen"]
            else:
                # Use net cash needed to derive a unit count, then
                # quantize per asset-type rules.
                units_needed = still_needed / price if price > 0 else 0.0
                if atype in ("stock", "etf"):
                    units = min(cand["qty"], float(math.ceil(units_needed)))
                else:  # crypto, custom — 8-decimal fractional
                    units = min(
                        cand["qty"],
                        math.ceil(units_needed * 1e8) / 1e8,
                    )
                if units <= _EPSILON:
                    continue
                qty_to_sell = units
                cash_gen = units * price

            # Display label: prefer the symbol so the note matches the
            # legacy "Forced liquidation — ... STK @ 100.00" format.
            # Fall back to name when symbol is missing (e.g., real estate).
            display_label = (
                (asset.symbol or "").strip()
                or (asset.name or "").strip()
                or f"asset #{asset.id}"
            )
            plan.assets_to_sell.append(ForceSellPlanItem(
                asset_id=asset.id,
                asset_name=display_label,
                asset_type=atype,
                sell_price_used=price,
                quantity_or_unit_sold=qty_to_sell,
                estimated_cash_generated=cash_gen,
            ))
            cumulative_cash += cash_gen

        if cumulative_cash >= target_cash - _EPSILON:
            break

    plan.estimated_cash_after_selling = cumulative_cash
    plan.can_cover_payment = cumulative_cash >= target_cash - _EPSILON
    plan.bankruptcy_triggered = not plan.can_cover_payment
    return plan


def _build_note(plan: ForceSellPlan, item: ForceSellPlanItem) -> str:
    """Single canonical note format used by every forced-sale transaction.

    Names the obligation that triggered the liquidation when the caller
    supplied one. The "scheduled debt payment" phrase is preserved
    (existing tests + auto-settle dedupe logic match on it). The debt
    name follows ``for`` so a combined label like
    ``debt 'Auto Loan' + mortgage on 'House'`` reads cleanly without
    nested quotes (spec §4 #4).
    """
    if plan.debt_name:
        return (
            f"Forced liquidation — sold to cover scheduled debt payment "
            f"for {plan.debt_name}: {item.quantity_or_unit_sold} "
            f"{item.asset_name} @ {item.sell_price_used:,.2f}"
        )
    return (
        f"Forced liquidation — sold to cover scheduled debt/mortgage "
        f"payment: {item.quantity_or_unit_sold} {item.asset_name} "
        f"@ {item.sell_price_used:,.2f}"
    )


def execute_force_sell_plan(
    conn: sqlite3.Connection,
    plan: ForceSellPlan,
    today: str,
    *,
    strict: bool = False,
):
    """Realize a plan as ``sell`` / ``sell_property`` transactions.

    Always executes the plan's items, regardless of
    ``bankruptcy_triggered``. The flag is informational — the strict
    spec-compliant "no partial execution under bankruptcy" gate is
    enforced by ``ledger.force_sell_to_raise_cash`` (debt-driven path),
    not here.

    Per-item failure handling is selected by ``strict``:

    - ``strict=False`` (default, mop-up path): a ``ValueError`` from one
      item is logged and the loop continues. The mop-up entrypoint
      (``force_sell_to_cover_negative_cash``) prefers partial recovery
      over inaction.
    - ``strict=True`` (debt-driven path): a ``ValueError`` propagates so
      the caller learns the obligation could not be fully funded and
      can record bankruptcy. Earlier items in the plan that already
      committed remain — partial state is logged via the exception so
      the caller can audit. Without this, a silent swallow let the
      caller believe sales succeeded when they had not, breaking the
      "no partial sale under bankruptcy" semantic.

    Returns the list of ``Transaction`` objects written, in the order
    they were executed.
    """
    # Local imports to avoid a circular module import (force_sell ↔ ledger).
    from src.engines.ledger import sell, sell_property

    sales = []
    for item in plan.assets_to_sell:
        note = _build_note(plan, item)
        try:
            if item.asset_type == "real_estate":
                txn = sell_property(
                    conn, today, item.asset_id,
                    sale_price=item.sell_price_used, fees=0.0, notes=note,
                )
            else:
                txn = sell(
                    conn, today, item.asset_id,
                    item.quantity_or_unit_sold, item.sell_price_used,
                    notes=note,
                )
            sales.append(txn)
        except ValueError:
            _log.exception(
                "Force-sell execution failed for asset id=%s",
                item.asset_id,
            )
            if strict:
                raise
    return sales
