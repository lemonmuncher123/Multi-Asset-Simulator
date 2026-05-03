import sqlite3
from dataclasses import dataclass
from src.models.risk_warning import RiskWarning
from src.engines.portfolio import (
    calc_cash_balance,
    calc_positions,
    calc_net_worth,
    calc_total_assets,
    calc_total_liabilities,
    get_portfolio_summary,
)
from src.engines.allocation import (
    calc_allocation_by_asset,
    calc_crypto_pct,
    calc_cash_pct,
    calc_debt_ratio,
    calc_illiquid_assets,
    get_full_allocation,
)
from src.engines.holdings import get_asset_quantity
from src.engines.debt_math import compute_debt_schedule, normalize_period_to_months
from src.storage.debt_repo import list_debts
from src.storage.property_repo import list_active_properties
from src.storage.price_repo import get_latest_price
from src.storage.asset_repo import list_assets
from src.storage.bankruptcy_event_repo import list_active_bankruptcy_events
from src.storage.settings_repo import get_threshold, get_max_debt_payoff_months


@dataclass
class _WarningCtx:
    """Precomputed aggregates shared across check_* helpers.

    ``get_all_warnings`` builds this once per call and passes it
    through. Each check_* still works without a context (default
    ``None`` triggers a per-call rebuild) so direct callers and
    existing tests are unaffected.

    Stored as plain dicts so adding a new aggregate later does not
    bump a typed contract.
    """
    summary: dict      # get_portfolio_summary(conn)
    allocation: dict   # get_full_allocation(conn)

    @classmethod
    def from_conn(cls, conn: sqlite3.Connection) -> "_WarningCtx":
        return cls(
            summary=get_portfolio_summary(conn),
            allocation=get_full_allocation(conn),
        )


def check_concentration(
    conn: sqlite3.Connection, *, _ctx: "_WarningCtx | None" = None,
) -> list[RiskWarning]:
    warnings = []
    if _ctx is None:
        net_worth = calc_net_worth(conn)
        items = calc_allocation_by_asset(conn)
    else:
        net_worth = _ctx.summary["net_worth"]
        items = _ctx.allocation["by_asset"]
    if net_worth <= 0:
        return warnings

    medium_threshold = get_threshold(conn, "concentration_threshold")
    high_threshold = 0.40

    for item in items:
        if item["asset_type"] == "cash":
            continue
        pct = item["value"] / net_worth
        if pct > high_threshold:
            warnings.append(RiskWarning(
                severity="high",
                category="concentration",
                message=f"This portfolio is concentrated in {item['name']}. "
                        f"It represents {pct:.0%} of net worth.",
                metric_value=pct,
                threshold=high_threshold,
            ))
        elif pct > medium_threshold:
            warnings.append(RiskWarning(
                severity="medium",
                category="concentration",
                message=f"This portfolio has significant exposure to {item['name']}. "
                        f"It represents {pct:.0%} of net worth.",
                metric_value=pct,
                threshold=medium_threshold,
            ))
    return warnings


def check_crypto_exposure(
    conn: sqlite3.Connection, *, _ctx: "_WarningCtx | None" = None,
) -> list[RiskWarning]:
    threshold = get_threshold(conn, "crypto_threshold")
    if _ctx is None:
        pct = calc_crypto_pct(conn)
    else:
        pct = _ctx.allocation["crypto_pct"]
    if pct > threshold:
        return [RiskWarning(
            severity="high",
            category="volatility",
            message=f"Crypto exposure is above the selected threshold ({threshold:.0%}). "
                    f"Crypto represents {pct:.0%} of total assets.",
            metric_value=pct,
            threshold=threshold,
        )]
    return []


def check_low_cash(
    conn: sqlite3.Connection, *, _ctx: "_WarningCtx | None" = None,
) -> list[RiskWarning]:
    warnings = []
    if _ctx is None:
        cash = calc_cash_balance(conn)
    else:
        cash = _ctx.summary["cash"]

    if cash < 0:
        warnings.append(RiskWarning(
            severity="critical",
            category="cash",
            message=f"Cash balance is negative ({cash:,.2f}). "
                    f"Transactions may exceed available funds.",
            metric_value=cash,
            threshold=0.0,
        ))
        return warnings

    threshold = get_threshold(conn, "low_cash_threshold")
    if _ctx is None:
        pct = calc_cash_pct(conn)
        total_assets = calc_total_assets(conn)
    else:
        pct = _ctx.allocation["cash_pct"]
        total_assets = _ctx.summary["total_assets"]
    if 0 < total_assets and pct < threshold:
        warnings.append(RiskWarning(
            severity="medium",
            category="liquidity",
            message=f"Cash balance is below the selected threshold ({threshold:.0%}). "
                    f"Cash is {pct:.1%} of total assets.",
            metric_value=pct,
            threshold=threshold,
        ))
    return warnings


def check_leverage(
    conn: sqlite3.Connection, *, _ctx: "_WarningCtx | None" = None,
) -> list[RiskWarning]:
    threshold = get_threshold(conn, "debt_threshold")
    if _ctx is None:
        ratio = calc_debt_ratio(conn)
    else:
        ratio = _ctx.allocation["debt_ratio"]
    if ratio > threshold:
        return [RiskWarning(
            severity="high",
            category="leverage",
            message=f"Debt ratio is above the selected threshold ({threshold:.0%}). "
                    f"Total liabilities are {ratio:.0%} of total assets.",
            metric_value=ratio,
            threshold=threshold,
        )]
    return []


def check_illiquidity(
    conn: sqlite3.Connection, *, _ctx: "_WarningCtx | None" = None,
) -> list[RiskWarning]:
    if _ctx is None:
        net_worth = calc_net_worth(conn)
        illiquid = calc_illiquid_assets(conn)
    else:
        net_worth = _ctx.summary["net_worth"]
        illiquid = _ctx.allocation["illiquid_assets"]
    if net_worth <= 0:
        return []
    pct = illiquid / net_worth
    if pct > 0.60:
        return [RiskWarning(
            severity="medium",
            category="liquidity",
            message=f"Illiquid assets are a large share of net worth. "
                    f"Illiquid assets represent {pct:.0%} of net worth.",
            metric_value=pct,
            threshold=0.60,
        )]
    return []


def check_real_estate_ltv(conn: sqlite3.Connection) -> list[RiskWarning]:
    from src.storage.mortgage_repo import get_mortgage_by_property
    warnings = []
    props = list_active_properties(conn)
    for prop in props:
        value = prop.current_value or 0
        if value <= 0:
            continue
        mortgage = get_mortgage_by_property(conn, prop.id)
        balance = mortgage.current_balance if mortgage is not None else 0.0
        if balance <= 0:
            continue
        ltv = balance / value
        if ltv > 0.80:
            warnings.append(RiskWarning(
                severity="high",
                category="leverage",
                message=f"Real estate loan-to-value ratio is {ltv:.0%}. "
                        f"Mortgage balance is high relative to property value.",
                metric_value=ltv,
                threshold=0.80,
                related_asset_id=prop.asset_id,
            ))
    return warnings


def check_missing_prices(conn: sqlite3.Connection) -> list[RiskWarning]:
    # Single SQL replaces the N+1 loop (one get_latest_price + one
    # get_asset_quantity per asset). Same contract as before:
    #   - only stock/etf/crypto assets are eligible
    #   - the asset must have NO row in market_prices
    #   - net quantity (buys - sells + manual_adjustments-with-qty)
    #     must be strictly > 0
    # Symbol-asc ordering matches the prior list_assets() default.
    rows = conn.execute(
        """
        SELECT a.id AS asset_id, a.symbol AS symbol
        FROM assets a
        WHERE a.asset_type IN ('stock', 'etf', 'crypto')
          AND NOT EXISTS (
              SELECT 1 FROM market_prices WHERE asset_id = a.id
          )
          AND COALESCE((
              SELECT SUM(CASE
                  WHEN txn_type = 'buy' THEN quantity
                  WHEN txn_type = 'sell' THEN -quantity
                  WHEN txn_type = 'manual_adjustment'
                       AND quantity IS NOT NULL THEN quantity
                  ELSE 0
              END)
              FROM transactions WHERE asset_id = a.id
          ), 0) > 0
        ORDER BY a.symbol
        """
    ).fetchall()
    return [
        RiskWarning(
            severity="info",
            category="data_quality",
            message=f"No market price data for {row['symbol']}. "
                    f"Position value is based on cost basis only.",
            related_asset_id=row["asset_id"],
        )
        for row in rows
    ]


def check_debt_payoff_horizon(conn: sqlite3.Connection) -> list[RiskWarning]:
    """Flag any debt whose projected payoff exceeds the user's max.

    Setting key: ``max_debt_payoff_months`` (default 60). Each debt's
    ``schedule_frequency`` and stored ``monthly_payment_amount`` are run
    through the amortization engine; if the resulting horizon (expressed
    in months) is greater than the cap, an observation is emitted —
    phrased as an observation, not advice, per the project's risk-warning
    style.
    """
    cap_months = get_max_debt_payoff_months(conn)
    warnings: list[RiskWarning] = []
    for debt in list_debts(conn):
        if debt.current_balance <= 0:
            continue
        if debt.monthly_payment_amount <= 0:
            continue
        sched = compute_debt_schedule(
            principal=debt.current_balance,
            annual_rate=debt.interest_rate,
            schedule=debt.schedule_frequency,
            payment=debt.monthly_payment_amount,
        )
        if not sched.feasible:
            warnings.append(RiskWarning(
                severity="high",
                category="debt_horizon",
                message=(
                    f"Scheduled payment for '{debt.name or 'unnamed debt'}' "
                    f"does not cover the periodic interest. The balance "
                    f"will not pay down at the current rate."
                ),
                related_asset_id=debt.asset_id,
            ))
            continue
        months = normalize_period_to_months(sched.num_periods, debt.schedule_frequency)
        if months > cap_months:
            warnings.append(RiskWarning(
                severity="high",
                category="debt_horizon",
                message=(
                    f"'{debt.name or 'unnamed debt'}' is projected to take "
                    f"{months} months to pay off, above the {cap_months}-month "
                    f"limit set in Settings."
                ),
                metric_value=float(months),
                threshold=float(cap_months),
                related_asset_id=debt.asset_id,
            ))
    return warnings


def check_debt_affordability(conn: sqlite3.Connection) -> list[RiskWarning]:
    """Flag debts whose monthly obligation outstrips current cash on hand.

    Heuristic: total scheduled monthly debt+mortgage obligation divided
    into available cash gives the number of months the cash buffer can
    sustain. If that's less than 6, surface an observation. (6 months is
    the standard "emergency fund" rule of thumb; the simulator merely
    *names* the gap, not a recommendation.)
    """
    cash = calc_cash_balance(conn)
    monthly_obligation = 0.0
    for debt in list_debts(conn):
        if debt.current_balance <= 0:
            continue
        if debt.schedule_frequency == "yearly":
            monthly_obligation += debt.monthly_payment_amount / 12.0
        else:
            monthly_obligation += debt.monthly_payment_amount
    from src.storage.mortgage_repo import list_active_mortgages
    for mortgage in list_active_mortgages(conn):
        if mortgage.monthly_payment_amount > 0:
            monthly_obligation += mortgage.monthly_payment_amount

    if monthly_obligation <= 0:
        return []
    if cash <= 0:
        return [RiskWarning(
            severity="critical",
            category="debt_affordability",
            message=(
                f"Cash on hand ({cash:,.2f}) is at or below zero while "
                f"scheduled debt/mortgage obligations are {monthly_obligation:,.2f}/month."
            ),
            metric_value=cash,
            threshold=monthly_obligation,
        )]
    months_of_runway = cash / monthly_obligation
    if months_of_runway < 6:
        return [RiskWarning(
            severity="high",
            category="debt_affordability",
            message=(
                f"Current cash covers about {months_of_runway:.1f} months of "
                f"scheduled debt and mortgage obligations ({monthly_obligation:,.2f}/month)."
            ),
            metric_value=months_of_runway,
            threshold=6.0,
        )]
    return []


def check_bankruptcy(
    conn: sqlite3.Connection, *, _ctx: "_WarningCtx | None" = None,
) -> list[RiskWarning]:
    """Fire when the simulator has declared the portfolio bankrupt.

    Trigger paths:

    1. **Active bankruptcy_events row.** The auto-settle pipeline could
       not fund a scheduled debt or mortgage payment after force-selling
       all sellable assets, so it recorded a bankruptcy event. This is
       the canonical path under the new rule (scheduled obligations are
       not allowed to remain "overdue" — they either pay or trigger
       bankruptcy immediately).
    2. **Negative cash with no sellable holdings.** Classic insolvency:
       cash overdrew (e.g. via manual_adjustment) and there's nothing
       left to liquidate.
    3. **Legacy unresolved `missed_payments` rows.** Older databases may
       still hold these from a prior build. We surface them as bankruptcy
       (gated on no-sellable-holdings, matching the original semantic) so
       upgrade paths don't silently lose the warning. New auto-settle
       writes only bankruptcy_events.

    Critical severity, category ``bankruptcy`` so the Dashboard banner
    can render it distinctly from the routine low-cash signal.
    """
    if _ctx is None:
        cash = calc_cash_balance(conn)
    else:
        cash = _ctx.summary["cash"]

    bankruptcy_events = list_active_bankruptcy_events(conn)

    # Trigger path 1: active bankruptcy event(s) recorded by auto-settle.
    # No "sellable holdings" gate — by construction the pipeline only
    # records the event after force-selling has failed to raise cash.
    if bankruptcy_events:
        unfunded = [
            e for e in bankruptcy_events
            if e.trigger_kind in ("debt", "mortgage", "multiple")
        ]
        if unfunded:
            total = sum(e.amount_due for e in unfunded)
            cash_clause = f" Cash balance is {cash:,.2f}." if cash < 0 else ""
            return [RiskWarning(
                severity="critical",
                category="bankruptcy",
                message=(
                    f"Bankruptcy: the simulator declared bankruptcy because "
                    f"{len(unfunded)} scheduled debt/mortgage obligation(s) "
                    f"totaling {total:,.2f} could not be funded after all "
                    f"sellable assets were liquidated.{cash_clause}"
                ),
                metric_value=total,
                threshold=0.0,
            )]
        # All active events are negative_cash — fall through to the cash
        # path below so we present the same message.

    # Path 2 gates on "no sellable holdings remain" — we mustn't fire
    # for transient negative cash while the user still has things to
    # sell. (Legacy missed_payments path was removed in schema v10; any
    # unresolved rows were migrated to bankruptcy_events at upgrade.)
    if cash >= 0 and not bankruptcy_events:
        return []

    rows = conn.execute(
        """
        SELECT a.id AS asset_id
        FROM assets a
        WHERE a.asset_type IN ('stock', 'etf', 'crypto', 'custom')
          AND COALESCE((
              SELECT SUM(CASE
                  WHEN txn_type = 'buy' THEN quantity
                  WHEN txn_type = 'sell' THEN -quantity
                  WHEN txn_type = 'manual_adjustment'
                       AND quantity IS NOT NULL THEN quantity
                  ELSE 0
              END)
              FROM transactions WHERE asset_id = a.id
          ), 0) > 1e-9
        LIMIT 1
        """
    ).fetchall()
    if rows:
        return []

    return [RiskWarning(
        severity="critical",
        category="bankruptcy",
        message=(
            f"Bankruptcy: cash balance is {cash:,.2f} and all sellable "
            f"assets have been liquidated. The portfolio cannot cover "
            f"its remaining obligations."
        ),
        metric_value=cash,
        threshold=0.0,
    )]


def is_bankrupt(conn: sqlite3.Connection) -> bool:
    """Thin predicate over ``check_bankruptcy``.

    Returns ``True`` iff the simulator has declared the portfolio
    bankrupt — same trigger paths as ``check_bankruptcy`` (active
    ``bankruptcy_events``; cash<0 with no sellable holdings; legacy
    ``missed_payments`` with no sellable holdings). Used by the GUI
    transaction guards and the per-page banner so the same predicate
    drives both: there's no way for the banner and the guard to
    disagree.

    Cheap query — runs once per page refresh; no caching needed.
    """
    return bool(check_bankruptcy(conn))


def check_missing_journal(conn: sqlite3.Connection) -> list[RiskWarning]:
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM transactions t "
        "WHERE t.txn_type IN ('buy', 'sell') "
        "AND NOT EXISTS ("
        "    SELECT 1 FROM decision_journal j WHERE j.transaction_id = t.id"
        ")"
    ).fetchone()
    count = row["cnt"]
    if count > 0:
        return [RiskWarning(
            severity="info",
            category="discipline",
            message=f"{count} trade transaction(s) have no linked decision journal entry.",
            metric_value=float(count),
        )]
    return []


def get_all_warnings(conn: sqlite3.Connection) -> list[RiskWarning]:
    # Build aggregates once and pass them through the heavy checks.
    # The portfolio-summary + full-allocation pair covers everything
    # net_worth/cash/total_assets/positions/by_asset/cash_pct/etc.
    # consumers below need, so we don't pay calc_total_assets +
    # calc_positions inside each check_* helper.
    ctx = _WarningCtx.from_conn(conn)

    warnings = []
    warnings.extend(check_concentration(conn, _ctx=ctx))
    warnings.extend(check_crypto_exposure(conn, _ctx=ctx))
    warnings.extend(check_low_cash(conn, _ctx=ctx))
    warnings.extend(check_leverage(conn, _ctx=ctx))
    warnings.extend(check_illiquidity(conn, _ctx=ctx))
    warnings.extend(check_bankruptcy(conn, _ctx=ctx))
    warnings.extend(check_debt_payoff_horizon(conn))
    warnings.extend(check_debt_affordability(conn))
    # The remaining checks operate on different state (active
    # properties, market_prices, decision_journal) and don't share
    # aggregates with the heavy ones, so they stay direct.
    warnings.extend(check_real_estate_ltv(conn))
    warnings.extend(check_missing_prices(conn))
    warnings.extend(check_missing_journal(conn))

    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
    warnings.sort(key=lambda w: severity_order.get(w.severity, 5))
    return warnings
