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
from src.storage.property_repo import list_active_properties
from src.storage.price_repo import get_latest_price
from src.storage.asset_repo import list_assets
from src.storage.settings_repo import get_threshold


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
    warnings = []
    props = list_active_properties(conn)
    for prop in props:
        value = prop.current_value or 0
        if value <= 0:
            continue
        ltv = prop.mortgage_balance / value
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
    # The remaining checks operate on different state (active
    # properties, market_prices, decision_journal) and don't share
    # aggregates with the heavy ones, so they stay direct.
    warnings.extend(check_real_estate_ltv(conn))
    warnings.extend(check_missing_prices(conn))
    warnings.extend(check_missing_journal(conn))

    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
    warnings.sort(key=lambda w: severity_order.get(w.severity, 5))
    return warnings
