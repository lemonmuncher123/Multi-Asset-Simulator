import sqlite3
from datetime import date, timedelta

from src.engines.portfolio import get_portfolio_summary, calc_positions
from src.engines.risk import get_all_warnings
from src.engines.real_estate import analyze_all_properties
from src.storage.snapshot_repo import list_snapshots
from src.storage.property_repo import list_active_properties


def get_dashboard_summary(conn: sqlite3.Connection, *, warnings=None) -> dict:
    summary = get_portfolio_summary(conn)
    if warnings is None:
        warnings = get_all_warnings(conn)
    non_info = [w for w in warnings if w.severity != "info"]
    top_message = non_info[0].message if non_info else None

    return {
        "net_worth": summary["net_worth"],
        "total_assets": summary["total_assets"],
        "total_liabilities": summary["total_liabilities"],
        "cash": summary["cash"],
        "position_value": summary["positions_value"],
        "property_value": summary["property_value"],
        "mortgage": summary["mortgage"],
        "debt": summary["debt"],
        "risk_warning_count": len(non_info),
        "top_risk_message": top_message,
    }


def get_net_worth_trend(
    conn: sqlite3.Connection, days: int = 90, today: date | None = None,
) -> list[dict]:
    snapshots = list_snapshots(conn)
    if not snapshots:
        return []

    end = today or date.today()
    cutoff = (end - timedelta(days=days)).isoformat()
    result = []
    for s in snapshots:
        if s.date >= cutoff:
            result.append({
                "date": s.date,
                "cash": s.cash,
                "total_assets": s.total_assets,
                "total_liabilities": s.total_liabilities,
                "net_worth": s.net_worth,
            })
    return result


def get_cash_flow_snapshot(
    conn: sqlite3.Connection, days: int = 30, today: date | None = None,
) -> dict:
    end = today or date.today()
    start = end - timedelta(days=days)
    start_str = start.isoformat()
    end_str = end.isoformat()

    rows = conn.execute(
        "SELECT total_amount, fees FROM transactions "
        "WHERE date >= ? AND date <= ?",
        (start_str, end_str),
    ).fetchall()

    inflow = 0.0
    outflow = 0.0
    total_fees = 0.0
    for row in rows:
        amt = row["total_amount"]
        if amt > 0:
            inflow += amt
        else:
            outflow += abs(amt)
        total_fees += row["fees"] or 0.0

    return {
        "start_date": start_str,
        "end_date": end_str,
        "inflow": inflow,
        "outflow": outflow,
        "net_cash_flow": inflow - outflow,
        "fees": total_fees,
        "transaction_count": len(rows),
    }


def get_return_drivers(conn: sqlite3.Connection, limit: int = 5) -> dict:
    positions = calc_positions(conn)

    priced = [
        p for p in positions
        if p.market_value is not None and p.unrealized_pnl is not None
    ]
    missing_price_count = len(positions) - len(priced)

    items = []
    for p in priced:
        pnl_pct = (p.unrealized_pnl / p.cost_basis) if p.cost_basis > 0 else None
        items.append({
            "symbol": p.symbol,
            "name": p.name,
            "asset_type": p.asset_type,
            "market_value": p.market_value,
            "cost_basis": p.cost_basis,
            "unrealized_pnl": p.unrealized_pnl,
            "unrealized_pnl_pct": pnl_pct,
        })

    items.sort(key=lambda x: x["unrealized_pnl"], reverse=True)
    gainers = [i for i in items if i["unrealized_pnl"] > 0][:limit]
    losers = [i for i in items if i["unrealized_pnl"] < 0]
    losers.sort(key=lambda x: x["unrealized_pnl"])
    losers = losers[:limit]

    return {
        "gainers": gainers,
        "losers": losers,
        "missing_price_count": missing_price_count,
    }


def get_recent_activity(conn: sqlite3.Connection, limit: int = 5) -> list[dict]:
    rows = conn.execute(
        "SELECT t.*, a.symbol AS asset_symbol, a.name AS asset_name "
        "FROM transactions t "
        "LEFT JOIN assets a ON t.asset_id = a.id "
        "ORDER BY t.date DESC, t.id DESC "
        "LIMIT ?",
        (limit,),
    ).fetchall()

    result = []
    for r in rows:
        result.append({
            "date": r["date"],
            "txn_type": r["txn_type"],
            "asset_symbol": r["asset_symbol"],
            "asset_name": r["asset_name"],
            "amount": r["total_amount"],
            "fees": r["fees"] or 0.0,
            "notes": r["notes"],
        })
    return result


def get_real_estate_snapshot(conn: sqlite3.Connection) -> dict | None:
    analyses = analyze_all_properties(conn)
    if not analyses:
        return None

    total_value = sum(a.prop.current_value or 0 for a in analyses)
    total_mortgage = sum(a.prop.mortgage_balance for a in analyses)
    total_equity = sum(a.equity for a in analyses)
    monthly_ncf = sum(a.net_monthly_cash_flow for a in analyses)
    annual_ncf = sum(a.annual_net_cash_flow for a in analyses)
    neg_cf_count = sum(1 for a in analyses if a.net_monthly_cash_flow < 0)

    ltvs = [a.ltv for a in analyses if a.ltv is not None]
    avg_ltv = sum(ltvs) / len(ltvs) if ltvs else None

    return {
        "property_count": len(analyses),
        "total_property_value": total_value,
        "total_mortgage": total_mortgage,
        "total_equity": total_equity,
        "monthly_net_cash_flow": monthly_ncf,
        "annual_net_cash_flow": annual_ncf,
        "average_ltv": avg_ltv,
        "negative_cash_flow_count": neg_cf_count,
    }
