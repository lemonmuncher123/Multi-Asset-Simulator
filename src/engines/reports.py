import json
import sqlite3
from datetime import date, datetime

from src.models.report import Report
from src.models.portfolio_snapshot import PortfolioSnapshot
from src.storage.report_repo import create_or_replace_report, report_exists
from src.storage.snapshot_repo import get_latest_snapshot_on_or_before
from src.engines.portfolio import get_portfolio_summary
from src.engines.allocation import get_full_allocation
from src.engines.risk import get_all_warnings
from src.utils.dates import next_month_start


TRADE_TYPES = {"buy", "sell"}
RE_TYPES = {"add_property", "sell_property", "receive_rent", "pay_property_expense", "pay_mortgage"}
DEBT_TYPES = {"add_debt", "pay_debt"}

# Cash flow breakdown buckets. These are intentionally separate from the
# RE_TYPES/DEBT_TYPES display-section sets above: pay_mortgage shows up in
# the real_estate display section but is treated as debt-related cash flow
# here, because the cash impact is debt servicing.
FUNDING_FLOW_TYPES = {"deposit_cash", "withdraw_cash"}
RE_CASH_FLOW_TYPES = {"add_property", "sell_property", "receive_rent", "pay_property_expense"}
DEBT_CASH_FLOW_TYPES = {"add_debt", "pay_debt", "pay_mortgage"}

TOP_ASSETS_LIMIT = 10


def _build_allocation_section(
    conn: sqlite3.Connection,
    ending_snap_obj: PortfolioSnapshot | None,
) -> dict:
    """Build the report's allocation section.

    Prefers the period-end stored snapshot's allocation_json. Falls back
    to the live portfolio allocation with a clear data quality note when
    the snapshot is missing, has no allocation, or has unparseable JSON.
    """
    if ending_snap_obj is not None and ending_snap_obj.allocation_json:
        try:
            alloc = json.loads(ending_snap_obj.allocation_json)
        except (json.JSONDecodeError, TypeError):
            alloc = None
            fallback_note = (
                "Stored snapshot allocation could not be parsed; using "
                "current portfolio allocation as fallback."
            )
        else:
            return {
                "source": "snapshot",
                "as_of": ending_snap_obj.date,
                "data_quality_note": (
                    f"Allocation from stored snapshot dated "
                    f"{ending_snap_obj.date} (period-end balance sheet)."
                ),
                "cash_amount": ending_snap_obj.cash,
                "total_assets": ending_snap_obj.total_assets,
                "total_liabilities": ending_snap_obj.total_liabilities,
                "net_worth": ending_snap_obj.net_worth,
                "cash_pct": alloc.get("cash_pct"),
                "by_asset_type": alloc.get("by_asset_type", {}),
                "top_assets": list(alloc.get("by_asset") or [])[:TOP_ASSETS_LIMIT],
                "by_liquidity": alloc.get("by_liquidity", {}),
                "real_estate_equity_pct": alloc.get("real_estate_equity_pct"),
                "debt_ratio": alloc.get("debt_ratio"),
                "liquid_assets": alloc.get("liquid_assets"),
                "illiquid_assets": alloc.get("illiquid_assets"),
            }
    elif ending_snap_obj is None:
        fallback_note = (
            "No stored snapshot at or before period end; allocation "
            "reflects current portfolio state at report generation time."
        )
    else:
        fallback_note = (
            "Stored snapshot has no allocation data; allocation reflects "
            "current portfolio state at report generation time."
        )

    summary = get_portfolio_summary(conn)
    alloc = get_full_allocation(conn)
    return {
        "source": "current",
        "as_of": None,
        "data_quality_note": fallback_note,
        "cash_amount": summary["cash"],
        "total_assets": summary["total_assets"],
        "total_liabilities": summary["total_liabilities"],
        "net_worth": summary["net_worth"],
        "cash_pct": alloc.get("cash_pct"),
        "by_asset_type": alloc.get("by_asset_type", {}),
        "top_assets": list(alloc.get("by_asset") or [])[:TOP_ASSETS_LIMIT],
        "by_liquidity": alloc.get("by_liquidity", {}),
        "real_estate_equity_pct": alloc.get("real_estate_equity_pct"),
        "debt_ratio": alloc.get("debt_ratio"),
        "liquid_assets": alloc.get("liquid_assets"),
        "illiquid_assets": alloc.get("illiquid_assets"),
    }


def _build_risk_summary_section(
    conn: sqlite3.Connection,
    ending_snap_obj: PortfolioSnapshot | None,
) -> dict:
    """Build the report's risk summary section.

    Reuses the existing risk engine via `get_all_warnings`. Risk warnings
    are not historically stored, so this section is always evaluated
    against current portfolio state. The data quality note makes that
    explicit, especially when a period-end snapshot exists (the rest of
    the report may reflect historical state but this section does not).
    """
    warnings = get_all_warnings(conn)
    actionable = [w for w in warnings if w.severity != "info"]
    info_only = [w for w in warnings if w.severity == "info"]

    by_severity: dict[str, int] = {}
    by_category: dict[str, int] = {}
    for w in warnings:
        by_severity[w.severity] = by_severity.get(w.severity, 0) + 1
        by_category[w.category] = by_category.get(w.category, 0) + 1

    if ending_snap_obj is not None:
        note = (
            "Risk warnings reflect CURRENT portfolio state at report "
            "generation time. Risk warnings are not historically stored; "
            "they may differ from the period-end balance sheet shown in "
            "the snapshot/allocation sections."
        )
    else:
        note = (
            "Risk warnings reflect current portfolio state at report "
            "generation time. Risk warnings are not historically stored."
        )

    return {
        "source": "current",
        "warning_count": len(actionable),
        "info_count": len(info_only),
        "total_count": len(warnings),
        "by_severity": by_severity,
        "by_category": by_category,
        "warnings": [
            {
                "severity": w.severity,
                "category": w.category,
                "message": w.message,
                "metric_value": w.metric_value,
                "threshold": w.threshold,
                "related_asset_id": w.related_asset_id,
            }
            for w in warnings
        ],
        "data_quality_note": note,
    }


def build_period_report(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    label: str,
    report_type: str,
) -> Report:
    txns = conn.execute(
        "SELECT t.*, a.symbol AS asset_symbol, a.name AS asset_name "
        "FROM transactions t "
        "LEFT JOIN assets a ON t.asset_id = a.id "
        "WHERE t.date >= ? AND t.date < ? "
        "ORDER BY t.date, t.id",
        (start_date, end_date),
    ).fetchall()

    txn_count = len(txns)
    net_cash_flow = sum(r["total_amount"] for r in txns)
    total_fees = sum(r["fees"] for r in txns)

    total_inflow = sum(r["total_amount"] for r in txns if r["total_amount"] > 0)
    total_outflow = sum(r["total_amount"] for r in txns if r["total_amount"] < 0)

    rent_income = sum(r["total_amount"] for r in txns if r["txn_type"] == "receive_rent")
    property_expense = sum(abs(r["total_amount"]) for r in txns if r["txn_type"] == "pay_property_expense")
    operating_net_income = rent_income - property_expense

    cash_before = conn.execute(
        "SELECT COALESCE(SUM(total_amount), 0) FROM transactions WHERE date < ?",
        (start_date,),
    ).fetchone()[0]

    cash_after = conn.execute(
        "SELECT COALESCE(SUM(total_amount), 0) FROM transactions WHERE date < ?",
        (end_date,),
    ).fetchone()[0]

    ops_by_type: dict[str, dict] = {}
    for r in txns:
        tt = r["txn_type"]
        if tt not in ops_by_type:
            ops_by_type[tt] = {"txn_type": tt, "count": 0, "total_amount": 0.0, "total_fees": 0.0}
        ops_by_type[tt]["count"] += 1
        ops_by_type[tt]["total_amount"] += r["total_amount"]
        ops_by_type[tt]["total_fees"] += r["fees"]
    operations = sorted(ops_by_type.values(), key=lambda x: x["txn_type"])

    txn_details = []
    for r in txns:
        txn_details.append({
            "date": r["date"],
            "txn_type": r["txn_type"],
            "asset_symbol": r["asset_symbol"] or "",
            "asset_name": r["asset_name"] or "",
            "quantity": r["quantity"],
            "price": r["price"],
            "total_amount": r["total_amount"],
            "fees": r["fees"],
            "notes": r["notes"] or "",
        })

    trades = [t for t in txn_details if t["txn_type"] in TRADE_TYPES]
    re_ops = [t for t in txn_details if t["txn_type"] in RE_TYPES]
    debt_ops = [t for t in txn_details if t["txn_type"] in DEBT_TYPES]

    journal_rows = conn.execute(
        "SELECT id, date, title, thesis, confidence_level, tags "
        "FROM decision_journal "
        "WHERE date >= ? AND date < ? "
        "ORDER BY date, id",
        (start_date, end_date),
    ).fetchall()
    journal_entries = [
        {
            "id": j["id"],
            "date": j["date"],
            "title": j["title"],
            "thesis": j["thesis"] or "",
            "confidence_level": j["confidence_level"],
            "tags": j["tags"] or "",
        }
        for j in journal_rows
    ]

    # Cash flow breakdown — split net_cash_flow into named categories so a
    # reader can see which kinds of activity moved cash. Sums of category
    # nets equal net_cash_flow (fees_total is informational; fees are
    # already reflected in each transaction's total_amount).
    cfb_funding_deposits = 0.0
    cfb_funding_withdrawals = 0.0
    cfb_trade_buys = 0.0
    cfb_trade_sells = 0.0
    cfb_re_property_purchases = 0.0
    cfb_re_property_sales = 0.0
    cfb_re_rent_received = 0.0
    cfb_re_property_expenses = 0.0
    cfb_debt_borrowed = 0.0
    cfb_debt_payments = 0.0
    cfb_mortgage_payments = 0.0
    cfb_other = 0.0
    for r in txns:
        tt = r["txn_type"]
        amt = r["total_amount"]
        if tt == "deposit_cash":
            cfb_funding_deposits += amt
        elif tt == "withdraw_cash":
            cfb_funding_withdrawals += amt
        elif tt == "buy":
            cfb_trade_buys += amt
        elif tt == "sell":
            cfb_trade_sells += amt
        elif tt == "add_property":
            cfb_re_property_purchases += amt
        elif tt == "sell_property":
            cfb_re_property_sales += amt
        elif tt == "receive_rent":
            cfb_re_rent_received += amt
        elif tt == "pay_property_expense":
            cfb_re_property_expenses += amt
        elif tt == "add_debt":
            cfb_debt_borrowed += amt
        elif tt == "pay_debt":
            cfb_debt_payments += amt
        elif tt == "pay_mortgage":
            cfb_mortgage_payments += amt
        else:
            cfb_other += amt

    funding_flow_net = cfb_funding_deposits + cfb_funding_withdrawals

    cash_flow_breakdown = {
        "funding_flow": {
            "deposits": cfb_funding_deposits,
            "withdrawals": cfb_funding_withdrawals,
            "net": funding_flow_net,
        },
        "trade_cash_flow": {
            "buys": cfb_trade_buys,
            "sells": cfb_trade_sells,
            "net": cfb_trade_buys + cfb_trade_sells,
        },
        "real_estate_cash_flow": {
            "rent_received": cfb_re_rent_received,
            "property_expenses": cfb_re_property_expenses,
            "property_purchases": cfb_re_property_purchases,
            "property_sales": cfb_re_property_sales,
            "net": (
                cfb_re_rent_received
                + cfb_re_property_expenses
                + cfb_re_property_purchases
                + cfb_re_property_sales
            ),
        },
        "debt_cash_flow": {
            "borrowed": cfb_debt_borrowed,
            "debt_payments": cfb_debt_payments,
            "mortgage_payments": cfb_mortgage_payments,
            "net": cfb_debt_borrowed + cfb_debt_payments + cfb_mortgage_payments,
        },
        "fees_total": total_fees,
        "other_cash_flow": cfb_other,
    }

    # Beginning and ending snapshots. Both fall back to None values + a
    # note when no stored snapshot is available at or before the boundary.
    beginning_snap_obj = get_latest_snapshot_on_or_before(conn, start_date)
    ending_snap_obj = get_latest_snapshot_on_or_before(conn, end_date)

    if beginning_snap_obj is not None:
        beginning_snapshot = {
            "snapshot_date": beginning_snap_obj.date,
            "cash": beginning_snap_obj.cash,
            "total_assets": beginning_snap_obj.total_assets,
            "total_liabilities": beginning_snap_obj.total_liabilities,
            "net_worth": beginning_snap_obj.net_worth,
            "note": (
                f"Snapshot from {beginning_snap_obj.date} (most recent stored "
                f"snapshot on or before period start {start_date})."
            ),
        }
    else:
        beginning_snapshot = {
            "snapshot_date": None,
            "cash": None,
            "total_assets": None,
            "total_liabilities": None,
            "net_worth": None,
            "note": (
                f"No stored snapshot on or before period start {start_date}; "
                f"beginning balance sheet unavailable."
            ),
        }

    if ending_snap_obj is not None:
        ending_snapshot = {
            "snapshot_date": ending_snap_obj.date,
            "cash": ending_snap_obj.cash,
            "total_assets": ending_snap_obj.total_assets,
            "total_liabilities": ending_snap_obj.total_liabilities,
            "net_worth": ending_snap_obj.net_worth,
            "note": (
                f"Snapshot from {ending_snap_obj.date} (most recent stored "
                f"snapshot on or before period end {end_date})."
            ),
        }
    else:
        ending_snapshot = {
            "snapshot_date": None,
            "cash": None,
            "total_assets": None,
            "total_liabilities": None,
            "net_worth": None,
            "note": (
                f"No stored snapshot on or before period end {end_date}; "
                f"ending balance sheet unavailable."
            ),
        }

    # current_snapshot is kept for backward compatibility with older saved
    # report_json. When an ending snapshot exists, mirror it; otherwise
    # fall back to the live portfolio summary so existing UI/export code
    # that relies on non-None values continues to work.
    if ending_snap_obj is not None:
        current_snapshot = dict(ending_snapshot)
    else:
        summary = get_portfolio_summary(conn)
        current_snapshot = {
            "note": (
                "No stored snapshot at or before period end; showing "
                "current state at report generation time."
            ),
            "cash": summary["cash"],
            "total_assets": summary["total_assets"],
            "total_liabilities": summary["total_liabilities"],
            "net_worth": summary["net_worth"],
        }

    # Performance — approximate, snapshot-based. Funding flow is always
    # available from transactions; the rest depend on whether snapshots
    # exist at the period boundaries.
    beginning_nw = beginning_snap_obj.net_worth if beginning_snap_obj else None
    ending_nw = ending_snap_obj.net_worth if ending_snap_obj else None

    if beginning_snap_obj is not None and ending_snap_obj is not None:
        net_worth_change = ending_nw - beginning_nw
        approximate_investment_result = net_worth_change - funding_flow_net
        if beginning_nw is not None and beginning_nw > 0:
            approximate_return_pct = (approximate_investment_result / beginning_nw) * 100.0
        else:
            approximate_return_pct = None
        data_quality_note = (
            "Beginning and ending snapshots available. Performance figures "
            "are approximate (snapshot-based, no time-weighting; does not "
            "separate realized and unrealized P&L)."
        )
    else:
        net_worth_change = None
        approximate_investment_result = None
        approximate_return_pct = None
        missing = []
        if beginning_snap_obj is None:
            missing.append("beginning")
        if ending_snap_obj is None:
            missing.append("ending")
        data_quality_note = (
            f"Missing {' and '.join(missing)} snapshot(s); net-worth-based "
            f"performance metrics unavailable. Funding flow is still "
            f"computed from transaction history."
        )

    performance = {
        "beginning_net_worth": beginning_nw,
        "ending_net_worth": ending_nw,
        "net_worth_change": net_worth_change,
        "funding_flow": funding_flow_net,
        "approximate_investment_result": approximate_investment_result,
        "approximate_return_pct": approximate_return_pct,
        "data_quality_note": data_quality_note,
    }

    allocation = _build_allocation_section(conn, ending_snap_obj)
    risk_summary = _build_risk_summary_section(conn, ending_snap_obj)

    generated_at = datetime.now().isoformat()

    if report_type == "monthly":
        title = f"Monthly Report - {label}"
    else:
        title = f"Annual Report - {label}"

    report_data = {
        "summary": {
            "report_type": report_type,
            "period_label": label,
            "period_start": start_date,
            "period_end": end_date,
            "generated_at": generated_at,
            "transaction_count": txn_count,
            "beginning_cash": cash_before,
            "ending_cash": cash_after,
            "net_cash_flow": net_cash_flow,
            "operating_net_income": operating_net_income,
            "total_inflow": total_inflow,
            "total_outflow": total_outflow,
            "total_fees": total_fees,
        },
        "operations": operations,
        "transactions": txn_details,
        "trades": trades,
        "real_estate": re_ops,
        "debt": debt_ops,
        "journal": journal_entries,
        "current_snapshot": current_snapshot,
        "beginning_snapshot": beginning_snapshot,
        "ending_snapshot": ending_snapshot,
        "cash_flow_breakdown": cash_flow_breakdown,
        "performance": performance,
        "allocation": allocation,
        "risk_summary": risk_summary,
    }

    return Report(
        report_type=report_type,
        period_start=start_date,
        period_end=end_date,
        period_label=label,
        generated_at=generated_at,
        title=title,
        report_json=json.dumps(report_data),
    )


def generate_monthly_report(conn: sqlite3.Connection, year: int, month: int) -> Report:
    start = date(year, month, 1)
    end = next_month_start(start)
    label = f"{year}-{month:02d}"
    report = build_period_report(conn, start.isoformat(), end.isoformat(), label, "monthly")
    create_or_replace_report(conn, report)
    return report


def generate_annual_report(conn: sqlite3.Connection, year: int) -> Report:
    start = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    label = str(year)
    report = build_period_report(conn, start.isoformat(), end.isoformat(), label, "annual")
    create_or_replace_report(conn, report)
    return report


def get_auto_report_start_date(conn: sqlite3.Connection) -> date | None:
    # Filter out the synthetic add_property markers used for "existing
    # property" and "planned purchase" entry modes — those have
    # total_amount=0 and don't represent actual cash activity, so they
    # shouldn't pin the auto-report start date (especially planned
    # purchases, which can be in the future).
    row = conn.execute(
        "SELECT MIN(date) as min_date FROM transactions "
        "WHERE NOT (txn_type = 'add_property' AND total_amount = 0 "
        "AND ("
        "    notes LIKE '%Existing property entry%' "
        "    OR notes LIKE '%Planned purchase scenario%'"
        "))"
    ).fetchone()
    if row is None or row["min_date"] is None:
        return None
    return date.fromisoformat(row["min_date"])


def count_due_reports(conn: sqlite3.Connection, today: date | None = None) -> int:
    if today is None:
        today = date.today()

    earliest = get_auto_report_start_date(conn)
    if earliest is None:
        return 0

    earliest_start = date(earliest.year, earliest.month, 1)
    count = 0

    current = earliest_start
    while True:
        nxt = next_month_start(current)
        if nxt > today:
            break

        label = f"{current.year}-{current.month:02d}"
        if not report_exists(conn, "monthly", label):
            count += 1

        current = nxt

    for year in range(earliest.year, today.year):
        label = str(year)
        if not report_exists(conn, "annual", label):
            count += 1

    return count


def generate_due_reports(conn: sqlite3.Connection, today: date | None = None) -> list[Report]:
    if today is None:
        today = date.today()

    earliest = get_auto_report_start_date(conn)
    if earliest is None:
        return []

    earliest_start = date(earliest.year, earliest.month, 1)

    generated = []

    current = earliest_start
    while True:
        nxt = next_month_start(current)
        if nxt > today:
            break

        label = f"{current.year}-{current.month:02d}"
        if not report_exists(conn, "monthly", label):
            report = generate_monthly_report(conn, current.year, current.month)
            generated.append(report)

        current = nxt

    for year in range(earliest.year, today.year):
        label = str(year)
        if not report_exists(conn, "annual", label):
            report = generate_annual_report(conn, year)
            generated.append(report)

    return generated
