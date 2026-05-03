import json
from pathlib import Path

import pandas as pd

from src.utils.display import (
    money_or_na as _fmt,
    fraction_as_percent_or_na as _pct,
    format_period_inclusive,
)


HOW_TO_READ = """HOW TO READ THIS REPORT
=======================

WHAT THIS IS:
- A periodic review of cash movement, operating activity, and approximate
  performance, derived from your transaction history and stored daily
  portfolio snapshots. This is a TRAINING tool — not a tax document, not
  a strict investment return calculation, and not financial advice.

CASH MOVEMENT VS PROFIT (IMPORTANT):
- Net Cash Flow is the sum of total_amount across ALL transactions in the
  period. It reflects CASH MOVEMENT, not profit. A large positive Net Cash
  Flow may simply mean you deposited money — it does not imply a gain.
  To understand what actually moved your portfolio, see Cash Flow Breakdown
  and Performance below.

CASH FLOW BREAKDOWN:
- Net Cash Flow split into named categories so you can see which kinds of
  activity moved cash:
  * Funding Flow: deposits and withdrawals — your money going IN/OUT of
    the portfolio. This is funding, NOT income or profit.
  * Trade Cash Flow: buys and sells of stocks/ETFs/crypto.
  * Real Estate Cash Flow: rent received, property expenses, and the cash
    impact of property purchases and property sales.
  * Debt Cash Flow: new borrowing (add_debt), debt repayments, and
    mortgage payments. (Mortgage payments appear here as debt servicing.)
  * Fees Total: total fees paid during the period (informational; fees are
    already reflected in each transaction's total_amount).
  * Other Cash Flow: anything not categorized above (e.g.,
    manual_adjustment).

OPERATING NET INCOME:
- A narrower measure focused on recurring real-estate operating activity.
  Equals rent received minus property expenses. Excludes deposits,
  withdrawals, trades, property buys/sells, and debt operations.

PERFORMANCE (APPROXIMATE):
- Beginning Net Worth: net worth from the latest stored snapshot AT OR
  BEFORE the period start date. None if no snapshot is available.
- Ending Net Worth: net worth from the latest stored snapshot AT OR
  BEFORE the period end date. None if no snapshot is available.
- Net Worth Change: ending minus beginning, when both are available.
- Funding Flow: same as in the Cash Flow Breakdown.
- Approximate Investment Result = Net Worth Change minus Funding Flow.
  Subtracting funding flow removes the effect of money you put in or took
  out, so the residual reflects portfolio movement (gains/losses,
  valuation changes, etc.). This is an APPROXIMATION:
    * It does not separate realized vs. unrealized P&L.
    * It does not weight cash flows by date (no time-weighting).
    * It depends on the quality and timing of stored snapshots.
- Approximate Return % = Approximate Investment Result / Beginning Net
  Worth. Reported only when Beginning Net Worth is at least $100. A
  smaller starting balance combined with normal cash movement produces
  a meaninglessly large percentage; reporting it would mislead.
- Data Quality Note: indicates whether snapshot data needed for these
  metrics was available. When snapshots are missing, the dependent
  metrics are reported as N/A rather than guessed.

SNAPSHOTS:
- Beginning Snapshot / Ending Snapshot are taken from the daily portfolio
  snapshot table at the period boundaries. If no snapshot exists at or
  before a boundary, fields are reported as N/A and a note explains.
- Current Snapshot (legacy section): kept for backward compatibility with
  older saved reports. Mirrors the Ending Snapshot when one is available;
  otherwise reports the current portfolio state at the time the report
  was generated.

ALLOCATION:
- Prefers the period-end stored snapshot's allocation_json so the report
  shows the balance sheet AS OF period end. Falls back to the live
  portfolio allocation at report generation time when no snapshot is
  available; the data quality note makes the source explicit
  ("snapshot" vs "current").

RISK SUMMARY:
- Re-uses the same risk-warning engine as the Risk page. Risk warnings
  are not stored historically, so this section ALWAYS reflects current
  portfolio state at report generation time, even when other sections
  reflect period-end state. The data quality note calls this out.
- Risk warnings are observations, not recommendations.

SNAPSHOTS (BEGINNING / ENDING):
- Net worth and balance sheet at the period start and period end, taken
  from the daily portfolio_snapshots table. If no snapshot exists at or
  before a boundary, the section reports N/A and explains why.

FEES BREAKDOWN:
- Fees paid during the period grouped by fee type (broker commission,
  SEC §31, FINRA TAF, etc.). Sourced from transaction_fee_breakdown.
  Rendered only when at least one breakdown row exists in the period.
"""


def export_report_txt(report_data: dict, path: str | Path) -> None:
    path = Path(path)
    s = report_data["summary"]
    lines = []

    lines.append(HOW_TO_READ)
    lines.append("")
    lines.append("=" * 60)
    lines.append(f"  {s['report_type'].upper()} REPORT: {s['period_label']}")
    lines.append("=" * 60)
    lines.append("")

    lines.append("SUMMARY")
    lines.append("-" * 40)
    lines.append(
        f"Period:              {format_period_inclusive(s['period_start'], s['period_end'])}"
    )
    lines.append(f"Generated:           {s['generated_at']}")
    lines.append(f"Transaction Count:   {s['transaction_count']}")
    lines.append(f"Beginning Cash:      {_fmt(s['beginning_cash'])}")
    lines.append(f"Ending Cash:         {_fmt(s['ending_cash'])}")
    lines.append(f"Net Cash Flow:       {_fmt(s['net_cash_flow'])}")
    lines.append(f"Operating Net Income:{_fmt(s['operating_net_income'])}")
    lines.append(f"Total Inflow:        {_fmt(s['total_inflow'])}")
    lines.append(f"Total Outflow:       {_fmt(s['total_outflow'])}")
    lines.append(f"Total Fees:          {_fmt(s['total_fees'])}")
    lines.append("")
    lines.append("Note: Net Cash Flow is cash movement, not profit. See the")
    lines.append("Cash Flow Breakdown and Performance sections below.")
    lines.append("")

    cfb = report_data.get("cash_flow_breakdown")
    if cfb:
        lines.append("CASH FLOW BREAKDOWN")
        lines.append("-" * 40)
        ff = cfb.get("funding_flow", {})
        lines.append("Funding Flow (deposits/withdrawals — funding, not income)")
        lines.append(f"  Deposits:           {_fmt(ff.get('deposits'))}")
        lines.append(f"  Withdrawals:        {_fmt(ff.get('withdrawals'))}")
        lines.append(f"  Net:                {_fmt(ff.get('net'))}")
        tcf = cfb.get("trade_cash_flow", {})
        lines.append("Trade Cash Flow (buys/sells of stocks, ETFs, crypto)")
        lines.append(f"  Buys:               {_fmt(tcf.get('buys'))}")
        lines.append(f"  Sells:              {_fmt(tcf.get('sells'))}")
        lines.append(f"  Net:                {_fmt(tcf.get('net'))}")
        rcf = cfb.get("real_estate_cash_flow", {})
        lines.append("Real Estate Cash Flow")
        lines.append(f"  Rent Received:      {_fmt(rcf.get('rent_received'))}")
        lines.append(f"  Property Expenses:  {_fmt(rcf.get('property_expenses'))}")
        lines.append(f"  Property Purchases: {_fmt(rcf.get('property_purchases'))}")
        lines.append(f"  Property Sales:     {_fmt(rcf.get('property_sales'))}")
        lines.append(f"  Net:                {_fmt(rcf.get('net'))}")
        dcf = cfb.get("debt_cash_flow", {})
        lines.append("Debt Cash Flow (borrow/repay/mortgage)")
        lines.append(f"  Borrowed:           {_fmt(dcf.get('borrowed'))}")
        lines.append(f"  Debt Payments:      {_fmt(dcf.get('debt_payments'))}")
        lines.append(f"  Mortgage Payments:  {_fmt(dcf.get('mortgage_payments'))}")
        lines.append(f"  Net:                {_fmt(dcf.get('net'))}")
        lines.append(f"Fees Total:           {_fmt(cfb.get('fees_total'))}")
        lines.append(f"Other Cash Flow:      {_fmt(cfb.get('other_cash_flow'))}")
        lines.append("")

    perf = report_data.get("performance")
    if perf:
        lines.append("PERFORMANCE (APPROXIMATE)")
        lines.append("-" * 40)
        lines.append(f"Beginning Net Worth:           {_fmt(perf.get('beginning_net_worth'))}")
        lines.append(f"Ending Net Worth:              {_fmt(perf.get('ending_net_worth'))}")
        lines.append(f"Net Worth Change:              {_fmt(perf.get('net_worth_change'))}")
        lines.append(f"Funding Flow:                  {_fmt(perf.get('funding_flow'))}")
        lines.append(f"Approximate Investment Result: {_fmt(perf.get('approximate_investment_result'))}")
        pct = perf.get("approximate_return_pct")
        pct_str = "N/A" if pct is None else f"{pct:.2f}%"
        lines.append(f"Approximate Return %:          {pct_str}")
        lines.append("")
        lines.append(f"Note: {perf.get('data_quality_note', '')}")
        lines.append(
            "Approximate Investment Result = Net Worth Change - Funding Flow."
        )
        lines.append(
            "This is not a strict return; see HOW TO READ for caveats."
        )
        lines.append("")

    alloc = report_data.get("allocation")
    if alloc:
        lines.append("ALLOCATION")
        lines.append("-" * 40)
        lines.append(f"Source:               {alloc.get('source', 'unknown')}")
        as_of = alloc.get("as_of")
        lines.append(f"As Of:                {as_of if as_of else 'N/A (current state)'}")
        lines.append(f"Cash Amount:          {_fmt(alloc.get('cash_amount'))}")
        lines.append(f"Cash %:               {_pct(alloc.get('cash_pct'))}")
        lines.append(f"Total Assets:         {_fmt(alloc.get('total_assets'))}")
        lines.append(f"Total Liabilities:    {_fmt(alloc.get('total_liabilities'))}")
        lines.append(f"Net Worth:            {_fmt(alloc.get('net_worth'))}")
        lines.append(f"Liquid Assets:        {_fmt(alloc.get('liquid_assets'))}")
        lines.append(f"Illiquid Assets:      {_fmt(alloc.get('illiquid_assets'))}")
        lines.append(f"Real Estate Equity %: {_pct(alloc.get('real_estate_equity_pct'))}")
        lines.append(f"Debt Ratio:           {_pct(alloc.get('debt_ratio'))}")
        lines.append("")
        bat = alloc.get("by_asset_type") or {}
        if bat:
            lines.append("By Asset Type:")
            for atype, info in bat.items():
                info = info or {}
                lines.append(
                    f"  {atype:<14} {_fmt(info.get('value')):>14}  "
                    f"({_pct(info.get('pct')):>7})"
                )
        top = alloc.get("top_assets") or []
        if top:
            lines.append("")
            lines.append("Top Assets:")
            for item in top:
                item = item or {}
                name = item.get("name", "")
                lines.append(
                    f"  {name[:30]:<30} {_fmt(item.get('value')):>14}  "
                    f"({_pct(item.get('pct')):>7})"
                )
        bl = alloc.get("by_liquidity") or {}
        if bl:
            lines.append("")
            lines.append("By Liquidity:")
            for cat, info in bl.items():
                info = info or {}
                lines.append(
                    f"  {cat:<14} {_fmt(info.get('value')):>14}  "
                    f"({_pct(info.get('pct')):>7})"
                )
        lines.append("")
        lines.append(f"Note: {alloc.get('data_quality_note', '')}")
        lines.append("")

    risk = report_data.get("risk_summary")
    if risk:
        lines.append("RISK SUMMARY")
        lines.append("-" * 40)
        lines.append(
            f"Total: {risk.get('total_count', 0)} warnings "
            f"({risk.get('warning_count', 0)} actionable, "
            f"{risk.get('info_count', 0)} info)"
        )
        bs = risk.get("by_severity") or {}
        if bs:
            lines.append(
                "By Severity: "
                + ", ".join(f"{sev}={cnt}" for sev, cnt in bs.items())
            )
        bc = risk.get("by_category") or {}
        if bc:
            lines.append(
                "By Category: "
                + ", ".join(f"{cat}={cnt}" for cat, cnt in bc.items())
            )
        warnings_list = risk.get("warnings") or []
        if warnings_list:
            lines.append("")
            for w in warnings_list:
                w = w or {}
                lines.append(
                    f"  [{(w.get('severity') or '').upper():<8}] "
                    f"{w.get('category', '')}: {w.get('message', '')}"
                )
        lines.append("")
        lines.append(f"Note: {risk.get('data_quality_note', '')}")
        lines.append("Risk warnings are observations, not recommendations.")
        lines.append("")

    beg = report_data.get("beginning_snapshot") or {}
    end = report_data.get("ending_snapshot") or {}
    if beg or end:
        lines.append("BEGINNING SNAPSHOT")
        lines.append("-" * 40)
        lines.append(f"  Snapshot Date:     {beg.get('snapshot_date') or 'N/A'}")
        lines.append(f"  Cash:              {_fmt(beg.get('cash'))}")
        lines.append(f"  Total Assets:      {_fmt(beg.get('total_assets'))}")
        lines.append(f"  Total Liabilities: {_fmt(beg.get('total_liabilities'))}")
        lines.append(f"  Net Worth:         {_fmt(beg.get('net_worth'))}")
        if beg.get("note"):
            lines.append(f"  Note: {beg['note']}")
        lines.append("")
        lines.append("ENDING SNAPSHOT")
        lines.append("-" * 40)
        lines.append(f"  Snapshot Date:     {end.get('snapshot_date') or 'N/A'}")
        lines.append(f"  Cash:              {_fmt(end.get('cash'))}")
        lines.append(f"  Total Assets:      {_fmt(end.get('total_assets'))}")
        lines.append(f"  Total Liabilities: {_fmt(end.get('total_liabilities'))}")
        lines.append(f"  Net Worth:         {_fmt(end.get('net_worth'))}")
        if end.get("note"):
            lines.append(f"  Note: {end['note']}")
        lines.append("")

    fb = report_data.get("fees_breakdown") or {}
    fb_rows = fb.get("by_type") or []
    if fb_rows:
        lines.append("FEES BREAKDOWN")
        lines.append("-" * 40)
        lines.append(f"{'Fee Type':<25} {'Count':>6} {'Total':>14}")
        for row in fb_rows:
            lines.append(
                f"{row.get('fee_type', ''):<25} "
                f"{row.get('count', 0):>6} "
                f"{_fmt(row.get('total')):>14}"
            )
        lines.append(
            f"{'GRAND TOTAL':<25} {'':>6} {_fmt(fb.get('grand_total')):>14}"
        )
        lines.append("")

    ops = report_data.get("operations", [])
    if ops:
        lines.append("OPERATIONS BY TYPE")
        lines.append("-" * 40)
        lines.append(f"{'Type':<25} {'Count':>6} {'Amount':>14} {'Fees':>10}")
        for op in ops:
            lines.append(
                f"{op['txn_type']:<25} {op['count']:>6} "
                f"{_fmt(op['total_amount']):>14} {_fmt(op['total_fees']):>10}"
            )
        lines.append("")

    txns = report_data.get("transactions", [])
    if txns:
        lines.append("TRANSACTIONS")
        lines.append("-" * 40)
        for t in txns:
            asset = t.get("asset_symbol") or ""
            qty = t.get("quantity")
            qty_str = f" x{qty}" if qty else ""
            lines.append(
                f"  {t['date']}  {t['txn_type']:<22} "
                f"{asset}{qty_str}  {_fmt(t['total_amount'])}"
            )
        lines.append("")

    trades = report_data.get("trades", [])
    if trades:
        lines.append("TRADES")
        lines.append("-" * 40)
        for t in trades:
            lines.append(
                f"  {t['date']}  {t['txn_type']:<6} "
                f"{t.get('asset_symbol', ''):<8} "
                f"qty={t.get('quantity', '')} @ {_fmt(t.get('price', 0))}  "
                f"total={_fmt(t['total_amount'])}"
            )
        lines.append("")

    re = report_data.get("real_estate", [])
    if re:
        lines.append("REAL ESTATE OPERATIONS")
        lines.append("-" * 40)
        for t in re:
            lines.append(
                f"  {t['date']}  {t['txn_type']:<25} {_fmt(t['total_amount'])}"
            )
        lines.append("")

    debt = report_data.get("debt", [])
    if debt:
        lines.append("DEBT OPERATIONS")
        lines.append("-" * 40)
        for t in debt:
            lines.append(
                f"  {t['date']}  {t['txn_type']:<25} {_fmt(t['total_amount'])}"
            )
        lines.append("")

    journal = report_data.get("journal", [])
    if journal:
        lines.append("JOURNAL ENTRIES")
        lines.append("-" * 40)
        for j in journal:
            lines.append(f"  {j['date']}  {j['title']}")
            if j.get("thesis"):
                lines.append(f"    Thesis: {j['thesis']}")
        lines.append("")

    snap = report_data.get("current_snapshot", {})
    if snap:
        lines.append("CURRENT SNAPSHOT (at generation time)")
        lines.append("-" * 40)
        lines.append(f"  Note: {snap.get('note', '')}")
        lines.append(f"  Cash:              {_fmt(snap.get('cash'))}")
        lines.append(f"  Total Assets:      {_fmt(snap.get('total_assets'))}")
        lines.append(f"  Total Liabilities: {_fmt(snap.get('total_liabilities'))}")
        lines.append(f"  Net Worth:         {_fmt(snap.get('net_worth'))}")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def export_report_xlsx(report_data: dict, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    s = report_data["summary"]

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        how_to = pd.DataFrame({"How To Read This Report": HOW_TO_READ.strip().split("\n")})
        how_to.to_excel(writer, sheet_name="How To Read", index=False)

        summary_rows = [
            ("Report Type", s["report_type"]),
            ("Period Label", s["period_label"]),
            ("Period", format_period_inclusive(s["period_start"], s["period_end"])),
            ("Generated At", s["generated_at"]),
            ("Transaction Count", s["transaction_count"]),
            ("Beginning Cash", s["beginning_cash"]),
            ("Ending Cash", s["ending_cash"]),
            ("Net Cash Flow", s["net_cash_flow"]),
            ("Operating Net Income", s["operating_net_income"]),
            ("Total Inflow", s["total_inflow"]),
            ("Total Outflow", s["total_outflow"]),
            ("Total Fees", s["total_fees"]),
        ]
        df_summary = pd.DataFrame(summary_rows, columns=["Metric", "Value"])
        df_summary.to_excel(writer, sheet_name="Summary", index=False)

        cfb = report_data.get("cash_flow_breakdown") or {}
        ff = cfb.get("funding_flow", {})
        tcf = cfb.get("trade_cash_flow", {})
        rcf = cfb.get("real_estate_cash_flow", {})
        dcf = cfb.get("debt_cash_flow", {})
        cfb_rows = [
            ("Funding Flow", "Deposits", ff.get("deposits")),
            ("Funding Flow", "Withdrawals", ff.get("withdrawals")),
            ("Funding Flow", "Net", ff.get("net")),
            ("Trade Cash Flow", "Buys", tcf.get("buys")),
            ("Trade Cash Flow", "Sells", tcf.get("sells")),
            ("Trade Cash Flow", "Net", tcf.get("net")),
            ("Real Estate Cash Flow", "Rent Received", rcf.get("rent_received")),
            ("Real Estate Cash Flow", "Property Expenses", rcf.get("property_expenses")),
            ("Real Estate Cash Flow", "Property Purchases", rcf.get("property_purchases")),
            ("Real Estate Cash Flow", "Property Sales", rcf.get("property_sales")),
            ("Real Estate Cash Flow", "Net", rcf.get("net")),
            ("Debt Cash Flow", "Borrowed", dcf.get("borrowed")),
            ("Debt Cash Flow", "Debt Payments", dcf.get("debt_payments")),
            ("Debt Cash Flow", "Mortgage Payments", dcf.get("mortgage_payments")),
            ("Debt Cash Flow", "Net", dcf.get("net")),
            ("Fees Total", "", cfb.get("fees_total")),
            ("Other Cash Flow", "", cfb.get("other_cash_flow")),
        ]
        df_cfb = pd.DataFrame(cfb_rows, columns=["Category", "Sub-item", "Amount"])
        df_cfb.to_excel(writer, sheet_name="Cash Flow Breakdown", index=False)

        perf = report_data.get("performance") or {}
        perf_rows = [
            ("Beginning Net Worth", perf.get("beginning_net_worth")),
            ("Ending Net Worth", perf.get("ending_net_worth")),
            ("Net Worth Change", perf.get("net_worth_change")),
            ("Funding Flow", perf.get("funding_flow")),
            ("Approximate Investment Result", perf.get("approximate_investment_result")),
            ("Approximate Return %", perf.get("approximate_return_pct")),
            ("Data Quality Note", perf.get("data_quality_note", "")),
        ]
        df_perf = pd.DataFrame(perf_rows, columns=["Metric", "Value"])
        df_perf.to_excel(writer, sheet_name="Performance", index=False)

        alloc = report_data.get("allocation") or {}
        alloc_rows = [
            ("Source", alloc.get("source", "")),
            ("As Of", alloc.get("as_of") or ""),
            ("Cash Amount", alloc.get("cash_amount")),
            ("Cash %", alloc.get("cash_pct")),
            ("Total Assets", alloc.get("total_assets")),
            ("Total Liabilities", alloc.get("total_liabilities")),
            ("Net Worth", alloc.get("net_worth")),
            ("Liquid Assets", alloc.get("liquid_assets")),
            ("Illiquid Assets", alloc.get("illiquid_assets")),
            ("Real Estate Equity %", alloc.get("real_estate_equity_pct")),
            ("Debt Ratio", alloc.get("debt_ratio")),
        ]
        for atype, info in (alloc.get("by_asset_type") or {}).items():
            info = info or {}
            alloc_rows.append((f"By Asset Type - {atype}", info.get("value")))
            alloc_rows.append((f"By Asset Type - {atype} (pct)", info.get("pct")))
        for i, item in enumerate((alloc.get("top_assets") or []), start=1):
            item = item or {}
            alloc_rows.append((f"Top Asset {i} - {item.get('name', '')}", item.get("value")))
            alloc_rows.append((f"Top Asset {i} - {item.get('name', '')} (pct)", item.get("pct")))
        for cat, info in (alloc.get("by_liquidity") or {}).items():
            info = info or {}
            alloc_rows.append((f"By Liquidity - {cat}", info.get("value")))
            alloc_rows.append((f"By Liquidity - {cat} (pct)", info.get("pct")))
        alloc_rows.append(("Data Quality Note", alloc.get("data_quality_note", "")))
        df_alloc = pd.DataFrame(alloc_rows, columns=["Metric", "Value"])
        df_alloc.to_excel(writer, sheet_name="Allocation", index=False)

        risk = report_data.get("risk_summary") or {}
        risk_rows = [
            ("Total Count", risk.get("total_count", 0)),
            ("Warning Count (actionable)", risk.get("warning_count", 0)),
            ("Info Count", risk.get("info_count", 0)),
        ]
        for sev, cnt in (risk.get("by_severity") or {}).items():
            risk_rows.append((f"By Severity - {sev}", cnt))
        for cat, cnt in (risk.get("by_category") or {}).items():
            risk_rows.append((f"By Category - {cat}", cnt))
        risk_rows.append(("Data Quality Note", risk.get("data_quality_note", "")))
        df_risk_summary = pd.DataFrame(risk_rows, columns=["Metric", "Value"])
        df_risk_summary.to_excel(writer, sheet_name="Risk Summary", index=False)

        warnings_list = risk.get("warnings") or []
        if warnings_list:
            df_risk_warnings = pd.DataFrame(warnings_list)
        else:
            df_risk_warnings = pd.DataFrame(
                columns=["severity", "category", "message", "metric_value",
                         "threshold", "related_asset_id"]
            )
        df_risk_warnings.to_excel(writer, sheet_name="Risk Warnings", index=False)

        ops = report_data.get("operations", [])
        df_ops = pd.DataFrame(ops) if ops else pd.DataFrame(columns=["txn_type", "count", "total_amount", "total_fees"])
        df_ops.to_excel(writer, sheet_name="Operations", index=False)

        txns = report_data.get("transactions", [])
        df_txns = pd.DataFrame(txns) if txns else pd.DataFrame(
            columns=["date", "txn_type", "asset_symbol", "asset_name", "quantity", "price", "total_amount", "fees", "notes"]
        )
        df_txns.to_excel(writer, sheet_name="Transactions", index=False)

        trades = report_data.get("trades", [])
        df_trades = pd.DataFrame(trades) if trades else pd.DataFrame(
            columns=["date", "txn_type", "asset_symbol", "asset_name", "quantity", "price", "total_amount", "fees", "notes"]
        )
        df_trades.to_excel(writer, sheet_name="Trades", index=False)

        re_ops = report_data.get("real_estate", [])
        df_re = pd.DataFrame(re_ops) if re_ops else pd.DataFrame(
            columns=["date", "txn_type", "asset_symbol", "asset_name", "quantity", "price", "total_amount", "fees", "notes"]
        )
        df_re.to_excel(writer, sheet_name="Real Estate", index=False)

        debt_ops = report_data.get("debt", [])
        df_debt = pd.DataFrame(debt_ops) if debt_ops else pd.DataFrame(
            columns=["date", "txn_type", "asset_symbol", "asset_name", "quantity", "price", "total_amount", "fees", "notes"]
        )
        df_debt.to_excel(writer, sheet_name="Debt", index=False)

        journal = report_data.get("journal", [])
        df_journal = pd.DataFrame(journal) if journal else pd.DataFrame(
            columns=["id", "date", "title", "thesis", "confidence_level", "tags"]
        )
        df_journal.to_excel(writer, sheet_name="Journal", index=False)

        beg = report_data.get("beginning_snapshot") or {}
        end = report_data.get("ending_snapshot") or {}
        snap_be_rows = [
            ("Beginning - Snapshot Date", beg.get("snapshot_date") or ""),
            ("Beginning - Cash", beg.get("cash")),
            ("Beginning - Total Assets", beg.get("total_assets")),
            ("Beginning - Total Liabilities", beg.get("total_liabilities")),
            ("Beginning - Net Worth", beg.get("net_worth")),
            ("Beginning - Note", beg.get("note", "")),
            ("Ending - Snapshot Date", end.get("snapshot_date") or ""),
            ("Ending - Cash", end.get("cash")),
            ("Ending - Total Assets", end.get("total_assets")),
            ("Ending - Total Liabilities", end.get("total_liabilities")),
            ("Ending - Net Worth", end.get("net_worth")),
            ("Ending - Note", end.get("note", "")),
        ]
        df_snap_be = pd.DataFrame(snap_be_rows, columns=["Metric", "Value"])
        df_snap_be.to_excel(writer, sheet_name="Snapshots", index=False)

        fb = report_data.get("fees_breakdown") or {}
        fb_rows = fb.get("by_type") or []
        if fb_rows:
            df_fb = pd.DataFrame(
                [{"fee_type": r["fee_type"], "count": r["count"], "total": r["total"]}
                 for r in fb_rows]
            )
        else:
            df_fb = pd.DataFrame(columns=["fee_type", "count", "total"])
        df_fb.to_excel(writer, sheet_name="Fees Breakdown", index=False)

        snap = report_data.get("current_snapshot", {})
        snap_rows = [
            ("Note", snap.get("note", "")),
            ("Cash", snap.get("cash")),
            ("Total Assets", snap.get("total_assets")),
            ("Total Liabilities", snap.get("total_liabilities")),
            ("Net Worth", snap.get("net_worth")),
        ]
        df_snap = pd.DataFrame(snap_rows, columns=["Metric", "Value"])
        df_snap.to_excel(writer, sheet_name="Current Snapshot", index=False)
