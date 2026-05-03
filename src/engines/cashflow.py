"""Multi-period cashflow series engine.

Builds a list of `CashflowPeriod` rows (one per calendar month or year) by
looping `compute_cash_flow_breakdown` (the canonical category mapping in
reports.py) across consecutive periods. Empty periods are kept with
zero values so the chart x-axis stays continuous.
"""
import sqlite3
from dataclasses import dataclass
from datetime import date
from typing import Literal

from src.engines.reports import compute_cash_flow_breakdown
from src.utils.dates import next_month_start


@dataclass
class CashflowPeriod:
    label: str
    period_start: date
    period_end: date
    funding_flow: float
    trade_cash_flow: float
    real_estate_cash_flow: float
    debt_cash_flow: float
    other_cash_flow: float
    net: float


def _next_year_start(d: date) -> date:
    return date(d.year + 1, 1, 1)


def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def _year_start(d: date) -> date:
    return date(d.year, 1, 1)


def _generate_monthly_periods(
    start: date, end: date,
) -> list[tuple[str, date, date]]:
    periods: list[tuple[str, date, date]] = []
    cur = _month_start(start)
    last = _month_start(end)
    while cur <= last:
        nxt = next_month_start(cur)
        label = f"{cur.year}-{cur.month:02d}"
        periods.append((label, cur, nxt))
        cur = nxt
    return periods


def _generate_yearly_periods(
    start: date, end: date,
) -> list[tuple[str, date, date]]:
    periods: list[tuple[str, date, date]] = []
    cur = _year_start(start)
    last = _year_start(end)
    while cur <= last:
        nxt = _next_year_start(cur)
        label = str(cur.year)
        periods.append((label, cur, nxt))
        cur = nxt
    return periods


def _default_window(granularity: str, today: date) -> tuple[date, date]:
    """Default range when caller doesn't pass start/end.

    monthly → last 12 calendar months ending in `today`'s month.
    yearly  → last 5 calendar years ending in `today`'s year.
    """
    end = today
    if granularity == "monthly":
        # Walk back 11 months from today's month to get 12 inclusive periods.
        y = today.year
        m = today.month - 11
        while m <= 0:
            m += 12
            y -= 1
        start = date(y, m, 1)
    else:
        start = date(today.year - 4, 1, 1)
    return start, end


def compute_cashflow_series(
    conn: sqlite3.Connection,
    granularity: Literal["monthly", "yearly"],
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[CashflowPeriod]:
    """Build a list of `CashflowPeriod` from start_date through end_date.

    `start_date` and `end_date` snap to their period boundaries
    (month-start / year-start). Empty periods are included with zeros so
    the resulting list is contiguous and chart-friendly.
    """
    if granularity not in ("monthly", "yearly"):
        raise ValueError(f"granularity must be 'monthly' or 'yearly', got {granularity!r}")

    if start_date is None or end_date is None:
        ds, de = _default_window(granularity, date.today())
        if start_date is None:
            start_date = ds
        if end_date is None:
            end_date = de

    if granularity == "monthly":
        period_specs = _generate_monthly_periods(start_date, end_date)
    else:
        period_specs = _generate_yearly_periods(start_date, end_date)

    out: list[CashflowPeriod] = []
    for label, p_start, p_end in period_specs:
        rows = conn.execute(
            "SELECT txn_type, total_amount, fees FROM transactions "
            "WHERE date >= ? AND date < ?",
            (p_start.isoformat(), p_end.isoformat()),
        ).fetchall()
        cfb = compute_cash_flow_breakdown(rows)
        funding = cfb["funding_flow"]["net"]
        trade = cfb["trade_cash_flow"]["net"]
        re = cfb["real_estate_cash_flow"]["net"]
        debt = cfb["debt_cash_flow"]["net"]
        other = cfb["other_cash_flow"]
        out.append(CashflowPeriod(
            label=label,
            period_start=p_start,
            period_end=p_end,
            funding_flow=funding,
            trade_cash_flow=trade,
            real_estate_cash_flow=re,
            debt_cash_flow=debt,
            other_cash_flow=other,
            net=funding + trade + re + debt + other,
        ))
    return out
