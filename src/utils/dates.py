"""Date arithmetic helpers used across multiple engines."""
from datetime import date


def next_month_start(d: date) -> date:
    """First day of the calendar month after `d`.

    `d` itself can be any day; only its year+month are consulted.
    """
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def quarter_bounds(year: int, quarter: int) -> tuple[date, date]:
    """Return (start, end_exclusive) for calendar quarter `quarter` of `year`.

    Q1 = Jan 1 – Apr 1, Q2 = Apr 1 – Jul 1, Q3 = Jul 1 – Oct 1,
    Q4 = Oct 1 – Jan 1 (next year).
    """
    if quarter not in (1, 2, 3, 4):
        raise ValueError(f"quarter must be 1..4, got {quarter}")
    start_month = (quarter - 1) * 3 + 1
    start = date(year, start_month, 1)
    if quarter == 4:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, start_month + 3, 1)
    return start, end


def half_year_bounds(year: int, half: int) -> tuple[date, date]:
    """Return (start, end_exclusive) for half-year `half` of `year`.

    H1 = Jan 1 – Jul 1, H2 = Jul 1 – Jan 1 (next year).
    """
    if half not in (1, 2):
        raise ValueError(f"half must be 1 or 2, got {half}")
    if half == 1:
        return date(year, 1, 1), date(year, 7, 1)
    return date(year, 7, 1), date(year + 1, 1, 1)


def quarter_of(d: date) -> int:
    """Calendar quarter (1..4) containing date `d`."""
    return (d.month - 1) // 3 + 1


def half_of(d: date) -> int:
    """Calendar half (1 or 2) containing date `d`."""
    return 1 if d.month <= 6 else 2
