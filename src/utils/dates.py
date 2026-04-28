"""Date arithmetic helpers used across multiple engines."""
from datetime import date


def next_month_start(d: date) -> date:
    """First day of the calendar month after `d`.

    `d` itself can be any day; only its year+month are consulted.
    """
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)
