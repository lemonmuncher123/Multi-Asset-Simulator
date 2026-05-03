from datetime import date

import pytest

from src.utils.dates import (
    next_month_start,
    quarter_bounds,
    half_year_bounds,
    quarter_of,
    half_of,
)


def test_next_month_start_basic():
    assert next_month_start(date(2026, 3, 15)) == date(2026, 4, 1)


def test_next_month_start_december_rolls_year():
    assert next_month_start(date(2026, 12, 31)) == date(2027, 1, 1)


def test_quarter_bounds_q1():
    start, end = quarter_bounds(2026, 1)
    assert start == date(2026, 1, 1)
    assert end == date(2026, 4, 1)


def test_quarter_bounds_q2():
    start, end = quarter_bounds(2026, 2)
    assert start == date(2026, 4, 1)
    assert end == date(2026, 7, 1)


def test_quarter_bounds_q3():
    start, end = quarter_bounds(2026, 3)
    assert start == date(2026, 7, 1)
    assert end == date(2026, 10, 1)


def test_quarter_bounds_q4_rolls_year():
    start, end = quarter_bounds(2026, 4)
    assert start == date(2026, 10, 1)
    assert end == date(2027, 1, 1)


@pytest.mark.parametrize("q", [0, 5, -1])
def test_quarter_bounds_invalid(q):
    with pytest.raises(ValueError):
        quarter_bounds(2026, q)


def test_half_year_bounds_h1():
    start, end = half_year_bounds(2026, 1)
    assert start == date(2026, 1, 1)
    assert end == date(2026, 7, 1)


def test_half_year_bounds_h2_rolls_year():
    start, end = half_year_bounds(2026, 2)
    assert start == date(2026, 7, 1)
    assert end == date(2027, 1, 1)


@pytest.mark.parametrize("h", [0, 3, -1])
def test_half_year_bounds_invalid(h):
    with pytest.raises(ValueError):
        half_year_bounds(2026, h)


@pytest.mark.parametrize("month,expected_q", [
    (1, 1), (2, 1), (3, 1),
    (4, 2), (5, 2), (6, 2),
    (7, 3), (8, 3), (9, 3),
    (10, 4), (11, 4), (12, 4),
])
def test_quarter_of(month, expected_q):
    assert quarter_of(date(2026, month, 15)) == expected_q


@pytest.mark.parametrize("month,expected_h", [
    (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1),
    (7, 2), (8, 2), (9, 2), (10, 2), (11, 2), (12, 2),
])
def test_half_of(month, expected_h):
    assert half_of(date(2026, month, 15)) == expected_h
