# Reports Improvements + Quarter / Half-Year Period Extension

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close visible gaps between report data and report rendering (beginning/ending snapshots, fees breakdown, staleness, overwrite confirm, HOW-TO-READ in GUI, falsy-price bug, stale defensive guards), and add Quarterly + Semi-Annual reports as first-class peers of Monthly/Annual. After Phase 1 ships and verifies green, execute the previously-deferred polish items (Phase 2).

**Naming.** The new half-year cadence uses `semi_annual` as the formal type identifier (DB string and code), `Semi-Annual` as the user-facing label, and `H1`/`H2` in period labels (`2026-H1`, `2026-H2`) — `H` is the well-established short form for "half" in financial reporting. The internal date-math helper in `utils/dates.py` keeps the descriptive name `half_year_bounds(year, half)` since it is generic period math, not policy.

**Architecture:** Two new report types (`quarterly`, `semi_annual`) reuse the existing `build_period_report(conn, start, end, label, type)` pipeline — only date helpers, period iteration in `generate_due_reports` / `count_due_reports`, and the GUI type dropdown change. Improvements layer on top: `build_period_report` adds a `fees_breakdown` section pulled from `transaction_fee_breakdown`; the GUI and both export writers (TXT, XLSX) gain Beginning/Ending Snapshot + Fees Breakdown rendering; staleness is computed from `COUNT(transactions in period)` vs the report's stored `transaction_count`. No schema migration needed — `reports.report_type` is `TEXT NOT NULL` with no CHECK constraint.

**Tech Stack:** Python 3.12+, PySide6 (Qt 6), SQLite, pandas / openpyxl (Excel export), pytest with `QT_QPA_PLATFORM=offscreen`.

---

## Scope

**Phase 1 (Tasks 1–14):**
- Quarter and Semi-Annual reports (calendar Q1–Q4, H1/H2; labels `YYYY-Q#` / `YYYY-H#`).
- Improvements #1 Beginning/Ending Snapshot rendering, #2 Fees Breakdown, #3 Staleness detection, #4 Falsy-price fix, #5 Stale `if column in keys` cleanup, #6 HOW-TO-READ in GUI, #7 Overwrite confirm.

**Phase 2 (Tasks 15–19, executed after Phase 1 verifies green):**
- #8 Decompose `_render_report_detail` into per-section helpers (worthwhile *after* Phase 1 has added two new tabs to it).
- #9 Consolidate duplicate formatter helpers into `utils/display.py`.
- #11 Inclusive period-end display (replace exclusive ISO end-date in user-facing strings).
- #10 Replace free-text `QInputDialog.getText` period input with a typed picker dialog.
- #12 Return % magnitude guard (skip computation when `beginning_net_worth < threshold`).

If Phase 1 verification (Task 14) reveals a regression that can't be cleanly fixed, stop and surface the failure before starting Phase 2. Phase 2 tasks are independent — abort after any individual task if it surfaces unexpected breakage.

**Schema:** No migration. `reports.report_type` accepts arbitrary strings already.

**Tests:** Existing tests using `"monthly"` / `"annual"` strings stay valid. New tests added per task; no existing test should need to change unless a default ordering / count assertion fails (call those out as we hit them).

---

## File Structure

| File | Role | Change |
|---|---|---|
| `src/utils/dates.py` | date math helpers | Add `quarter_bounds`, `half_year_bounds`, `quarter_of`, `half_of` |
| `src/engines/reports.py` | report generation | Add `generate_quarterly_report`, `generate_semi_annual_report`; extend `count_due_reports` + `generate_due_reports`; build `fees_breakdown` section in `build_period_report`; title-string for new types |
| `src/storage/report_repo.py` | persistence | `get_report_stats` returns counts for all 4 types; `_row_to_report` strips stale defensive guards (#5); add `count_transactions_in_period` helper for staleness |
| `src/engines/report_export.py` | TXT/XLSX writers | Add Beginning/Ending Snapshot + Fees Breakdown to both formats |
| `src/gui/pages/import_export.py` | GUI Reports tab | Add Quarterly / Semi-Annual to type combo + selected-period parser + filename defaults; new Beginning/Ending Snapshot tab; new Fees Breakdown tab; staleness column / italic; HOW-TO-READ button; overwrite confirm; fix `if price` falsy check; type-aware stats label |
| `tests/test_reports.py` | engine tests | Cover quarter/half-year generation, fees_breakdown |
| `tests/test_report_export.py` | export tests | Cover snapshot + fees rendering in TXT/XLSX |
| `tests/test_main_window_reports.py` | GUI tests | Cover new types + staleness + overwrite confirm |
| `PROJECT_UNDERSTANDING.md` | running memory | Update §3 (4 report types), §4 #6 (extended generation), and §5 (fees breakdown source) per CLAUDE.md Rule 2 |

---

## Task 1: Date helpers for quarter and half-year

**Files:**
- Modify: `src/utils/dates.py`
- Test: `tests/test_dates.py` (new file)

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_dates.py
from datetime import date
import pytest
from src.utils.dates import (
    next_month_start, quarter_bounds, half_year_bounds, quarter_of, half_of,
)


def test_quarter_bounds_q1():
    start, end = quarter_bounds(2026, 1)
    assert start == date(2026, 1, 1)
    assert end == date(2026, 4, 1)


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
    (1, 1), (3, 1), (4, 2), (6, 2), (7, 3), (9, 3), (10, 4), (12, 4),
])
def test_quarter_of(month, expected_q):
    assert quarter_of(date(2026, month, 15)) == expected_q


@pytest.mark.parametrize("month,expected_h", [
    (1, 1), (6, 1), (7, 2), (12, 2),
])
def test_half_of(month, expected_h):
    assert half_of(date(2026, month, 15)) == expected_h
```

- [ ] **Step 2: Run tests to verify they fail**

```
QT_QPA_PLATFORM=offscreen pytest tests/test_dates.py -v
```
Expected: FAIL — helpers not defined.

- [ ] **Step 3: Implement helpers**

Append to `src/utils/dates.py`:

```python
def quarter_bounds(year: int, quarter: int) -> tuple[date, date]:
    """Return (start, end_exclusive) for calendar quarter `quarter` of `year`.

    Q1 = Jan 1 – Apr 1, Q2 = Apr 1 – Jul 1, Q3 = Jul 1 – Oct 1, Q4 = Oct 1 – Jan 1 (next year).
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
    return (d.month - 1) // 3 + 1


def half_of(d: date) -> int:
    return 1 if d.month <= 6 else 2
```

- [ ] **Step 4: Run tests, expect pass**

```
QT_QPA_PLATFORM=offscreen pytest tests/test_dates.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/utils/dates.py tests/test_dates.py
git commit -m "feat(dates): add quarter/half-year period helpers"
```

---

## Task 2: Generate quarterly + half-yearly reports

**Files:**
- Modify: `src/engines/reports.py`
- Test: `tests/test_reports.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_reports.py`:

```python
def test_generate_quarterly_report_q1(db_conn):
    from src.engines.ledger import deposit_cash
    deposit_cash(db_conn, 100.0, transaction_date="2026-01-15")
    deposit_cash(db_conn, 200.0, transaction_date="2026-02-20")
    deposit_cash(db_conn, 50.0, transaction_date="2026-04-05")  # outside Q1

    from src.engines.reports import generate_quarterly_report
    r = generate_quarterly_report(db_conn, 2026, 1)
    assert r.report_type == "quarterly"
    assert r.period_label == "2026-Q1"
    assert r.period_start == "2026-01-01"
    assert r.period_end == "2026-04-01"
    assert r.title.startswith("Quarterly Report")

    import json
    data = json.loads(r.report_json)
    assert data["summary"]["transaction_count"] == 2
    assert data["summary"]["net_cash_flow"] == 300.0


def test_generate_semi_annual_report_h2(db_conn):
    from src.engines.ledger import deposit_cash
    deposit_cash(db_conn, 100.0, transaction_date="2026-08-15")

    from src.engines.reports import generate_semi_annual_report
    r = generate_semi_annual_report(db_conn, 2026, 2)
    assert r.report_type == "semi_annual"
    assert r.period_label == "2026-H2"
    assert r.period_start == "2026-07-01"
    assert r.period_end == "2027-01-01"


def test_generate_due_reports_includes_quarters_and_halves(db_conn):
    """When earliest txn is 2025-03-15 and today is 2026-08-01, due set
    should include 2025-Q1 (incomplete — Q2 onward), 2025-Q2, 2025-Q3,
    2025-Q4, 2026-Q1, 2026-Q2 (Q3 not yet ended), 2025-H1, 2025-H2,
    2026-H1, plus the existing monthly + 2025 annual."""
    from datetime import date
    from src.engines.ledger import deposit_cash
    from src.engines.reports import generate_due_reports

    deposit_cash(db_conn, 1.0, transaction_date="2025-03-15")
    generated = generate_due_reports(db_conn, today=date(2026, 8, 1))

    types = {(r.report_type, r.period_label) for r in generated}
    # quarters fully past as of today=2026-08-01:
    for q in ("2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4", "2026-Q1", "2026-Q2"):
        assert ("quarterly", q) in types
    # 2026-Q3 ends 2026-10-01 > today → not generated
    assert ("quarterly", "2026-Q3") not in types
    # halves fully past: 2025-H1, 2025-H2, 2026-H1
    for h in ("2025-H1", "2025-H2", "2026-H1"):
        assert ("semi_annual", h) in types
    assert ("semi_annual", "2026-H2") not in types


def test_count_due_reports_includes_new_types(db_conn):
    from datetime import date
    from src.engines.ledger import deposit_cash
    from src.engines.reports import count_due_reports

    deposit_cash(db_conn, 1.0, transaction_date="2026-01-15")
    # today=2026-08-01:
    # monthly: Jan, Feb, Mar, Apr, May, Jun, Jul = 7
    # quarterly: Q1, Q2 = 2
    # half-yearly: H1 = 1
    # annual: none (current year)
    n = count_due_reports(db_conn, today=date(2026, 8, 1))
    assert n == 7 + 2 + 1
```

- [ ] **Step 2: Run them, expect FAIL**

```
QT_QPA_PLATFORM=offscreen pytest tests/test_reports.py -k "quarterly or semi_annual or new_types" -v
```

- [ ] **Step 3: Implement engine support**

In `src/engines/reports.py`:

1. Update title selector inside `build_period_report`:

```python
if report_type == "monthly":
    title = f"Monthly Report - {label}"
elif report_type == "quarterly":
    title = f"Quarterly Report - {label}"
elif report_type == "semi_annual":
    title = f"Semi-Annual Report - {label}"
else:  # annual
    title = f"Annual Report - {label}"
```

2. Add new generators (place them next to `generate_monthly_report` / `generate_annual_report`):

```python
def generate_quarterly_report(conn: sqlite3.Connection, year: int, quarter: int) -> Report:
    from src.utils.dates import quarter_bounds
    start, end = quarter_bounds(year, quarter)
    label = f"{year}-Q{quarter}"
    report = build_period_report(conn, start.isoformat(), end.isoformat(), label, "quarterly")
    create_or_replace_report(conn, report)
    return report


def generate_semi_annual_report(conn: sqlite3.Connection, year: int, half: int) -> Report:
    from src.utils.dates import half_year_bounds
    start, end = half_year_bounds(year, half)
    label = f"{year}-H{half}"
    report = build_period_report(conn, start.isoformat(), end.isoformat(), label, "semi_annual")
    create_or_replace_report(conn, report)
    return report
```

3. Extend `count_due_reports` and `generate_due_reports`. Add quarterly + half-yearly loops after the existing monthly loop, before the annual loop. Both `count_due_reports` and `generate_due_reports` already share the same iteration shape — replicate it:

```python
# after the monthly while-loop, before the annual for-loop, in both functions:
from src.utils.dates import quarter_bounds, half_year_bounds

for yr in range(earliest.year, today.year + 1):
    for q in (1, 2, 3, 4):
        _qstart, qend = quarter_bounds(yr, q)
        if qend > today:
            break
        label = f"{yr}-Q{q}"
        if not report_exists(conn, "quarterly", label):
            # in count_due_reports: count += 1
            # in generate_due_reports: report = generate_quarterly_report(conn, yr, q); generated.append(report)
            ...

for yr in range(earliest.year, today.year + 1):
    for h in (1, 2):
        _hstart, hend = half_year_bounds(yr, h)
        if hend > today:
            break
        label = f"{yr}-H{h}"
        if not report_exists(conn, "semi_annual", label):
            ...
```

- [ ] **Step 4: Tests pass**

```
QT_QPA_PLATFORM=offscreen pytest tests/test_reports.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/engines/reports.py tests/test_reports.py
git commit -m "feat(reports): add quarterly and half-yearly period reports"
```

---

## Task 3: Stats + repo cleanup

**Files:**
- Modify: `src/storage/report_repo.py`
- Test: `tests/test_reports.py`

- [ ] **Step 1: Failing test for stats**

```python
def test_get_report_stats_includes_quarterly_and_semi_annual(db_conn):
    from src.engines.ledger import deposit_cash
    from src.engines.reports import (
        generate_monthly_report, generate_annual_report,
        generate_quarterly_report, generate_semi_annual_report,
    )
    from src.storage.report_repo import get_report_stats
    deposit_cash(db_conn, 1.0, transaction_date="2025-01-15")
    generate_monthly_report(db_conn, 2025, 1)
    generate_quarterly_report(db_conn, 2025, 1)
    generate_semi_annual_report(db_conn, 2025, 1)
    generate_annual_report(db_conn, 2025)

    stats = get_report_stats(db_conn)
    assert stats == {
        "total": 4, "monthly": 1, "quarterly": 1,
        "semi_annual": 1, "annual": 1,
    }
```

- [ ] **Step 2: Update `get_report_stats`**

```python
def get_report_stats(conn: sqlite3.Connection) -> dict:
    rows = conn.execute(
        "SELECT report_type, COUNT(*) as cnt FROM reports GROUP BY report_type"
    ).fetchall()
    by_type = {r["report_type"]: r["cnt"] for r in rows}
    return {
        "total": sum(by_type.values()),
        "monthly": by_type.get("monthly", 0),
        "quarterly": by_type.get("quarterly", 0),
        "semi_annual": by_type.get("semi_annual", 0),
        "annual": by_type.get("annual", 0),
    }
```

- [ ] **Step 3: Strip stale `if "X" in keys else default` from `_row_to_report`**

Per PROJECT_UNDERSTANDING.md §5 (v10 cleanup pattern), columns are guaranteed present post-migration. Replace the bottom of `_row_to_report` with:

```python
def _row_to_report(row: sqlite3.Row) -> Report:
    return Report(
        id=row["id"],
        report_type=row["report_type"],
        period_start=row["period_start"],
        period_end=row["period_end"],
        period_label=row["period_label"],
        generated_at=row["generated_at"],
        title=row["title"],
        report_json=row["report_json"],
        notes=row["notes"],
        net_cash_flow=row["net_cash_flow"],
        operating_net_income=row["operating_net_income"],
        transaction_count=row["transaction_count"],
        net_worth_change=row["net_worth_change"],
        funding_flow=row["funding_flow"] or 0.0,
        approximate_return_pct=row["approximate_return_pct"],
    )
```

- [ ] **Step 4: Run repo tests**

```
QT_QPA_PLATFORM=offscreen pytest tests/test_reports.py tests/test_storage.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/storage/report_repo.py tests/test_reports.py
git commit -m "refactor(reports): expand get_report_stats and drop stale row guards"
```

---

## Task 4: Add `fees_breakdown` section to report data

**Files:**
- Modify: `src/engines/reports.py:build_period_report`
- Test: `tests/test_reports.py`

- [ ] **Step 1: Failing test**

```python
def test_build_period_report_includes_fees_breakdown(db_conn):
    """A buy + sell with broker commission + SEC §31 fees should produce
    a fees_breakdown section keyed by fee_type."""
    from src.engines.ledger import deposit_cash, buy, sell
    from src.storage.fee_breakdown_repo import create_fee_breakdown, FeeBreakdownRow
    from src.engines.reports import generate_monthly_report
    import json

    deposit_cash(db_conn, 10000.0, transaction_date="2026-03-01")
    buy_txn = buy(
        db_conn, "AAPL", "stock", 10, 150.0, fees=2.5,
        transaction_date="2026-03-10",
    )
    create_fee_breakdown(db_conn, FeeBreakdownRow(
        transaction_id=buy_txn.id, fee_type="broker_commission", amount=2.5,
    ))
    sell_txn = sell(
        db_conn, "AAPL", 5, 160.0, fees=3.0,
        transaction_date="2026-03-20",
    )
    create_fee_breakdown(db_conn, FeeBreakdownRow(
        transaction_id=sell_txn.id, fee_type="broker_commission", amount=2.5,
    ))
    create_fee_breakdown(db_conn, FeeBreakdownRow(
        transaction_id=sell_txn.id, fee_type="sec_section31", amount=0.5,
    ))

    r = generate_monthly_report(db_conn, 2026, 3)
    data = json.loads(r.report_json)

    fb = data["fees_breakdown"]
    by_type = {row["fee_type"]: row for row in fb["by_type"]}
    assert by_type["broker_commission"]["total"] == 5.0
    assert by_type["broker_commission"]["count"] == 2
    assert by_type["sec_section31"]["total"] == 0.5
    assert by_type["sec_section31"]["count"] == 1
    assert fb["grand_total"] == 5.5


def test_build_period_report_fees_breakdown_empty(db_conn):
    """No fee_breakdown rows in period → fees_breakdown is present but empty."""
    from src.engines.ledger import deposit_cash
    from src.engines.reports import generate_monthly_report
    import json

    deposit_cash(db_conn, 100.0, transaction_date="2026-03-01")
    r = generate_monthly_report(db_conn, 2026, 3)
    data = json.loads(r.report_json)
    assert data["fees_breakdown"] == {"by_type": [], "grand_total": 0.0}
```

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Implement in `build_period_report`**

After the existing `cash_flow_breakdown = compute_cash_flow_breakdown(txns)` line, add:

```python
fee_rows = conn.execute(
    "SELECT fee_type, SUM(amount) AS total, COUNT(*) AS cnt "
    "FROM transaction_fee_breakdown "
    "WHERE transaction_id IN ("
    "  SELECT id FROM transactions WHERE date >= ? AND date < ?"
    ") "
    "GROUP BY fee_type "
    "ORDER BY total DESC",
    (start_date, end_date),
).fetchall()
fees_breakdown = {
    "by_type": [
        {"fee_type": r["fee_type"], "total": r["total"], "count": r["cnt"]}
        for r in fee_rows
    ],
    "grand_total": sum(r["total"] for r in fee_rows),
}
```

Then add `"fees_breakdown": fees_breakdown,` to the `report_data` dict.

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit**

```bash
git add src/engines/reports.py tests/test_reports.py
git commit -m "feat(reports): add fees_breakdown section grouped by fee_type"
```

---

## Task 5: TXT export — Beginning/Ending Snapshots + Fees Breakdown

**Files:**
- Modify: `src/engines/report_export.py`
- Test: `tests/test_report_export.py`

- [ ] **Step 1: Failing tests**

```python
def test_txt_export_includes_beginning_and_ending_snapshots(sample_report_data):
    """sample_report_data must already populate beginning_snapshot /
    ending_snapshot keys (build_period_report does)."""
    import tempfile
    from pathlib import Path
    from src.engines.report_export import export_report_txt

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "r.txt"
        export_report_txt(sample_report_data, path)
        text = path.read_text()
        assert "BEGINNING SNAPSHOT" in text
        assert "ENDING SNAPSHOT" in text


def test_txt_export_includes_fees_breakdown_section(sample_report_data_with_fees):
    import tempfile
    from pathlib import Path
    from src.engines.report_export import export_report_txt

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "r.txt"
        export_report_txt(sample_report_data_with_fees, path)
        text = path.read_text()
        assert "FEES BREAKDOWN" in text
        assert "broker_commission" in text


def test_txt_export_omits_fees_breakdown_when_empty(sample_report_data):
    """sample_report_data has empty fees_breakdown — section should not render
    or should render with a clear 'no fees' message (we choose: omit)."""
    import tempfile
    from pathlib import Path
    from src.engines.report_export import export_report_txt
    sample_report_data["fees_breakdown"] = {"by_type": [], "grand_total": 0.0}

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "r.txt"
        export_report_txt(sample_report_data, path)
        text = path.read_text()
        assert "FEES BREAKDOWN" not in text
```

(The existing `sample_report_data` fixture in this file already calls `build_period_report` so beginning/ending snapshots are populated. Add a `sample_report_data_with_fees` fixture that adds fee_breakdown rows.)

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Add rendering**

In `export_report_txt`, after the existing `risk` section block and before `ops`, insert:

```python
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
            f"{row.get('count', 0):>6} {_fmt(row.get('total')):>14}"
        )
    lines.append(f"{'GRAND TOTAL':<25} {'':>6} {_fmt(fb.get('grand_total')):>14}")
    lines.append("")
```

Update `HOW_TO_READ` to mention the new sections — append after the RISK SUMMARY block:

```python
SNAPSHOTS (BEGINNING / ENDING):
- Net worth and balance sheet at the period start and period end, taken
  from the daily portfolio_snapshots table. If no snapshot exists at or
  before a boundary, the section reports N/A and explains why.

FEES BREAKDOWN:
- Fees paid during the period grouped by fee type (broker commission,
  SEC §31, FINRA TAF, etc.). Sourced from transaction_fee_breakdown.
  Rendered only when at least one breakdown row exists in the period.
```

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit**

```bash
git add src/engines/report_export.py tests/test_report_export.py
git commit -m "feat(report-export): render beginning/ending snapshots and fees breakdown in txt"
```

---

## Task 6: XLSX export — same two sections

**Files:**
- Modify: `src/engines/report_export.py:export_report_xlsx`
- Test: `tests/test_report_export.py`

- [ ] **Step 1: Failing tests**

```python
def test_xlsx_export_has_snapshots_sheet(sample_report_data):
    import tempfile
    from pathlib import Path
    from openpyxl import load_workbook
    from src.engines.report_export import export_report_xlsx

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "r.xlsx"
        export_report_xlsx(sample_report_data, path)
        wb = load_workbook(path)
        assert "Snapshots" in wb.sheetnames


def test_xlsx_export_has_fees_breakdown_sheet(sample_report_data_with_fees):
    import tempfile
    from pathlib import Path
    from openpyxl import load_workbook
    from src.engines.report_export import export_report_xlsx

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "r.xlsx"
        export_report_xlsx(sample_report_data_with_fees, path)
        wb = load_workbook(path)
        assert "Fees Breakdown" in wb.sheetnames
        ws = wb["Fees Breakdown"]
        # header row + at least one fee_type row
        assert ws.max_row >= 2
```

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Implement**

In `export_report_xlsx`, before the existing `Current Snapshot` block, add:

```python
beg = report_data.get("beginning_snapshot") or {}
end = report_data.get("ending_snapshot") or {}
snap_rows = [
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
df_snap_be = pd.DataFrame(snap_rows, columns=["Metric", "Value"])
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
```

Note: Always write the "Snapshots" sheet (snapshot fields fall back to None / "" when no snapshot exists). Always write "Fees Breakdown" too — empty is fine in XLSX (gives the user a discoverable place even when there are no fees).

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit**

```bash
git add src/engines/report_export.py tests/test_report_export.py
git commit -m "feat(report-export): add snapshots and fees-breakdown sheets to xlsx"
```

---

## Task 7: GUI — Quarterly + Semi-Annual in dropdown, generation, filenames

**Files:**
- Modify: `src/gui/pages/import_export.py`
- Test: `tests/test_main_window_reports.py`

- [ ] **Step 1: Failing test**

```python
def test_reports_tab_supports_quarterly_and_semi_annual(qapp, db_conn):
    from src.gui.pages.import_export import ImportExportPage
    page = ImportExportPage(db_conn)
    items = [page.report_type_combo.itemData(i) for i in range(page.report_type_combo.count())]
    assert items == ["monthly", "quarterly", "semi_annual", "annual"]
```

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Wire up combo + parser + filenames**

In `ImportExportPage.__init__`, replace the combo build:

```python
self.report_type_combo.addItem("Monthly", "monthly")
self.report_type_combo.addItem("Quarterly", "quarterly")
self.report_type_combo.addItem("Semi-Annual", "semi_annual")
self.report_type_combo.addItem("Annual", "annual")
```

In `_generate_selected_period`, replace the if/else with a four-branch dispatch. (Use a helper to keep this readable.)

```python
def _generate_selected_period(self):
    rtype = self.report_type_combo.currentData()
    prompts = {
        "monthly":     ("Generate Monthly Report",     "Enter period label (YYYY-MM):"),
        "quarterly":   ("Generate Quarterly Report",   "Enter period label (YYYY-Q1..Q4):"),
        "semi_annual": ("Generate Semi-Annual Report", "Enter period label (YYYY-H1 or YYYY-H2):"),
        "annual":      ("Generate Annual Report",      "Enter year (YYYY):"),
    }
    title, prompt = prompts[rtype]
    label, ok = QInputDialog.getText(self, title, prompt)
    if not ok or not label.strip():
        return
    label = label.strip()
    try:
        if rtype == "monthly":
            year, month = (int(p) for p in label.split("-"))
            existing = report_exists(self.conn, "monthly", label)
            if existing and not self._confirm_overwrite(label):
                return
            generate_monthly_report(self.conn, year, month)
        elif rtype == "quarterly":
            year_str, q_str = label.split("-")
            quarter = int(q_str.lstrip("Qq"))
            normalized = f"{int(year_str)}-Q{quarter}"
            existing = report_exists(self.conn, "quarterly", normalized)
            if existing and not self._confirm_overwrite(normalized):
                return
            generate_quarterly_report(self.conn, int(year_str), quarter)
            label = normalized
        elif rtype == "semi_annual":
            year_str, h_str = label.split("-")
            half = int(h_str.lstrip("Hh"))
            normalized = f"{int(year_str)}-H{half}"
            existing = report_exists(self.conn, "semi_annual", normalized)
            if existing and not self._confirm_overwrite(normalized):
                return
            generate_semi_annual_report(self.conn, int(year_str), half)
            label = normalized
        else:  # annual
            year = int(label)
            existing = report_exists(self.conn, "annual", str(year))
            if existing and not self._confirm_overwrite(str(year)):
                return
            generate_annual_report(self.conn, year)
            label = str(year)
        self._refresh_report_list()
        QMessageBox.information(self, "Reports", f"Generated {rtype} report for {label}.")
    except (ValueError, IndexError) as e:
        QMessageBox.critical(self, "Report Error", f"Could not parse period '{label}': {e}")
    except Exception as e:
        QMessageBox.critical(self, "Report Error", str(e))


def _confirm_overwrite(self, label: str) -> bool:
    reply = QMessageBox.question(
        self, "Overwrite Report",
        f"A report for {label} already exists. Replace it?",
        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        QMessageBox.StandardButton.No,
    )
    return reply == QMessageBox.StandardButton.Yes
```

Imports at top:

```python
from src.engines.reports import (
    generate_due_reports, generate_monthly_report, generate_annual_report,
    generate_quarterly_report, generate_semi_annual_report,
    get_auto_report_start_date, count_due_reports,
)
from src.storage.report_repo import (
    list_reports, get_report, list_report_summaries, report_count,
    delete_reports_before_date, delete_report, delete_reports_by_type,
    delete_reports_in_period_range, delete_all_reports, get_report_stats,
    report_exists,
)
```

In `_export_selected_report`, extend the filename branch:

```python
rtype = report.report_type
label = report.period_label
if rtype == "monthly":
    default_base = f"monthly_report_{label.replace('-', '_')}"
elif rtype == "quarterly":
    default_base = f"quarterly_report_{label.replace('-', '_')}"
elif rtype == "semi_annual":
    default_base = f"semi_annual_report_{label.replace('-', '_')}"
else:
    default_base = f"annual_report_{label}"
```

Update the stats label to render dynamically:

```python
stats = get_report_stats(self.conn)
self._report_stats_label.setText(
    f"Total: {stats['total']} reports  |  "
    f"Monthly: {stats['monthly']}  |  "
    f"Quarterly: {stats['quarterly']}  |  "
    f"Semi-Annual: {stats['semi_annual']}  |  "
    f"Annual: {stats['annual']}"
)
```

- [ ] **Step 4: Run all GUI report tests**

```
QT_QPA_PLATFORM=offscreen pytest tests/test_main_window_reports.py tests/test_import_export_page.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/gui/pages/import_export.py tests/test_main_window_reports.py
git commit -m "feat(gui): add quarterly/half-yearly to report dropdown + overwrite confirm"
```

---

## Task 8: GUI — Beginning/Ending Snapshot tab

**Files:**
- Modify: `src/gui/pages/import_export.py`
- Test: `tests/test_main_window_reports.py`

- [ ] **Step 1: Failing test**

```python
def test_report_detail_renders_beginning_and_ending_snapshots(qapp, db_conn):
    """When a report has beginning/ending snapshot data, the new
    Snapshots tab should populate."""
    from src.engines.ledger import deposit_cash
    from src.engines.snapshots import write_daily_snapshot
    from src.engines.reports import generate_monthly_report
    from src.gui.pages.import_export import ImportExportPage

    deposit_cash(db_conn, 1000.0, transaction_date="2026-01-15")
    write_daily_snapshot(db_conn, "2026-01-01")
    write_daily_snapshot(db_conn, "2026-02-01")
    generate_monthly_report(db_conn, 2026, 1)

    page = ImportExportPage(db_conn)
    page.refresh()
    page.report_list_table.setCurrentCell(0, 0)

    rows = page.report_snapshots_table.rowCount()
    assert rows >= 10  # 5 metrics × 2 snapshots (beg + end), plus notes
    # First cell should mention "Beginning"
    assert "Beginning" in page.report_snapshots_table.item(0, 0).text()
```

- [ ] **Step 2: Run, expect FAIL** (the table doesn't exist yet)

- [ ] **Step 3: Add the tab**

In `__init__`, where the existing tabs are added, replace the single `report_snapshot_table` with a new `report_snapshots_table` that holds beg + end together. Add a tab between Allocation and Risk Summary:

```python
self.report_snapshots_table = make_table(["Metric", "Value"], stretch_last=True)
# existing self.report_snapshot_table stays for the legacy 'Current Snapshot' tab
```

Tab insertion (just before the existing "Current Snapshot" tab):

```python
self.report_detail_tabs.addTab(self.report_snapshots_table, "Snapshots")
```

In `_render_report_detail`, populate `report_snapshots_table` with both beginning and ending:

```python
beg = data.get("beginning_snapshot", {}) or {}
end = data.get("ending_snapshot", {}) or {}
snap_rows = [
    ("Beginning — Snapshot Date", beg.get("snapshot_date") or "N/A"),
    ("Beginning — Cash", _money_or_na(beg.get("cash"))),
    ("Beginning — Total Assets", _money_or_na(beg.get("total_assets"))),
    ("Beginning — Total Liabilities", _money_or_na(beg.get("total_liabilities"))),
    ("Beginning — Net Worth", _money_or_na(beg.get("net_worth"))),
    ("Beginning — Note", beg.get("note", "")),
    ("Ending — Snapshot Date", end.get("snapshot_date") or "N/A"),
    ("Ending — Cash", _money_or_na(end.get("cash"))),
    ("Ending — Total Assets", _money_or_na(end.get("total_assets"))),
    ("Ending — Total Liabilities", _money_or_na(end.get("total_liabilities"))),
    ("Ending — Net Worth", _money_or_na(end.get("net_worth"))),
    ("Ending — Note", end.get("note", "")),
]
self.report_snapshots_table.setRowCount(len(snap_rows))
for i, (metric, value) in enumerate(snap_rows):
    self.report_snapshots_table.setItem(i, 0, QTableWidgetItem(metric))
    self.report_snapshots_table.setItem(i, 1, QTableWidgetItem(value))
```

Add `report_snapshots_table` to the `_clear_report_detail` tuple.

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit**

```bash
git add src/gui/pages/import_export.py tests/test_main_window_reports.py
git commit -m "feat(gui): render beginning and ending snapshots in report detail"
```

---

## Task 9: GUI — Fees Breakdown tab

**Files:**
- Modify: `src/gui/pages/import_export.py`
- Test: `tests/test_main_window_reports.py`

- [ ] **Step 1: Failing test**

```python
def test_report_detail_renders_fees_breakdown_when_present(qapp, db_conn):
    from src.engines.ledger import deposit_cash, buy
    from src.storage.fee_breakdown_repo import create_fee_breakdown, FeeBreakdownRow
    from src.engines.reports import generate_monthly_report
    from src.gui.pages.import_export import ImportExportPage

    deposit_cash(db_conn, 10000.0, transaction_date="2026-03-01")
    txn = buy(db_conn, "AAPL", "stock", 10, 150.0, fees=2.5,
              transaction_date="2026-03-15")
    create_fee_breakdown(db_conn, FeeBreakdownRow(
        transaction_id=txn.id, fee_type="broker_commission", amount=2.5))
    generate_monthly_report(db_conn, 2026, 3)

    page = ImportExportPage(db_conn)
    page.refresh()
    page.report_list_table.setCurrentCell(0, 0)

    assert page.report_fees_table.rowCount() == 1
    assert page.report_fees_table.item(0, 0).text() == "broker_commission"
```

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Add the tab**

```python
self.report_fees_table = make_table(["Fee Type", "Count", "Total"], stretch_last=True)
self.report_detail_tabs.addTab(self.report_fees_table, "Fees Breakdown")
```

In `_render_report_detail`:

```python
fb = data.get("fees_breakdown", {}) or {}
fb_rows = fb.get("by_type") or []
self.report_fees_table.setRowCount(len(fb_rows))
for i, row in enumerate(fb_rows):
    self.report_fees_table.setItem(i, 0, QTableWidgetItem(row.get("fee_type", "")))
    self.report_fees_table.setItem(i, 1, QTableWidgetItem(str(row.get("count", 0))))
    self.report_fees_table.setItem(i, 2, QTableWidgetItem(_money_or_na(row.get("total"))))
```

Add `report_fees_table` to `_clear_report_detail`.

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit**

```bash
git add src/gui/pages/import_export.py tests/test_main_window_reports.py
git commit -m "feat(gui): add fees breakdown tab to report detail"
```

---

## Task 10: Staleness detection

**Files:**
- Modify: `src/storage/report_repo.py` (add helper), `src/gui/pages/import_export.py`
- Test: `tests/test_reports.py`, `tests/test_main_window_reports.py`

- [ ] **Step 1: Failing test (engine)**

```python
def test_count_transactions_in_period_matches_report(db_conn):
    from src.engines.ledger import deposit_cash
    from src.engines.reports import generate_monthly_report
    from src.storage.report_repo import count_transactions_in_period

    deposit_cash(db_conn, 100.0, transaction_date="2026-03-05")
    deposit_cash(db_conn, 200.0, transaction_date="2026-03-15")
    r = generate_monthly_report(db_conn, 2026, 3)
    assert count_transactions_in_period(db_conn, r.period_start, r.period_end) == 2

    deposit_cash(db_conn, 50.0, transaction_date="2026-03-20")
    assert count_transactions_in_period(db_conn, r.period_start, r.period_end) == 3
    # report row's transaction_count is still 2 → caller can detect drift
```

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Implement helper in `report_repo.py`**

```python
def count_transactions_in_period(
    conn: sqlite3.Connection, period_start: str, period_end: str
) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE date >= ? AND date < ?",
        (period_start, period_end),
    ).fetchone()
    return row[0]
```

Extend `ReportSummaryRow` with `period_start: str` and `period_end: str` fields, and have `list_report_summaries` SELECT them too. (Required so the GUI can call `count_transactions_in_period` per row without a second JSON parse.)

```python
@dataclass
class ReportSummaryRow:
    id: int
    report_type: str
    period_label: str
    period_start: str
    period_end: str
    generated_at: str
    title: str
    net_cash_flow: float
    operating_net_income: float
    transaction_count: int
    net_worth_change: float | None = None
    funding_flow: float = 0.0
    approximate_return_pct: float | None = None
```

```python
query = (
    "SELECT id, report_type, period_label, period_start, period_end, "
    "generated_at, title, net_cash_flow, operating_net_income, "
    "transaction_count, net_worth_change, funding_flow, "
    "approximate_return_pct "
    "FROM reports"
)
# add period_start, period_end to the constructor.
```

- [ ] **Step 4: GUI staleness rendering**

In `_refresh_report_list`, after populating the row, compare the live count and italicize the period cell if drifted. Add a tooltip explaining what changed.

```python
from PySide6.QtGui import QFont
italic = QFont()
italic.setItalic(True)

for i, s in enumerate(summaries):
    live = count_transactions_in_period(self.conn, s.period_start, s.period_end)
    period_item = QTableWidgetItem(s.period_label)
    if live != s.transaction_count:
        period_item.setFont(italic)
        period_item.setToolTip(
            f"Stale: report has {s.transaction_count} txns, "
            f"period now has {live}. Use 'Generate Selected Period' to refresh."
        )
    self.report_list_table.setItem(i, 0, period_item)
    # ... rest of the columns unchanged
```

GUI test:

```python
def test_report_list_marks_stale_after_backdated_txn(qapp, db_conn):
    from src.engines.ledger import deposit_cash
    from src.engines.reports import generate_monthly_report
    from src.gui.pages.import_export import ImportExportPage

    deposit_cash(db_conn, 100.0, transaction_date="2026-03-05")
    generate_monthly_report(db_conn, 2026, 3)

    page = ImportExportPage(db_conn)
    page.refresh()
    period_item = page.report_list_table.item(0, 0)
    assert not period_item.font().italic()  # fresh

    deposit_cash(db_conn, 50.0, transaction_date="2026-03-20")
    page.refresh()
    period_item = page.report_list_table.item(0, 0)
    assert period_item.font().italic()
    assert "Stale" in period_item.toolTip()
```

- [ ] **Step 5: Commit**

```bash
git add src/storage/report_repo.py src/gui/pages/import_export.py tests/test_reports.py tests/test_main_window_reports.py
git commit -m "feat(reports): mark stale reports when period transaction count drifts"
```

---

## Task 11: HOW-TO-READ button in GUI

**Files:**
- Modify: `src/gui/pages/import_export.py`
- Test: `tests/test_main_window_reports.py`

- [ ] **Step 1: Failing test**

```python
def test_how_to_read_button_exists_and_shows_help(qapp, db_conn, monkeypatch):
    from PySide6.QtWidgets import QMessageBox
    from src.gui.pages.import_export import ImportExportPage

    captured = {}
    def fake_information(parent, title, text):
        captured["title"] = title
        captured["text"] = text
        return QMessageBox.StandardButton.Ok
    monkeypatch.setattr(QMessageBox, "information", fake_information)

    page = ImportExportPage(db_conn)
    page._show_how_to_read()
    assert "HOW TO READ" in captured["text"]
```

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Add button + handler**

In the Reports tab button grid (`btn_grid`), add at row 3, col 0 (push other rows accordingly OR put it next to the type combo):

```python
btn_how_to_read = QPushButton("How to read this report")
btn_how_to_read.setStyleSheet(BTN_STYLE)
btn_how_to_read.clicked.connect(self._show_how_to_read)
type_row.addWidget(btn_how_to_read)  # next to the type combo
```

```python
def _show_how_to_read(self):
    from src.engines.report_export import HOW_TO_READ
    QMessageBox.information(self, "How to read this report", HOW_TO_READ)
```

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit**

```bash
git add src/gui/pages/import_export.py tests/test_main_window_reports.py
git commit -m "feat(gui): add 'How to read' help button to reports tab"
```

---

## Task 12: Fix falsy `if price` checks

**Files:**
- Modify: `src/gui/pages/import_export.py`
- Test: `tests/test_main_window_reports.py`

- [ ] **Step 1: Failing test**

```python
def test_report_detail_renders_zero_price_explicitly(qapp, db_conn):
    """A buy with price 0 (synthetic) should render as $0.00, not blank."""
    import json
    from src.gui.pages.import_export import ImportExportPage

    txn_data = {
        "summary": {
            "report_type": "monthly", "period_label": "2026-03",
            "period_start": "2026-03-01", "period_end": "2026-04-01",
            "generated_at": "2026-04-01T00:00:00", "transaction_count": 1,
            "beginning_cash": 0, "ending_cash": 0, "net_cash_flow": 0,
            "operating_net_income": 0, "total_inflow": 0, "total_outflow": 0,
            "total_fees": 0,
        },
        "operations": [], "transactions": [{
            "date": "2026-03-15", "txn_type": "buy", "asset_symbol": "X",
            "asset_name": "X", "quantity": 1, "price": 0.0,
            "total_amount": 0.0, "fees": 0.0, "notes": "",
        }],
        "trades": [{
            "date": "2026-03-15", "txn_type": "buy", "asset_symbol": "X",
            "asset_name": "X", "quantity": 1, "price": 0.0,
            "total_amount": 0.0, "fees": 0.0, "notes": "",
        }],
        "real_estate": [], "debt": [], "journal": [],
        "current_snapshot": {}, "beginning_snapshot": {}, "ending_snapshot": {},
        "cash_flow_breakdown": {}, "performance": {}, "allocation": {},
        "risk_summary": {}, "fees_breakdown": {"by_type": [], "grand_total": 0.0},
    }
    page = ImportExportPage(db_conn)
    page._render_report_detail(txn_data)
    assert page.report_txns_table.item(0, 4).text() == "$0.00"
    assert page.report_trades_table.item(0, 4).text() == "$0.00"
```

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Fix both falsy checks**

In `_render_report_detail`, replace `if price` with `if price is not None`:

```python
# transactions table
price = t.get("price")
self.report_txns_table.setItem(
    i, 4, QTableWidgetItem(f"${price:,.2f}" if price is not None else "")
)
# trades table
price = t.get("price")
self.report_trades_table.setItem(
    i, 4, QTableWidgetItem(f"${price:,.2f}" if price is not None else "")
)
```

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit**

```bash
git add src/gui/pages/import_export.py tests/test_main_window_reports.py
git commit -m "fix(gui): render zero price as \$0.00 instead of blank"
```

---

## Task 13: Update `PROJECT_UNDERSTANDING.md`

**Files:**
- Modify: `PROJECT_UNDERSTANDING.md`

- [ ] **Step 1: Add / update entries**

Per CLAUDE.md Rule 2. Concrete edits:

- §3 / Reports section in §1: replace "monthly and annual" with "monthly, quarterly, half-yearly, and annual" everywhere it appears. Note labels: `YYYY-MM`, `YYYY-Q#`, `YYYY-H#`, `YYYY`.
- §4 #6 (Reports workflow): mention generation includes 4 cadences; auto-generation runs all completed periods of every cadence.
- §5: append a bullet under "Reports section composition":
  > **Fees Breakdown.** `build_period_report` queries `transaction_fee_breakdown` grouped by `fee_type` and adds a `fees_breakdown` section to `report_json`. Rendered in the GUI ("Fees Breakdown" tab), TXT export ("FEES BREAKDOWN" section, omitted when empty), and XLSX export ("Fees Breakdown" sheet, always present even when empty).
  > **Beginning + Ending Snapshots are now rendered in all three surfaces.** Previously stored in `report_json` but invisible — fixed in this change.
  > **Stale-report indicator.** The Reports list italicizes the period column when `count_transactions_in_period(period)` differs from the report's stored `transaction_count`. Tooltip explains the drift. The user can use "Generate Selected Period" (now confirms before overwriting) to refresh.
  > **Add Selected Period overwrite confirm.** `_generate_selected_period` calls `report_exists` first and prompts "A report for {label} already exists. Replace it?" before regenerating.
- §6 / Drift hot spots: note that `get_report_stats` now returns 4 type counts; any caller assuming the previous 3-key dict will need updates (only the GUI stats label currently consumes it — already updated).
- §7 / TODOs: remove any stale entry about "no quarterly/half-yearly support" if present (none currently — just a confirmatory check).

- [ ] **Step 2: Commit**

```bash
git add PROJECT_UNDERSTANDING.md
git commit -m "docs: update PROJECT_UNDERSTANDING for new report cadences and improvements"
```

---

## Task 14: Final verification

**Files:**
- None (verification only)

- [ ] **Step 1: Run the full suite**

```
QT_QPA_PLATFORM=offscreen pytest -v
```

Expected: green. Diagnose any failure as a real regression — do not silence.

- [ ] **Step 2: Smoke-launch the GUI**

```
python -c "
import sys
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QTimer
app = QApplication(sys.argv)
from src.storage.database import init_db
from src.gui.main_window import MainWindow
conn = init_db(':memory:')
w = MainWindow(conn); w.show()
QTimer.singleShot(1500, app.quit)
sys.exit(app.exec())
"
```

Expected: window opens, no exceptions on stderr.

- [ ] **Step 3: Manually verify the Reports tab in the running app**

Generate a couple of fixture transactions across periods, click "Generate Missing Reports", and confirm:
- 4 entries in the type combo.
- Stats label shows all 4 counts.
- Selecting a report populates the new "Snapshots" and "Fees Breakdown" tabs.
- "How to read this report" button shows the help text.

(If running the GUI is impractical, document the manual-test checklist as covered by GUI tests.)

---

# Phase 2 — Deferred Polish (executed only after Task 14 passes)

Phase 2 tasks are independent. Run them in the order below for cleanest diffs (decompose first, then reuse the now-smaller surface for the picker rewrite). Stop and surface any failure that can't be cleanly fixed.

---

## Task 15: Decompose `_render_report_detail` (#8)

**Why now:** Phase 1 added two new tabs (Snapshots, Fees Breakdown) to a method already over 200 lines. Deferring the split until *after* the additions means the final per-section helpers are extracted from the post-feature shape, not the pre-feature shape — one decomposition instead of two.

**Files:**
- Modify: `src/gui/pages/import_export.py`
- Test: `tests/test_main_window_reports.py`

- [ ] **Step 1: Verify existing tests cover each rendered section**

A renaming-only refactor needs coverage on every section to catch typos. Confirm that `tests/test_main_window_reports.py` has at least one assertion against each of: summary, performance, cash flow breakdown, allocation, risk summary, snapshots, fees breakdown, operations, transactions, trades, real estate, debt, journal, current snapshot. Add a missing test before refactoring if any section is uncovered.

- [ ] **Step 2: Extract per-section helpers**

Split the body of `_render_report_detail` into private methods, one per section. Method signatures all take the parsed `data: dict` and write directly to the relevant `self.report_*_table`. The dispatcher becomes:

```python
def _render_report_detail(self, data: dict):
    self._render_summary_section(data)
    self._render_performance_section(data)
    self._render_cash_flow_breakdown_section(data)
    self._render_allocation_section(data)
    self._render_risk_summary_section(data)
    self._render_snapshots_section(data)
    self._render_fees_breakdown_section(data)
    self._render_operations_section(data)
    self._render_transactions_section(data)
    self._render_trades_section(data)
    self._render_real_estate_section(data)
    self._render_debt_section(data)
    self._render_journal_section(data)
    self._render_current_snapshot_section(data)
```

Each helper is a near-verbatim move of the existing block — no logic changes. Keep the local variable names (`s`, `perf`, `cfb`, etc.) inside each helper to minimize diff churn.

- [ ] **Step 3: Run the full GUI test suite**

```
QT_QPA_PLATFORM=offscreen pytest tests/test_main_window_reports.py tests/test_import_export_page.py -v
```

Expected: all green; the refactor is behavior-preserving.

- [ ] **Step 4: Commit**

```bash
git add src/gui/pages/import_export.py tests/test_main_window_reports.py
git commit -m "refactor(gui): split _render_report_detail into per-section helpers"
```

---

## Task 16: Consolidate formatter helpers in `utils/display.py` (#9)

**Files:**
- Modify: `src/utils/display.py`, `src/gui/pages/import_export.py`, `src/engines/report_export.py`
- Test: `tests/test_display.py`

- [ ] **Step 1: Failing tests for the consolidated helpers**

Append to `tests/test_display.py`:

```python
def test_money_or_na():
    from src.utils.display import money_or_na
    assert money_or_na(None) == "N/A"
    assert money_or_na(0) == "$0.00"
    assert money_or_na(1234.5) == "$1,234.50"
    assert money_or_na(-1234.5) == "$-1,234.50"


def test_percent_or_na_raw():
    """Already-percent values: 12.5 → '12.50%'."""
    from src.utils.display import percent_or_na
    assert percent_or_na(None) == "N/A"
    assert percent_or_na(12.5) == "12.50%"
    assert percent_or_na(0) == "0.00%"


def test_fraction_as_percent_or_na():
    """Fraction values from get_full_allocation: 0.10 → '10.00%'."""
    from src.utils.display import fraction_as_percent_or_na
    assert fraction_as_percent_or_na(None) == "N/A"
    assert fraction_as_percent_or_na(0.10) == "10.00%"
    assert fraction_as_percent_or_na(0) == "0.00%"
```

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Add helpers to `src/utils/display.py`**

Append to the existing `display.py` (whatever it currently exports — leave intact):

```python
def money_or_na(val) -> str:
    if val is None:
        return "N/A"
    return f"${val:,.2f}"


def percent_or_na(val) -> str:
    """For values that are already percentages (e.g. approximate_return_pct)."""
    if val is None:
        return "N/A"
    return f"{val:.2f}%"


def fraction_as_percent_or_na(val) -> str:
    """For values stored as fractions (0.10 = 10%) — used by allocation."""
    if val is None:
        return "N/A"
    return f"{val * 100:.2f}%"
```

- [ ] **Step 4: Replace duplicated helpers**

In `src/gui/pages/import_export.py`:
- Delete the local `_money_or_na`, `_pct_or_na`, `_alloc_pct_or_na` definitions at the top.
- Add `from src.utils.display import money_or_na, percent_or_na, fraction_as_percent_or_na` at the top.
- Replace every call: `_money_or_na(...)` → `money_or_na(...)`, `_pct_or_na(...)` → `percent_or_na(...)`, `_alloc_pct_or_na(...)` → `fraction_as_percent_or_na(...)`.

In `src/engines/report_export.py`:
- Delete the local `_fmt`, `_pct` helpers.
- Add `from src.utils.display import money_or_na, fraction_as_percent_or_na` at the top.
- Replace `_fmt(...)` → `money_or_na(...)` and `_pct(...)` → `fraction_as_percent_or_na(...)` throughout the file (the existing `_pct` helper consumes fractions per its own docstring — keep that semantic by mapping to `fraction_as_percent_or_na`).

- [ ] **Step 5: Run the full suite**

```
QT_QPA_PLATFORM=offscreen pytest -v
```

Expected: green. If any export-format test fails on whitespace alignment ("$  0.00" vs "$0.00"), update the test to match the canonical helper output — the helper is the new source of truth.

- [ ] **Step 6: Commit**

```bash
git add src/utils/display.py src/gui/pages/import_export.py src/engines/report_export.py tests/test_display.py
git commit -m "refactor(reports): consolidate money/percent formatters in utils.display"
```

---

## Task 17: Inclusive period-end display (#11)

**Why:** Period-end is stored as exclusive (Apr 2026 has period_end `2026-05-01`), so users currently see "2026-04-01 to 2026-05-01" and read the second date as inclusive — wrong by one day. Fix is display-only; the underlying ISO range stays exclusive.

**Files:**
- Modify: `src/utils/display.py`, `src/engines/report_export.py`, `src/gui/pages/import_export.py`
- Test: `tests/test_display.py`, `tests/test_report_export.py`

- [ ] **Step 1: Failing tests for the helper**

```python
# tests/test_display.py
def test_format_period_inclusive():
    from src.utils.display import format_period_inclusive
    # Monthly: "2026-03-01" / "2026-04-01" → "2026-03-01 to 2026-03-31"
    assert format_period_inclusive("2026-03-01", "2026-04-01") == \
        "2026-03-01 to 2026-03-31"
    # Quarterly Q2: "2026-04-01" / "2026-07-01" → ".. to 2026-06-30"
    assert format_period_inclusive("2026-04-01", "2026-07-01") == \
        "2026-04-01 to 2026-06-30"
    # Annual: "2026-01-01" / "2027-01-01" → ".. to 2026-12-31"
    assert format_period_inclusive("2026-01-01", "2027-01-01") == \
        "2026-01-01 to 2026-12-31"
```

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Implement helper**

Append to `src/utils/display.py`:

```python
from datetime import date, timedelta


def format_period_inclusive(period_start_iso: str, period_end_exclusive_iso: str) -> str:
    """Render a period as an inclusive range. Storage is exclusive-end
    (Apr = '2026-04-01' to '2026-05-01'); users read the second date as
    inclusive ('Apr 1 – May 1?'). This subtracts one day for display."""
    end_inclusive = date.fromisoformat(period_end_exclusive_iso) - timedelta(days=1)
    return f"{period_start_iso} to {end_inclusive.isoformat()}"
```

- [ ] **Step 4: Use the helper in three places**

1. `src/gui/pages/import_export.py:_render_summary_section`. Replace:
   ```python
   ("Period", f"{s.get('period_start', '')} to {s.get('period_end', '')}"),
   ```
   with:
   ```python
   ("Period", format_period_inclusive(s.get('period_start', ''), s.get('period_end', ''))),
   ```
   Add the import.

2. `src/engines/report_export.py:export_report_txt`. Replace:
   ```python
   lines.append(f"Period:              {s['period_start']} to {s['period_end']}")
   ```
   with:
   ```python
   lines.append(f"Period:              {format_period_inclusive(s['period_start'], s['period_end'])}")
   ```

3. `src/engines/report_export.py:export_report_xlsx`. Replace the existing two summary rows:
   ```python
   ("Period Start", s["period_start"]),
   ("Period End", s["period_end"]),
   ```
   with one row:
   ```python
   ("Period", format_period_inclusive(s["period_start"], s["period_end"])),
   ```

- [ ] **Step 5: Update affected export tests**

Existing tests asserting "Period Start" / "Period End" XLSX rows or the old txt format string need updating to the new shape. Search:

```
QT_QPA_PLATFORM=offscreen pytest tests/test_report_export.py -v
```

Update assertions inline to match the new inclusive-range format.

- [ ] **Step 6: Commit**

```bash
git add src/utils/display.py src/engines/report_export.py src/gui/pages/import_export.py tests/test_display.py tests/test_report_export.py
git commit -m "feat(reports): show inclusive period end-date in user-facing strings"
```

---

## Task 18: Typed period picker (#10)

**Why:** `QInputDialog.getText` accepts arbitrary strings ("Mar 26", "2026 Q1", "2026Q1") that produce confusing parse errors. A small typed dialog removes the footgun.

**Design.** A reusable `PeriodPickerDialog(parent, cadence)` that returns `(year: int, sub: int) | None`:
- `cadence="monthly"` → year `QSpinBox(2000–2099)` + month `QSpinBox(1–12)`
- `cadence="quarterly"` → year + `QComboBox(["Q1","Q2","Q3","Q4"])`
- `cadence="semi_annual"` → year + `QComboBox(["H1","H2"])`
- `cadence="annual"` → year only

Annual returns `(year, 0)` — the caller ignores `sub` for annual.

**Files:**
- Create: `src/gui/widgets/period_picker.py`
- Modify: `src/gui/pages/import_export.py`
- Test: `tests/test_period_picker.py` (new), `tests/test_main_window_reports.py`

- [ ] **Step 1: Failing test for the dialog**

```python
# tests/test_period_picker.py
import pytest
from PySide6.QtWidgets import QDialog
from src.gui.widgets.period_picker import PeriodPickerDialog


def test_picker_monthly_returns_year_and_month(qapp):
    dlg = PeriodPickerDialog(parent=None, cadence="monthly")
    dlg.year_spin.setValue(2026)
    dlg.month_spin.setValue(7)
    assert dlg.values() == (2026, 7)


def test_picker_quarterly_returns_year_and_quarter(qapp):
    dlg = PeriodPickerDialog(parent=None, cadence="quarterly")
    dlg.year_spin.setValue(2026)
    dlg.sub_combo.setCurrentIndex(2)  # Q3
    assert dlg.values() == (2026, 3)


def test_picker_semi_annual_returns_year_and_half(qapp):
    dlg = PeriodPickerDialog(parent=None, cadence="semi_annual")
    dlg.year_spin.setValue(2026)
    dlg.sub_combo.setCurrentIndex(1)  # H2
    assert dlg.values() == (2026, 2)


def test_picker_annual_returns_year_with_zero_sub(qapp):
    dlg = PeriodPickerDialog(parent=None, cadence="annual")
    dlg.year_spin.setValue(2025)
    assert dlg.values() == (2025, 0)


def test_picker_invalid_cadence(qapp):
    with pytest.raises(ValueError):
        PeriodPickerDialog(parent=None, cadence="weekly")
```

- [ ] **Step 2: Run, expect FAIL** (file doesn't exist)

- [ ] **Step 3: Implement the dialog**

```python
# src/gui/widgets/period_picker.py
from typing import Literal
from PySide6.QtWidgets import (
    QDialog, QFormLayout, QSpinBox, QComboBox, QDialogButtonBox, QLabel,
)


Cadence = Literal["monthly", "quarterly", "semi_annual", "annual"]


class PeriodPickerDialog(QDialog):
    """Typed period picker. Caller invokes `.exec()` and reads `.values()`."""

    def __init__(self, parent, cadence: Cadence):
        super().__init__(parent)
        if cadence not in ("monthly", "quarterly", "semi_annual", "annual"):
            raise ValueError(f"unknown cadence {cadence!r}")
        self.cadence = cadence
        titles = {
            "monthly": "Generate Monthly Report",
            "quarterly": "Generate Quarterly Report",
            "semi_annual": "Generate Semi-Annual Report",
            "annual": "Generate Annual Report",
        }
        self.setWindowTitle(titles[cadence])

        form = QFormLayout(self)
        self.year_spin = QSpinBox()
        self.year_spin.setRange(2000, 2099)
        self.year_spin.setValue(2026)
        form.addRow(QLabel("Year:"), self.year_spin)

        self.month_spin = None
        self.sub_combo = None
        if cadence == "monthly":
            self.month_spin = QSpinBox()
            self.month_spin.setRange(1, 12)
            self.month_spin.setValue(1)
            form.addRow(QLabel("Month:"), self.month_spin)
        elif cadence == "quarterly":
            self.sub_combo = QComboBox()
            self.sub_combo.addItems(["Q1", "Q2", "Q3", "Q4"])
            form.addRow(QLabel("Quarter:"), self.sub_combo)
        elif cadence == "semi_annual":
            self.sub_combo = QComboBox()
            self.sub_combo.addItems(["H1", "H2"])
            form.addRow(QLabel("Half:"), self.sub_combo)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        form.addRow(buttons)

    def values(self) -> tuple[int, int]:
        year = self.year_spin.value()
        if self.cadence == "monthly":
            return (year, self.month_spin.value())
        if self.cadence in ("quarterly", "semi_annual"):
            return (year, self.sub_combo.currentIndex() + 1)
        return (year, 0)  # annual
```

- [ ] **Step 4: Wire into `_generate_selected_period`**

Replace the body with:

```python
def _generate_selected_period(self):
    from src.gui.widgets.period_picker import PeriodPickerDialog
    rtype = self.report_type_combo.currentData()
    dlg = PeriodPickerDialog(self, cadence=rtype)
    if dlg.exec() != QDialog.DialogCode.Accepted:
        return
    year, sub = dlg.values()

    label_map = {
        "monthly": f"{year}-{sub:02d}",
        "quarterly": f"{year}-Q{sub}",
        "semi_annual": f"{year}-H{sub}",
        "annual": str(year),
    }
    label = label_map[rtype]

    if report_exists(self.conn, rtype, label) and not self._confirm_overwrite(label):
        return

    try:
        if rtype == "monthly":
            generate_monthly_report(self.conn, year, sub)
        elif rtype == "quarterly":
            generate_quarterly_report(self.conn, year, sub)
        elif rtype == "semi_annual":
            generate_semi_annual_report(self.conn, year, sub)
        else:
            generate_annual_report(self.conn, year)
    except Exception as e:
        QMessageBox.critical(self, "Report Error", str(e))
        return

    self._refresh_report_list()
    QMessageBox.information(self, "Reports", f"Generated {rtype} report for {label}.")
```

Add `from PySide6.QtWidgets import QDialog` to the top of the file if not already present (it should be — `QDialog` is already used).

- [ ] **Step 5: Run the suite**

```
QT_QPA_PLATFORM=offscreen pytest tests/test_period_picker.py tests/test_main_window_reports.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/gui/widgets/period_picker.py src/gui/pages/import_export.py tests/test_period_picker.py
git commit -m "feat(gui): replace free-text period prompt with typed PeriodPickerDialog"
```

---

## Task 19: Return-% magnitude guard (#12)

**Why:** With a tiny positive `beginning_net_worth` (e.g. \$5 starter cash), `approximate_return_pct` can balloon to 6 figures and mislead users on the very first report. Add a sentinel threshold below which we skip the % calculation and surface a clear data-quality note.

**Threshold pick:** `MIN_NW_FOR_RETURN_PCT = 100.0`. Conservative enough that a one-coffee starting balance does not produce a 100,000% return; permissive enough that any meaningful first-month deposit clears it.

**Files:**
- Modify: `src/engines/reports.py:build_period_report`, `src/engines/report_export.py` (HOW_TO_READ note)
- Test: `tests/test_reports.py`

- [ ] **Step 1: Failing tests**

```python
def test_approximate_return_pct_skipped_when_beginning_nw_below_threshold(db_conn):
    """Tiny beginning net worth (< $100) should produce return_pct=None
    even when end is large, with an explanatory data-quality note."""
    from src.engines.ledger import deposit_cash
    from src.engines.snapshots import write_daily_snapshot
    from src.engines.reports import generate_monthly_report
    import json

    deposit_cash(db_conn, 5.0, transaction_date="2025-12-15")  # tiny start
    write_daily_snapshot(db_conn, "2026-03-01")  # beg of period
    deposit_cash(db_conn, 10_000.0, transaction_date="2026-03-15")
    write_daily_snapshot(db_conn, "2026-04-01")  # end of period

    r = generate_monthly_report(db_conn, 2026, 3)
    data = json.loads(r.report_json)
    perf = data["performance"]
    assert perf["beginning_net_worth"] is not None
    assert perf["beginning_net_worth"] < 100.0
    assert perf["approximate_return_pct"] is None
    assert "below" in perf["data_quality_note"].lower() or \
           "small beginning" in perf["data_quality_note"].lower()


def test_approximate_return_pct_computed_when_beginning_nw_above_threshold(db_conn):
    from src.engines.ledger import deposit_cash
    from src.engines.snapshots import write_daily_snapshot
    from src.engines.reports import generate_monthly_report
    import json

    deposit_cash(db_conn, 1000.0, transaction_date="2025-12-15")
    write_daily_snapshot(db_conn, "2026-03-01")
    write_daily_snapshot(db_conn, "2026-04-01")

    r = generate_monthly_report(db_conn, 2026, 3)
    data = json.loads(r.report_json)
    # No new transactions in March, beginning_nw ≥ $100 → pct should be computed
    # (it'll be ~0% since no movement, but it should NOT be None)
    assert data["performance"]["approximate_return_pct"] is not None
```

- [ ] **Step 2: Run, expect FAIL**

- [ ] **Step 3: Implement guard**

In `src/engines/reports.py`, add a module-level constant near the top:

```python
MIN_NW_FOR_RETURN_PCT = 100.0
```

Then update the relevant branch in `build_period_report`:

```python
if beginning_snap_obj is not None and ending_snap_obj is not None:
    net_worth_change = ending_nw - beginning_nw
    approximate_investment_result = net_worth_change - funding_flow_net
    if beginning_nw is not None and beginning_nw >= MIN_NW_FOR_RETURN_PCT:
        approximate_return_pct = (approximate_investment_result / beginning_nw) * 100.0
        data_quality_note = (
            "Beginning and ending snapshots available. Performance figures "
            "are approximate (snapshot-based, no time-weighting; does not "
            "separate realized and unrealized P&L)."
        )
    else:
        approximate_return_pct = None
        data_quality_note = (
            f"Beginning net worth is below ${MIN_NW_FOR_RETURN_PCT:.0f} — "
            f"return % skipped to avoid a misleading magnitude. Net Worth "
            f"Change and Approximate Investment Result are still shown."
        )
else:
    # ... existing missing-snapshot branch unchanged
```

- [ ] **Step 4: Document in HOW_TO_READ**

In `src/engines/report_export.py`, append to the PERFORMANCE block:

```
- Approximate Return % is skipped (reported N/A) when Beginning Net Worth
  is below $100. A tiny starting balance combined with normal cash movement
  produces a meaninglessly large percentage; reporting it would mislead.
```

- [ ] **Step 5: Tests pass**

- [ ] **Step 6: Commit**

```bash
git add src/engines/reports.py src/engines/report_export.py tests/test_reports.py
git commit -m "feat(reports): skip approximate_return_pct when beginning_nw below \$100"
```

---

## Task 20: Phase 2 verification

**Files:**
- None (verification only)

- [ ] **Step 1: Full suite**

```
QT_QPA_PLATFORM=offscreen pytest -v
```

- [ ] **Step 2: Smoke launch**

(Same one-liner as Task 14 step 2.)

- [ ] **Step 3: Update PROJECT_UNDERSTANDING.md (Phase 2 entries)**

Append to the relevant §5 bullets:

> **Inclusive period-end display.** All user-facing surfaces (Reports tab Summary, TXT export, XLSX export) render the period as `start ISO to (end - 1 day) ISO`. Storage stays exclusive-end. Helper `utils.display.format_period_inclusive`.
>
> **Typed period picker.** `gui/widgets/period_picker.PeriodPickerDialog(parent, cadence)` replaces `QInputDialog.getText` for the "Generate Selected Period" flow. Returns `(year, sub)` where `sub` is month / quarter index / half index, or 0 for annual.
>
> **Return-% magnitude guard.** `engines/reports.MIN_NW_FOR_RETURN_PCT = 100.0`; below this threshold `approximate_return_pct` is `None` with a clear data-quality note, preventing six-figure return % artifacts on tiny starter portfolios.
>
> **Formatter consolidation.** `utils.display` exports `money_or_na`, `percent_or_na`, `fraction_as_percent_or_na`. The previous duplicates in `gui/pages/import_export.py` and `engines/report_export.py` were removed.

```bash
git add PROJECT_UNDERSTANDING.md
git commit -m "docs: update PROJECT_UNDERSTANDING for Phase 2 polish"
```

---

## Self-Review

**Spec coverage:**

Phase 1:
- Quarterly + Semi-Annual reports → Tasks 1, 2, 3 (engine + repo), 7 (GUI).
- #1 Beginning/Ending snapshots → Tasks 5 (TXT), 6 (XLSX), 8 (GUI).
- #2 Fees breakdown → Tasks 4 (engine), 5 (TXT), 6 (XLSX), 9 (GUI).
- #3 Staleness → Task 10.
- #4 Falsy price fix → Task 12.
- #5 Stale `if column in keys` cleanup → Task 3 step 3.
- #6 HOW-TO-READ in GUI → Task 11.
- #7 Overwrite confirm → Task 7 step 3 (`_confirm_overwrite`).
- Docs → Task 13.
- Verification → Task 14.

Phase 2:
- #8 Decompose `_render_report_detail` → Task 15.
- #9 Consolidate formatters → Task 16.
- #11 Inclusive period-end display → Task 17.
- #10 Typed period picker → Task 18.
- #12 Return-% magnitude guard → Task 19.
- Phase 2 verification + docs → Task 20.

**Placeholder scan:** none — every step lists files, code, and commands.

**Type consistency:**
- `generate_quarterly_report(conn, year, quarter)` and `generate_semi_annual_report(conn, year, half)` are referenced consistently in Task 2 (declaration), Task 7 (GUI dispatch).
- `quarter_bounds(year, quarter) → (start, end)` and `half_year_bounds(year, half) → (start, end)` referenced consistently in Task 1 (declaration), Task 2 (use).
- `count_transactions_in_period(conn, period_start, period_end)` referenced consistently in Task 10 (declaration + use).
- `ReportSummaryRow` extension in Task 10 — `period_start` and `period_end` are added to the dataclass AND to the SELECT in `list_report_summaries`. GUI in Task 10 reads them via `s.period_start` / `s.period_end`.
- `report_exists` is added to the import block in Task 7. ✓
- New widgets `report_snapshots_table`, `report_fees_table` registered in `_clear_report_detail` per their respective tasks (8 + 9). ✓
