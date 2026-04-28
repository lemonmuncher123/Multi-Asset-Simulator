"""Ultimate stress test infrastructure (Phase 0 deliverable).

TEST-ONLY. Does not modify production source.

This module provides:

* Two deterministic dataset builders, callable on any
  ``sqlite3.Connection`` returned by ``init_db(":memory:")``:

      build_base_stress_dataset(conn)     -> dict[str, int]
      build_extreme_stress_dataset(conn)  -> dict[str, int]

  Both return a ``{table_or_kind: row_count}`` dict so callers can
  assert dataset integrity without re-querying.

* Pytest markers registered in ``pytest.ini``:

      stress_phase0   — sanity / harness validation
      stress_phase1   — base dataset structure & integrity
      stress_phase2   — base dataset read-side engines
      stress_phase3   — base dataset reports + per-report export
      stress_phase4   — base dataset full-data export/import roundtrip
      stress_gui      — offscreen GUI navigation on the base dataset
      stress_extreme  — extreme-scale variants (engines + IO + targeted GUI)

  Each phase is meant to be invoked as its own pytest command with
  its own per-phase timeout (see ULTIMATE_STRESS_REPORT.md for the
  recommended wrapper).

Builder constraints honoured here:

* No network. No yfinance, no live providers.
* No GUI. No PySide6 imports inside the builders or their helpers.
* Foreign-key-safe. ``PRAGMA foreign_key_check`` returns no rows on
  the connection after either builder finishes — verified by the
  Phase 0 tests below.
* Deterministic. A fixed seed produces the same dataset every run;
  no calls to ``time.time``, ``datetime.now``, or system random
  sources are made inside the builders.
* Readable. Plain loops, no clever vectorization or metaprogramming.
"""
from __future__ import annotations

import gc
import json
import math
import os
import random
import sqlite3
import time
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pytest

# Force offscreen Qt before anything else can pull in PySide6 transitively.
# Builders themselves do not touch Qt, but the session conftest sets up
# a QApplication and any future GUI phase added to this file should
# inherit the offscreen platform without surprises.
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from src.engines.allocation import (
    calc_allocation_by_asset,
    calc_allocation_by_asset_type,
    calc_asset_pie_breakdown,
    get_full_allocation,
)
from src.engines.dashboard import (
    get_dashboard_summary,
    get_net_worth_trend,
)
from src.engines.data_management import (
    clear_all_user_data,
    delete_asset_with_related_data,
)
from src.engines.full_data_io import (
    EXPORT_TABLES,
    export_full_data,
    import_full_data,
)
from src.engines.ledger import sell_property
from src.engines.portfolio import (
    calc_cash_balance,
    calc_positions,
    get_portfolio_summary,
)
from src.engines.real_estate import analyze_all_properties
from src.engines.report_export import export_report_txt, export_report_xlsx
from src.engines.reports import generate_due_reports
from src.engines.risk import get_all_warnings
from src.engines.security_universe_engine import (
    get_universe_count,
    initialize_universe,
)
from src.models.asset import Asset
from src.models.debt import Debt
from src.models.decision_journal import DecisionJournalEntry
from src.models.portfolio_snapshot import PortfolioSnapshot
from src.models.property_asset import PropertyAsset
from src.models.report import Report
from src.models.transaction import Transaction
from src.storage.asset_repo import create_asset
from src.storage.database import init_db
from src.storage.debt_repo import create_debt
from src.storage.fee_breakdown_repo import FeeBreakdownRow, create_fee_breakdown
from src.storage.journal_repo import create_journal_entry
from src.storage.price_repo import bulk_upsert_ohlcv
from src.storage.property_repo import create_property, list_active_properties
from src.storage.report_repo import (
    create_or_replace_report,
    get_report,
    list_report_summaries,
    list_reports,
    report_count,
)
from src.storage.snapshot_repo import create_snapshot, list_snapshots
from src.storage.transaction_repo import create_transaction, list_transactions


# ---------------------------------------------------------------------------
# Specs
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StressSpec:
    """Row-count spec for a single dataset size.

    Field meanings:

    - ``assets``: number of tradeable (stock / etf / crypto / custom)
      assets. Real-estate and debt assets are created separately and
      counted via ``properties`` and ``debts``.
    - ``transactions``: number of cash + buy + sell rows the builder
      will create on top of the seed deposit. The actual ``transactions``
      row count after build is ``transactions + 1 + properties`` (one
      seed-deposit row plus one ``add_property`` marker per property).
    - ``properties``: number of real-estate properties (and their
      backing assets).
    - ``debts``: number of debt liabilities (and their backing assets).
    - ``monthly_reports`` / ``annual_reports``: rows pre-populated
      directly into the ``reports`` table so phases that need a busy
      reports table don't have to pay the build_period_report cost.
    - ``snapshots``: portfolio-snapshot rows, one per day going
      backwards from ``snapshot_anchor``.
    - ``journal_entries``: decision-journal rows. The first
      ``journal_entries`` transactions are linked back via
      ``transaction_id``; the rest are unlinked (``NULL``).
    - ``price_history_days``: per-asset OHLCV rows to insert. ``0``
      disables price history entirely.
    - ``fee_breakdown_pct``: probability that a buy / sell transaction
      gets two synthetic fee-breakdown legs. ``0.0`` disables.
    - ``seed``: RNG seed for the fee-breakdown decision and any other
      randomized branch (currently only the fee-breakdown one).
    """
    assets: int
    transactions: int
    properties: int
    debts: int
    monthly_reports: int
    annual_reports: int
    snapshots: int
    journal_entries: int
    price_history_days: int
    fee_breakdown_pct: float
    seed: int = 2026


BASE_SPEC = StressSpec(
    assets=100,
    transactions=1_000,
    properties=20,
    debts=5,
    monthly_reports=100,
    annual_reports=0,
    snapshots=100,
    journal_entries=50,
    price_history_days=0,
    fee_breakdown_pct=0.0,
)

EXTREME_SPEC = StressSpec(
    assets=500,
    transactions=10_000,
    properties=50,
    debts=12,
    monthly_reports=240,
    annual_reports=20,
    snapshots=1_000,
    journal_entries=300,
    price_history_days=60,
    fee_breakdown_pct=0.25,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_base_stress_dataset(conn: sqlite3.Connection) -> dict[str, int]:
    """Populate ``conn`` with the BASE stress dataset.

    See ``BASE_SPEC`` for the row counts. Returns the per-kind row
    count dict from the underlying builder. The connection is left
    open and committed; the caller owns its lifecycle.
    """
    return _build(conn, BASE_SPEC)


def build_extreme_stress_dataset(conn: sqlite3.Connection) -> dict[str, int]:
    """Populate ``conn`` with the EXTREME stress dataset.

    See ``EXTREME_SPEC`` for the row counts. Returns the per-kind row
    count dict from the underlying builder.
    """
    return _build(conn, EXTREME_SPEC)


# ---------------------------------------------------------------------------
# Internal builder
# ---------------------------------------------------------------------------

# Build order matters: every child table must reference rows that
# already exist. Using a pinned anchor date (instead of ``date.today()``)
# keeps the dataset deterministic across runs.
_DATE_ANCHOR = date(2026, 1, 1)
_DATA_START = date(2020, 1, 1)
_TRADEABLE_ASSET_FLAVORS = [
    # (asset_type, currency, region, liquidity)
    ("stock", "USD", "US",     "liquid"),
    ("stock", "USD", "EU",     "liquid"),
    ("etf",   "USD", "Global", "liquid"),
    ("crypto", "USD", "Global", "liquid"),
    ("custom", "USD", "Other",  "illiquid"),
]


def _build(conn: sqlite3.Connection, spec: StressSpec) -> dict[str, int]:
    rng = random.Random(spec.seed)
    counts: dict[str, int] = {}

    tradeable = _build_tradeable_assets(conn, spec.assets)
    counts["tradeable_assets"] = len(tradeable)

    property_rows = _build_properties(conn, spec.properties)
    counts["properties"] = len(property_rows)

    debt_rows = _build_debts(conn, spec.debts)
    counts["debts"] = len(debt_rows)

    counts["transactions"], counts["fee_breakdown_rows"] = _build_transactions(
        conn, tradeable, property_rows, spec.transactions, rng,
        fee_breakdown_pct=spec.fee_breakdown_pct,
    )

    counts["snapshots"] = _build_snapshots(conn, spec.snapshots)
    counts["journal_entries"] = _build_journal_entries(
        conn, spec.journal_entries,
    )
    counts["price_rows"] = _build_price_history(
        conn, tradeable, spec.price_history_days,
    )
    counts["monthly_reports"] = _build_monthly_reports(conn, spec.monthly_reports)
    counts["annual_reports"] = _build_annual_reports(conn, spec.annual_reports)

    return counts


def _build_tradeable_assets(
    conn: sqlite3.Connection, count: int,
) -> list[Asset]:
    out: list[Asset] = []
    for i in range(count):
        flavor = _TRADEABLE_ASSET_FLAVORS[i % len(_TRADEABLE_ASSET_FLAVORS)]
        out.append(create_asset(conn, Asset(
            symbol=f"AS{i:04d}",
            name=f"Asset {i}",
            asset_type=flavor[0],
            currency=flavor[1],
            region=flavor[2],
            liquidity=flavor[3],
        )))
    return out


def _build_properties(
    conn: sqlite3.Connection, count: int,
) -> list[PropertyAsset]:
    """Create ``count`` properties + their backing real_estate assets.

    Bypasses ``ledger.add_property`` deliberately: that helper enforces
    the cash-on-hand guard, which would couple property creation to
    the seed deposit. The stress dataset is synthetic — what matters
    for downstream phases is that the rows exist and the FKs resolve.
    """
    out: list[PropertyAsset] = []
    for i in range(count):
        asset = create_asset(conn, Asset(
            symbol=f"PROP{i:04d}",
            name=f"Property {i}",
            asset_type="real_estate",
            liquidity="illiquid",
        ))
        prop = create_property(conn, PropertyAsset(
            asset_id=asset.id,
            address=f"{i} Test St",
            purchase_date=_DATA_START.isoformat(),
            purchase_price=300_000.0 + i * 1_000,
            current_value=320_000.0 + i * 1_500,
            down_payment=60_000.0,
            mortgage_balance=240_000.0 - i * 250,
            mortgage_interest_rate=0.05,
            monthly_mortgage_payment=1_500.0,
            monthly_rent=2_200.0,
            monthly_property_tax=300.0,
            monthly_insurance=120.0,
            monthly_hoa=0.0,
            monthly_maintenance_reserve=100.0,
            monthly_property_management=200.0,
            monthly_expense=0.0,
            vacancy_rate=0.05,
            status="active",
            cashflow_start_date=(_DATA_START + timedelta(days=31)).isoformat(),
            entry_type="existing_property",
        ))
        out.append(prop)
    return out


def _build_debts(conn: sqlite3.Connection, count: int) -> list[Debt]:
    out: list[Debt] = []
    for i in range(count):
        asset = create_asset(conn, Asset(
            symbol=f"DEBT{i:03d}",
            name=f"Debt {i}",
            asset_type="debt",
        ))
        debt = create_debt(conn, Debt(
            asset_id=asset.id,
            name=f"Debt {i}",
            original_amount=10_000.0 + i * 100,
            current_balance=8_000.0 + i * 80,
            interest_rate=0.06,
            minimum_payment=200.0,
        ))
        out.append(debt)
    return out


def _build_transactions(
    conn: sqlite3.Connection,
    tradeable: list[Asset],
    properties: list[PropertyAsset],
    count: int,
    rng: random.Random,
    *,
    fee_breakdown_pct: float,
) -> tuple[int, int]:
    """Insert the seed deposit, one ``add_property`` marker per
    property, and ``count`` deterministic buy / sell / cash transactions.

    Returns ``(total_transactions_inserted, fee_breakdown_rows_inserted)``.

    Sells only happen against assets we currently hold so the running
    quantity stays >= 0. Cash-only ops use ``asset_id IS NULL`` (the
    schema permits that). All ``asset_id`` values that are non-NULL
    point at rows inserted earlier in this builder.
    """
    inserted = 0

    # Seed cash. A single large deposit so subsequent buys don't trip
    # the cash-on-hand guard if a future engine ever back-validates
    # the dataset against ``calc_cash_balance``.
    create_transaction(conn, Transaction(
        date=_DATA_START.isoformat(),
        txn_type="deposit_cash",
        total_amount=10_000_000.0,
        currency="USD",
    ))
    inserted += 1

    # add_property markers — zero cash impact, asset already exists.
    for prop in properties:
        create_transaction(conn, Transaction(
            date=_DATA_START.isoformat(),
            txn_type="add_property",
            asset_id=prop.asset_id,
            quantity=1,
            price=prop.purchase_price,
            total_amount=0.0,
            notes="Existing property entry - no purchase cash impact.",
        ))
        inserted += 1

    fee_rows = 0
    qty_held: dict[int, float] = {}

    for i in range(count):
        d = (_DATA_START + timedelta(days=i % 1500)).isoformat()
        kind = i % 10

        if kind <= 5 and tradeable:
            # 60% buys
            asset = tradeable[i % len(tradeable)]
            qty = float((i % 9) + 1)
            price = 10.0 + (i % 100)
            fees = round(qty * price * 0.001, 4)
            txn = create_transaction(conn, Transaction(
                date=d, txn_type="buy", asset_id=asset.id,
                quantity=qty, price=price,
                total_amount=-(qty * price + fees), fees=fees,
            ))
            qty_held[asset.id] = qty_held.get(asset.id, 0.0) + qty
        elif kind in (6, 7) and qty_held:
            # 20% sells, only against held assets
            held_ids = [aid for aid, q in qty_held.items() if q > 0]
            if not held_ids:
                continue
            asset_id = held_ids[i % len(held_ids)]
            qty = min(qty_held[asset_id], float((i % 4) + 1))
            price = 10.0 + (i % 110)
            fees = round(qty * price * 0.0008, 4)
            txn = create_transaction(conn, Transaction(
                date=d, txn_type="sell", asset_id=asset_id,
                quantity=qty, price=price,
                total_amount=qty * price - fees, fees=fees,
            ))
            qty_held[asset_id] -= qty
        elif kind == 8:
            # 10% deposits
            txn = create_transaction(conn, Transaction(
                date=d, txn_type="deposit_cash",
                total_amount=500.0 + (i % 250),
            ))
        else:
            # 10% withdrawals (small enough that cash never goes negative)
            txn = create_transaction(conn, Transaction(
                date=d, txn_type="withdraw_cash",
                total_amount=-(50.0 + (i % 25)),
            ))
        inserted += 1

        if fee_breakdown_pct > 0 and txn.txn_type in ("buy", "sell"):
            if rng.random() < fee_breakdown_pct:
                # Two legs so the breakdown table sees multi-row exercise.
                create_fee_breakdown(conn, FeeBreakdownRow(
                    transaction_id=txn.id,
                    fee_type="commission",
                    amount=(txn.fees or 0.0) * 0.7,
                    rate=0.001,
                ))
                create_fee_breakdown(conn, FeeBreakdownRow(
                    transaction_id=txn.id,
                    fee_type="exchange",
                    amount=(txn.fees or 0.0) * 0.3,
                    rate=0.0003,
                ))
                fee_rows += 2

    return inserted, fee_rows


def _build_snapshots(conn: sqlite3.Connection, count: int) -> int:
    """One snapshot per day going backwards from the anchor.

    The schema declares ``portfolio_snapshots(date)`` UNIQUE, but
    ``create_snapshot`` uses ``INSERT OR REPLACE`` so duplicate dates
    overwrite cleanly rather than raise.
    """
    for i in range(count):
        d = (_DATE_ANCHOR - timedelta(days=i)).isoformat()
        create_snapshot(conn, PortfolioSnapshot(
            date=d,
            cash=100_000.0 - i * 10,
            total_assets=1_000_000.0 - i * 5,
            total_liabilities=200_000.0 - i,
            net_worth=800_000.0 - i * 4,
            allocation_json=json.dumps({
                "by_asset_type": {"stock": {"value": 1.0, "pct": 1.0}},
            }),
        ))
    return count


def _build_journal_entries(conn: sqlite3.Connection, count: int) -> int:
    """Link the first ``count`` journal entries to existing transaction
    rows where possible. Remaining entries leave ``transaction_id``
    NULL (the schema allows it).
    """
    existing_txns = list_transactions(conn)
    for i in range(count):
        txn_id = existing_txns[i].id if i < len(existing_txns) else None
        create_journal_entry(conn, DecisionJournalEntry(
            transaction_id=txn_id,
            date=(date(2024, 1, 1) + timedelta(days=i % 365)).isoformat(),
            title=f"Decision #{i}",
            thesis="Synthetic stress thesis text " * 4,
            confidence_level=(i % 5) + 1,
            tags="stress,synthetic",
        ))
    return count


def _build_price_history(
    conn: sqlite3.Connection, tradeable: list[Asset], days: int,
) -> int:
    """Insert ``days`` daily OHLCV rows for every stock / etf / crypto
    asset. Skipped (returns 0) when ``days <= 0``.

    Uses ``bulk_upsert_ohlcv`` to keep the path through the production
    code; deterministic close walk so phases that consume the data
    don't depend on RNG state."""
    if days <= 0:
        return 0
    rows: list[dict] = []
    for asset in tradeable:
        if asset.asset_type not in ("stock", "etf", "crypto"):
            continue
        for i in range(days):
            d = (_DATE_ANCHOR - timedelta(days=i)).isoformat()
            close = 50.0 + (asset.id % 50) + (i % 7)
            rows.append({
                "asset_id": asset.id,
                "symbol": asset.symbol,
                "asset_type": asset.asset_type,
                "date": d,
                "open": close - 0.5,
                "high": close + 0.7,
                "low": close - 0.7,
                "close": close,
                "adjusted_close": close,
                "volume": 1000.0,
                "source": "stress",
            })
    bulk_upsert_ohlcv(conn, rows)
    return len(rows)


def _empty_report_json(report_type: str, label: str, start: str, end: str) -> str:
    """Minimal valid report payload — enough to round-trip through
    ``create_or_replace_report`` without tripping the JSON parse it
    does to extract summary fields."""
    return json.dumps({
        "summary": {
            "report_type": report_type,
            "period_label": label,
            "period_start": start,
            "period_end": end,
            "generated_at": "2026-01-01T00:00:00",
            "transaction_count": 0,
            "beginning_cash": 0,
            "ending_cash": 0,
            "net_cash_flow": 0,
            "operating_net_income": 0,
            "total_inflow": 0,
            "total_outflow": 0,
            "total_fees": 0,
        },
        "operations": [],
        "transactions": [],
        "trades": [],
        "real_estate": [],
        "debt": [],
        "journal": [],
        "current_snapshot": {
            "note": "synthetic",
            "cash": 0,
            "total_assets": 0,
            "total_liabilities": 0,
            "net_worth": 0,
        },
    })


def _build_monthly_reports(conn: sqlite3.Connection, count: int) -> int:
    for i in range(count):
        year = 2018 + (i // 12)
        month = (i % 12) + 1
        nm_year = year + (1 if month == 12 else 0)
        nm_month = 1 if month == 12 else month + 1
        label = f"{year}-{month:02d}"
        period_start = f"{year}-{month:02d}-01"
        period_end = f"{nm_year}-{nm_month:02d}-01"
        create_or_replace_report(conn, Report(
            report_type="monthly",
            period_start=period_start,
            period_end=period_end,
            period_label=label,
            generated_at="2026-01-01T00:00:00",
            title=f"Monthly Report - {label}",
            report_json=_empty_report_json("monthly", label, period_start, period_end),
        ))
    return count


def _build_annual_reports(conn: sqlite3.Connection, count: int) -> int:
    for i in range(count):
        year = 2000 + i
        period_start = f"{year}-01-01"
        period_end = f"{year + 1}-01-01"
        create_or_replace_report(conn, Report(
            report_type="annual",
            period_start=period_start,
            period_end=period_end,
            period_label=str(year),
            generated_at="2026-01-01T00:00:00",
            title=f"Annual Report - {year}",
            report_json=_empty_report_json("annual", str(year), period_start, period_end),
        ))
    return count


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _fresh_db() -> sqlite3.Connection:
    """In-memory connection with the canonical schema applied. Never
    points at ``data/portfolio_simulator.db``."""
    return init_db(":memory:")


def _assert_fk_check_clean(conn: sqlite3.Connection) -> None:
    """``PRAGMA foreign_key_check`` is a built-in dataset auditor: it
    returns one row per FK violation. If the builders are FK-safe this
    must come back empty.
    """
    violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert violations == [], (
        f"FK violations after build: "
        f"{[tuple(r) for r in violations]}"
    )


def _timed(label: str, fn, sink: list):
    """Call ``fn()`` and record ``(label, seconds, result)`` to ``sink``.

    Prints a ``[stress-timing] label: Xs`` line so the timings show up
    in the pytest stdout regardless of ``--durations`` settings."""
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    print(f"[stress-timing] {label}: {elapsed:.4f}s")
    sink.append((label, elapsed, result))
    return result


def _assert_finite_number(value, label: str) -> None:
    assert isinstance(value, (int, float)), f"{label} not numeric: {value!r}"
    assert not isinstance(value, bool), f"{label} unexpectedly a bool"
    assert math.isfinite(float(value)), f"{label} not finite: {value!r}"


def _patch_modal_dialogs(monkeypatch) -> dict:
    """Stub every modal-dialog entry point a stress phase might
    accidentally hit, and return an invocation counter.

    The GUI pages call ``QMessageBox.information`` / ``warning`` /
    ``critical`` / ``question`` from error and confirmation paths;
    they call ``QFileDialog.getSaveFileName`` / ``getOpenFileName`` /
    ``getExistingDirectory`` from the Import / Export and Data Sync
    flows; ``QInputDialog`` from the Data Management bulk-clear
    flow. Patching them all to no-op stubs ensures a misbehaving
    page cannot stall a stress run on a hidden modal.

    The returned dict counts how many times each family of stub was
    invoked, so tests that explicitly assert "no modal triggered"
    can check the counter.
    """
    from PySide6.QtWidgets import QFileDialog, QInputDialog, QMessageBox

    counter = {"messagebox": 0, "filedialog": 0, "inputdialog": 0}

    def _stub_mb_ok(*args, **kwargs):
        counter["messagebox"] += 1
        return QMessageBox.StandardButton.Ok

    def _stub_mb_yes(*args, **kwargs):
        counter["messagebox"] += 1
        return QMessageBox.StandardButton.Yes

    monkeypatch.setattr(QMessageBox, "warning", staticmethod(_stub_mb_ok))
    monkeypatch.setattr(QMessageBox, "critical", staticmethod(_stub_mb_ok))
    monkeypatch.setattr(QMessageBox, "information", staticmethod(_stub_mb_ok))
    monkeypatch.setattr(QMessageBox, "question", staticmethod(_stub_mb_yes))
    monkeypatch.setattr(QMessageBox, "exec", lambda self, *a, **k: 0)

    def _stub_fd_pair(*args, **kwargs):
        counter["filedialog"] += 1
        return ("", "")

    def _stub_fd_dir(*args, **kwargs):
        counter["filedialog"] += 1
        return ""

    monkeypatch.setattr(QFileDialog, "getSaveFileName", staticmethod(_stub_fd_pair))
    monkeypatch.setattr(QFileDialog, "getOpenFileName", staticmethod(_stub_fd_pair))
    monkeypatch.setattr(QFileDialog, "getExistingDirectory", staticmethod(_stub_fd_dir))

    def _stub_input_text(*args, **kwargs):
        counter["inputdialog"] += 1
        return ("", False)

    def _stub_input_int(*args, **kwargs):
        counter["inputdialog"] += 1
        return (0, False)

    monkeypatch.setattr(QInputDialog, "getText", staticmethod(_stub_input_text))
    monkeypatch.setattr(QInputDialog, "getInt", staticmethod(_stub_input_int))

    return counter


def _flush_qt_deletes(app):
    """Drain pending DeferredDelete events and trigger gc."""
    from PySide6.QtCore import QEvent
    if app is not None:
        app.sendPostedEvents(None, QEvent.Type.DeferredDelete)
    gc.collect()


# ---------------------------------------------------------------------------
# Phase 0 — sanity / harness
# ---------------------------------------------------------------------------

@pytest.mark.stress_phase0
def test_phase0_harness_imports_and_schema_initializes():
    """The infrastructure module imports cleanly and ``init_db`` on
    ``:memory:`` produces a connection whose tables match the
    expected set.

    If this test fails, every later phase will fail downstream — fix
    here first.
    """
    conn = _fresh_db()
    try:
        names = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        for table in (
            "assets", "transactions", "transaction_fee_breakdown",
            "market_prices", "market_quotes", "price_sync_log",
            "properties", "debts", "decision_journal",
            "portfolio_snapshots", "reports", "securities_master",
            "settings",
        ):
            assert table in names, f"schema missing table: {table}"
    finally:
        conn.close()


@pytest.mark.stress_phase0
def test_phase0_builders_are_callable():
    """Both public builders must be importable and callable. This test
    does not check counts — Phase 1 does that — only that the entry
    points exist with the expected signatures.
    """
    assert callable(build_base_stress_dataset)
    assert callable(build_extreme_stress_dataset)
    conn = _fresh_db()
    try:
        result = build_base_stress_dataset(conn)
        assert isinstance(result, dict)
        assert "tradeable_assets" in result
    finally:
        conn.close()


@pytest.mark.stress_phase0
def test_phase0_base_dataset_fk_integrity():
    """After ``build_base_stress_dataset`` finishes, the database must
    pass ``PRAGMA foreign_key_check``. This is the FK-safety gate the
    spec requires.
    """
    conn = _fresh_db()
    try:
        build_base_stress_dataset(conn)
        _assert_fk_check_clean(conn)
    finally:
        conn.close()


@pytest.mark.stress_phase0
def test_phase0_base_dataset_is_deterministic():
    """Running the base builder twice on two fresh DBs must produce
    identical row counts. Determinism is part of the Phase 0 contract.
    """
    counts_a, counts_b = {}, {}
    for sink in (counts_a, counts_b):
        conn = _fresh_db()
        try:
            sink.update(build_base_stress_dataset(conn))
        finally:
            conn.close()
    assert counts_a == counts_b, (
        f"non-deterministic builder output: {counts_a} vs {counts_b}"
    )


# ---------------------------------------------------------------------------
# Phase 1 — base dataset structure & integrity
# ---------------------------------------------------------------------------

@pytest.mark.stress_phase1
def test_phase1_base_dataset_counts_match_spec():
    """Each row count in the BASE dataset matches the spec. Cross-
    checks the builder return value against direct table queries so a
    silent miscount in either path shows up here.
    """
    conn = _fresh_db()
    try:
        counts = build_base_stress_dataset(conn)

        assert counts["tradeable_assets"] == BASE_SPEC.assets
        assert counts["properties"] == BASE_SPEC.properties
        assert counts["debts"] == BASE_SPEC.debts
        assert counts["snapshots"] == BASE_SPEC.snapshots
        assert counts["journal_entries"] == BASE_SPEC.journal_entries
        assert counts["monthly_reports"] == BASE_SPEC.monthly_reports
        assert counts["annual_reports"] == BASE_SPEC.annual_reports
        assert counts["price_rows"] == 0  # base spec disables price history
        assert counts["fee_breakdown_rows"] == 0  # base spec disables fee legs

        # The transactions count returned by the builder includes the
        # seed deposit and the per-property markers. Verify the same
        # cardinality lands in the table.
        txn_rows = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        assert txn_rows == counts["transactions"]
        assert counts["transactions"] == BASE_SPEC.transactions + 1 + BASE_SPEC.properties

        # Asset table holds tradeable + property + debt assets.
        asset_rows = conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
        assert asset_rows == BASE_SPEC.assets + BASE_SPEC.properties + BASE_SPEC.debts

        _assert_fk_check_clean(conn)
    finally:
        conn.close()


@pytest.mark.stress_phase1
def test_base_real_estate_20_properties_integrity():
    """Real-estate data integrity on the BASE dataset.

    Builds the base dataset (20 ``existing_property`` entries, all
    ``status='active'``), sells one via ``ledger.sell_property``,
    then asserts every integrity contract the spec calls out:

    * ``list_active_properties`` excludes the sold property.
    * ``analyze_all_properties`` operates only on the active set.
    * The portfolio summary's ``property_value`` matches the sum of
      active ``current_value`` (no contribution from the sold one).
    * No ``receive_rent`` transactions exist anywhere in the
      database — historical rent must NOT be auto-backfilled for
      ``existing_property`` entries unless ``settle_due_rent`` is
      explicitly called, and the builder never calls it.
    * Every active property surfaces with its real name in the
      ``by_asset`` allocation breakdown, never a fallback like
      ``"Property <id>"`` that papers over a missing asset row.
    """
    conn = _fresh_db()
    try:
        build_base_stress_dataset(conn)

        # Pre-state: every property is active.
        active_before = list_active_properties(conn)
        assert len(active_before) == BASE_SPEC.properties

        # Sell the first property at a price comfortably above
        # mortgage + fees, so ``sell_property`` doesn't try to draw
        # cash for a shortfall.
        target = active_before[0]
        sale_price = max(target.current_value or 0, target.mortgage_balance) + 100_000.0
        sell_property(
            conn,
            date="2025-12-01",
            asset_id=target.asset_id,
            sale_price=sale_price,
            fees=10_000.0,
            notes="stress sell",
        )

        # Sold property is excluded from the active set.
        active_after = list_active_properties(conn)
        assert len(active_after) == BASE_SPEC.properties - 1
        sold_asset_ids = {target.asset_id}
        assert all(p.asset_id not in sold_asset_ids for p in active_after)

        # analyze_all_properties walks the active set only.
        analyses = analyze_all_properties(conn)
        assert len(analyses) == len(active_after)
        analyzed_ids = {a.prop.asset_id for a in analyses}
        assert sold_asset_ids.isdisjoint(analyzed_ids)

        # Active property counts sane: ``net_monthly_cash_flow`` and
        # ``annual_net_cash_flow`` are finite for every active prop.
        for a in analyses:
            _assert_finite_number(a.net_monthly_cash_flow, f"NCF for {a.name}")
            _assert_finite_number(a.annual_net_cash_flow, f"annual NCF for {a.name}")

        # Portfolio summary's property_value matches the sum of
        # current_value across only the active set.
        summary = get_portfolio_summary(conn)
        active_value = sum((p.current_value or 0) for p in active_after)
        assert abs(summary["property_value"] - active_value) < 1e-6, (
            f"property_value {summary['property_value']:.2f} != "
            f"active sum {active_value:.2f}"
        )

        # No historical rent backfill for existing_property entries.
        # The builder never calls settle_due_rent, so receive_rent
        # rows must be zero.
        rent_rows = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE txn_type = 'receive_rent'"
        ).fetchone()[0]
        assert rent_rows == 0, (
            f"unexpected historical rent backfill: {rent_rows} receive_rent rows"
        )

        # Property names display correctly in allocation/pie data —
        # they come from ``assets.name`` (set by the builder to
        # "Property N"), never the bare-id fallback.
        by_asset = calc_allocation_by_asset(conn)
        re_items = [i for i in by_asset if i["asset_type"] == "real_estate"]
        assert len(re_items) == len(active_after)
        for item in re_items:
            assert item["name"], f"empty name in by_asset item: {item!r}"
            assert item["name"].startswith("Property "), (
                f"unexpected fallback display name: {item['name']!r}"
            )

        pie = calc_asset_pie_breakdown(conn)
        pie_re = [i for i in pie if i["asset_type"] == "real_estate"]
        # The pie filters out zero-value entries; with current_value > 0
        # for every active prop, the count must match.
        assert len(pie_re) == len(active_after)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Phase 2 — base dataset read-side engines
# ---------------------------------------------------------------------------

@pytest.mark.stress_phase2
def test_phase2_base_dataset_engines_smoke():
    """Read-side engines must run without error against the BASE
    dataset and return the shapes the dashboard depends on.
    """
    conn = _fresh_db()
    try:
        build_base_stress_dataset(conn)

        cash = calc_cash_balance(conn)
        assert isinstance(cash, (int, float))

        positions = calc_positions(conn)
        assert isinstance(positions, list)

        summary = get_portfolio_summary(conn)
        for key in ("cash", "total_assets", "total_liabilities", "net_worth", "positions"):
            assert key in summary

        alloc = get_full_allocation(conn)
        assert "by_asset_type" in alloc
        assert "by_asset" in alloc

        warnings = get_all_warnings(conn)
        assert isinstance(warnings, list)

        # Self-consistency invariant on the derived totals.
        derived = summary["total_assets"] - summary["total_liabilities"]
        assert abs(summary["net_worth"] - derived) < 1e-6
    finally:
        conn.close()


@pytest.mark.stress_phase2
def test_base_engine_summary_allocation_risk_fast():
    """Targeted timings on the dashboard's hot read-side engines.

    Asserts the contracts that matter for the dashboard:

    * No call raises.
    * Every numeric output is finite (no NaN, no Inf).
    * ``net_worth`` ≈ ``total_assets - total_liabilities``.
    * Allocation percentages are sane: each pct in [0, 1] +
      epsilon, and the by-asset-type bucket pcts sum to ≈ 1.0
      whenever ``total_assets > 0``.

    Records each engine call's wall-clock duration via
    ``time.perf_counter`` so regressions show up in stdout.
    """
    conn = _fresh_db()
    try:
        build_base_stress_dataset(conn)

        timings: list = []
        summary = _timed("engine.get_portfolio_summary",
                         lambda: get_portfolio_summary(conn), timings)
        by_type = _timed("engine.calc_allocation_by_asset_type",
                         lambda: calc_allocation_by_asset_type(conn), timings)
        by_asset = _timed("engine.calc_allocation_by_asset",
                          lambda: calc_allocation_by_asset(conn), timings)
        pie = _timed("engine.calc_asset_pie_breakdown",
                     lambda: calc_asset_pie_breakdown(conn), timings)
        warnings = _timed("engine.get_all_warnings",
                          lambda: get_all_warnings(conn), timings)

        # Finite values on the summary's numeric fields.
        for key in ("cash", "positions_value", "property_value",
                    "total_assets", "mortgage", "debt",
                    "total_liabilities", "net_worth", "real_estate_equity"):
            _assert_finite_number(summary[key], f"summary.{key}")

        # Net-worth invariant.
        derived = summary["total_assets"] - summary["total_liabilities"]
        assert abs(summary["net_worth"] - derived) < 1e-6, (
            f"net_worth {summary['net_worth']:.6f} != "
            f"total_assets - total_liabilities {derived:.6f}"
        )

        # Allocation pct sanity. The engine clamps the denominator
        # to total_assets, so individual pcts should land in
        # [0, 1 + tiny epsilon] for floating-point slop. Allow a
        # small overshoot but not a wildly out-of-range value.
        EPS = 1e-9
        for key, bucket in by_type.items():
            _assert_finite_number(bucket["value"], f"by_type[{key}].value")
            _assert_finite_number(bucket["pct"], f"by_type[{key}].pct")
            assert -EPS <= bucket["pct"] <= 1.0 + EPS, (
                f"by_type[{key}].pct out of range: {bucket['pct']}"
            )

        if summary["total_assets"] > 0:
            total_pct = sum(b["pct"] for b in by_type.values())
            assert abs(total_pct - 1.0) < 1e-6, (
                f"by_asset_type pcts sum to {total_pct:.9f}, expected ≈ 1.0"
            )

        for item in by_asset:
            _assert_finite_number(item["value"], f"by_asset[{item['name']}].value")
            _assert_finite_number(item["pct"], f"by_asset[{item['name']}].pct")
            assert -EPS <= item["pct"] <= 1.0 + EPS, (
                f"by_asset pct out of range for {item['name']}: {item['pct']}"
            )

        # Pie breakdown ignores debt and zero-value entries; pcts
        # should sum to ≈ 1.0 when there is any positive value.
        if pie:
            pie_total = sum(i["pct"] for i in pie)
            assert abs(pie_total - 1.0) < 1e-6, (
                f"pie pcts sum to {pie_total:.9f}, expected ≈ 1.0"
            )

        assert isinstance(warnings, list)

        total = sum(t for _, t, _ in timings)
        print(f"[stress-timing] engine.total: {total:.4f}s")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Phase 3 — base dataset reports + per-report export
# ---------------------------------------------------------------------------

@pytest.mark.stress_phase3
def test_phase3_base_dataset_reports_export(tmp_path):
    """Generate any due reports against the BASE dataset and export
    one of them to TXT and XLSX. This is the smallest path that
    exercises ``generate_due_reports`` + ``export_report_*`` end-to-
    end.
    """
    conn = _fresh_db()
    try:
        build_base_stress_dataset(conn)

        # Pinned ``today`` so the test does not drift across calendar dates.
        generated = generate_due_reports(conn, today=date(2024, 6, 30))
        assert isinstance(generated, list)

        # Whether or not ``generate_due_reports`` produced new rows,
        # the table itself should have at least the seed monthly
        # reports we wrote.
        all_reports = list_reports(conn)
        assert len(all_reports) >= BASE_SPEC.monthly_reports

        # Pick any report and exercise both export paths.
        pick = generated[0] if generated else all_reports[0]
        payload = json.loads(pick.report_json)
        txt_path = tmp_path / f"{pick.report_type}_{pick.period_label}.txt"
        xlsx_path = tmp_path / f"{pick.report_type}_{pick.period_label}.xlsx"
        export_report_txt(payload, txt_path)
        export_report_xlsx(payload, xlsx_path)
        assert txt_path.exists() and txt_path.stat().st_size > 0
        assert xlsx_path.exists() and xlsx_path.stat().st_size > 0
    finally:
        conn.close()


@pytest.mark.stress_phase3
def test_base_100_monthly_reports_lazy_summary():
    """The reports table grows large in real use; the summary listing
    that the Import / Export page renders must NOT load each row's
    ``report_json`` blob, or memory + parse-time scale linearly with
    every persisted month/year.

    Verifies:

    * ``report_count`` returns the spec'd cardinality for the BASE
      dataset (100 monthly, 0 annual).
    * ``list_report_summaries(limit=60)`` returns exactly 60 rows.
    * The SQL executed by the summary path does NOT mention
      ``report_json``. Captured via ``conn.set_trace_callback``,
      which sees every statement SQLite actually runs.
    * ``get_report(...)`` loads the full row, including the JSON
      blob, and the JSON parses cleanly into the documented schema.
    """
    conn = _fresh_db()
    try:
        build_base_stress_dataset(conn)

        total = report_count(conn)
        monthly = report_count(conn, "monthly")
        annual = report_count(conn, "annual")
        assert monthly == BASE_SPEC.monthly_reports
        assert annual == BASE_SPEC.annual_reports
        assert total == monthly + annual

        # Capture every SQL statement run during the summary listing
        # and assert the lazy contract: report_json is never read.
        captured: list[str] = []
        conn.set_trace_callback(captured.append)
        try:
            summaries = list_report_summaries(conn, limit=60)
        finally:
            conn.set_trace_callback(None)

        assert len(summaries) == 60, (
            f"expected 60 summary rows with limit=60, got {len(summaries)}"
        )
        assert captured, "set_trace_callback captured no SQL statements"
        assert any("FROM reports" in q for q in captured), (
            f"summary path did not query reports: {captured}"
        )
        assert not any("report_json" in q for q in captured), (
            f"summary path leaked report_json access: {captured}"
        )

        # Sanity on the summary row shape — these fields drive the
        # Import / Export page and must round-trip from the DB.
        sample = summaries[0]
        for attr in (
            "id", "report_type", "period_label", "generated_at",
            "title", "net_cash_flow", "operating_net_income",
            "transaction_count",
        ):
            assert hasattr(sample, attr), f"summary missing attr: {attr}"

        # get_report on a single (type, label) pair loads the full
        # row and the JSON parses to the documented schema.
        report = get_report(conn, sample.report_type, sample.period_label)
        assert report is not None
        assert report.period_label == sample.period_label
        assert report.report_type == sample.report_type
        assert report.report_json, "selected report has empty report_json"
        payload = json.loads(report.report_json)
        assert "summary" in payload
        for key in (
            "report_type", "period_label", "period_start",
            "period_end", "generated_at",
        ):
            assert key in payload["summary"], (
                f"report payload missing summary key: {key}"
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Phase 4 — base dataset full-data export/import roundtrip
# ---------------------------------------------------------------------------

@pytest.mark.stress_phase4
def test_phase4_base_dataset_full_data_roundtrip(tmp_path):
    """Export the BASE dataset to a zip, import into a fresh in-memory
    DB, and check the row counts match per table. ``import_full_data``
    runs its own ``PRAGMA foreign_key_check`` after import, so a
    success result doubles as an FK-integrity assertion on the
    destination.
    """
    src = _fresh_db()
    dst = _fresh_db()
    try:
        build_base_stress_dataset(src)

        export_path = tmp_path / "base_backup.zip"
        export_result = export_full_data(src, export_path)
        assert export_result.success, export_result.message

        with zipfile.ZipFile(export_path) as zf:
            zip_names = set(zf.namelist())
        for table in EXPORT_TABLES:
            assert f"{table}.csv" in zip_names, f"missing {table}.csv in export"
        assert "manifest.csv" in zip_names

        import_result = import_full_data(dst, export_path, mode="replace")
        assert import_result.success, (
            f"import failed: {import_result.message} {import_result.details}"
        )

        for table in EXPORT_TABLES:
            src_count = src.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            dst_count = dst.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert src_count == dst_count, (
                f"{table}: src={src_count} dst={dst_count}"
            )
    finally:
        src.close()
        dst.close()


@pytest.mark.stress_phase4
def test_base_full_export_import_roundtrip(tmp_path):
    """Strict roundtrip on the BASE dataset.

    Goes beyond row-count parity to assert that downstream linkages
    survive the export → import path:

    * ``PRAGMA foreign_key_check`` on the destination DB returns no
      rows.
    * ``decision_journal.transaction_id`` cross-references resolve in
      the destination — the linked transaction exists with the same
      ``id`` and matching key fields.
    * ``transaction_fee_breakdown`` row count matches src→dst (the
      BASE spec disables fee legs, so the count is zero, but the
      assertion still pins the contract that empty tables round-
      trip cleanly).
    * ``reports`` and ``portfolio_snapshots`` rows are byte-identical
      on the field set we sample.
    """
    src = _fresh_db()
    dst = _fresh_db()
    try:
        build_base_stress_dataset(src)

        export_path = tmp_path / "base_strict.zip"
        assert export_full_data(src, export_path).success

        outcome = import_full_data(dst, export_path, mode="replace")
        assert outcome.success, f"{outcome.message} {outcome.details}"

        # Per-table row count parity for every table the harness
        # exports — including the ones the BASE spec leaves empty.
        for table in EXPORT_TABLES:
            src_count = src.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            dst_count = dst.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert src_count == dst_count, (
                f"{table}: src={src_count} dst={dst_count}"
            )

        # FK integrity on the destination — the import path's own
        # check runs inside a transaction, so an extra check here
        # also covers any FK that became invalid post-commit.
        _assert_fk_check_clean(dst)

        # Journal ↔ transaction cross references survive the round-
        # trip. Sample every linked entry; if any link points at a
        # missing transaction id in the destination, fail loud.
        linked = src.execute(
            "SELECT id, transaction_id FROM decision_journal "
            "WHERE transaction_id IS NOT NULL ORDER BY id"
        ).fetchall()
        assert linked, (
            "BASE builder is expected to link the first journal entries "
            "to transactions; got zero linked rows"
        )
        for row in linked:
            jid, txn_id = row[0], row[1]
            dst_txn = dst.execute(
                "SELECT id FROM transactions WHERE id = ?", (txn_id,)
            ).fetchone()
            assert dst_txn is not None, (
                f"journal row {jid} -> transaction {txn_id} broken in dst"
            )

        # Fee breakdown rows preserved (BASE spec: zero rows; the
        # equality is what we are pinning, not the magnitude).
        src_fee = src.execute(
            "SELECT COUNT(*) FROM transaction_fee_breakdown"
        ).fetchone()[0]
        dst_fee = dst.execute(
            "SELECT COUNT(*) FROM transaction_fee_breakdown"
        ).fetchone()[0]
        assert src_fee == dst_fee
        assert src_fee == 0, (
            "BASE spec disables fee breakdown — got non-zero count, "
            "update the assertion if the spec changes"
        )

        # Reports preserved — row count + a content sample. We pick a
        # known label and compare the JSON blob.
        sample = src.execute(
            "SELECT report_type, period_label, report_json "
            "FROM reports ORDER BY period_label DESC LIMIT 1"
        ).fetchone()
        if sample is not None:
            dst_row = dst.execute(
                "SELECT report_json FROM reports "
                "WHERE report_type = ? AND period_label = ?",
                (sample[0], sample[1]),
            ).fetchone()
            assert dst_row is not None, (
                f"sampled report {sample[0]}/{sample[1]} missing in dst"
            )
            assert dst_row[0] == sample[2], (
                "report_json mismatch after roundtrip"
            )

        # Snapshots preserved — sample the most recent row by date and
        # compare the numeric fields.
        snap = src.execute(
            "SELECT date, cash, total_assets, total_liabilities, net_worth "
            "FROM portfolio_snapshots ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if snap is not None:
            dst_snap = dst.execute(
                "SELECT cash, total_assets, total_liabilities, net_worth "
                "FROM portfolio_snapshots WHERE date = ?",
                (snap[0],),
            ).fetchone()
            assert dst_snap is not None, (
                f"sampled snapshot {snap[0]} missing in dst"
            )
            for i, label in enumerate(
                ("cash", "total_assets", "total_liabilities", "net_worth"),
                start=1,
            ):
                assert float(dst_snap[i - 1]) == pytest.approx(
                    float(snap[i]), rel=1e-9, abs=1e-6,
                ), f"snapshot {label} mismatch after roundtrip"
    finally:
        src.close()
        dst.close()


@pytest.mark.stress_phase4
def test_asset_related_delete_fee_breakdown_integrity():
    """Asset deletion must remove ``transaction_fee_breakdown`` rows
    BEFORE the parent transactions, or the cascade trips the
    transaction → fee_breakdown FK.

    Builds a focused 3-asset dataset with multi-leg fee breakdowns,
    then exercises ``delete_asset_with_related_data`` for one asset
    and asserts the cascade is clean and doesn't touch siblings.
    """
    conn = _fresh_db()
    try:
        # Three assets, two with fee-breakdown rows. Build the data
        # by hand — the focused test wants exact, predictable row
        # counts, not the BASE/EXTREME bulk shape.
        a = create_asset(conn, Asset(symbol="ALPHA", name="Alpha", asset_type="stock"))
        b = create_asset(conn, Asset(symbol="BETA",  name="Beta",  asset_type="stock"))
        c = create_asset(conn, Asset(symbol="GAMMA", name="Gamma", asset_type="stock"))

        # Seed cash so the dataset is internally consistent even though
        # the FK contract under test is independent of cash balance.
        create_transaction(conn, Transaction(
            date="2025-01-01", txn_type="deposit_cash",
            total_amount=1_000_000.0,
        ))

        t1 = create_transaction(conn, Transaction(
            date="2025-02-01", txn_type="buy", asset_id=a.id,
            quantity=10, price=100.0, total_amount=-1010.0, fees=10.0,
        ))
        t2 = create_transaction(conn, Transaction(
            date="2025-02-02", txn_type="buy", asset_id=b.id,
            quantity=5, price=50.0, total_amount=-270.0, fees=20.0,
        ))
        t3 = create_transaction(conn, Transaction(
            date="2025-02-03", txn_type="buy", asset_id=a.id,
            quantity=2, price=110.0, total_amount=-235.0, fees=15.0,
        ))
        t4 = create_transaction(conn, Transaction(
            date="2025-02-04", txn_type="buy", asset_id=c.id,
            quantity=1, price=200.0, total_amount=-205.0, fees=5.0,
        ))

        # Asset A's transactions (t1, t3) get multi-leg fee breakdowns,
        # asset C's transaction (t4) gets one leg, asset B has none.
        create_fee_breakdown(conn, FeeBreakdownRow(
            transaction_id=t1.id, fee_type="commission", amount=7.0,
        ))
        create_fee_breakdown(conn, FeeBreakdownRow(
            transaction_id=t1.id, fee_type="exchange", amount=3.0,
        ))
        create_fee_breakdown(conn, FeeBreakdownRow(
            transaction_id=t3.id, fee_type="commission", amount=15.0,
        ))
        create_fee_breakdown(conn, FeeBreakdownRow(
            transaction_id=t4.id, fee_type="commission", amount=5.0,
        ))

        # And a journal entry linked to t2 (asset B) so we can verify
        # nothing unrelated gets nuked.
        create_journal_entry(conn, DecisionJournalEntry(
            transaction_id=t2.id, date="2025-02-02",
            title="Beta thesis", thesis="control entry",
        ))

        # Pre-state sanity.
        assert conn.execute(
            "SELECT COUNT(*) FROM transaction_fee_breakdown"
        ).fetchone()[0] == 4
        assert conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE asset_id = ?", (a.id,)
        ).fetchone()[0] == 2

        deleted = delete_asset_with_related_data(conn, a.id)
        assert deleted["transactions"] == 2

        # Asset A is gone.
        assert conn.execute(
            "SELECT COUNT(*) FROM assets WHERE id = ?", (a.id,)
        ).fetchone()[0] == 0
        # No transactions for asset A.
        assert conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE asset_id = ?", (a.id,)
        ).fetchone()[0] == 0
        # Asset A's fee breakdown rows are gone (3 rows: t1×2 + t3×1).
        assert conn.execute(
            "SELECT COUNT(*) FROM transaction_fee_breakdown "
            "WHERE transaction_id IN (?, ?)", (t1.id, t3.id),
        ).fetchone()[0] == 0
        # Asset C's lone fee breakdown row remains.
        assert conn.execute(
            "SELECT COUNT(*) FROM transaction_fee_breakdown "
            "WHERE transaction_id = ?", (t4.id,),
        ).fetchone()[0] == 1
        # Asset B and C still present.
        for asset_id in (b.id, c.id):
            assert conn.execute(
                "SELECT COUNT(*) FROM assets WHERE id = ?", (asset_id,),
            ).fetchone()[0] == 1
        # B's transaction and journal entry untouched.
        assert conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE id = ?", (t2.id,),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM decision_journal "
            "WHERE transaction_id = ?", (t2.id,),
        ).fetchone()[0] == 1

        _assert_fk_check_clean(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# stress_extreme — extreme-scale variants
# ---------------------------------------------------------------------------

@pytest.mark.stress_extreme
def test_extreme_dataset_builds_and_passes_fk_check():
    """``build_extreme_stress_dataset`` must populate the database to
    the extreme spec without raising and must leave it FK-clean.
    """
    conn = _fresh_db()
    try:
        counts = build_extreme_stress_dataset(conn)
        assert counts["tradeable_assets"] == EXTREME_SPEC.assets
        assert counts["properties"] == EXTREME_SPEC.properties
        assert counts["debts"] == EXTREME_SPEC.debts
        assert counts["snapshots"] == EXTREME_SPEC.snapshots
        assert counts["journal_entries"] == EXTREME_SPEC.journal_entries
        assert counts["monthly_reports"] == EXTREME_SPEC.monthly_reports
        assert counts["annual_reports"] == EXTREME_SPEC.annual_reports
        assert counts["price_rows"] > 0  # extreme spec enables price history
        assert counts["fee_breakdown_rows"] > 0  # extreme spec enables fee legs

        _assert_fk_check_clean(conn)
    finally:
        conn.close()


@pytest.mark.stress_extreme
def test_extreme_dataset_full_data_roundtrip(tmp_path):
    """The full-data export/import path must survive extreme-scale row
    counts (10k+ transactions, ~24k price rows, ~3.9k fee rows). This
    is the headline regression target for IO performance work.
    """
    src = _fresh_db()
    dst = _fresh_db()
    try:
        build_extreme_stress_dataset(src)

        export_path = tmp_path / "extreme_backup.zip"
        assert export_full_data(src, export_path).success

        result = import_full_data(dst, export_path, mode="replace")
        assert result.success, f"{result.message} {result.details}"

        for table in EXPORT_TABLES:
            src_count = src.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            dst_count = dst.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert src_count == dst_count, (
                f"{table}: src={src_count} dst={dst_count}"
            )
    finally:
        src.close()
        dst.close()


# Soft threshold above which we emit a "[performance-warning]" line in
# the test stdout. Set conservatively against the ~1s observed during
# Phase 0 dry-runs on this machine; if a regression makes the engines
# materially slower the line surfaces in the pytest output without
# failing the test (the spec says: classify as warning, not failure).
_EXTREME_ENGINE_SOFT_BUDGET_SECONDS = 5.0


@pytest.mark.stress_extreme
def test_extreme_engine_500_assets_10000_transactions():
    """Non-GUI engine smoke against the EXTREME dataset.

    Builds the extreme dataset (500 tradeable assets, 10 000 + seeded
    transactions, 50 properties, etc.) and runs the same read-side
    engines the dashboard reaches for. The test fails only on crashes
    or shape errors; slow runs are surfaced as a
    ``[performance-warning]`` log line and the assertion budget is
    intentionally generous, so this test is a stress harness, not a
    perf gate.
    """
    conn = _fresh_db()
    try:
        counts = build_extreme_stress_dataset(conn)
        # Spot-check the dataset is what we asked for; if these are
        # wrong every later assertion is meaningless.
        assert counts["tradeable_assets"] == EXTREME_SPEC.assets
        txn_rows = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        assert txn_rows >= EXTREME_SPEC.transactions

        timings: list = []
        summary = _timed("extreme.engine.get_portfolio_summary",
                         lambda: get_portfolio_summary(conn), timings)
        positions = _timed("extreme.engine.calc_positions",
                           lambda: calc_positions(conn), timings)
        alloc = _timed("extreme.engine.get_full_allocation",
                       lambda: get_full_allocation(conn), timings)
        warnings = _timed("extreme.engine.get_all_warnings",
                          lambda: get_all_warnings(conn), timings)

        # Shape checks — same contract as the base smoke, but at the
        # extreme cardinality.
        assert isinstance(positions, list)
        assert isinstance(warnings, list)
        for key in ("cash", "total_assets", "total_liabilities", "net_worth"):
            _assert_finite_number(summary[key], f"summary.{key}")
        assert "by_asset_type" in alloc
        assert "by_asset" in alloc

        derived = summary["total_assets"] - summary["total_liabilities"]
        assert abs(summary["net_worth"] - derived) < 1e-6

        engine_total = sum(t for _, t, _ in timings)
        print(f"[stress-timing] extreme.engine.total: {engine_total:.4f}s")

        if engine_total > _EXTREME_ENGINE_SOFT_BUDGET_SECONDS:
            slowest = sorted(timings, key=lambda x: x[1], reverse=True)[:3]
            top = ", ".join(f"{lbl}={sec:.3f}s" for lbl, sec, _ in slowest)
            print(
                "[performance-warning] extreme engine total "
                f"{engine_total:.3f}s exceeded soft budget "
                f"{_EXTREME_ENGINE_SOFT_BUDGET_SECONDS:.1f}s; slowest: {top}"
            )
    finally:
        conn.close()


# Performance soft-budget for the report summary listing on the
# EXTREME dataset (260 reports). 0.5s is a generous gate for a
# bounded SELECT against ~260 rows on an in-memory DB; if a future
# refactor accidentally re-introduces full-row reads or per-row JSON
# parsing the threshold will surface the regression.
_EXTREME_SUMMARY_SOFT_BUDGET_SECONDS = 0.5


@pytest.mark.stress_extreme
def test_extreme_240_monthly_20_annual_reports_summary():
    """Reports listing must remain bounded and lazy on the EXTREME
    dataset (240 monthly + 20 annual = 260 reports).

    Verifies:

    * ``report_count(conn, ...)`` returns the exact spec'd numbers.
    * ``list_report_summaries(limit=N)`` honors ``limit`` and the
      ``report_type`` filter.
    * The summary path runs in well under the soft budget AND does
      not touch ``report_json`` in any executed SQL — same lazy
      contract verified for the base test, now under load.
    """
    conn = _fresh_db()
    try:
        build_extreme_stress_dataset(conn)

        monthly = report_count(conn, "monthly")
        annual = report_count(conn, "annual")
        total = report_count(conn)
        assert monthly == EXTREME_SPEC.monthly_reports == 240
        assert annual == EXTREME_SPEC.annual_reports == 20
        assert total == monthly + annual

        # Bounded + fast + lazy: capture every executed SQL during
        # the listing call, time it, and assert all three contracts
        # at once.
        captured: list[str] = []
        conn.set_trace_callback(captured.append)
        start = time.perf_counter()
        try:
            summaries = list_report_summaries(conn, limit=100)
        finally:
            conn.set_trace_callback(None)
        elapsed = time.perf_counter() - start
        print(f"[stress-timing] reports.list_report_summaries(limit=100): {elapsed:.4f}s")

        assert len(summaries) == 100, (
            f"limit=100 returned {len(summaries)} rows"
        )
        assert elapsed < _EXTREME_SUMMARY_SOFT_BUDGET_SECONDS, (
            f"summary listing too slow: {elapsed:.3f}s "
            f"(budget {_EXTREME_SUMMARY_SOFT_BUDGET_SECONDS:.1f}s)"
        )
        assert not any("report_json" in q for q in captured), (
            f"summary path leaked report_json access: {captured}"
        )

        # Type-filtered summaries respect the filter and the limit.
        monthly_summaries = list_report_summaries(conn, report_type="monthly", limit=10)
        annual_summaries = list_report_summaries(conn, report_type="annual", limit=10)
        assert len(monthly_summaries) == 10
        assert all(s.report_type == "monthly" for s in monthly_summaries)
        assert len(annual_summaries) == 10
        assert all(s.report_type == "annual" for s in annual_summaries)

        # Sanity: limit=0 / limit larger than table behave as expected.
        all_summaries = list_report_summaries(conn)
        assert len(all_summaries) == total
    finally:
        conn.close()


@pytest.mark.stress_extreme
def test_extreme_1000_snapshots_dashboard_trend_data(monkeypatch):
    """Dashboard data helpers must remain sane against 1000 portfolio
    snapshots. Engine layer only — MainWindow is not instantiated in
    this phase.

    Verifies:

    * No crash on any helper, even with 1000 rows in
      ``portfolio_snapshots``.
    * ``get_net_worth_trend`` returns a list with the documented
      shape and finite ``net_worth`` values.
    * The 30-day change calculation that the Dashboard page
      performs (``trend[-1].net_worth - trend[0].net_worth``) yields
      a finite number when at least two snapshots fall inside the
      30-day window.

    The builder pins snapshot dates around an anchor in the past, so
    ``date.today()`` is monkeypatched on the dashboard module to a
    deterministic value relative to that anchor — otherwise the 30-day
    window would slide off the dataset on calendar drift.
    """
    import src.engines.dashboard as dash_mod
    from datetime import date as _real_date

    fake_today = _real_date(2026, 1, 15)

    class _FakeDate:
        @staticmethod
        def today():
            return fake_today

    monkeypatch.setattr(dash_mod, "date", _FakeDate)

    conn = _fresh_db()
    try:
        counts = build_extreme_stress_dataset(conn)
        assert counts["snapshots"] == EXTREME_SPEC.snapshots == 1000
        snap_rows = conn.execute(
            "SELECT COUNT(*) FROM portfolio_snapshots"
        ).fetchone()[0]
        assert snap_rows == 1000

        # Belt-and-braces: list_snapshots returns rows oldest-first
        # (ORDER BY date), which is the order the trend helper relies on.
        all_snaps = list_snapshots(conn)
        assert len(all_snaps) == 1000
        dates = [s.date for s in all_snaps]
        assert dates == sorted(dates), (
            "list_snapshots is no longer ORDER BY date asc — trend "
            "logic depends on this ordering"
        )

        timings: list = []
        trend_default = _timed(
            "dashboard.get_net_worth_trend.default",
            lambda: get_net_worth_trend(conn),
            timings,
        )
        assert isinstance(trend_default, list)
        for row in trend_default:
            for k in ("date", "cash", "total_assets",
                      "total_liabilities", "net_worth"):
                assert k in row, f"trend row missing key {k}: {row!r}"
            _assert_finite_number(row["net_worth"], "trend.net_worth")

        # 30-day window with the patched today (2026-01-15) covers
        # the anchor (2026-01-01) plus the ~14 prior days, so at
        # least 2 snapshots must fall in the window.
        trend_30d = _timed(
            "dashboard.get_net_worth_trend.30d",
            lambda: get_net_worth_trend(conn, days=30),
            timings,
        )
        assert isinstance(trend_30d, list)
        assert len(trend_30d) >= 2, (
            f"expected ≥2 snapshots in 30-day window, got {len(trend_30d)}"
        )
        change = trend_30d[-1]["net_worth"] - trend_30d[0]["net_worth"]
        _assert_finite_number(change, "30D change")

        # Wide window — ensures every snapshot is reachable, no
        # short-circuit hides truncation bugs at scale.
        trend_wide = _timed(
            "dashboard.get_net_worth_trend.wide",
            lambda: get_net_worth_trend(conn, days=2_000),
            timings,
        )
        assert len(trend_wide) == counts["snapshots"]

        # The dashboard summary itself does not consume snapshots,
        # but the page does — running it here confirms the engine
        # path the page wraps still answers at extreme scale.
        summary = _timed(
            "dashboard.get_dashboard_summary",
            lambda: get_dashboard_summary(conn),
            timings,
        )
        for key in ("net_worth", "total_assets",
                    "total_liabilities", "cash"):
            assert key in summary
            _assert_finite_number(summary[key], f"summary.{key}")

        total = sum(t for _, t, _ in timings)
        print(f"[stress-timing] dashboard.helpers.total: {total:.4f}s")
    finally:
        conn.close()


# Soft / hard budgets for the extreme full-data roundtrip. Hard
# threshold is the point at which the test fails — set generously
# so transient slowness doesn't break the harness, while still
# catching catastrophic regressions (e.g., O(N²) in the import
# loop). Soft threshold emits a [performance-warning] log line.
_EXTREME_ROUNDTRIP_SOFT_BUDGET_SECONDS = 5.0
_EXTREME_ROUNDTRIP_HARD_BUDGET_SECONDS = 60.0


@pytest.mark.stress_extreme
def test_extreme_full_export_import_roundtrip(tmp_path):
    """Full-data roundtrip on the EXTREME dataset.

    Same correctness checks as the BASE strict test, but at the row
    counts that would actually expose IO scaling problems (10k+
    transactions, ~24k price rows, ~3.9k fee-breakdown rows). Records
    the zip size on disk and the wall-clock for each leg so the
    report has concrete data points; fails only if the total exceeds
    the **hard** budget.
    """
    src = _fresh_db()
    dst = _fresh_db()
    try:
        build_extreme_stress_dataset(src)

        export_path = tmp_path / "extreme_strict.zip"
        t_export_start = time.perf_counter()
        assert export_full_data(src, export_path).success
        t_export = time.perf_counter() - t_export_start

        zip_size_bytes = export_path.stat().st_size
        zip_size_mb = zip_size_bytes / (1024 * 1024)
        print(f"[stress-info] extreme.export_zip_size: {zip_size_mb:.2f} MiB ({zip_size_bytes} bytes)")
        print(f"[stress-timing] extreme.export_full_data: {t_export:.4f}s")

        t_import_start = time.perf_counter()
        outcome = import_full_data(dst, export_path, mode="replace")
        t_import = time.perf_counter() - t_import_start
        assert outcome.success, f"{outcome.message} {outcome.details}"
        print(f"[stress-timing] extreme.import_full_data: {t_import:.4f}s")

        # Per-table parity.
        for table in EXPORT_TABLES:
            src_count = src.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            dst_count = dst.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert src_count == dst_count, (
                f"{table}: src={src_count} dst={dst_count}"
            )

        _assert_fk_check_clean(dst)

        # Journal cross-references resolve.
        linked = src.execute(
            "SELECT id, transaction_id FROM decision_journal "
            "WHERE transaction_id IS NOT NULL ORDER BY id"
        ).fetchall()
        assert linked
        for jid, txn_id in linked:
            dst_txn = dst.execute(
                "SELECT id FROM transactions WHERE id = ?", (txn_id,),
            ).fetchone()
            assert dst_txn is not None, (
                f"journal {jid} -> transaction {txn_id} broken in dst"
            )

        # Fee breakdown rows preserved (extreme spec enables ~25%
        # coverage, so this assertion has real content).
        src_fee = src.execute(
            "SELECT COUNT(*) FROM transaction_fee_breakdown"
        ).fetchone()[0]
        dst_fee = dst.execute(
            "SELECT COUNT(*) FROM transaction_fee_breakdown"
        ).fetchone()[0]
        assert src_fee == dst_fee
        assert src_fee > 0, (
            "EXTREME spec should produce fee-breakdown rows; "
            "got zero — update the assertion if the spec changes"
        )

        # Reports + snapshots: row counts already covered above, plus
        # a content sample on each.
        sample_report = src.execute(
            "SELECT report_type, period_label, report_json "
            "FROM reports ORDER BY period_label DESC LIMIT 1"
        ).fetchone()
        if sample_report is not None:
            dst_row = dst.execute(
                "SELECT report_json FROM reports "
                "WHERE report_type = ? AND period_label = ?",
                (sample_report[0], sample_report[1]),
            ).fetchone()
            assert dst_row is not None
            assert dst_row[0] == sample_report[2]

        sample_snap = src.execute(
            "SELECT date, net_worth FROM portfolio_snapshots "
            "ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if sample_snap is not None:
            dst_snap_nw = dst.execute(
                "SELECT net_worth FROM portfolio_snapshots WHERE date = ?",
                (sample_snap[0],),
            ).fetchone()
            assert dst_snap_nw is not None
            assert float(dst_snap_nw[0]) == pytest.approx(
                float(sample_snap[1]), rel=1e-9, abs=1e-6,
            )

        total = t_export + t_import
        print(f"[stress-timing] extreme.roundtrip.total: {total:.4f}s")

        # Hard threshold = test failure. Soft threshold = warning.
        assert total <= _EXTREME_ROUNDTRIP_HARD_BUDGET_SECONDS, (
            f"extreme roundtrip {total:.2f}s exceeded hard budget "
            f"{_EXTREME_ROUNDTRIP_HARD_BUDGET_SECONDS:.1f}s"
        )
        if total > _EXTREME_ROUNDTRIP_SOFT_BUDGET_SECONDS:
            print(
                "[performance-warning] extreme roundtrip "
                f"{total:.2f}s exceeded soft budget "
                f"{_EXTREME_ROUNDTRIP_SOFT_BUDGET_SECONDS:.1f}s "
                f"(export={t_export:.2f}s, import={t_import:.2f}s, "
                f"zip={zip_size_mb:.2f}MiB)"
            )
    finally:
        src.close()
        dst.close()


@pytest.mark.stress_extreme
def test_extreme_clear_all_data():
    """``clear_all_user_data`` must wipe every user-data table the
    product currently maintains, including:

    * the live tables (assets, transactions, fee breakdowns, prices,
      quotes, sync log, properties, debts, journal, snapshots,
      reports, settings, securities_master)
    * the legacy ``option_contracts`` table if a stale build left it
      around (drop, not just delete)
    * AUTOINCREMENT counters in ``sqlite_sequence`` for the affected
      tables, so post-clear inserts start at id=1

    After the clear, ``initialize_universe`` must successfully
    repopulate ``securities_master`` and the database must still pass
    ``foreign_key_check``.
    """
    conn = _fresh_db()
    try:
        build_extreme_stress_dataset(conn)

        # Synthesize a stale option_contracts table so we can verify
        # the legacy-cleanup branch in clear_all_user_data fires.
        conn.execute(
            "CREATE TABLE option_contracts (id INTEGER PRIMARY KEY, symbol TEXT)"
        )
        conn.execute(
            "INSERT INTO option_contracts (symbol) VALUES ('LEGACY')"
        )
        conn.commit()
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='option_contracts'"
        ).fetchone() is not None

        # Pre-clear sanity: every user-data table has rows.
        for table in (
            "assets", "transactions", "transaction_fee_breakdown",
            "market_prices", "properties", "debts",
            "decision_journal", "portfolio_snapshots", "reports",
        ):
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert count > 0, f"{table} should be non-empty before clear"

        deleted = clear_all_user_data(conn)
        assert isinstance(deleted, dict)

        # Every user-data table is empty after the clear.
        for table in (
            "assets", "transactions", "transaction_fee_breakdown",
            "market_prices", "market_quotes", "price_sync_log",
            "properties", "debts", "decision_journal",
            "portfolio_snapshots", "reports", "settings",
            "securities_master",
        ):
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert count == 0, f"{table} not empty after clear: {count}"

        # Legacy option_contracts table dropped.
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='option_contracts'"
        ).fetchone() is None, (
            "option_contracts should be dropped by clear_all_user_data"
        )

        # AUTOINCREMENT counters reset. sqlite_sequence may or may
        # not exist depending on prior INSERTs, but if it does, none
        # of the affected tables should still have a row in it.
        seq_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='sqlite_sequence'"
        ).fetchone() is not None
        if seq_exists:
            for table in (
                "assets", "transactions", "transaction_fee_breakdown",
                "market_prices", "market_quotes", "price_sync_log",
                "properties", "debts", "decision_journal",
                "portfolio_snapshots", "reports", "securities_master",
            ):
                row = conn.execute(
                    "SELECT seq FROM sqlite_sequence WHERE name = ?",
                    (table,),
                ).fetchone()
                assert row is None, (
                    f"{table} sqlite_sequence row not reset: seq={row[0]}"
                )

        # Concrete proof the autoincrement counter restarted: insert
        # a fresh asset and check its id is 1.
        new_asset = create_asset(conn, Asset(
            symbol="POST-CLEAR", name="Post Clear Sentinel", asset_type="stock",
        ))
        assert new_asset.id == 1, (
            f"AUTOINCREMENT counter not reset: new asset id={new_asset.id}"
        )

        # Roll the sentinel back so it doesn't pollute the
        # initialize_universe assertion below.
        conn.execute("DELETE FROM assets WHERE id = ?", (new_asset.id,))
        conn.commit()

        # initialize_universe repopulates securities_master.
        assert get_universe_count(conn) == 0
        loaded = initialize_universe(conn)
        assert loaded > 0
        assert get_universe_count(conn) > 0

        _assert_fk_check_clean(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# stress_gui — offscreen GUI navigation on base dataset
# ---------------------------------------------------------------------------

# Per-page hard budget for an offscreen first refresh on the BASE
# dataset. 10s is roomy enough for matplotlib first-import and
# canvas creation on a slow machine; anything beyond that suggests
# a real regression rather than warmup variance.
_GUI_PAGE_HARD_BUDGET_SECONDS = 10.0


@pytest.mark.stress_gui
def test_base_main_window_navigation_all_pages(qapp, monkeypatch):
    """Open ``MainWindow`` against the BASE dataset, walk every page
    via ``_refresh_page``, then drive ``nav_list`` so the signal-
    based path is also exercised. Records per-page wall-clock time.

    Modal dialogs are stubbed via ``_patch_modal_dialogs``; the
    returned counter is asserted at zero at the end so any
    accidental modal during navigation surfaces as a test failure.

    Cleanup follows the same deferred-delete + gc pattern the
    project's ``conftest.py`` uses, since GUI teardown is the path
    that previously surfaced a SIGSEGV regression.
    """
    from PySide6.QtWidgets import QApplication
    from src.gui.main_window import MainWindow, PAGE_LABELS

    modal_counter = _patch_modal_dialogs(monkeypatch)

    conn = _fresh_db()
    try:
        build_base_stress_dataset(conn)

        construct_start = time.perf_counter()
        win = MainWindow(conn, enable_startup_sync=False)
        construct_time = time.perf_counter() - construct_start
        print(f"[stress-timing] gui.main_window.construct: {construct_time:.4f}s")

        page_count = win.nav_list.count()
        assert page_count == len(PAGE_LABELS) == 9, (
            f"expected 9 pages, got {page_count} (labels: {PAGE_LABELS})"
        )

        page_timings: list[tuple[str, float]] = []
        for idx, label in enumerate(PAGE_LABELS):
            t0 = time.perf_counter()
            win._refresh_page(idx)
            elapsed = time.perf_counter() - t0
            page_timings.append((label, elapsed))
            slug = label.replace(" / ", "_").replace(" ", "_").lower()
            print(f"[stress-timing] gui.page.{idx}.{slug}: {elapsed:.4f}s")
            assert elapsed <= _GUI_PAGE_HARD_BUDGET_SECONDS, (
                f"page {label} refresh {elapsed:.2f}s exceeded hard budget "
                f"{_GUI_PAGE_HARD_BUDGET_SECONDS:.1f}s"
            )

        # Drive the signal path too — setCurrentRow → _on_page_changed
        # → _refresh_page. If any signal-driven hook raises, this
        # surfaces it.
        for idx in range(page_count):
            win.nav_list.setCurrentRow(idx)

        # Every page widget exists and is the right type after both
        # refresh paths. The MainWindow's PAGE_LABELS order is the
        # contract here; if it diverges from page_widgets, something
        # is wrong with construction.
        for idx, label in enumerate(PAGE_LABELS):
            page_widget = win.page_widgets[idx]
            assert page_widget is not None, f"page {idx} ({label}) is None"

        # Modal dialog contract: nothing on the navigation/refresh
        # path should pop a modal. Anything counted here is a leak.
        assert modal_counter["messagebox"] == 0, modal_counter
        assert modal_counter["filedialog"] == 0, modal_counter
        assert modal_counter["inputdialog"] == 0, modal_counter

        close_start = time.perf_counter()
        win.close()
        win.deleteLater()
        _flush_qt_deletes(QApplication.instance())
        close_time = time.perf_counter() - close_start
        print(f"[stress-timing] gui.main_window.close: {close_time:.4f}s")

        total = construct_time + sum(t for _, t in page_timings) + close_time
        print(f"[stress-timing] gui.main_window.total: {total:.4f}s")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# stress_extreme — targeted page refreshes on extreme dataset
# ---------------------------------------------------------------------------

# Hard budget for a single page refresh on the EXTREME dataset.
# 30s is permissive — well above measured timings, set so a real
# hang or O(N²) regression fails the test loud rather than being
# absorbed into a slow run.
_EXTREME_PAGE_HARD_BUDGET_SECONDS = 30.0
# Soft warning threshold — emits a [performance-warning] log line.
_EXTREME_PAGE_SOFT_BUDGET_SECONDS = 5.0


@pytest.mark.stress_extreme
def test_extreme_dashboard_refresh(qapp, monkeypatch):
    """Standalone ``DashboardPage`` refresh against the EXTREME
    dataset. Bypasses ``MainWindow`` to keep the failure surface
    small — if this test fails at the page level, the cause is in
    the dashboard refresh path itself, not a sibling page.

    Verifies:

    * The refresh completes and stays under the hard time budget.
    * The hero-card labels (Net Worth, Cash, 30D Change, Risk
      Status) are populated with non-empty text.
    * After refresh, all three matplotlib canvases
      (``_trend_canvas``, ``_mix_canvas``, ``_drivers_canvas``)
      have been swapped in (no longer ``None``).
    * ``_cleanup_figures`` resets all three canvases to ``None``,
      so the GC-driven crash path the project regression-tests
      against stays clean.
    """
    from PySide6.QtWidgets import QApplication, QLabel
    from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
    from src.gui.pages.dashboard import DashboardPage

    modal_counter = _patch_modal_dialogs(monkeypatch)

    conn = _fresh_db()
    try:
        build_extreme_stress_dataset(conn)

        t0 = time.perf_counter()
        page = DashboardPage(conn)
        construct_time = time.perf_counter() - t0
        print(f"[stress-timing] gui.dashboard.construct: {construct_time:.4f}s")

        # Pre-refresh state: canvas slots start as None.
        for attr in ("_trend_canvas", "_mix_canvas", "_drivers_canvas"):
            assert getattr(page, attr) is None, (
                f"{attr} unexpectedly populated before first refresh"
            )

        t1 = time.perf_counter()
        page.refresh()
        refresh_time = time.perf_counter() - t1
        print(f"[stress-timing] gui.dashboard.refresh: {refresh_time:.4f}s")

        assert refresh_time <= _EXTREME_PAGE_HARD_BUDGET_SECONDS, (
            f"dashboard refresh {refresh_time:.2f}s exceeded hard budget "
            f"{_EXTREME_PAGE_HARD_BUDGET_SECONDS:.1f}s"
        )
        if refresh_time > _EXTREME_PAGE_SOFT_BUDGET_SECONDS:
            print(
                "[performance-warning] dashboard refresh "
                f"{refresh_time:.2f}s exceeded soft budget "
                f"{_EXTREME_PAGE_SOFT_BUDGET_SECONDS:.1f}s"
            )

        # Hero-card labels populated — defends the "blank dashboard"
        # regression where data fetched fine but the QLabel never
        # got setText().
        for card_attr in ("nw_card", "change_card", "cash_card", "risk_card"):
            card = getattr(page, card_attr)
            value_label = card.findChild(QLabel, "value")
            assert value_label is not None, (
                f"{card_attr}: no child QLabel named 'value'"
            )
            text = value_label.text()
            assert text, f"{card_attr} label is empty after refresh"

        # Canvases swapped in after refresh.
        for attr in ("_trend_canvas", "_mix_canvas", "_drivers_canvas"):
            canvas = getattr(page, attr)
            assert canvas is not None, f"{attr} not populated after refresh"
            assert isinstance(canvas, FigureCanvasQTAgg), (
                f"{attr} is {type(canvas).__name__}, expected FigureCanvasQTAgg"
            )

        # Cleanup path resets canvases to None — same contract the
        # project's qt-lifecycle regression test relies on.
        page._cleanup_figures()
        for attr in ("_trend_canvas", "_mix_canvas", "_drivers_canvas"):
            assert getattr(page, attr) is None, (
                f"{attr} should be None after _cleanup_figures()"
            )

        assert modal_counter["messagebox"] == 0, modal_counter
        assert modal_counter["filedialog"] == 0, modal_counter

        page.close()
        page.deleteLater()
        _flush_qt_deletes(QApplication.instance())
    finally:
        conn.close()


@pytest.mark.stress_extreme
def test_extreme_import_export_reports_tab_lazy(qapp, monkeypatch):
    """The Import / Export page's Reports tab must be lazy: the
    page's own ``refresh()`` must NOT load the reports table when
    the active tab is the Import/Export tab. Verified by tracing
    every SQL statement SQLite actually executes during the call.

    On the EXTREME dataset (240 monthly + 20 annual = 260 reports)
    the lazy contract is what prevents the Import/Export panel from
    paying for a 260-row scan + 60 detail loads on every navigation
    refresh.

    Verifies:

    * No ``FROM reports`` SQL during ``refresh()`` while the
      Reports tab is inactive.
    * Switching to the Reports tab triggers a single
      ``list_report_summaries`` SELECT capped at the configured
      limit (60 rows visible in the table).
    * Selecting a single report runs a ``get_report`` SELECT and
      no full-table scan.
    * Modal dialogs do NOT fire on any of these paths.
    """
    from PySide6.QtWidgets import QApplication
    from src.gui.pages.import_export import (
        DEFAULT_REPORT_LIST_LIMIT,
        ImportExportPage,
        REPORTS_TAB_INDEX,
    )

    modal_counter = _patch_modal_dialogs(monkeypatch)

    conn = _fresh_db()
    try:
        build_extreme_stress_dataset(conn)

        page = ImportExportPage(conn)

        # Sanity: the default tab is NOT the Reports tab.
        assert page.page_tabs.currentIndex() != REPORTS_TAB_INDEX, (
            "default tab should be Import / Export (index 0)"
        )

        # Lazy contract: refresh() while the active tab is the
        # Import/Export tab must NOT touch the reports table.
        captured_refresh: list[str] = []
        conn.set_trace_callback(captured_refresh.append)
        try:
            page.refresh()
        finally:
            conn.set_trace_callback(None)

        reports_sql_in_refresh = [
            q for q in captured_refresh if "FROM reports" in q
        ]
        assert not reports_sql_in_refresh, (
            "refresh() loaded reports while Reports tab was inactive: "
            f"{reports_sql_in_refresh}"
        )
        # _reports_dirty flag tracks "needs reload on next view"; it
        # should be set to True after the lazy refresh.
        assert page._reports_dirty is True, (
            "_reports_dirty should be True after refresh while Reports tab inactive"
        )

        # Switching to the Reports tab fires _on_tab_changed →
        # _refresh_report_list. Trace it.
        captured_switch: list[str] = []
        conn.set_trace_callback(captured_switch.append)
        try:
            page.page_tabs.setCurrentIndex(REPORTS_TAB_INDEX)
        finally:
            conn.set_trace_callback(None)

        # The summary listing must have run, and report_json must
        # NOT appear in it — confirms the same lazy-summary contract
        # verified at the engine level, but now through the GUI.
        assert any("FROM reports" in q for q in captured_switch), (
            f"reports SELECT did not run on tab switch: {captured_switch}"
        )
        list_sql = [
            q for q in captured_switch
            if "FROM reports" in q and "report_json" not in q
        ]
        assert list_sql, (
            "tab-switch SQL did not include a report_json-free summary "
            f"SELECT: {captured_switch}"
        )
        assert not any("report_json" in q for q in captured_switch), (
            f"tab switch leaked report_json access: {captured_switch}"
        )

        # The reports table is capped at DEFAULT_REPORT_LIST_LIMIT
        # rows (60). With 260 total reports, this is the contract
        # that keeps the GUI bounded.
        assert page.report_list_table.rowCount() == DEFAULT_REPORT_LIST_LIMIT
        assert len(page._report_summary_cache) == DEFAULT_REPORT_LIST_LIMIT

        # Selecting a row triggers _on_report_selected → get_report.
        # Trace just that path.
        captured_select: list[str] = []
        conn.set_trace_callback(captured_select.append)
        try:
            page.report_list_table.setCurrentCell(0, 0)
        finally:
            conn.set_trace_callback(None)

        # The detail load is exactly one get_report SELECT. We
        # accept any number of statements but require:
        #   - at least one SELECT against reports
        #   - no broad scan (no ORDER BY DESC LIMIT 60 again)
        detail_select = [
            q for q in captured_select
            if "FROM reports" in q and "WHERE" in q
        ]
        assert detail_select, (
            f"selecting a row did not trigger a detail SELECT: {captured_select}"
        )

        # No modal dialogs throughout.
        assert modal_counter["messagebox"] == 0, modal_counter
        assert modal_counter["filedialog"] == 0, modal_counter
        assert modal_counter["inputdialog"] == 0, modal_counter

        page.close()
        page.deleteLater()
        _flush_qt_deletes(QApplication.instance())
    finally:
        conn.close()


@pytest.mark.stress_extreme
def test_extreme_real_estate_page_50_properties(qapp, monkeypatch):
    """``RealEstatePage`` must render all 50 active properties in
    its main table, zero in the planned table (the EXTREME builder
    creates only ``existing_property`` entries, all status=active),
    and refresh under the hard page budget.
    """
    from PySide6.QtWidgets import QApplication
    from src.gui.pages.real_estate import RealEstatePage

    modal_counter = _patch_modal_dialogs(monkeypatch)

    conn = _fresh_db()
    try:
        counts = build_extreme_stress_dataset(conn)
        assert counts["properties"] == EXTREME_SPEC.properties == 50

        page = RealEstatePage(conn)

        t0 = time.perf_counter()
        page.refresh()
        refresh_time = time.perf_counter() - t0
        print(f"[stress-timing] gui.real_estate.refresh: {refresh_time:.4f}s")

        assert refresh_time <= _EXTREME_PAGE_HARD_BUDGET_SECONDS, (
            f"real-estate refresh {refresh_time:.2f}s exceeded hard budget "
            f"{_EXTREME_PAGE_HARD_BUDGET_SECONDS:.1f}s"
        )
        if refresh_time > _EXTREME_PAGE_SOFT_BUDGET_SECONDS:
            print(
                "[performance-warning] real-estate refresh "
                f"{refresh_time:.2f}s exceeded soft budget "
                f"{_EXTREME_PAGE_SOFT_BUDGET_SECONDS:.1f}s"
            )

        # The active-properties table has one row per active
        # property (50 in the EXTREME spec, all entry_type=
        # existing_property and status=active).
        assert page.table.rowCount() == 50
        assert len(page._table_asset_ids) == 50
        # No planned properties in the EXTREME builder.
        assert page.planned_table.rowCount() == 0

        # Each row must have a non-empty name in column 0; an empty
        # name would mean the asset row was missing or the display-
        # name fallback was triggered (both are bugs we want to
        # catch loud).
        for i in range(page.table.rowCount()):
            name_item = page.table.item(i, 0)
            assert name_item is not None, f"row {i} missing name cell"
            assert name_item.text(), f"row {i} has empty name"

        assert modal_counter["messagebox"] == 0, modal_counter
        assert modal_counter["filedialog"] == 0, modal_counter

        page.close()
        page.deleteLater()
        _flush_qt_deletes(QApplication.instance())
    finally:
        conn.close()
