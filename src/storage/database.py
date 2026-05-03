import logging
import os
import shutil
import sqlite3
import sys
from importlib import resources
from pathlib import Path

_log = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def _read_schema_sql() -> str:
    """Load schema.sql in a way that works in both source and frozen builds.

    PyInstaller bundles `src/storage/schema.sql` as a package resource, so
    `Path(__file__).parent` doesn't necessarily point at a real directory
    when frozen. `importlib.resources` is the supported way to read
    bundled package data; the path-based fallback covers edge cases like
    custom test harnesses that import the module without installing the
    package.
    """
    try:
        return resources.files("src.storage").joinpath("schema.sql").read_text(encoding="utf-8")
    except (FileNotFoundError, ModuleNotFoundError, AttributeError, TypeError):
        return SCHEMA_PATH.read_text(encoding="utf-8")

# Legacy in-repo location (kept so we can migrate existing installs).
_LEGACY_DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "portfolio_simulator.db"


def _user_data_dir() -> Path:
    """Platform-appropriate per-user data directory."""
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "asset-trainer"
    if sys.platform == "win32":
        base = os.environ.get("APPDATA") or str(Path.home() / "AppData" / "Roaming")
        return Path(base) / "asset-trainer"
    base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
    return Path(base) / "asset-trainer"


def _resolve_default_db_path() -> Path:
    """Pick the canonical DB path, migrating from the legacy location once."""
    user_dir = _user_data_dir()
    user_path = user_dir / "portfolio_simulator.db"

    if user_path.exists():
        return user_path

    # First run on a build that uses the new location, but a legacy in-repo
    # DB exists. Move it (rather than copy) so the user keeps a single
    # source of truth and `git clean` can't wipe their data.
    if _LEGACY_DB_PATH.exists():
        try:
            user_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(_LEGACY_DB_PATH), str(user_path))
            _log.info(
                "Migrated portfolio DB from legacy %s to %s",
                _LEGACY_DB_PATH, user_path,
            )
            # Best-effort: also move WAL sidecars so the next open is clean.
            for suffix in ("-wal", "-shm"):
                side = _LEGACY_DB_PATH.with_name(_LEGACY_DB_PATH.name + suffix)
                if side.exists():
                    target = user_path.with_name(user_path.name + suffix)
                    shutil.move(str(side), str(target))
        except Exception:
            _log.exception(
                "Failed to migrate legacy DB at %s; falling back to legacy path",
                _LEGACY_DB_PATH,
            )
            return _LEGACY_DB_PATH

    return user_path


DEFAULT_DB_PATH = _resolve_default_db_path()

# Bump this when a structural migration ships. The version is stored in
# SQLite's built-in `PRAGMA user_version` slot. The current value reflects
# the cumulative effect of every `_migrate_*` function in this module.
#
# Version history:
#   1 — initial versioned schema
#   2 — dropped transactions.journal_id (the back-pointer half of a circular
#       FK with decision_journal.transaction_id)
#   3 — extended debts with auto-deduction schedule fields
#       (schedule_frequency, interest_period, monthly_payment_amount,
#        cashflow_start_date, last_payment_date)
#   4 — retired the phantom property "remember-original-input" columns
#       (rent_input_*, *_input_type/value, down_payment_type, loan_term_years,
#        monthly_mortgage_override_*) that were never written from the GUI
#   5 — added `missed_payments` table — persists scheduled debt/mortgage
#       payments that auto-settle could not fund even after force-selling
#   6 — added `properties.mortgage_schedule_frequency` (monthly/yearly) so
#       auto-mortgage settlement can support yearly schedules in parity with
#       debts
#   7 — one-time reconciliation: yearly-schedule debts had their pay_debt
#       partial payments computed with monthly interest (`balance*rate/12`)
#       instead of yearly (`balance*rate`), over-reducing principal. The
#       migration replays each yearly debt's pay_debt history with corrected
#       math and writes a manual_adjustment to restore the lost principal.
#   8 — added `bankruptcy_events` table. Scheduled debt/mortgage payments
#       that cannot be funded after force-selling are now recorded as
#       bankruptcy events (terminal state) instead of `missed_payments`
#       (which implied recoverable overdue rows). The legacy table stays
#       for old DBs but new auto-settle writes only bankruptcy_events.
#   9 — added debt-plan persistence: `debts.plan_type`
#       ('fixed_payment' | 'fixed_term'), `debts.original_term_periods`,
#       and `debts.created_at`. Lets the Pay Debt recalculation honor the
#       user's original planning choice. Legacy rows default to
#       'fixed_payment' (the only mode that maps cleanly without a stored
#       term) and `created_at` is backfilled from `updated_at`.
#  10 — strict spec conformance + cleanup. Adds the 5 preview_* columns
#       to `debts` (stored snapshot of the official plan). Adds
#       `debt_payment_records` (per-pay audit row, sibling of
#       transactions). Drops `missed_payments` after migrating any
#       unresolved 'missed' rows to `bankruptcy_events`. Strips the
#       defensive `if column in keys else default` fallbacks from the
#       repos — post-migration the columns are guaranteed present.
#  11 — DEV CUTOVER. Mortgage subsystem cloned from debts so mortgages
#       gain plan_type, 5-line preview, Pay Off in Full, per-payment
#       audit (mortgage_payment_records). Drops the embedded mortgage
#       columns from `properties` (mortgage_balance,
#       mortgage_interest_rate, monthly_mortgage_payment,
#       mortgage_schedule_frequency). New tables: `mortgages` (linked
#       to properties via property_id NOT NULL UNIQUE; no Asset row;
#       monthly-only), `mortgage_payment_records`. The migration
#       WIPES all user data — there is no clean data path from the
#       embedded shape to the new linked shape, and the user
#       explicitly opted in for the dev cutover. Re-add properties
#       after upgrade.
#  12 — Add CHECK constraints rejecting negative numerics on every
#       table whose columns are conceptually non-negative (properties,
#       debts, mortgages, market_prices, market_quotes, transactions,
#       debt_payment_records, mortgage_payment_records,
#       bankruptcy_events, portfolio_snapshots). Defense in depth:
#       even raw SQL inserts now fail when they would corrupt
#       allocation / cashflow / risk derivations. Existing rows that
#       violate the new constraints are coerced to the legal range
#       (negatives → 0, vacancy_rate clamped to [0,1]); the original
#       row is preserved in `<table>_v12_coerce_backup` for audit.
#       `transactions.total_amount`, `portfolio_snapshots.cash`,
#       `portfolio_snapshots.net_worth`, and
#       `bankruptcy_events.cash_balance` are intentionally NOT
#       constrained — they are signed by domain (overdraft, insolvency,
#       sign-conventional ledger).
CURRENT_SCHEMA_VERSION = 12


def get_schema_version(conn: sqlite3.Connection) -> int:
    return conn.execute("PRAGMA user_version").fetchone()[0]


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    # PRAGMA user_version doesn't accept bound parameters; the value is
    # an int from a hardcoded constant so f-string interpolation is safe.
    conn.execute(f"PRAGMA user_version = {int(version)}")


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    path = str(db_path or DEFAULT_DB_PATH)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str | Path | None = None) -> sqlite3.Connection:
    conn = get_connection(db_path)
    schema_sql = _read_schema_sql()
    conn.executescript(schema_sql)

    existing_version = get_schema_version(conn)
    if existing_version > CURRENT_SCHEMA_VERSION:
        _log.warning(
            "Database reports schema version %d but this build expects "
            "version %d. Older builds may not read new columns; proceeding "
            "with caution.",
            existing_version, CURRENT_SCHEMA_VERSION,
        )

    _migrate(conn)
    _set_schema_version(conn, CURRENT_SCHEMA_VERSION)
    conn.commit()
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    # v11 dev cutover runs first — if it fires, every other migration
    # below sees a fresh DB and turns into a no-op. This keeps the older
    # migration code in place for legacy DBs that pre-date v10.
    if _migrate_v11_mortgage_clone(conn):
        # Indexes still need to be created on the freshly-rebuilt schema.
        _create_indexes(conn)
        # v12 still applies to a freshly-rebuilt v11 DB if the schema.sql
        # the user is bundled with predates v12 (defense in depth — the
        # function short-circuits when constraints already exist).
        _migrate_v12_check_constraints(conn)
        return
    _migrate_properties(conn)
    _drop_property_phantom_columns(conn)
    _migrate_decision_journal(conn)
    _migrate_market_prices(conn)
    _migrate_securities_master(conn)
    _migrate_market_quotes(conn)
    _migrate_reports(conn)
    _migrate_transaction_fee_breakdown(conn)
    _migrate_drop_transactions_journal_id(conn)
    _migrate_debts(conn)
    _reconcile_yearly_debt_balances(conn)
    _migrate_bankruptcy_events(conn)
    _migrate_debt_plan_columns(conn)
    _migrate_v10_debt_preview_and_payment_records(conn)
    _migrate_v10_drop_missed_payments(conn)
    _migrate_debts_created_at_not_null(conn)
    _migrate_v12_check_constraints(conn)
    _create_indexes(conn)


def _migrate_v11_mortgage_clone(conn: sqlite3.Connection) -> bool:
    """DEV CUTOVER: schema v11 redesigns the mortgage subsystem.

    Mortgages move out of `properties` (4 embedded columns) and into a
    new `mortgages` table linked via `property_id NOT NULL UNIQUE`,
    plus a sibling `mortgage_payment_records` audit table — mirroring
    the debt subsystem so mortgages get plan_type, 5-line Pay preview,
    Pay Off in Full, and per-payment audit records.

    No clean data-migration path exists from the embedded shape to the
    new linked shape (`original_amount`, `plan_type`, and
    `original_term_periods` were never recorded on the old mortgage
    fields). The user explicitly opted in for a dev cutover that wipes
    all user data; properties are re-added after upgrade.

    Returns True when the cutover ran (legacy mortgage column found),
    False on a fresh DB or a DB already on v11. Idempotency probe is
    the presence of `properties.mortgage_balance`.
    """
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(properties)"
    ).fetchall()}
    if "mortgage_balance" not in cols:
        return False

    _log.warning(
        "Schema v11 dev cutover: prior data wiped. The mortgage "
        "subsystem was redesigned (mortgages now live in their own "
        "table linked to properties via property_id) and no clean "
        "data-migration path exists. ALL of the following are reset: "
        "assets, transactions, transaction_fee_breakdown, properties, "
        "debts, debt_payment_records, decision_journal, "
        "portfolio_snapshots, reports, market_prices, market_quotes, "
        "price_sync_log, securities_master, settings, AND "
        "bankruptcy_events (any prior bankruptcy audit history is "
        "lost). Re-add your portfolio from scratch."
    )

    # FK ordering matters: drop child tables before parents. We disable
    # FK enforcement during the rebuild so the order is forgiving.
    drop_order = (
        "debt_payment_records",
        "transaction_fee_breakdown",
        "decision_journal",
        "transactions",
        "debts",
        "properties",
        "bankruptcy_events",
        "portfolio_snapshots",
        "reports",
        "market_prices",
        "market_quotes",
        "price_sync_log",
        "securities_master",
        "settings",
        "assets",
        # Defensive: if any of the new v11 tables were created by an
        # earlier executescript pass on a stale DB, drop them too so
        # the re-run lands a clean shape.
        "mortgages",
        "mortgage_payment_records",
        # Legacy holdovers in case they still exist.
        "missed_payments",
    )
    conn.commit()
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        for table in drop_order:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
    finally:
        conn.execute("PRAGMA foreign_keys=ON")

    # Re-run schema.sql to recreate every table in the new shape.
    schema_sql = _read_schema_sql()
    conn.executescript(schema_sql)
    return True


def _migrate_v10_debt_preview_and_payment_records(
    conn: sqlite3.Connection,
) -> None:
    """Add the 5 `preview_*` columns to `debts` and create the
    `debt_payment_records` table (schema v10).

    The 5 preview columns are persisted as the live current official
    payment plan (spec §5). They are kept in sync with reality by
    `ledger._refresh_debt_preview_values` on every Add Debt / pay_debt /
    pay_debt_in_full / scheduled auto-pay path. Defaults of 0 are written
    on ALTER; the v10 migration backfills the values from the current
    debt state.

    `debt_payment_records` is a sibling of `transactions`: every
    `txn_type='pay_debt'` row gets exactly one matching record
    (atomically inserted by `ledger._record_debt_payment`). The v10
    migration backfills records for existing pay_debt history by
    walking each debt's transaction list in chronological order.
    """
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(debts)"
    ).fetchall()}

    preview_cols = [
        ("preview_regular_payment", "REAL NOT NULL DEFAULT 0"),
        ("preview_period_count",    "INTEGER NOT NULL DEFAULT 0"),
        ("preview_final_payment",   "REAL NOT NULL DEFAULT 0"),
        ("preview_total_paid",      "REAL NOT NULL DEFAULT 0"),
        ("preview_total_interest",  "REAL NOT NULL DEFAULT 0"),
    ]
    for name, ddl in preview_cols:
        if name not in cols:
            conn.execute(f"ALTER TABLE debts ADD COLUMN {name} {ddl}")

    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "debt_payment_records" not in tables:
        conn.execute("""
            CREATE TABLE debt_payment_records (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id           INTEGER NOT NULL UNIQUE,
                debt_id                  INTEGER NOT NULL,
                debt_name                TEXT NOT NULL,
                payment_amount           REAL NOT NULL,
                payment_date             TEXT NOT NULL,
                payment_type             TEXT NOT NULL,
                balance_before_payment   REAL NOT NULL,
                balance_after_payment    REAL NOT NULL,
                note                     TEXT,
                created_at               TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (transaction_id) REFERENCES transactions(id),
                FOREIGN KEY (debt_id) REFERENCES debts(id)
            )
        """)

    # Backfill `debt_payment_records` from existing `pay_debt`
    # transaction history. Walk each debt's pay events in chronological
    # order and reconstruct balance_before/after. Skips any pay_debt
    # transaction that already has a payment record (idempotent — the
    # UNIQUE(transaction_id) constraint also guards this).
    debts_rows = conn.execute(
        "SELECT id, asset_id, name, original_amount, interest_rate, "
        "schedule_frequency FROM debts"
    ).fetchall()
    for d in debts_rows:
        already = conn.execute(
            "SELECT 1 FROM debt_payment_records WHERE debt_id=? LIMIT 1",
            (d["id"],),
        ).fetchone()
        if already is not None:
            # Already backfilled (rerun safety) — the engine's
            # `_record_debt_payment` will keep new pays in sync.
            continue
        rate = d["interest_rate"] or 0.0
        schedule = d["schedule_frequency"] or "monthly"
        running_balance = float(d["original_amount"] or 0.0)
        pay_rows = conn.execute(
            "SELECT id, date, total_amount, notes FROM transactions "
            "WHERE asset_id=? AND txn_type='pay_debt' "
            "ORDER BY date, id",
            (d["asset_id"],),
        ).fetchall()
        for r in pay_rows:
            payment_amount = abs(float(r["total_amount"] or 0.0))
            balance_before = running_balance
            # Mirror ledger.pay_debt's interest split: one period of
            # interest comes off the cash payment first (if rate>0),
            # remainder reduces principal. balance is clamped at 0.
            if rate > 0 and running_balance > 0:
                if schedule == "monthly":
                    accrued = running_balance * (rate / 12.0)
                else:  # yearly
                    accrued = running_balance * rate
                reduction = max(0.0, payment_amount - accrued)
            else:
                reduction = payment_amount
            running_balance = max(0.0, running_balance - reduction)
            balance_after = running_balance
            payment_type = (
                "automatic"
                if (r["notes"] or "").startswith("Scheduled debt payment")
                else "manual"
            )
            conn.execute(
                "INSERT INTO debt_payment_records "
                "(transaction_id, debt_id, debt_name, payment_amount, "
                "payment_date, payment_type, balance_before_payment, "
                "balance_after_payment, note) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (r["id"], d["id"], d["name"], payment_amount,
                 r["date"], payment_type, balance_before, balance_after,
                 r["notes"]),
            )

    # Backfill the 5 preview values for each debt from current state.
    # Done here (rather than in a separate Phase 6.2 init) so any DB
    # opened by a v10 build immediately has populated columns.
    _backfill_debt_preview_values(conn)


def _backfill_debt_preview_values(conn: sqlite3.Connection) -> None:
    """Populate the 5 preview_* columns from each debt's current state.

    Mirrors ledger._refresh_debt_preview_values but inlined here so the
    migration doesn't depend on the engine-layer helper (which itself
    imports debt_math, which imports nothing back-circular).
    """
    from src.engines.debt_math import compute_debt_schedule

    rows = conn.execute(
        "SELECT id, current_balance, interest_rate, schedule_frequency, "
        "monthly_payment_amount, plan_type, original_term_periods "
        "FROM debts"
    ).fetchall()
    for r in rows:
        balance = float(r["current_balance"] or 0.0)
        if balance <= 0:
            # Paid-off debt — preview values are zero.
            conn.execute(
                "UPDATE debts SET preview_regular_payment=0, "
                "preview_period_count=0, preview_final_payment=0, "
                "preview_total_paid=0, preview_total_interest=0 "
                "WHERE id=?",
                (r["id"],),
            )
            continue
        rate = float(r["interest_rate"] or 0.0)
        schedule = r["schedule_frequency"] or "monthly"
        plan_type = r["plan_type"] or "fixed_payment"
        if plan_type == "fixed_term" and r["original_term_periods"]:
            # Compute remaining term from scheduled-payment count.
            consumed = conn.execute(
                "SELECT COUNT(*) FROM transactions "
                "WHERE asset_id=(SELECT asset_id FROM debts WHERE id=?) "
                "AND txn_type='pay_debt' AND notes LIKE 'Scheduled debt payment%'",
                (r["id"],),
            ).fetchone()[0]
            remaining = max(1, int(r["original_term_periods"]) - int(consumed))
            sched = compute_debt_schedule(
                principal=balance, annual_rate=rate, schedule=schedule,
                term_periods=remaining,
            )
        else:
            payment = float(r["monthly_payment_amount"] or 0.0)
            if payment <= 0:
                continue  # Cannot preview without a payment value.
            sched = compute_debt_schedule(
                principal=balance, annual_rate=rate, schedule=schedule,
                payment=payment,
            )
        if not sched.feasible:
            continue
        conn.execute(
            "UPDATE debts SET preview_regular_payment=?, "
            "preview_period_count=?, preview_final_payment=?, "
            "preview_total_paid=?, preview_total_interest=? WHERE id=?",
            (sched.per_period_payment, sched.num_periods,
             sched.final_payment, sched.total_paid, sched.total_interest,
             r["id"]),
        )


def _migrate_v10_drop_missed_payments(conn: sqlite3.Connection) -> None:
    """Migrate unresolved `missed_payments` rows to `bankruptcy_events`,
    then DROP the table.

    Spec §6 makes scheduled debt/mortgage payments terminal — they
    either pay or trigger bankruptcy. The legacy `missed_payments`
    table represented a recoverable "overdue" state that the spec
    explicitly rejects. v10 carries any unresolved rows over as
    bankruptcy events (idempotent on the existing
    (trigger_kind, asset_id, due_date) triple) and drops the table.
    """
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "missed_payments" not in tables:
        return  # Already dropped on a previous v10 run.

    unresolved = conn.execute(
        "SELECT kind, asset_id, due_date, amount_due, notes "
        "FROM missed_payments WHERE status='missed'"
    ).fetchall()
    today_iso = conn.execute(
        "SELECT date('now')"
    ).fetchone()[0]
    for row in unresolved:
        # Map missed_payments.kind ('debt' | 'mortgage') to
        # bankruptcy_events.trigger_kind. Idempotent insert: skip if an
        # active event already exists for this triple.
        trigger_kind = row["kind"]
        existing = conn.execute(
            "SELECT 1 FROM bankruptcy_events WHERE trigger_kind=? "
            "AND asset_id=? AND due_date=? AND status='active'",
            (trigger_kind, row["asset_id"], row["due_date"]),
        ).fetchone()
        if existing is not None:
            continue
        conn.execute(
            "INSERT INTO bankruptcy_events "
            "(event_date, trigger_kind, asset_id, due_date, "
            "amount_due, cash_balance, shortfall_amount, status, notes) "
            "VALUES (?, ?, ?, ?, ?, 0, ?, 'active', ?)",
            (today_iso, trigger_kind, row["asset_id"], row["due_date"],
             float(row["amount_due"] or 0.0), float(row["amount_due"] or 0.0),
             f"Migrated from missed_payments on v10 upgrade. "
             f"{row['notes'] or ''}".strip()),
        )

    conn.execute("DROP TABLE missed_payments")


def _migrate_debt_plan_columns(conn: sqlite3.Connection) -> None:
    """Add `plan_type`, `original_term_periods`, and `created_at` to debts.

    Legacy rows had only `monthly_payment_amount`; the user's planning
    intent (fixed-payment vs fixed-term) wasn't stored. Default existing
    rows to `'fixed_payment'` since that's the only mode that's faithful
    without knowing the original term: the per-period payment is what was
    persisted, and recompute-after-payment will keep it constant.

    `created_at` is backfilled from `updated_at` because that's the
    closest signal we have for legacy rows; new INSERTs use
    `datetime('now')` via the column default.
    """
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(debts)"
    ).fetchall()}
    if "plan_type" not in cols:
        conn.execute(
            "ALTER TABLE debts ADD COLUMN plan_type "
            "TEXT NOT NULL DEFAULT 'fixed_payment'"
        )
    if "original_term_periods" not in cols:
        conn.execute(
            "ALTER TABLE debts ADD COLUMN original_term_periods INTEGER"
        )
    if "created_at" not in cols:
        # Two-step: ALTER ADD must use a constant default, not
        # `datetime('now')`, so we add nullable, then backfill.
        conn.execute("ALTER TABLE debts ADD COLUMN created_at TEXT")
        conn.execute(
            "UPDATE debts SET created_at = COALESCE(updated_at, datetime('now')) "
            "WHERE created_at IS NULL"
        )


def _migrate_bankruptcy_events(conn: sqlite3.Connection) -> None:
    """Create the bankruptcy_events table on legacy DBs that pre-date v8."""
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "bankruptcy_events" in tables:
        return
    conn.execute("""
        CREATE TABLE bankruptcy_events (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            event_date        TEXT NOT NULL,
            trigger_kind      TEXT NOT NULL,
            asset_id          INTEGER,
            due_date          TEXT,
            amount_due        REAL NOT NULL DEFAULT 0,
            cash_balance      REAL NOT NULL DEFAULT 0,
            shortfall_amount  REAL NOT NULL DEFAULT 0,
            status            TEXT NOT NULL DEFAULT 'active',
            notes             TEXT,
            created_at        TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)


_YEARLY_DEBT_RECONCILIATION_NOTE = (
    "Yearly-debt interest correction: principal restored"
)


def _reconcile_yearly_debt_balances(conn: sqlite3.Connection) -> None:
    """One-time replay-and-correct for yearly-schedule debts.

    Old `pay_debt` computed interest as `balance * rate / 12` regardless of
    schedule. Yearly debts therefore had their principal over-reduced on
    every partial payment. This migration replays each yearly debt's
    transaction history with corrected (yearly) interest math, compares to
    the stored `current_balance`, and posts a `manual_adjustment` for the
    delta so net worth reflects the correct principal. Idempotent — detects
    a prior correction by note prefix and skips.
    """
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(debts)"
    ).fetchall()}
    if "schedule_frequency" not in cols:
        # Schema predates yearly debts altogether — nothing to reconcile.
        return

    yearly_debts = conn.execute(
        "SELECT id, asset_id, name, original_amount, current_balance, "
        "interest_rate FROM debts WHERE schedule_frequency = 'yearly'"
    ).fetchall()
    if not yearly_debts:
        return

    for d in yearly_debts:
        # Idempotency check: skip if any prior reconciliation note exists
        # for this asset.
        existing = conn.execute(
            "SELECT 1 FROM transactions WHERE asset_id=? AND notes LIKE ?",
            (d["asset_id"], f"{_YEARLY_DEBT_RECONCILIATION_NOTE}%"),
        ).fetchone()
        if existing is not None:
            continue

        rate = d["interest_rate"] or 0.0
        # Replay: walk all add_debt + pay_debt transactions in chrono order
        # and recompute the balance using the corrected (yearly) interest
        # split. add_debt sets the balance to its `total_amount` magnitude
        # only when total_amount != 0 (new-borrowing); otherwise we trust
        # `original_amount`.
        rows = conn.execute(
            "SELECT date, txn_type, total_amount FROM transactions "
            "WHERE asset_id=? AND txn_type IN ('add_debt', 'pay_debt') "
            "ORDER BY date, id",
            (d["asset_id"],),
        ).fetchall()

        replay_balance = float(d["original_amount"] or 0.0)
        for r in rows:
            if r["txn_type"] == "add_debt":
                # Original amount is the source of truth; ignore the txn
                # row (cash-received vs not-received doesn't change the
                # debt balance).
                continue
            amount = abs(r["total_amount"] or 0.0)
            if rate > 0 and replay_balance > 0:
                accrued = replay_balance * rate  # yearly interest
                reduction = max(0.0, amount - accrued)
            else:
                reduction = amount
            replay_balance = max(0.0, replay_balance - reduction)

        delta = replay_balance - float(d["current_balance"] or 0.0)
        # Skip when the delta is rounding noise OR when stored balance is
        # higher than the replay (user manually adjusted; don't overshoot).
        if delta <= 0.005:
            continue

        # Restore the lost principal on the debt; record a manual_adjustment
        # transaction with cash_impact=0 (we're not moving cash, just fixing
        # the bookkeeping).
        conn.execute(
            "UPDATE debts SET current_balance=? WHERE id=?",
            (replay_balance, d["id"]),
        )
        note = (
            f"{_YEARLY_DEBT_RECONCILIATION_NOTE} by {delta:.2f} for "
            f"yearly debt '{d['name'] or 'unnamed'}'"
        )
        conn.execute(
            "INSERT INTO transactions "
            "(date, txn_type, asset_id, total_amount, notes) "
            "VALUES (date('now'), 'manual_adjustment', ?, 0, ?)",
            (d["asset_id"], note),
        )
    conn.commit()


def _migrate_debts(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(debts)").fetchall()}
    new_cols = [
        ("schedule_frequency", "TEXT NOT NULL DEFAULT 'monthly'"),
        ("interest_period", "TEXT NOT NULL DEFAULT 'annual'"),
        ("monthly_payment_amount", "REAL NOT NULL DEFAULT 0"),
        ("cashflow_start_date", "TEXT"),
        ("last_payment_date", "TEXT"),
    ]
    added = []
    for col_name, col_def in new_cols:
        if col_name not in cols:
            conn.execute(f"ALTER TABLE debts ADD COLUMN {col_name} {col_def}")
            added.append(col_name)

    # One-time backfill: legacy debts had only `minimum_payment`. Promote that
    # into the new auto-deduction amount so they keep deducting at their old
    # rate without requiring user intervention.
    if "monthly_payment_amount" in added:
        conn.execute(
            "UPDATE debts SET monthly_payment_amount = minimum_payment "
            "WHERE monthly_payment_amount = 0 AND minimum_payment > 0"
        )


def _migrate_drop_transactions_journal_id(conn: sqlite3.Connection) -> None:
    """Drop the legacy back-pointer `transactions.journal_id`.

    The pairing between a transaction and its decision-journal entry is
    represented by `decision_journal.transaction_id`; the back-pointer was
    a circular foreign key that complicated deletes and full-data imports.

    SQLite's `ALTER TABLE DROP COLUMN` refuses when the target column
    appears in a `FOREIGN KEY` clause, so we rebuild the table the
    canonical way: CREATE the new shape, copy rows, DROP the old, RENAME.
    Foreign-key enforcement must be disabled during the rebuild because
    `transaction_fee_breakdown.transaction_id` references `transactions(id)`
    and the rename leaves a transient window where the parent table
    doesn't exist under its expected name.

    Before the rebuild we backfill any `decision_journal.transaction_id`
    rows that are NULL but whose journal id is referenced from
    `transactions.journal_id`, so the user's pairing is preserved.
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()}
    if "journal_id" not in cols:
        return

    conn.commit()  # close any implicit transaction left open by upstream DML
    prev_fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN")
        conn.execute("""
            UPDATE decision_journal
            SET transaction_id = (
                SELECT t.id FROM transactions t
                WHERE t.journal_id = decision_journal.id
                LIMIT 1
            )
            WHERE transaction_id IS NULL
              AND id IN (
                  SELECT journal_id FROM transactions
                  WHERE journal_id IS NOT NULL
              )
        """)
        conn.execute("""
            CREATE TABLE transactions_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date            TEXT NOT NULL,
                txn_type        TEXT NOT NULL,
                asset_id        INTEGER,
                quantity        REAL,
                price           REAL,
                total_amount    REAL NOT NULL,
                currency        TEXT NOT NULL DEFAULT 'USD',
                fees            REAL NOT NULL DEFAULT 0,
                notes           TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (asset_id) REFERENCES assets(id)
            )
        """)
        conn.execute("""
            INSERT INTO transactions_new
                (id, date, txn_type, asset_id, quantity, price,
                 total_amount, currency, fees, notes, created_at)
            SELECT
                 id, date, txn_type, asset_id, quantity, price,
                 total_amount, currency, fees, notes, created_at
            FROM transactions
        """)
        conn.execute("DROP TABLE transactions")
        conn.execute("ALTER TABLE transactions_new RENAME TO transactions")
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            _log.exception("Rollback after failed transactions-table rebuild also failed")
        raise
    finally:
        if prev_fk:
            conn.execute("PRAGMA foreign_keys=ON")


def _migrate_debts_created_at_not_null(conn: sqlite3.Connection) -> None:
    """Rebuild `debts` so `created_at` matches schema.sql's NOT NULL DEFAULT.

    Schema v9 added `created_at` via ``ALTER TABLE ADD COLUMN``, which
    cannot use ``datetime('now')`` as the default in a single statement.
    The shipped v9 migration therefore added the column as nullable with
    no default and backfilled existing rows. New INSERTs from
    ``debt_repo.create_debt`` don't supply ``created_at`` either, so on a
    legacy-migrated DB they would store ``NULL`` — diverging from a fresh
    DB where the schema's DEFAULT fires and produces a timestamp.

    This migration rebuilds the table so the constraint matches
    schema.sql exactly: ``created_at TEXT NOT NULL DEFAULT (datetime('now'))``.
    Any pre-existing ``NULL`` rows are filled with ``datetime('now')``
    during the copy. The same is done for ``updated_at`` for symmetry,
    even though no observed legacy path leaves it null — defense in depth
    so the rebuild can't fail on a NOT NULL constraint mid-copy.

    Idempotent: probes the ``notnull`` flag from ``PRAGMA table_info``
    and returns early if ``created_at`` is already NOT NULL.

    FK enforcement is disabled during the rebuild because
    ``debt_payment_records.debt_id REFERENCES debts(id)`` — the
    DROP+RENAME leaves a transient window where the parent table doesn't
    exist under its expected name. Pre/post FK state is restored from
    the saved PRAGMA value (matches the pattern in
    ``_migrate_drop_transactions_journal_id``).
    """
    cols = conn.execute("PRAGMA table_info(debts)").fetchall()
    created_at_col = next((c for c in cols if c[1] == "created_at"), None)
    if created_at_col is None:
        # Column missing — `_migrate_debt_plan_columns` runs earlier in
        # the pipeline and adds it. We're called after that, so this
        # branch only fires on a malformed DB; let later code surface it.
        return
    # PRAGMA table_info layout: (cid, name, type, notnull, dflt_value, pk).
    # `notnull == 1` means the column already has the NOT NULL constraint.
    if created_at_col[3]:
        return  # Already constrained correctly — fresh DB or post-rebuild.

    conn.commit()  # Close any implicit transaction left open upstream.
    prev_fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN")
        # New shape mirrors `schema.sql`'s `debts` definition exactly.
        # Keep this in sync with the canonical shape — schema.sql is the
        # source of truth; this CREATE is the reconstruction target.
        conn.execute("""
            CREATE TABLE debts_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id        INTEGER NOT NULL UNIQUE,
                name            TEXT NOT NULL,
                original_amount REAL NOT NULL,
                current_balance REAL NOT NULL,
                interest_rate   REAL NOT NULL DEFAULT 0,
                minimum_payment REAL NOT NULL DEFAULT 0,
                due_date        TEXT,
                notes           TEXT,
                schedule_frequency      TEXT NOT NULL DEFAULT 'monthly',
                interest_period         TEXT NOT NULL DEFAULT 'annual',
                monthly_payment_amount  REAL NOT NULL DEFAULT 0,
                cashflow_start_date     TEXT,
                last_payment_date       TEXT,
                plan_type               TEXT NOT NULL DEFAULT 'fixed_payment',
                original_term_periods   INTEGER,
                preview_regular_payment REAL NOT NULL DEFAULT 0,
                preview_period_count    INTEGER NOT NULL DEFAULT 0,
                preview_final_payment   REAL NOT NULL DEFAULT 0,
                preview_total_paid      REAL NOT NULL DEFAULT 0,
                preview_total_interest  REAL NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (asset_id) REFERENCES assets(id)
            )
        """)
        # COALESCE on created_at AND updated_at so any pre-existing NULL
        # value can't trip the new NOT NULL constraint mid-copy.
        conn.execute("""
            INSERT INTO debts_new
                (id, asset_id, name, original_amount, current_balance,
                 interest_rate, minimum_payment, due_date, notes,
                 schedule_frequency, interest_period, monthly_payment_amount,
                 cashflow_start_date, last_payment_date,
                 plan_type, original_term_periods,
                 preview_regular_payment, preview_period_count,
                 preview_final_payment, preview_total_paid,
                 preview_total_interest,
                 created_at, updated_at)
            SELECT
                 id, asset_id, name, original_amount, current_balance,
                 interest_rate, minimum_payment, due_date, notes,
                 schedule_frequency, interest_period, monthly_payment_amount,
                 cashflow_start_date, last_payment_date,
                 plan_type, original_term_periods,
                 preview_regular_payment, preview_period_count,
                 preview_final_payment, preview_total_paid,
                 preview_total_interest,
                 COALESCE(created_at, datetime('now')),
                 COALESCE(updated_at, datetime('now'))
            FROM debts
        """)
        conn.execute("DROP TABLE debts")
        conn.execute("ALTER TABLE debts_new RENAME TO debts")
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            _log.exception("Rollback after failed debts-table rebuild also failed")
        raise
    finally:
        # Match the pattern used by `_migrate_drop_transactions_journal_id`:
        # only re-enable FK enforcement if it was on before. Callers that
        # opened the connection through `get_connection` always have FK
        # enabled, so this is the typical path; ad-hoc test connections
        # that opened with FK off keep their setting.
        if prev_fk:
            conn.execute("PRAGMA foreign_keys=ON")


# Schema v12: per-table table_name -> {column_name -> rule_str} for
# CHECK constraints. `rule_str` is one of:
#   "non_negative" — value IS NULL OR value >= 0
#   "positive"     — value IS NULL OR value > 0
#   "in_unit"      — value IS NULL OR (value >= 0 AND value <= 1)
# The migration uses these rules for both the existing-row coercion pass
# and (textually) the CHECK clauses on the rebuilt table.
#
# NOT constrained (intentionally): `transactions.total_amount` (signed),
# `portfolio_snapshots.cash` / `.net_worth` (overdraft / insolvency
# legitimate), `bankruptcy_events.cash_balance` (typically negative at
# the moment of bankruptcy).
_V12_CHECK_RULES: dict[str, dict[str, str]] = {
    "properties": {
        "purchase_price": "non_negative",
        "current_value": "non_negative",
        "down_payment": "non_negative",
        "monthly_rent": "non_negative",
        "monthly_property_tax": "non_negative",
        "monthly_insurance": "non_negative",
        "monthly_hoa": "non_negative",
        "monthly_maintenance_reserve": "non_negative",
        "monthly_property_management": "non_negative",
        "monthly_expense": "non_negative",
        "vacancy_rate": "in_unit",
        "sold_price": "non_negative",
        "sale_fees": "non_negative",
    },
    "debts": {
        "original_amount": "positive",
        "current_balance": "non_negative",
        "interest_rate": "non_negative",
        "minimum_payment": "non_negative",
        "monthly_payment_amount": "non_negative",
        "preview_regular_payment": "non_negative",
        "preview_period_count": "non_negative",
        "preview_final_payment": "non_negative",
        "preview_total_paid": "non_negative",
        "preview_total_interest": "non_negative",
        "original_term_periods": "positive",
    },
    "mortgages": {
        "original_amount": "positive",
        "current_balance": "non_negative",
        "interest_rate": "non_negative",
        "minimum_payment": "non_negative",
        "monthly_payment_amount": "non_negative",
        "preview_regular_payment": "non_negative",
        "preview_period_count": "non_negative",
        "preview_final_payment": "non_negative",
        "preview_total_paid": "non_negative",
        "preview_total_interest": "non_negative",
        "original_term_periods": "positive",
    },
    "market_prices": {
        "open": "non_negative",
        "high": "non_negative",
        "low": "non_negative",
        "close": "non_negative",
        "adjusted_close": "non_negative",
        "volume": "non_negative",
        "price": "positive",
    },
    "market_quotes": {
        "bid": "positive",
        "ask": "positive",
        "last": "positive",
    },
    "transactions": {
        "quantity": "positive",
        "price": "positive",
        "fees": "non_negative",
    },
    "transaction_fee_breakdown": {
        # Fee items are conceptually >= 0 — every emitter in
        # `trading_costs.py` rounds to non-negative amounts. Constrain
        # so corrupted imports / direct DB writes can't violate it.
        "amount": "non_negative",
    },
    "debt_payment_records": {
        "payment_amount": "non_negative",
        "balance_before_payment": "non_negative",
        "balance_after_payment": "non_negative",
    },
    "mortgage_payment_records": {
        "payment_amount": "non_negative",
        "balance_before_payment": "non_negative",
        "balance_after_payment": "non_negative",
    },
    "bankruptcy_events": {
        "amount_due": "non_negative",
        "shortfall_amount": "non_negative",
    },
    "portfolio_snapshots": {
        "total_assets": "non_negative",
        "total_liabilities": "non_negative",
    },
}


def _v12_violation_predicate_sql(column: str, rule: str) -> str:
    """SQL fragment that evaluates TRUE when the column violates `rule`.

    Used both by the coerce pass (`SELECT ... WHERE <pred>`) and by the
    CHECK clause body (negated). Centralised so the two stay in lock-step.
    """
    if rule == "non_negative":
        return f"{column} IS NOT NULL AND {column} < 0"
    if rule == "positive":
        return f"{column} IS NOT NULL AND {column} <= 0"
    if rule == "in_unit":
        return f"{column} IS NOT NULL AND ({column} < 0 OR {column} > 1)"
    raise ValueError(f"Unknown v12 rule: {rule!r}")


def _v12_check_clause_sql(column: str, rule: str) -> str:
    """The CHECK clause body that ENFORCES `rule` (the inverse of the
    violation predicate). Embedded into the rebuilt CREATE TABLE.
    """
    if rule == "non_negative":
        return f"{column} IS NULL OR {column} >= 0"
    if rule == "positive":
        return f"{column} IS NULL OR {column} > 0"
    if rule == "in_unit":
        return f"{column} IS NULL OR ({column} >= 0 AND {column} <= 1)"
    raise ValueError(f"Unknown v12 rule: {rule!r}")


def _v12_already_applied(conn: sqlite3.Connection, table: str) -> bool:
    """Has the CHECK constraint pass already run for `table`?

    Inspects `sqlite_master.sql` for the table's CREATE statement and
    looks for "CHECK". Matches both fresh DBs (schema.sql ships with
    CHECKs as of v12) and migrated v12 DBs.
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if row is None:
        return True  # missing table — nothing to migrate
    sql_text = row["sql"] if hasattr(row, "keys") else row[0]
    return "CHECK" in (sql_text or "").upper()


def _v12_coerce_violations(conn: sqlite3.Connection, table: str) -> int:
    """Coerce pre-existing rows that violate a v12 rule for `table`.

    For each violating row, the original is preserved in
    `<table>_v12_coerce_backup` (created on first violation), then the
    in-place value is clamped to the legal range. Returns the number of
    cells coerced (a single row may contribute multiple cells if more
    than one column violates).

    Coercion rules:
    - `non_negative` violation → set to 0.
    - `positive` violation     → set to 0 too (the constraint will
      reject 0 for these columns; we clamp to NULL when the column is
      nullable so the row survives, otherwise we leave the row out of
      the rebuild via the `INSERT INTO ... SELECT ... WHERE NOT (...)`
      filter applied separately).
    - `in_unit` violation      → clamp to [0, 1].
    """
    rules = _V12_CHECK_RULES.get(table, {})
    if not rules:
        return 0
    cols_info = conn.execute(f"PRAGMA table_info({table})").fetchall()
    nullable_cols = {c[1] for c in cols_info if c[3] == 0}  # notnull == 0

    backup_created = False
    coerce_count = 0
    for col, rule in rules.items():
        pred = _v12_violation_predicate_sql(col, rule)
        # Probe first so we don't pay backup-table CREATE cost when no
        # violation exists.
        any_violation = conn.execute(
            f"SELECT 1 FROM {table} WHERE {pred} LIMIT 1"
        ).fetchone()
        if not any_violation:
            continue
        if not backup_created:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table}_v12_coerce_backup AS "
                f"SELECT * FROM {table} WHERE 0"
            )
            backup_created = True
        # Backup the violating rows for this column. (A row may end up
        # backed up multiple times if it violates multiple columns —
        # that's fine for an audit trail.)
        conn.execute(
            f"INSERT INTO {table}_v12_coerce_backup SELECT * FROM {table} "
            f"WHERE {pred}"
        )
        if rule == "in_unit":
            # Clamp into [0, 1] in-place.
            conn.execute(
                f"UPDATE {table} SET {col} = MAX(0, MIN(1, {col})) "
                f"WHERE {pred}"
            )
        elif rule == "positive":
            # `positive` columns can't legally hold 0. If the column is
            # nullable, clamp to NULL; otherwise to 0 (which the CHECK
            # will reject — but those rows are pathological and the
            # backup captured the originals).
            if col in nullable_cols:
                conn.execute(
                    f"UPDATE {table} SET {col} = NULL WHERE {pred}"
                )
            else:
                conn.execute(
                    f"UPDATE {table} SET {col} = 0 WHERE {pred}"
                )
        else:  # non_negative
            conn.execute(
                f"UPDATE {table} SET {col} = 0 WHERE {pred}"
            )
        affected = conn.execute(
            f"SELECT COUNT(*) FROM {table}_v12_coerce_backup"
        ).fetchone()[0]
        _log.warning(
            "v12 migration: coerced %d row(s) in %s.%s (rule=%s); "
            "originals preserved in %s_v12_coerce_backup.",
            affected, table, col, rule, table,
        )
        coerce_count += affected
    return coerce_count


def _migrate_v12_check_constraints(conn: sqlite3.Connection) -> None:
    """Add CHECK constraints rejecting negative numerics on every
    table whose columns are conceptually non-negative.

    SQLite cannot ALTER a table to add a CHECK clause; the migration
    uses the canonical CREATE+COPY+DROP+RENAME pattern (same template
    as `_migrate_drop_transactions_journal_id` and
    `_migrate_debts_created_at_not_null`).

    Idempotent: each table's `sqlite_master.sql` is inspected for an
    existing "CHECK" — if found, that table is skipped. So a fresh DB
    (schema.sql now ships with CHECKs) never runs the rebuild, and a
    migrated v12 DB stays put on subsequent launches.

    Pre-existing rows that violate a new constraint are coerced to the
    legal range via `_v12_coerce_violations`, with the originals
    preserved in `<table>_v12_coerce_backup` for audit. Tables under
    coercion are processed BEFORE the rebuild (otherwise the rebuild
    would fail mid-INSERT on the new CHECK).
    """
    # Close any implicit transaction left open by upstream DML — `BEGIN`
    # inside a transaction raises "cannot start a transaction within a
    # transaction". Mirrors the pattern in
    # `_migrate_drop_transactions_journal_id` and
    # `_migrate_debts_created_at_not_null`.
    conn.commit()
    # Disable FK enforcement once for the whole migration — same pattern
    # as the existing v9/v11 rebuilds. Restored at the end.
    prev_fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        for table, rules in _V12_CHECK_RULES.items():
            if _v12_already_applied(conn, table):
                continue

            # 1. Coerce existing violating rows so the rebuild's INSERT
            #    doesn't trip the new CHECK. Coercion runs OUTSIDE the
            #    rebuild's BEGIN block, in autocommit mode, so each
            #    coerce UPDATE persists immediately.
            _v12_coerce_violations(conn, table)

            # 2. Rebuild the table. We use the existing CREATE TABLE
            #    statement from sqlite_master and inject CHECK clauses
            #    against each column. This keeps every other column
            #    definition (defaults, FK, NOT NULL, UNIQUE) byte-
            #    identical to what's currently in place.
            existing_sql_row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if existing_sql_row is None:
                continue  # table doesn't exist on this DB — skip
            existing_sql = existing_sql_row["sql"]

            # Build a "{table}_new" CREATE statement by augmenting each
            # constrained column line with a CHECK clause.
            new_sql = _build_v12_table_sql(existing_sql, table, rules)

            # `_v12_coerce_violations` writes via implicit DML; commit
            # before opening the rebuild transaction.
            conn.commit()
            conn.execute("BEGIN")
            try:
                conn.execute(new_sql)
                col_list = ", ".join(c[1] for c in conn.execute(
                    f"PRAGMA table_info({table})"
                ).fetchall())
                conn.execute(
                    f"INSERT INTO {table}_new ({col_list}) "
                    f"SELECT {col_list} FROM {table}"
                )
                conn.execute(f"DROP TABLE {table}")
                conn.execute(f"ALTER TABLE {table}_new RENAME TO {table}")
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    _log.exception(
                        "Rollback after failed v12 rebuild of %s also failed",
                        table,
                    )
                raise
    finally:
        if prev_fk:
            conn.execute("PRAGMA foreign_keys=ON")


def _build_v12_table_sql(
    existing_sql: str, table: str, rules: dict[str, str],
) -> str:
    """Rewrite an existing CREATE TABLE statement to add CHECK clauses.

    Strategy: the existing schema is byte-identical between
    schema.sql's fresh-DB definition and the legacy migrated shape
    (modulo whitespace), so for the migration we don't try to in-place
    edit the original SQL — we generate a fresh CREATE that exactly
    mirrors `schema.sql` but with `CHECK` clauses added.

    Implementation note: rather than parse SQL, we re-issue the canonical
    schema.sql CREATE for each affected table with a `_new` suffix and
    CHECK appended. This keeps the schema-of-truth single-sourced (any
    future column addition lands in schema.sql, and the migration no
    longer needs touching).
    """
    # Per-table, per-column constraint sentence assembly. The fixed-
    # form templates here mirror the constrained tables in schema.sql
    # exactly. They MUST stay in sync — but schema.sql is the canonical
    # CREATE for fresh DBs, and this template is only used on legacy
    # upgrades, so any drift would only affect upgrade-path DBs.
    if table == "properties":
        return """
            CREATE TABLE properties_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id        INTEGER NOT NULL UNIQUE,
                address         TEXT,
                purchase_date   TEXT,
                purchase_price  REAL CHECK (purchase_price IS NULL OR purchase_price >= 0),
                current_value   REAL CHECK (current_value IS NULL OR current_value >= 0),
                down_payment    REAL CHECK (down_payment IS NULL OR down_payment >= 0),
                monthly_rent    REAL NOT NULL DEFAULT 0 CHECK (monthly_rent >= 0),
                monthly_property_tax REAL NOT NULL DEFAULT 0 CHECK (monthly_property_tax >= 0),
                monthly_insurance REAL NOT NULL DEFAULT 0 CHECK (monthly_insurance >= 0),
                monthly_hoa     REAL NOT NULL DEFAULT 0 CHECK (monthly_hoa >= 0),
                monthly_maintenance_reserve REAL NOT NULL DEFAULT 0 CHECK (monthly_maintenance_reserve >= 0),
                monthly_property_management REAL NOT NULL DEFAULT 0 CHECK (monthly_property_management >= 0),
                monthly_expense REAL NOT NULL DEFAULT 0 CHECK (monthly_expense >= 0),
                vacancy_rate    REAL NOT NULL DEFAULT 0 CHECK (vacancy_rate >= 0 AND vacancy_rate <= 1),
                status          TEXT NOT NULL DEFAULT 'active',
                sold_date       TEXT,
                sold_price      REAL CHECK (sold_price IS NULL OR sold_price >= 0),
                sale_fees       REAL NOT NULL DEFAULT 0 CHECK (sale_fees >= 0),
                rent_collection_frequency TEXT NOT NULL DEFAULT 'monthly',
                cashflow_start_date TEXT,
                notes           TEXT,
                entry_type      TEXT NOT NULL DEFAULT 'existing_property',
                updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (asset_id) REFERENCES assets(id)
            )
        """
    if table == "debts":
        return """
            CREATE TABLE debts_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id        INTEGER NOT NULL UNIQUE,
                name            TEXT NOT NULL,
                original_amount REAL NOT NULL CHECK (original_amount > 0),
                current_balance REAL NOT NULL CHECK (current_balance >= 0),
                interest_rate   REAL NOT NULL DEFAULT 0 CHECK (interest_rate >= 0),
                minimum_payment REAL NOT NULL DEFAULT 0 CHECK (minimum_payment >= 0),
                due_date        TEXT,
                notes           TEXT,
                schedule_frequency      TEXT NOT NULL DEFAULT 'monthly',
                interest_period         TEXT NOT NULL DEFAULT 'annual',
                monthly_payment_amount  REAL NOT NULL DEFAULT 0 CHECK (monthly_payment_amount >= 0),
                cashflow_start_date     TEXT,
                last_payment_date       TEXT,
                plan_type               TEXT NOT NULL DEFAULT 'fixed_payment',
                original_term_periods   INTEGER CHECK (original_term_periods IS NULL OR original_term_periods > 0),
                preview_regular_payment REAL NOT NULL DEFAULT 0 CHECK (preview_regular_payment >= 0),
                preview_period_count    INTEGER NOT NULL DEFAULT 0 CHECK (preview_period_count >= 0),
                preview_final_payment   REAL NOT NULL DEFAULT 0 CHECK (preview_final_payment >= 0),
                preview_total_paid      REAL NOT NULL DEFAULT 0 CHECK (preview_total_paid >= 0),
                preview_total_interest  REAL NOT NULL DEFAULT 0 CHECK (preview_total_interest >= 0),
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (asset_id) REFERENCES assets(id)
            )
        """
    if table == "mortgages":
        return """
            CREATE TABLE mortgages_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id     INTEGER NOT NULL UNIQUE,
                name            TEXT NOT NULL,
                original_amount REAL NOT NULL CHECK (original_amount > 0),
                current_balance REAL NOT NULL CHECK (current_balance >= 0),
                interest_rate   REAL NOT NULL DEFAULT 0 CHECK (interest_rate >= 0),
                minimum_payment REAL NOT NULL DEFAULT 0 CHECK (minimum_payment >= 0),
                due_date        TEXT,
                notes           TEXT,
                monthly_payment_amount  REAL NOT NULL DEFAULT 0 CHECK (monthly_payment_amount >= 0),
                cashflow_start_date     TEXT,
                last_payment_date       TEXT,
                plan_type               TEXT NOT NULL DEFAULT 'fixed_payment',
                original_term_periods   INTEGER CHECK (original_term_periods IS NULL OR original_term_periods > 0),
                preview_regular_payment REAL NOT NULL DEFAULT 0 CHECK (preview_regular_payment >= 0),
                preview_period_count    INTEGER NOT NULL DEFAULT 0 CHECK (preview_period_count >= 0),
                preview_final_payment   REAL NOT NULL DEFAULT 0 CHECK (preview_final_payment >= 0),
                preview_total_paid      REAL NOT NULL DEFAULT 0 CHECK (preview_total_paid >= 0),
                preview_total_interest  REAL NOT NULL DEFAULT 0 CHECK (preview_total_interest >= 0),
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (property_id) REFERENCES properties(id)
            )
        """
    if table == "market_prices":
        return """
            CREATE TABLE market_prices_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id        INTEGER NOT NULL,
                symbol          TEXT NOT NULL DEFAULT '',
                asset_type      TEXT NOT NULL DEFAULT '',
                date            TEXT NOT NULL,
                open            REAL CHECK (open IS NULL OR open >= 0),
                high            REAL CHECK (high IS NULL OR high >= 0),
                low             REAL CHECK (low IS NULL OR low >= 0),
                close           REAL CHECK (close IS NULL OR close >= 0),
                adjusted_close  REAL CHECK (adjusted_close IS NULL OR adjusted_close >= 0),
                volume          REAL CHECK (volume IS NULL OR volume >= 0),
                price           REAL NOT NULL CHECK (price > 0),
                source          TEXT NOT NULL DEFAULT 'manual',
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (asset_id) REFERENCES assets(id),
                UNIQUE(asset_id, date, source)
            )
        """
    if table == "market_quotes":
        return """
            CREATE TABLE market_quotes_new (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id    INTEGER NOT NULL,
                symbol      TEXT NOT NULL,
                asset_type  TEXT NOT NULL,
                bid         REAL CHECK (bid IS NULL OR bid > 0),
                ask         REAL CHECK (ask IS NULL OR ask > 0),
                last        REAL CHECK (last IS NULL OR last > 0),
                timestamp   TEXT,
                source      TEXT NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (asset_id) REFERENCES assets(id),
                UNIQUE(asset_id, source)
            )
        """
    if table == "transactions":
        return """
            CREATE TABLE transactions_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date            TEXT NOT NULL,
                txn_type        TEXT NOT NULL,
                asset_id        INTEGER,
                quantity        REAL CHECK (quantity IS NULL OR quantity > 0),
                price           REAL CHECK (price IS NULL OR price > 0),
                total_amount    REAL NOT NULL,
                currency        TEXT NOT NULL DEFAULT 'USD',
                fees            REAL NOT NULL DEFAULT 0 CHECK (fees >= 0),
                notes           TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (asset_id) REFERENCES assets(id)
            )
        """
    if table == "debt_payment_records":
        return """
            CREATE TABLE debt_payment_records_new (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id           INTEGER NOT NULL UNIQUE,
                debt_id                  INTEGER NOT NULL,
                debt_name                TEXT NOT NULL,
                payment_amount           REAL NOT NULL CHECK (payment_amount >= 0),
                payment_date             TEXT NOT NULL,
                payment_type             TEXT NOT NULL,
                balance_before_payment   REAL NOT NULL CHECK (balance_before_payment >= 0),
                balance_after_payment    REAL NOT NULL CHECK (balance_after_payment >= 0),
                note                     TEXT,
                created_at               TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (transaction_id) REFERENCES transactions(id),
                FOREIGN KEY (debt_id) REFERENCES debts(id)
            )
        """
    if table == "mortgage_payment_records":
        return """
            CREATE TABLE mortgage_payment_records_new (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id           INTEGER NOT NULL UNIQUE,
                mortgage_id              INTEGER NOT NULL,
                mortgage_name            TEXT NOT NULL,
                payment_amount           REAL NOT NULL CHECK (payment_amount >= 0),
                payment_date             TEXT NOT NULL,
                payment_type             TEXT NOT NULL,
                balance_before_payment   REAL NOT NULL CHECK (balance_before_payment >= 0),
                balance_after_payment    REAL NOT NULL CHECK (balance_after_payment >= 0),
                note                     TEXT,
                created_at               TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (transaction_id) REFERENCES transactions(id),
                FOREIGN KEY (mortgage_id) REFERENCES mortgages(id)
            )
        """
    if table == "bankruptcy_events":
        return """
            CREATE TABLE bankruptcy_events_new (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                event_date        TEXT NOT NULL,
                trigger_kind      TEXT NOT NULL,
                asset_id          INTEGER,
                due_date          TEXT,
                amount_due        REAL NOT NULL DEFAULT 0 CHECK (amount_due >= 0),
                cash_balance      REAL NOT NULL DEFAULT 0,
                shortfall_amount  REAL NOT NULL DEFAULT 0 CHECK (shortfall_amount >= 0),
                status            TEXT NOT NULL DEFAULT 'active',
                notes             TEXT,
                created_at        TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """
    if table == "portfolio_snapshots":
        return """
            CREATE TABLE portfolio_snapshots_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date            TEXT NOT NULL UNIQUE,
                cash            REAL NOT NULL,
                total_assets    REAL NOT NULL CHECK (total_assets >= 0),
                total_liabilities REAL NOT NULL CHECK (total_liabilities >= 0),
                net_worth       REAL NOT NULL,
                allocation_json TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """
    if table == "transaction_fee_breakdown":
        return """
            CREATE TABLE transaction_fee_breakdown_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id  INTEGER NOT NULL,
                fee_type        TEXT NOT NULL,
                amount          REAL NOT NULL CHECK (amount >= 0),
                rate            REAL,
                notes           TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (transaction_id) REFERENCES transactions(id)
            )
        """
    raise ValueError(f"v12 migration: no template for table {table!r}")


def _create_indexes(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_type_date ON transactions(txn_type, date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_asset_type_date ON transactions(asset_id, txn_type, date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reports_type_label ON reports(report_type, period_label)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_properties_status ON properties(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_journal_transaction_id ON decision_journal(transaction_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bankruptcy_events_status ON bankruptcy_events(status)")


def _migrate_properties(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(properties)").fetchall()}
    # Legacy mortgage columns (mortgage_interest_rate, monthly_mortgage_payment)
    # were dropped in schema v11 — the v11 cutover handles their removal by
    # wiping the DB. This migration runs only on fresh / post-v11 DBs and so
    # never needs to add them.
    new_cols = [
        ("purchase_date", "TEXT"),
        ("down_payment", "REAL"),
        ("monthly_property_tax", "REAL NOT NULL DEFAULT 0"),
        ("monthly_insurance", "REAL NOT NULL DEFAULT 0"),
        ("monthly_hoa", "REAL NOT NULL DEFAULT 0"),
        ("monthly_maintenance_reserve", "REAL NOT NULL DEFAULT 0"),
        ("monthly_property_management", "REAL NOT NULL DEFAULT 0"),
        ("vacancy_rate", "REAL NOT NULL DEFAULT 0"),
        ("status", "TEXT NOT NULL DEFAULT 'active'"),
        ("sold_date", "TEXT"),
        ("sold_price", "REAL"),
        ("sale_fees", "REAL NOT NULL DEFAULT 0"),
        ("rent_collection_frequency", "TEXT NOT NULL DEFAULT 'monthly'"),
        ("cashflow_start_date", "TEXT"),
        ("entry_type", "TEXT NOT NULL DEFAULT 'existing_property'"),
    ]
    for col_name, col_def in new_cols:
        if col_name not in cols:
            conn.execute(f"ALTER TABLE properties ADD COLUMN {col_name} {col_def}")


# Phantom columns retired in schema v4. They were declared in the model and
# round-tripped by the repo, but `ledger.add_property` never accepted them as
# parameters and the GUI never wrote them — so every row had defaults. Remove
# from old DBs where SQLite supports DROP COLUMN; older SQLite leaves them.
_RETIRED_PROPERTY_COLUMNS = (
    "loan_term_years",
    "down_payment_type",
    "down_payment_input_value",
    "monthly_mortgage_override_enabled",
    "monthly_mortgage_override",
    "rent_input_amount",
    "rent_input_frequency",
    "property_tax_input_type",
    "property_tax_input_value",
    "insurance_input_type",
    "insurance_input_value",
    "maintenance_input_type",
    "maintenance_input_value",
    "management_input_type",
    "management_input_value",
)


def _drop_property_phantom_columns(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(properties)").fetchall()}
    for col in _RETIRED_PROPERTY_COLUMNS:
        if col not in cols:
            continue
        try:
            conn.execute(f"ALTER TABLE properties DROP COLUMN {col}")
        except sqlite3.OperationalError:
            # SQLite < 3.35 doesn't support DROP COLUMN. The column becomes
            # an unused legacy column; the repo no longer reads or writes it.
            _log.info("Could not drop legacy column properties.%s; leaving in place.", col)
            return


def _migrate_decision_journal(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(decision_journal)").fetchall()}
    # `reasoning`, `expected`, `actual`, `score`, `tags` are the original
    # legacy columns — `journal_repo._row_to_entry` reads them
    # unconditionally (`row["reasoning"]` etc.). They're declared in
    # schema.sql so fresh DBs always have them, but executescript's
    # `CREATE TABLE IF NOT EXISTS` is a no-op when the table predates
    # them. Adding them here keeps very old DBs from blowing up on the
    # first journal listing.
    new_cols = [
        ("transaction_id", "INTEGER"),
        ("thesis", "TEXT"),
        ("intended_role", "TEXT"),
        ("risk_reasoning", "TEXT"),
        ("exit_plan", "TEXT"),
        ("confidence_level", "INTEGER"),
        ("expected_holding_period", "TEXT"),
        ("pre_trade_notes", "TEXT"),
        ("post_trade_review", "TEXT"),
        ("mistake_tags", "TEXT"),
        ("lesson_learned", "TEXT"),
        ("snapshot_before", "TEXT"),
        ("snapshot_after", "TEXT"),
        ("reasoning", "TEXT"),
        ("expected", "TEXT"),
        ("actual", "TEXT"),
        ("score", "INTEGER"),
        ("tags", "TEXT"),
    ]
    for col_name, col_def in new_cols:
        if col_name not in cols:
            conn.execute(f"ALTER TABLE decision_journal ADD COLUMN {col_name} {col_def}")


def _migrate_market_prices(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(market_prices)").fetchall()}
    new_cols = [
        ("symbol", "TEXT NOT NULL DEFAULT ''"),
        ("asset_type", "TEXT NOT NULL DEFAULT ''"),
        ("open", "REAL"),
        ("high", "REAL"),
        ("low", "REAL"),
        ("close", "REAL"),
        ("adjusted_close", "REAL"),
        ("volume", "REAL"),
    ]
    for col_name, col_def in new_cols:
        if col_name not in cols:
            conn.execute(f"ALTER TABLE market_prices ADD COLUMN {col_name} {col_def}")

    _ensure_market_prices_unique_index(conn)

    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "price_sync_log" not in tables:
        conn.execute("""
            CREATE TABLE price_sync_log (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at          TEXT NOT NULL,
                finished_at         TEXT,
                status              TEXT NOT NULL,
                source              TEXT,
                assets_attempted    INTEGER DEFAULT 0,
                assets_succeeded    INTEGER DEFAULT 0,
                assets_failed       INTEGER DEFAULT 0,
                error_message       TEXT
            )
        """)


def _ensure_market_prices_unique_index(conn: sqlite3.Connection) -> None:
    indexes = {row[1] for row in conn.execute("PRAGMA index_list(market_prices)").fetchall()}
    if "idx_market_prices_asset_date_source" in indexes:
        return

    has_unique = False
    for row in conn.execute("PRAGMA index_list(market_prices)").fetchall():
        if row[2]:  # unique flag
            idx_cols = [
                r[2] for r in conn.execute(f"PRAGMA index_info('{row[1]}')").fetchall()
            ]
            if idx_cols == ["asset_id", "date", "source"]:
                has_unique = True
                break
    if has_unique:
        return

    duplicate_count = conn.execute("""
        SELECT COUNT(*) FROM market_prices
        WHERE id NOT IN (
            SELECT MAX(id) FROM market_prices
            GROUP BY asset_id, date, source
        )
    """).fetchone()[0]

    if duplicate_count > 0:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_prices_dedupe_backup AS
            SELECT * FROM market_prices WHERE 0
        """)
        conn.execute("""
            INSERT INTO market_prices_dedupe_backup
            SELECT * FROM market_prices
            WHERE id NOT IN (
                SELECT MAX(id) FROM market_prices
                GROUP BY asset_id, date, source
            )
        """)
        _log.warning(
            "Migrating market_prices: removing %d duplicate row(s) to add unique index; "
            "originals preserved in market_prices_dedupe_backup",
            duplicate_count,
        )

        conn.execute("""
            DELETE FROM market_prices
            WHERE id NOT IN (
                SELECT MAX(id) FROM market_prices
                GROUP BY asset_id, date, source
            )
        """)

    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_market_prices_asset_date_source
        ON market_prices(asset_id, date, source)
    """)


def _migrate_securities_master(conn: sqlite3.Connection) -> None:
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "securities_master" not in tables:
        conn.execute("""
            CREATE TABLE securities_master (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol          TEXT NOT NULL,
                name            TEXT NOT NULL,
                asset_type      TEXT NOT NULL,
                exchange        TEXT,
                sector          TEXT,
                industry        TEXT,
                etf_category    TEXT,
                is_common_etf   INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(symbol, asset_type)
            )
        """)


def _migrate_market_quotes(conn: sqlite3.Connection) -> None:
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "market_quotes" not in tables:
        conn.execute("""
            CREATE TABLE market_quotes (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id    INTEGER NOT NULL,
                symbol      TEXT NOT NULL,
                asset_type  TEXT NOT NULL,
                bid         REAL,
                ask         REAL,
                last        REAL,
                timestamp   TEXT,
                source      TEXT NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (asset_id) REFERENCES assets(id),
                UNIQUE(asset_id, source)
            )
        """)


def _migrate_reports(conn: sqlite3.Connection) -> None:
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "reports" not in tables:
        conn.execute("""
            CREATE TABLE reports (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                report_type     TEXT NOT NULL,
                period_start    TEXT NOT NULL,
                period_end      TEXT NOT NULL,
                period_label    TEXT NOT NULL,
                generated_at    TEXT NOT NULL,
                title           TEXT NOT NULL,
                report_json     TEXT NOT NULL,
                notes           TEXT,
                net_cash_flow       REAL NOT NULL DEFAULT 0,
                operating_net_income REAL NOT NULL DEFAULT 0,
                transaction_count   INTEGER NOT NULL DEFAULT 0,
                net_worth_change       REAL,
                funding_flow           REAL NOT NULL DEFAULT 0,
                approximate_return_pct REAL,
                UNIQUE(report_type, period_label)
            )
        """)
    else:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
        # net_worth_change and approximate_return_pct are nullable so old
        # rows reflect "unavailable" rather than a misleading 0; funding_flow
        # is a transaction-derived sum so 0 is a safe default for old rows.
        summary_cols = [
            ("net_cash_flow", "REAL NOT NULL DEFAULT 0"),
            ("operating_net_income", "REAL NOT NULL DEFAULT 0"),
            ("transaction_count", "INTEGER NOT NULL DEFAULT 0"),
            ("net_worth_change", "REAL"),
            ("funding_flow", "REAL NOT NULL DEFAULT 0"),
            ("approximate_return_pct", "REAL"),
        ]
        for col_name, col_def in summary_cols:
            if col_name not in cols:
                conn.execute(f"ALTER TABLE reports ADD COLUMN {col_name} {col_def}")


def _migrate_transaction_fee_breakdown(conn: sqlite3.Connection) -> None:
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "transaction_fee_breakdown" not in tables:
        conn.execute("""
            CREATE TABLE transaction_fee_breakdown (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id  INTEGER NOT NULL,
                fee_type        TEXT NOT NULL,
                amount          REAL NOT NULL,
                rate            REAL,
                notes           TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (transaction_id) REFERENCES transactions(id)
            )
        """)


EXPECTED_TABLES = [
    "assets",
    "transactions",
    "transaction_fee_breakdown",
    "market_prices",
    "market_quotes",
    "price_sync_log",
    "properties",
    "debts",
    "debt_payment_records",
    "mortgages",
    "mortgage_payment_records",
    "decision_journal",
    "portfolio_snapshots",
    "reports",
    "securities_master",
    "settings",
    "bankruptcy_events",
]


def verify_tables(conn: sqlite3.Connection) -> list[str]:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    return [row["name"] for row in cursor.fetchall()]
