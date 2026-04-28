import logging
import os
import shutil
import sqlite3
import sys
from pathlib import Path

_log = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schema.sql"

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
CURRENT_SCHEMA_VERSION = 2


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
    schema_sql = SCHEMA_PATH.read_text()
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
    _migrate_properties(conn)
    _migrate_decision_journal(conn)
    _migrate_market_prices(conn)
    _migrate_securities_master(conn)
    _migrate_market_quotes(conn)
    _migrate_reports(conn)
    _migrate_transaction_fee_breakdown(conn)
    _migrate_drop_transactions_journal_id(conn)
    _create_indexes(conn)


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


def _create_indexes(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_type_date ON transactions(txn_type, date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_asset_type_date ON transactions(asset_id, txn_type, date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reports_type_label ON reports(report_type, period_label)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_properties_status ON properties(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_decision_journal_transaction_id ON decision_journal(transaction_id)")


def _migrate_properties(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(properties)").fetchall()}
    new_cols = [
        ("purchase_date", "TEXT"),
        ("down_payment", "REAL"),
        ("mortgage_interest_rate", "REAL NOT NULL DEFAULT 0"),
        ("monthly_mortgage_payment", "REAL NOT NULL DEFAULT 0"),
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
        ("loan_term_years", "INTEGER"),
        ("down_payment_type", "TEXT NOT NULL DEFAULT 'amount'"),
        ("down_payment_input_value", "REAL"),
        ("monthly_mortgage_override_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("monthly_mortgage_override", "REAL NOT NULL DEFAULT 0"),
        ("rent_input_amount", "REAL NOT NULL DEFAULT 0"),
        ("rent_input_frequency", "TEXT NOT NULL DEFAULT 'monthly'"),
        ("property_tax_input_type", "TEXT NOT NULL DEFAULT 'monthly'"),
        ("property_tax_input_value", "REAL NOT NULL DEFAULT 0"),
        ("insurance_input_type", "TEXT NOT NULL DEFAULT 'monthly'"),
        ("insurance_input_value", "REAL NOT NULL DEFAULT 0"),
        ("maintenance_input_type", "TEXT NOT NULL DEFAULT 'monthly'"),
        ("maintenance_input_value", "REAL NOT NULL DEFAULT 0"),
        ("management_input_type", "TEXT NOT NULL DEFAULT 'monthly'"),
        ("management_input_value", "REAL NOT NULL DEFAULT 0"),
    ]
    for col_name, col_def in new_cols:
        if col_name not in cols:
            conn.execute(f"ALTER TABLE properties ADD COLUMN {col_name} {col_def}")


def _migrate_decision_journal(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(decision_journal)").fetchall()}
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
    "decision_journal",
    "portfolio_snapshots",
    "reports",
    "securities_master",
    "settings",
]


def verify_tables(conn: sqlite3.Connection) -> list[str]:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    return [row["name"] for row in cursor.fetchall()]
