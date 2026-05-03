import sqlite3
import pytest
from src.storage.database import verify_tables, init_db, EXPECTED_TABLES
from src.storage.price_repo import upsert_ohlcv, bulk_upsert_ohlcv, get_latest_price
from src.storage.report_repo import (
    delete_reports_by_ids, delete_reports_by_type,
    delete_reports_in_period_range, delete_all_reports, get_report_stats,
)


def _create_old_market_prices_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            region TEXT NOT NULL DEFAULT 'US',
            liquidity TEXT NOT NULL DEFAULT 'liquid',
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE market_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            price REAL NOT NULL,
            source TEXT NOT NULL DEFAULT 'manual',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
        CREATE TABLE transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            txn_type TEXT NOT NULL,
            asset_id INTEGER,
            quantity REAL,
            price REAL,
            total_amount REAL NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            fees REAL NOT NULL DEFAULT 0,
            notes TEXT,
            journal_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
        CREATE TABLE properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL UNIQUE,
            address TEXT,
            purchase_price REAL,
            current_value REAL,
            mortgage_balance REAL NOT NULL DEFAULT 0,
            monthly_rent REAL NOT NULL DEFAULT 0,
            monthly_expense REAL NOT NULL DEFAULT 0,
            notes TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
        CREATE TABLE debts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL UNIQUE,
            name TEXT NOT NULL,
            original_amount REAL NOT NULL,
            current_balance REAL NOT NULL,
            interest_rate REAL NOT NULL DEFAULT 0,
            minimum_payment REAL NOT NULL DEFAULT 0,
            due_date TEXT,
            notes TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
        CREATE TABLE decision_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            title TEXT NOT NULL,
            reasoning TEXT,
            expected TEXT,
            actual TEXT,
            score INTEGER,
            tags TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE portfolio_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL UNIQUE,
            cash REAL NOT NULL,
            total_assets REAL NOT NULL,
            total_liabilities REAL NOT NULL,
            net_worth REAL NOT NULL,
            allocation_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    return conn


def test_all_tables_created(db_conn):
    tables = verify_tables(db_conn)
    for table in EXPECTED_TABLES:
        assert table in tables, f"Missing table: {table}"


def test_assets_table_empty(db_conn):
    rows = db_conn.execute("SELECT COUNT(*) as cnt FROM assets").fetchone()
    assert rows["cnt"] == 0


def test_settings_table_accepts_insert(db_conn):
    db_conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ("theme", "dark"))
    db_conn.commit()
    row = db_conn.execute("SELECT value FROM settings WHERE key = ?", ("theme",)).fetchone()
    assert row["value"] == "dark"


def test_foreign_keys_enabled(db_conn):
    result = db_conn.execute("PRAGMA foreign_keys").fetchone()
    assert result[0] == 1


def test_journal_table_insert(db_conn):
    db_conn.execute(
        "INSERT INTO decision_journal (date, title, reasoning) VALUES (?, ?, ?)",
        ("2025-01-01", "Test entry", "Testing the journal"),
    )
    db_conn.commit()
    row = db_conn.execute("SELECT title FROM decision_journal").fetchone()
    assert row["title"] == "Test entry"


def test_transaction_insert(db_conn):
    db_conn.execute(
        "INSERT INTO assets (symbol, name, asset_type) VALUES (?, ?, ?)",
        ("AAPL", "Apple Inc", "stock"),
    )
    db_conn.execute(
        "INSERT INTO transactions (date, txn_type, asset_id, quantity, price, total_amount) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("2025-01-15", "buy", 1, 10, 150.0, -1500.0),
    )
    db_conn.commit()
    row = db_conn.execute("SELECT * FROM transactions WHERE asset_id = 1").fetchone()
    assert row["quantity"] == 10
    assert row["total_amount"] == -1500.0


# --- Migration: market_prices unique index ---

def _has_unique_index_on(conn, table, columns):
    for row in conn.execute(f"PRAGMA index_list({table})").fetchall():
        if row[2]:  # unique flag
            idx_cols = [
                r[2] for r in conn.execute(f"PRAGMA index_info('{row[1]}')").fetchall()
            ]
            if idx_cols == columns:
                return True
    return False


def test_migration_adds_unique_index_to_old_db():
    conn = _create_old_market_prices_db()
    assert not _has_unique_index_on(conn, "market_prices", ["asset_id", "date", "source"])

    from src.storage.database import _migrate
    _migrate(conn)
    conn.commit()

    assert _has_unique_index_on(conn, "market_prices", ["asset_id", "date", "source"])
    conn.close()


def test_migration_deduplicates_old_rows():
    # Schema v11 is a dev cutover that wipes all user-data tables on
    # upgrade — pre-migration market_prices rows do not survive. After
    # migration, the table is empty but has the unique index in place.
    conn = _create_old_market_prices_db()
    conn.execute("INSERT INTO assets (symbol, name, asset_type) VALUES ('AAPL', 'Apple', 'stock')")
    conn.execute(
        "INSERT INTO market_prices (asset_id, date, price, source) VALUES (1, '2025-01-10', 100.0, 'manual')"
    )
    conn.commit()

    from src.storage.database import _migrate
    _migrate(conn)
    conn.commit()

    # User data was wiped by the v11 cutover.
    assert conn.execute("SELECT COUNT(*) FROM market_prices").fetchone()[0] == 0
    # Unique index is in place on the fresh schema.
    assert _has_unique_index_on(conn, "market_prices", ["asset_id", "date", "source"])
    conn.close()


def test_migration_upsert_works_after_index_created():
    conn = _create_old_market_prices_db()
    # Need to insert the asset AFTER migration since v11 wipes tables.
    from src.storage.database import _migrate
    _migrate(conn)
    conn.commit()
    conn.execute("INSERT INTO assets (symbol, name, asset_type) VALUES ('AAPL', 'Apple', 'stock')")
    conn.commit()

    upsert_ohlcv(conn, 1, "AAPL", "stock", "2025-01-10",
                 100, 105, 99, 103, 103, 1000000, "yfinance")
    upsert_ohlcv(conn, 1, "AAPL", "stock", "2025-01-10",
                 100, 106, 98, 104, 104, 1100000, "yfinance")

    assert conn.execute("SELECT COUNT(*) FROM market_prices").fetchone()[0] == 1
    assert get_latest_price(conn, 1) == 104.0
    conn.close()


def test_migration_bulk_upsert_works_after_index_created():
    conn = _create_old_market_prices_db()
    from src.storage.database import _migrate
    _migrate(conn)
    conn.commit()
    conn.execute("INSERT INTO assets (symbol, name, asset_type) VALUES ('AAPL', 'Apple', 'stock')")
    conn.commit()

    rows = [
        {"asset_id": 1, "symbol": "AAPL", "asset_type": "stock",
         "date": "2025-01-10", "open": 100, "high": 105, "low": 99,
         "close": 103, "adjusted_close": 103, "volume": 1e6, "source": "yfinance"},
        {"asset_id": 1, "symbol": "AAPL", "asset_type": "stock",
         "date": "2025-01-11", "open": 103, "high": 108, "low": 102,
         "close": 107, "adjusted_close": 107, "volume": 9e5, "source": "yfinance"},
    ]
    count = bulk_upsert_ohlcv(conn, rows)
    assert count == 2
    assert get_latest_price(conn, 1) == 107.0
    conn.close()


def test_new_db_has_unique_index(db_conn):
    assert _has_unique_index_on(db_conn, "market_prices", ["asset_id", "date", "source"])


# --- Reports table ---

def test_reports_table_exists_after_init(db_conn):
    tables = verify_tables(db_conn)
    assert "reports" in tables


def test_reports_in_expected_tables():
    assert "reports" in EXPECTED_TABLES


def test_migration_creates_reports_table_on_old_db():
    conn = _create_old_market_prices_db()
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "reports" not in tables

    from src.storage.database import _migrate
    _migrate(conn)
    conn.commit()

    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "reports" in tables
    conn.close()


# --- Migration: cashflow_start_date on properties ---

def test_migration_adds_cashflow_start_date_to_old_properties():
    conn = _create_old_market_prices_db()
    cols = {row[1] for row in conn.execute("PRAGMA table_info(properties)").fetchall()}
    assert "cashflow_start_date" not in cols

    from src.storage.database import _migrate
    _migrate(conn)
    conn.commit()

    cols = {row[1] for row in conn.execute("PRAGMA table_info(properties)").fetchall()}
    assert "cashflow_start_date" in cols
    conn.close()


def test_migration_v11_wipes_user_data_for_property_cashflow_start_date():
    # Schema v11 dev cutover: pre-existing properties are dropped during
    # migration since the legacy mortgage columns can't be safely
    # back-converted to the new mortgages table without user input.
    # This replaces the v6 "cashflow_start_date defaults to NULL on
    # legacy rows" check, which is no longer reachable.
    conn = _create_old_market_prices_db()
    conn.execute("INSERT INTO assets (symbol, name, asset_type) VALUES ('H1', 'House', 'real_estate')")
    conn.execute(
        "INSERT INTO properties (asset_id, purchase_price, current_value, mortgage_balance, "
        "monthly_rent, monthly_expense) VALUES (1, 500000, 500000, 400000, 2000, 1500)"
    )
    conn.commit()

    from src.storage.database import _migrate
    _migrate(conn)
    conn.commit()

    # User data wiped by v11 cutover.
    row = conn.execute("SELECT COUNT(*) FROM properties").fetchone()
    assert row[0] == 0
    # The new schema has cashflow_start_date and the new mortgages table.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(properties)").fetchall()}
    assert "cashflow_start_date" in cols
    assert "mortgage_balance" not in cols  # legacy column dropped
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "mortgages" in tables
    assert "mortgage_payment_records" in tables
    conn.close()


def test_reports_unique_constraint(db_conn):
    import sqlite3 as _sqlite3
    db_conn.execute(
        "INSERT INTO reports (report_type, period_start, period_end, period_label, "
        "generated_at, title, report_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("monthly", "2025-06-01", "2025-07-01", "2025-06", "now", "t1", "{}"),
    )
    db_conn.commit()
    with pytest.raises(_sqlite3.IntegrityError):
        db_conn.execute(
            "INSERT INTO reports (report_type, period_start, period_end, period_label, "
            "generated_at, title, report_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("monthly", "2025-06-01", "2025-07-01", "2025-06", "now", "t2", "{}"),
        )
    db_conn.rollback()


# --- Required database indexes ---


class TestRequiredIndexes:
    """Verify migration-safe indexes exist after init_db."""

    def _index_exists(self, conn, index_name):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
            (index_name,),
        ).fetchone()
        return row is not None

    def test_transactions_date_index(self, db_conn):
        assert self._index_exists(db_conn, "idx_transactions_date")

    def test_transactions_type_date_index(self, db_conn):
        assert self._index_exists(db_conn, "idx_transactions_type_date")

    def test_transactions_asset_type_date_index(self, db_conn):
        assert self._index_exists(db_conn, "idx_transactions_asset_type_date")

    def test_reports_type_label_index(self, db_conn):
        assert self._index_exists(db_conn, "idx_reports_type_label")

    def test_properties_status_index(self, db_conn):
        assert self._index_exists(db_conn, "idx_properties_status")


# --- Report repo helpers ---


def _insert_report(conn, rtype, period_start, period_end, period_label, title):
    conn.execute(
        "INSERT INTO reports (report_type, period_start, period_end, period_label, "
        "generated_at, title, report_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (rtype, period_start, period_end, period_label, "now", title, "{}"),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


class TestReportRepoHelpers:

    def test_delete_reports_by_ids(self, db_conn):
        id1 = _insert_report(db_conn, "monthly", "2025-01-01", "2025-02-01", "2025-01", "Jan")
        id2 = _insert_report(db_conn, "monthly", "2025-02-01", "2025-03-01", "2025-02", "Feb")
        _insert_report(db_conn, "monthly", "2025-03-01", "2025-04-01", "2025-03", "Mar")
        deleted = delete_reports_by_ids(db_conn, [id1, id2])
        assert deleted == 2
        assert db_conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 1

    def test_delete_reports_by_ids_empty_list(self, db_conn):
        _insert_report(db_conn, "monthly", "2025-01-01", "2025-02-01", "2025-01", "Jan")
        deleted = delete_reports_by_ids(db_conn, [])
        assert deleted == 0
        assert db_conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 1

    def test_delete_reports_by_type(self, db_conn):
        _insert_report(db_conn, "monthly", "2025-01-01", "2025-02-01", "2025-01", "Jan")
        _insert_report(db_conn, "monthly", "2025-02-01", "2025-03-01", "2025-02", "Feb")
        _insert_report(db_conn, "annual", "2025-01-01", "2026-01-01", "2025", "Year 2025")
        deleted = delete_reports_by_type(db_conn, "monthly")
        assert deleted == 2
        assert db_conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 1
        row = db_conn.execute("SELECT report_type FROM reports").fetchone()
        assert row[0] == "annual"

    def test_delete_reports_in_period_range(self, db_conn):
        _insert_report(db_conn, "monthly", "2025-01-01", "2025-02-01", "2025-01", "Jan")
        _insert_report(db_conn, "monthly", "2025-02-01", "2025-03-01", "2025-02", "Feb")
        _insert_report(db_conn, "monthly", "2025-03-01", "2025-04-01", "2025-03", "Mar")
        deleted = delete_reports_in_period_range(db_conn, "2025-01-01", "2025-03-01")
        assert deleted == 2
        assert db_conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 1
        row = db_conn.execute("SELECT period_label FROM reports").fetchone()
        assert row[0] == "2025-03"

    def test_delete_all_reports(self, db_conn):
        _insert_report(db_conn, "monthly", "2025-01-01", "2025-02-01", "2025-01", "Jan")
        _insert_report(db_conn, "annual", "2025-01-01", "2026-01-01", "2025", "Year 2025")
        deleted = delete_all_reports(db_conn)
        assert deleted == 2
        assert db_conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 0

    def test_get_report_stats(self, db_conn):
        assert get_report_stats(db_conn) == {
            "total": 0, "monthly": 0, "quarterly": 0,
            "semi_annual": 0, "annual": 0,
        }
        _insert_report(db_conn, "monthly", "2025-01-01", "2025-02-01", "2025-01", "Jan")
        _insert_report(db_conn, "monthly", "2025-02-01", "2025-03-01", "2025-02", "Feb")
        _insert_report(db_conn, "quarterly", "2025-01-01", "2025-04-01", "2025-Q1", "Q1")
        _insert_report(db_conn, "semi_annual", "2025-01-01", "2025-07-01", "2025-H1", "H1")
        _insert_report(db_conn, "annual", "2025-01-01", "2026-01-01", "2025", "Year 2025")
        stats = get_report_stats(db_conn)
        assert stats == {
            "total": 5, "monthly": 2, "quarterly": 1,
            "semi_annual": 1, "annual": 1,
        }


# --- Schema v2: drop transactions.journal_id ---

class TestMigrationV2DropJournalId:

    def _v1_db(self, tmp_path):
        """Build a fresh on-disk DB shaped like schema v1, with both
        forward (transactions.journal_id) and reverse
        (decision_journal.transaction_id) pointers wired up, plus an
        orphan that only has the reverse pointer set."""
        path = tmp_path / "v1.db"
        c = sqlite3.connect(str(path))
        c.executescript("""
            CREATE TABLE decision_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id INTEGER,
                date TEXT NOT NULL,
                title TEXT NOT NULL,
                reasoning TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, name TEXT, asset_type TEXT
            );
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, txn_type TEXT, asset_id INTEGER,
                quantity REAL, price REAL,
                total_amount REAL NOT NULL, currency TEXT DEFAULT 'USD',
                fees REAL DEFAULT 0, notes TEXT,
                journal_id INTEGER,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (asset_id) REFERENCES assets(id),
                FOREIGN KEY (journal_id) REFERENCES decision_journal(id)
            );
        """)
        c.execute("INSERT INTO transactions (date, txn_type, total_amount) VALUES ('2025-01-01', 'buy', -1500)")
        txn_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        c.execute("INSERT INTO decision_journal (transaction_id, date, title) VALUES (?, '2025-01-01', 'paired')", (txn_id,))
        paired_jid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        c.execute("UPDATE transactions SET journal_id = ? WHERE id = ?", (paired_jid, txn_id))
        c.execute("INSERT INTO decision_journal (transaction_id, date, title) VALUES (NULL, '2025-01-02', 'orphan')")
        orphan_jid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        c.execute("INSERT INTO transactions (date, txn_type, total_amount, journal_id) VALUES ('2025-01-02', 'sell', 1500, ?)", (orphan_jid,))
        orphan_txn_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        c.commit()
        c.close()
        return path, txn_id, paired_jid, orphan_txn_id, orphan_jid

    def test_drops_journal_id_column(self, tmp_path):
        path, *_ = self._v1_db(tmp_path)
        conn = init_db(str(path))
        cols = {row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()}
        assert "journal_id" not in cols
        conn.close()

    def test_bumps_schema_version(self, tmp_path):
        from src.storage.database import get_schema_version, CURRENT_SCHEMA_VERSION
        path, *_ = self._v1_db(tmp_path)
        conn = init_db(str(path))
        assert get_schema_version(conn) == CURRENT_SCHEMA_VERSION
        conn.close()

    def test_preserves_existing_pairing(self, tmp_path):
        path, txn_id, paired_jid, _, _ = self._v1_db(tmp_path)
        conn = init_db(str(path))
        row = conn.execute(
            "SELECT transaction_id FROM decision_journal WHERE id = ?", (paired_jid,)
        ).fetchone()
        assert row["transaction_id"] == txn_id
        conn.close()

    def test_backfills_orphan_reverse_pointer(self, tmp_path):
        path, _, _, orphan_txn_id, orphan_jid = self._v1_db(tmp_path)
        conn = init_db(str(path))
        # Before: orphan_jid had transaction_id NULL but transactions.journal_id pointed back.
        # After: the migration backfills decision_journal.transaction_id from the back-pointer.
        row = conn.execute(
            "SELECT transaction_id FROM decision_journal WHERE id = ?", (orphan_jid,)
        ).fetchone()
        assert row["transaction_id"] == orphan_txn_id
        conn.close()

    def test_no_data_loss(self, tmp_path):
        path, *_ = self._v1_db(tmp_path)
        conn = init_db(str(path))
        assert conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM decision_journal").fetchone()[0] == 2
        # Foreign-key integrity check should report no violations after the rebuild.
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        conn.close()

    def test_idempotent_on_v2_db(self, tmp_path):
        # Running init_db twice on the same DB must not crash, and the
        # second run should leave schema_version at 2.
        from src.storage.database import get_schema_version, CURRENT_SCHEMA_VERSION
        path, *_ = self._v1_db(tmp_path)
        conn = init_db(str(path))
        conn.close()
        conn = init_db(str(path))
        assert get_schema_version(conn) == CURRENT_SCHEMA_VERSION
        cols = {row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()}
        assert "journal_id" not in cols
        conn.close()


# ===================================================================
# Phase 3: lightweight summary columns on the reports table
# ===================================================================


def _create_pre_phase3_reports_db() -> sqlite3.Connection:
    """Build a connection whose `reports` table predates Phase 3 — i.e.,
    it has the Phase 1 summary columns but not net_worth_change /
    funding_flow / approximate_return_pct."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
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
            UNIQUE(report_type, period_label)
        );
    """)
    return conn


def test_phase3_migration_adds_new_summary_cols_to_old_reports():
    conn = _create_pre_phase3_reports_db()
    cols = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
    for new_col in ("net_worth_change", "funding_flow", "approximate_return_pct"):
        assert new_col not in cols

    from src.storage.database import _migrate_reports
    _migrate_reports(conn)
    conn.commit()

    cols = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
    for new_col in ("net_worth_change", "funding_flow", "approximate_return_pct"):
        assert new_col in cols
    conn.close()


def test_phase3_migration_creates_full_table_on_db_with_no_reports_table():
    """When migrating a DB that has no `reports` table at all (legacy),
    the full Phase 3 schema is created — including the new summary cols."""
    conn = _create_old_market_prices_db()
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "reports" not in tables

    from src.storage.database import _migrate_reports
    _migrate_reports(conn)
    conn.commit()

    cols = {row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()}
    for new_col in ("net_worth_change", "funding_flow", "approximate_return_pct"):
        assert new_col in cols
    conn.close()


def test_phase3_migration_preserves_old_rows_with_safe_defaults():
    conn = _create_pre_phase3_reports_db()
    conn.execute(
        "INSERT INTO reports (report_type, period_start, period_end, "
        "period_label, generated_at, title, report_json, "
        "net_cash_flow, operating_net_income, transaction_count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("monthly", "2025-06-01", "2025-07-01", "2025-06",
         "now", "Old Report", "{}", 1234.5, 200.0, 7),
    )
    conn.commit()

    from src.storage.database import _migrate_reports
    _migrate_reports(conn)
    conn.commit()

    row = conn.execute(
        "SELECT * FROM reports WHERE period_label='2025-06'"
    ).fetchone()
    # Pre-existing summary columns preserved.
    assert row["net_cash_flow"] == 1234.5
    assert row["operating_net_income"] == 200.0
    assert row["transaction_count"] == 7
    # New columns get safe defaults / NULL on old rows.
    assert row["net_worth_change"] is None
    assert row["funding_flow"] == 0.0
    assert row["approximate_return_pct"] is None
    conn.close()


def test_phase3_old_rows_load_via_repo_without_crash():
    """Reading old rows after Phase 3 migration must not crash either
    `get_report`, `list_report_summaries`, or `list_reports`."""
    conn = _create_pre_phase3_reports_db()
    conn.execute(
        "INSERT INTO reports (report_type, period_start, period_end, "
        "period_label, generated_at, title, report_json, "
        "net_cash_flow, operating_net_income, transaction_count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("monthly", "2025-06-01", "2025-07-01", "2025-06",
         "now", "Old", "{}", 100.0, 0.0, 0),
    )
    conn.commit()

    from src.storage.database import _migrate_reports
    _migrate_reports(conn)
    conn.commit()

    from src.storage.report_repo import (
        get_report, list_report_summaries, list_reports,
    )
    r = get_report(conn, "monthly", "2025-06")
    assert r is not None
    assert r.net_cash_flow == 100.0
    assert r.net_worth_change is None
    assert r.funding_flow == 0.0
    assert r.approximate_return_pct is None

    summaries = list_report_summaries(conn, report_type="monthly")
    assert len(summaries) == 1
    s = summaries[0]
    assert s.net_worth_change is None
    assert s.funding_flow == 0.0
    assert s.approximate_return_pct is None

    rows = list_reports(conn, "monthly")
    assert len(rows) == 1
    conn.close()


def test_phase3_migration_idempotent():
    """Running the migration twice must not error or duplicate columns."""
    conn = _create_pre_phase3_reports_db()
    from src.storage.database import _migrate_reports
    _migrate_reports(conn)
    conn.commit()
    _migrate_reports(conn)  # second run must be a no-op
    conn.commit()
    cols = [row[1] for row in conn.execute("PRAGMA table_info(reports)").fetchall()]
    for new_col in ("net_worth_change", "funding_flow", "approximate_return_pct"):
        assert cols.count(new_col) == 1
    conn.close()


# === Yearly-debt reconciliation migration ===========================

def _seed_yearly_debt_with_pay_history(conn, *, principal, rate, payments):
    """Helper: create a yearly debt and post pay_debt transactions using
    the OLD (buggy /12) interest formula, mirroring how a v6-or-earlier DB
    would have evolved.
    """
    from src.storage.asset_repo import create_asset
    from src.storage.debt_repo import create_debt
    from src.models.asset import Asset
    from src.models.debt import Debt
    asset = create_asset(conn, Asset(symbol="YL", name="Yearly Loan", asset_type="debt"))
    debt = Debt(
        asset_id=asset.id, name="Yearly Loan",
        original_amount=principal, current_balance=principal,
        interest_rate=rate, schedule_frequency="yearly",
    )
    create_debt(conn, debt)
    for date, amount in payments:
        conn.execute(
            "INSERT INTO transactions (date, txn_type, asset_id, total_amount, notes) "
            "VALUES (?, 'pay_debt', ?, ?, '')",
            (date, asset.id, -amount),
        )
        accrued_buggy = (debt.current_balance * rate / 12) if rate > 0 else 0
        reduction = max(0.0, amount - accrued_buggy)
        debt.current_balance = max(0.0, debt.current_balance - reduction)
    conn.execute(
        "UPDATE debts SET current_balance=? WHERE asset_id=?",
        (debt.current_balance, asset.id),
    )
    conn.commit()
    return asset.id, debt.current_balance


def test_reconcile_yearly_debt_skips_monthly_debts(db_conn):
    """Monthly-schedule debts must not be touched by the migration."""
    from src.storage.asset_repo import create_asset
    from src.storage.debt_repo import create_debt, get_debt_by_asset
    from src.storage.database import _reconcile_yearly_debt_balances
    from src.models.asset import Asset
    from src.models.debt import Debt
    asset = create_asset(db_conn, Asset(symbol="ML", name="Monthly", asset_type="debt"))
    create_debt(db_conn, Debt(
        asset_id=asset.id, name="Monthly",
        original_amount=10000.0, current_balance=8000.0,
        interest_rate=0.06, schedule_frequency="monthly",
    ))
    _reconcile_yearly_debt_balances(db_conn)
    assert get_debt_by_asset(db_conn, asset.id).current_balance == 8000.0


def test_reconcile_yearly_debt_corrects_balance(db_conn):
    """A yearly debt with prior partial pays gets its principal restored."""
    from src.storage.debt_repo import get_debt_by_asset
    from src.storage.database import _reconcile_yearly_debt_balances
    asset_id, buggy_balance = _seed_yearly_debt_with_pay_history(
        db_conn,
        principal=10000.0, rate=0.06,
        payments=[("2025-01-15", 1000.0), ("2026-01-15", 1000.0)],
    )
    correct_replay = 10000.0
    for _ in range(2):
        accrued = correct_replay * 0.06
        correct_replay = max(0.0, correct_replay - max(0.0, 1000.0 - accrued))
    assert correct_replay > buggy_balance + 0.5
    _reconcile_yearly_debt_balances(db_conn)
    fixed = get_debt_by_asset(db_conn, asset_id).current_balance
    assert fixed == pytest.approx(correct_replay, abs=0.01)


def test_reconcile_yearly_debt_idempotent(db_conn):
    """Re-running reconciliation must not double-correct."""
    from src.storage.debt_repo import get_debt_by_asset
    from src.storage.database import _reconcile_yearly_debt_balances
    asset_id, _ = _seed_yearly_debt_with_pay_history(
        db_conn, principal=10000.0, rate=0.06,
        payments=[("2025-01-15", 1000.0)],
    )
    _reconcile_yearly_debt_balances(db_conn)
    first = get_debt_by_asset(db_conn, asset_id).current_balance
    _reconcile_yearly_debt_balances(db_conn)
    second = get_debt_by_asset(db_conn, asset_id).current_balance
    assert first == second
    rows = db_conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE asset_id=? "
        "AND txn_type='manual_adjustment'",
        (asset_id,),
    ).fetchone()
    assert rows[0] == 1


def test_reconcile_yearly_debt_no_op_when_no_pay_history(db_conn):
    """Yearly debt with no pay_debt rows requires no correction."""
    from src.storage.asset_repo import create_asset
    from src.storage.debt_repo import create_debt, get_debt_by_asset
    from src.storage.database import _reconcile_yearly_debt_balances
    from src.models.asset import Asset
    from src.models.debt import Debt
    asset = create_asset(db_conn, Asset(symbol="YN", name="No Pay", asset_type="debt"))
    create_debt(db_conn, Debt(
        asset_id=asset.id, name="No Pay",
        original_amount=5000.0, current_balance=5000.0,
        interest_rate=0.06, schedule_frequency="yearly",
    ))
    _reconcile_yearly_debt_balances(db_conn)
    assert get_debt_by_asset(db_conn, asset.id).current_balance == 5000.0
    rows = db_conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE asset_id=? "
        "AND txn_type='manual_adjustment'",
        (asset.id,),
    ).fetchone()
    assert rows[0] == 0


# === Schema v9: debt plan columns (plan_type, original_term_periods, created_at) ===

def _create_v8_debts_db() -> sqlite3.Connection:
    """Build a v8-shaped DB: debts has all v3 columns but lacks the v9
    plan-persistence columns. Used to verify the v9 migration upgrades
    legacy DBs without data loss.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            region TEXT NOT NULL DEFAULT 'US',
            liquidity TEXT NOT NULL DEFAULT 'liquid',
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE debts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL UNIQUE,
            name TEXT NOT NULL,
            original_amount REAL NOT NULL,
            current_balance REAL NOT NULL,
            interest_rate REAL NOT NULL DEFAULT 0,
            minimum_payment REAL NOT NULL DEFAULT 0,
            due_date TEXT,
            notes TEXT,
            schedule_frequency TEXT NOT NULL DEFAULT 'monthly',
            interest_period TEXT NOT NULL DEFAULT 'annual',
            monthly_payment_amount REAL NOT NULL DEFAULT 0,
            cashflow_start_date TEXT,
            last_payment_date TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
    """)
    return conn


def test_v9_migration_adds_plan_columns_to_legacy_debts():
    conn = _create_v8_debts_db()
    conn.execute(
        "INSERT INTO assets (symbol, name, asset_type) VALUES ('L', 'Loan', 'debt')"
    )
    conn.execute(
        "INSERT INTO debts (asset_id, name, original_amount, current_balance, "
        "interest_rate, monthly_payment_amount, updated_at) "
        "VALUES (1, 'Legacy Loan', 5000.0, 4500.0, 0.06, 200.0, '2024-01-15T00:00:00')"
    )
    conn.commit()

    # Pre-migration: new columns absent.
    cols = {row[1] for row in conn.execute("PRAGMA table_info(debts)").fetchall()}
    assert "plan_type" not in cols
    assert "original_term_periods" not in cols
    assert "created_at" not in cols

    from src.storage.database import _migrate_debt_plan_columns
    _migrate_debt_plan_columns(conn)
    conn.commit()

    cols = {row[1] for row in conn.execute("PRAGMA table_info(debts)").fetchall()}
    assert "plan_type" in cols
    assert "original_term_periods" in cols
    assert "created_at" in cols

    row = conn.execute(
        "SELECT plan_type, original_term_periods, created_at, updated_at "
        "FROM debts WHERE asset_id = 1"
    ).fetchone()
    # Legacy rows default to fixed_payment (the only plan that's faithful
    # without a stored term).
    assert row["plan_type"] == "fixed_payment"
    assert row["original_term_periods"] is None
    # created_at backfilled from updated_at on legacy rows.
    assert row["created_at"] == "2024-01-15T00:00:00"
    conn.close()


def test_v9_migration_idempotent():
    """Running the migration twice must not double-add columns or
    regress backfill."""
    conn = _create_v8_debts_db()
    conn.execute(
        "INSERT INTO assets (symbol, name, asset_type) VALUES ('L', 'Loan', 'debt')"
    )
    conn.execute(
        "INSERT INTO debts (asset_id, name, original_amount, current_balance, "
        "monthly_payment_amount, updated_at) "
        "VALUES (1, 'Legacy', 1000.0, 1000.0, 50.0, '2024-06-01T00:00:00')"
    )
    conn.commit()
    from src.storage.database import _migrate_debt_plan_columns
    _migrate_debt_plan_columns(conn)
    _migrate_debt_plan_columns(conn)  # second call must be no-op
    conn.commit()
    cols = [row[1] for row in conn.execute("PRAGMA table_info(debts)").fetchall()]
    # Each new column appears exactly once.
    assert cols.count("plan_type") == 1
    assert cols.count("original_term_periods") == 1
    assert cols.count("created_at") == 1
    conn.close()


def test_v9_schema_version_constant_advanced():
    """Sanity check that CURRENT_SCHEMA_VERSION reflects the v9 bump.
    Catches a future migration that forgets to advance the constant."""
    from src.storage.database import CURRENT_SCHEMA_VERSION
    assert CURRENT_SCHEMA_VERSION >= 9


def test_fresh_db_has_v9_columns(db_conn):
    """End-to-end: the standard `db_conn` fixture (which calls init_db)
    must produce a debts table with all v9 columns."""
    cols = {row[1] for row in db_conn.execute(
        "PRAGMA table_info(debts)"
    ).fetchall()}
    for required in ("plan_type", "original_term_periods", "created_at"):
        assert required in cols, f"Missing column: {required}"


# === Schema v10: preview_* cols + debt_payment_records + drop missed_payments ===

def _create_v9_db_with_history() -> sqlite3.Connection:
    """Build a v9-shaped DB with a debt + a few pay_debt transactions
    + an unresolved missed_payments row. Exercises every facet of the
    v10 migration: preview-column add, debt_payment_records backfill,
    missed_payments → bankruptcy_events migration, missed_payments DROP.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            region TEXT NOT NULL DEFAULT 'US',
            liquidity TEXT NOT NULL DEFAULT 'liquid',
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            txn_type TEXT NOT NULL,
            asset_id INTEGER,
            quantity REAL,
            price REAL,
            total_amount REAL NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            fees REAL NOT NULL DEFAULT 0,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
        CREATE TABLE debts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL UNIQUE,
            name TEXT NOT NULL,
            original_amount REAL NOT NULL,
            current_balance REAL NOT NULL,
            interest_rate REAL NOT NULL DEFAULT 0,
            minimum_payment REAL NOT NULL DEFAULT 0,
            due_date TEXT,
            notes TEXT,
            schedule_frequency TEXT NOT NULL DEFAULT 'monthly',
            interest_period TEXT NOT NULL DEFAULT 'annual',
            monthly_payment_amount REAL NOT NULL DEFAULT 0,
            cashflow_start_date TEXT,
            last_payment_date TEXT,
            plan_type TEXT NOT NULL DEFAULT 'fixed_payment',
            original_term_periods INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
        CREATE TABLE missed_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            asset_id INTEGER NOT NULL,
            due_date TEXT NOT NULL,
            amount_due REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'missed',
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(kind, asset_id, due_date)
        );
        CREATE TABLE bankruptcy_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_date TEXT NOT NULL,
            trigger_kind TEXT NOT NULL,
            asset_id INTEGER,
            due_date TEXT,
            amount_due REAL NOT NULL DEFAULT 0,
            cash_balance REAL NOT NULL DEFAULT 0,
            shortfall_amount REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active',
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    # Seed: one debt asset + debt row, two pay_debt history rows (one
    # auto-settled with the canonical note prefix, one manual), and
    # one unresolved missed_payments row.
    conn.execute(
        "INSERT INTO assets (symbol, name, asset_type) VALUES ('LOAN', 'Loan', 'debt')"
    )
    conn.execute(
        "INSERT INTO debts (asset_id, name, original_amount, current_balance, "
        "interest_rate, schedule_frequency, monthly_payment_amount, "
        "plan_type, original_term_periods) "
        "VALUES (1, 'My Loan', 1000.0, 600.0, 0.0, 'monthly', 100.0, "
        "'fixed_payment', NULL)"
    )
    conn.execute(
        "INSERT INTO transactions (date, txn_type, asset_id, total_amount, notes) "
        "VALUES ('2025-01-01', 'pay_debt', 1, -100.0, "
        "'Scheduled debt payment 2025-01-01 — auto-deducted')"
    )
    conn.execute(
        "INSERT INTO transactions (date, txn_type, asset_id, total_amount, notes) "
        "VALUES ('2025-02-15', 'pay_debt', 1, -300.0, 'Manual extra')"
    )
    conn.execute(
        "INSERT INTO missed_payments (kind, asset_id, due_date, amount_due, "
        "status, notes) VALUES ('debt', 1, '2025-03-01', 100.0, 'missed', "
        "'overdue from auto-settle')"
    )
    conn.commit()
    conn.execute("PRAGMA user_version = 9")
    return conn


def test_v10_migration_adds_preview_columns():
    conn = _create_v9_db_with_history()
    from src.storage.database import (
        _migrate_v10_debt_preview_and_payment_records,
    )
    _migrate_v10_debt_preview_and_payment_records(conn)
    conn.commit()
    cols = {row[1] for row in conn.execute(
        "PRAGMA table_info(debts)"
    ).fetchall()}
    for required in (
        "preview_regular_payment", "preview_period_count",
        "preview_final_payment", "preview_total_paid",
        "preview_total_interest",
    ):
        assert required in cols, f"Missing column: {required}"
    conn.close()


def test_v10_migration_creates_debt_payment_records_table():
    conn = _create_v9_db_with_history()
    from src.storage.database import (
        _migrate_v10_debt_preview_and_payment_records,
    )
    _migrate_v10_debt_preview_and_payment_records(conn)
    conn.commit()
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "debt_payment_records" in tables
    conn.close()


def test_v10_migration_backfills_debt_payment_records_from_history():
    conn = _create_v9_db_with_history()
    from src.storage.database import (
        _migrate_v10_debt_preview_and_payment_records,
    )
    _migrate_v10_debt_preview_and_payment_records(conn)
    conn.commit()
    rows = conn.execute(
        "SELECT transaction_id, debt_id, payment_amount, payment_type, "
        "balance_before_payment, balance_after_payment FROM "
        "debt_payment_records ORDER BY id"
    ).fetchall()
    # Two pay_debt history rows → two backfilled records.
    assert len(rows) == 2
    # First (auto-settled, $100): balance went 1000 → 900.
    assert rows[0]["payment_type"] == "automatic"
    assert rows[0]["payment_amount"] == pytest.approx(100.0)
    assert rows[0]["balance_before_payment"] == pytest.approx(1000.0)
    assert rows[0]["balance_after_payment"] == pytest.approx(900.0)
    # Second (manual, $300): balance went 900 → 600.
    assert rows[1]["payment_type"] == "manual"
    assert rows[1]["payment_amount"] == pytest.approx(300.0)
    assert rows[1]["balance_before_payment"] == pytest.approx(900.0)
    assert rows[1]["balance_after_payment"] == pytest.approx(600.0)
    conn.close()


def test_v10_migration_drops_missed_payments_after_migrating_to_events():
    conn = _create_v9_db_with_history()
    from src.storage.database import _migrate_v10_drop_missed_payments
    _migrate_v10_drop_missed_payments(conn)
    conn.commit()
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "missed_payments" not in tables
    # The single unresolved row should now be a bankruptcy_events row.
    events = conn.execute(
        "SELECT trigger_kind, asset_id, due_date, amount_due "
        "FROM bankruptcy_events"
    ).fetchall()
    assert len(events) == 1
    assert events[0]["trigger_kind"] == "debt"
    assert events[0]["asset_id"] == 1
    assert events[0]["due_date"] == "2025-03-01"
    assert events[0]["amount_due"] == pytest.approx(100.0)
    conn.close()


def test_v10_migration_idempotent():
    """Running both v10 migrations twice must not duplicate records or
    bankruptcy_events rows."""
    conn = _create_v9_db_with_history()
    from src.storage.database import (
        _migrate_v10_debt_preview_and_payment_records,
        _migrate_v10_drop_missed_payments,
    )
    _migrate_v10_debt_preview_and_payment_records(conn)
    _migrate_v10_drop_missed_payments(conn)
    conn.commit()
    # Second run: no errors, no duplicate rows.
    _migrate_v10_debt_preview_and_payment_records(conn)
    # missed_payments table is gone — second drop call should no-op.
    _migrate_v10_drop_missed_payments(conn)
    conn.commit()
    pay_records = conn.execute(
        "SELECT COUNT(*) FROM debt_payment_records"
    ).fetchone()[0]
    events = conn.execute(
        "SELECT COUNT(*) FROM bankruptcy_events"
    ).fetchone()[0]
    assert pay_records == 2
    assert events == 1
    conn.close()


def test_v10_schema_version_constant_advanced():
    from src.storage.database import CURRENT_SCHEMA_VERSION
    assert CURRENT_SCHEMA_VERSION >= 10


def test_fresh_db_has_v10_columns_and_table(db_conn):
    """The standard `db_conn` fixture (init_db) produces a v10 schema
    with all the new state."""
    cols = {row[1] for row in db_conn.execute(
        "PRAGMA table_info(debts)"
    ).fetchall()}
    for required in (
        "preview_regular_payment", "preview_period_count",
        "preview_final_payment", "preview_total_paid",
        "preview_total_interest",
    ):
        assert required in cols, f"Missing column: {required}"
    tables = {row[0] for row in db_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "debt_payment_records" in tables
    assert "missed_payments" not in tables


# === debts.created_at NOT NULL rebuild (post-v10 schema-drift fix) ===

def _build_legacy_debts_db_with_nullable_created_at(path) -> None:
    """Build an on-disk DB shaped like a v9-migrated legacy install.

    The shape mirrors what a real user upgrade produces: every v10
    column is present (so we're past the column-add migrations), but
    `created_at` lacks `NOT NULL` and `DEFAULT (datetime('now'))` —
    matching exactly what `_migrate_debt_plan_columns`'s ALTER ADD
    COLUMN produced because SQLite forbids non-constant defaults on
    that statement form. Includes a row whose `created_at` is NULL,
    plus a row with a real timestamp, so we can test both backfill and
    pass-through.
    """
    c = sqlite3.connect(str(path))
    c.execute("PRAGMA foreign_keys=ON")
    c.executescript("""
        CREATE TABLE assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            region TEXT NOT NULL DEFAULT 'US',
            liquidity TEXT NOT NULL DEFAULT 'liquid',
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            txn_type TEXT NOT NULL,
            asset_id INTEGER,
            quantity REAL,
            price REAL,
            total_amount REAL NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            fees REAL NOT NULL DEFAULT 0,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
        CREATE TABLE debts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL UNIQUE,
            name TEXT NOT NULL,
            original_amount REAL NOT NULL,
            current_balance REAL NOT NULL,
            interest_rate REAL NOT NULL DEFAULT 0,
            minimum_payment REAL NOT NULL DEFAULT 0,
            due_date TEXT,
            notes TEXT,
            schedule_frequency TEXT NOT NULL DEFAULT 'monthly',
            interest_period TEXT NOT NULL DEFAULT 'annual',
            monthly_payment_amount REAL NOT NULL DEFAULT 0,
            cashflow_start_date TEXT,
            last_payment_date TEXT,
            plan_type TEXT NOT NULL DEFAULT 'fixed_payment',
            original_term_periods INTEGER,
            preview_regular_payment REAL NOT NULL DEFAULT 0,
            preview_period_count INTEGER NOT NULL DEFAULT 0,
            preview_final_payment REAL NOT NULL DEFAULT 0,
            preview_total_paid REAL NOT NULL DEFAULT 0,
            preview_total_interest REAL NOT NULL DEFAULT 0,
            created_at TEXT,                                     -- legacy: nullable, no default
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
        CREATE TABLE debt_payment_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id INTEGER NOT NULL UNIQUE,
            debt_id INTEGER NOT NULL,
            debt_name TEXT NOT NULL,
            payment_amount REAL NOT NULL,
            payment_date TEXT NOT NULL,
            payment_type TEXT NOT NULL,
            balance_before_payment REAL NOT NULL,
            balance_after_payment REAL NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (transaction_id) REFERENCES transactions(id),
            FOREIGN KEY (debt_id) REFERENCES debts(id)
        );
    """)
    c.execute(
        "INSERT INTO assets (symbol, name, asset_type) VALUES ('L1', 'Loan One', 'debt')"
    )
    c.execute(
        "INSERT INTO assets (symbol, name, asset_type) VALUES ('L2', 'Loan Two', 'debt')"
    )
    # Row 1: created_at populated (the v9 migration's backfill path)
    c.execute("""
        INSERT INTO debts (asset_id, name, original_amount, current_balance,
                           interest_rate, monthly_payment_amount, plan_type,
                           created_at, updated_at)
        VALUES (1, 'Loan One', 5000.0, 4500.0, 0.06, 200.0, 'fixed_payment',
                '2024-01-15T00:00:00', '2024-01-15T00:00:00')
    """)
    # Row 2: created_at NULL (a row inserted after the v9 migration by a
    # repo call that didn't supply created_at — the bug under repair)
    c.execute("""
        INSERT INTO debts (asset_id, name, original_amount, current_balance,
                           interest_rate, monthly_payment_amount, plan_type,
                           created_at, updated_at)
        VALUES (2, 'Loan Two', 3000.0, 3000.0, 0.05, 100.0, 'fixed_payment',
                NULL, '2024-06-01T00:00:00')
    """)
    # Add a debt_payment_records row to verify the FK survives the rebuild.
    c.execute("""
        INSERT INTO transactions (date, txn_type, asset_id, total_amount, notes)
        VALUES ('2024-02-01', 'pay_debt', 1, -200.0, 'manual pay')
    """)
    c.execute("""
        INSERT INTO debt_payment_records
            (transaction_id, debt_id, debt_name, payment_amount,
             payment_date, payment_type, balance_before_payment,
             balance_after_payment)
        VALUES (1, 1, 'Loan One', 200.0, '2024-02-01', 'manual', 4700.0, 4500.0)
    """)
    c.commit()
    c.close()


class TestDebtsCreatedAtRebuild:

    def test_legacy_nullable_column_becomes_not_null(self, tmp_path):
        from src.storage.database import init_db
        path = tmp_path / "legacy.db"
        _build_legacy_debts_db_with_nullable_created_at(path)

        # Pre-rebuild: created_at is nullable.
        c = sqlite3.connect(str(path))
        cols = c.execute("PRAGMA table_info(debts)").fetchall()
        c.close()
        created_at = next(c for c in cols if c[1] == "created_at")
        assert created_at[3] == 0, "precondition: created_at must start nullable"

        conn = init_db(str(path))
        try:
            cols = conn.execute("PRAGMA table_info(debts)").fetchall()
            created_at = next(c for c in cols if c[1] == "created_at")
            assert created_at[3] == 1, "created_at must be NOT NULL after rebuild"
        finally:
            conn.close()

    def test_existing_null_created_at_is_backfilled(self, tmp_path):
        from src.storage.database import init_db
        path = tmp_path / "legacy.db"
        _build_legacy_debts_db_with_nullable_created_at(path)
        conn = init_db(str(path))
        try:
            row = conn.execute(
                "SELECT created_at FROM debts WHERE name = 'Loan Two'"
            ).fetchone()
            assert row["created_at"] is not None
            assert row["created_at"]  # non-empty timestamp string
        finally:
            conn.close()

    def test_pre_existing_timestamp_preserved(self, tmp_path):
        """Rows that already had a real created_at must come through unchanged."""
        from src.storage.database import init_db
        path = tmp_path / "legacy.db"
        _build_legacy_debts_db_with_nullable_created_at(path)
        conn = init_db(str(path))
        try:
            row = conn.execute(
                "SELECT created_at FROM debts WHERE name = 'Loan One'"
            ).fetchone()
            assert row["created_at"] == "2024-01-15T00:00:00"
        finally:
            conn.close()

    def test_default_fires_for_new_inserts_after_rebuild(self, tmp_path):
        """The whole point of the rebuild: a `create_debt`-style INSERT
        that omits `created_at` must now get the schema's
        `datetime('now')` default, not NULL."""
        from src.storage.database import init_db
        path = tmp_path / "legacy.db"
        _build_legacy_debts_db_with_nullable_created_at(path)
        conn = init_db(str(path))
        try:
            conn.execute(
                "INSERT INTO assets (symbol, name, asset_type) "
                "VALUES ('L3', 'Loan Three', 'debt')"
            )
            asset_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            # Mirror the column list `debt_repo.create_debt` uses — note
            # `created_at` is NOT in the list.
            conn.execute(
                "INSERT INTO debts (asset_id, name, original_amount, "
                "current_balance, interest_rate, minimum_payment, due_date, "
                "notes, schedule_frequency, interest_period, "
                "monthly_payment_amount, cashflow_start_date, "
                "last_payment_date, plan_type, original_term_periods, "
                "preview_regular_payment, preview_period_count, "
                "preview_final_payment, preview_total_paid, "
                "preview_total_interest) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?)",
                (asset_id, 'Loan Three', 1000.0, 1000.0, 0.0, 0.0, None,
                 None, 'monthly', 'annual', 0.0, None, None,
                 'fixed_payment', None, 0.0, 0, 0.0, 0.0, 0.0),
            )
            conn.commit()
            row = conn.execute(
                "SELECT created_at FROM debts WHERE name = 'Loan Three'"
            ).fetchone()
            assert row["created_at"] is not None
            assert row["created_at"]  # non-empty timestamp from datetime('now')
        finally:
            conn.close()

    def test_rebuild_preserves_other_columns_and_data(self, tmp_path):
        """Smoke: every other field of the two seeded rows survives."""
        from src.storage.database import init_db
        path = tmp_path / "legacy.db"
        _build_legacy_debts_db_with_nullable_created_at(path)
        conn = init_db(str(path))
        try:
            row1 = conn.execute(
                "SELECT * FROM debts WHERE name = 'Loan One'"
            ).fetchone()
            assert row1["original_amount"] == 5000.0
            assert row1["current_balance"] == 4500.0
            assert row1["interest_rate"] == 0.06
            assert row1["monthly_payment_amount"] == 200.0
            assert row1["plan_type"] == "fixed_payment"

            row2 = conn.execute(
                "SELECT * FROM debts WHERE name = 'Loan Two'"
            ).fetchone()
            assert row2["original_amount"] == 3000.0
            assert row2["current_balance"] == 3000.0
            assert row2["interest_rate"] == 0.05
        finally:
            conn.close()

    def test_rebuild_preserves_ids(self, tmp_path):
        """IDs must roundtrip — `debt_payment_records.debt_id` references
        them, and any in-flight Debt object the GUI is holding still
        identifies its row by id."""
        from src.storage.database import init_db
        path = tmp_path / "legacy.db"
        _build_legacy_debts_db_with_nullable_created_at(path)
        conn = init_db(str(path))
        try:
            row1 = conn.execute(
                "SELECT id FROM debts WHERE name = 'Loan One'"
            ).fetchone()
            row2 = conn.execute(
                "SELECT id FROM debts WHERE name = 'Loan Two'"
            ).fetchone()
            assert row1["id"] == 1
            assert row2["id"] == 2
        finally:
            conn.close()

    def test_debt_payment_records_fk_still_resolves(self, tmp_path):
        """The rebuild DROPs and renames `debts`, which crosses
        `debt_payment_records.debt_id` FK boundary. Verify FK
        enforcement is back on AND the existing FK reference still
        resolves (the row still joins to its debt)."""
        from src.storage.database import init_db
        path = tmp_path / "legacy.db"
        _build_legacy_debts_db_with_nullable_created_at(path)
        conn = init_db(str(path))
        try:
            # FK is enforced
            assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
            # No orphans introduced by the rebuild
            issues = conn.execute("PRAGMA foreign_key_check").fetchall()
            assert issues == []
            # The seeded payment record still joins to its debt.
            row = conn.execute(
                "SELECT d.name FROM debt_payment_records r "
                "JOIN debts d ON d.id = r.debt_id"
            ).fetchone()
            assert row["name"] == "Loan One"
        finally:
            conn.close()

    def test_rebuild_is_idempotent_via_double_init(self, tmp_path):
        """A second `init_db` on the same on-disk DB must not raise and
        must keep the NOT NULL constraint. The migration's early-return
        on `notnull == 1` guards this."""
        from src.storage.database import init_db
        path = tmp_path / "legacy.db"
        _build_legacy_debts_db_with_nullable_created_at(path)
        # First run: rebuild fires.
        init_db(str(path)).close()
        # Second run: should be a no-op.
        conn = init_db(str(path))
        try:
            cols = conn.execute("PRAGMA table_info(debts)").fetchall()
            created_at = next(c for c in cols if c[1] == "created_at")
            assert created_at[3] == 1
            # Data still intact after the second open.
            count = conn.execute("SELECT COUNT(*) FROM debts").fetchone()[0]
            assert count == 2
        finally:
            conn.close()

    def test_fresh_db_has_not_null_created_at(self, db_conn):
        """The standard fixture (fresh DB via init_db on `:memory:`)
        already has the constraint thanks to schema.sql. The migration
        just happens to be a no-op on this path; this guard catches any
        future regression that swaps the schema's DDL."""
        cols = db_conn.execute("PRAGMA table_info(debts)").fetchall()
        created_at = next(c for c in cols if c[1] == "created_at")
        assert created_at[3] == 1, "fresh DB must have created_at NOT NULL"

