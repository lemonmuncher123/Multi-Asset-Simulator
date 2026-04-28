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
    conn = _create_old_market_prices_db()
    conn.execute("INSERT INTO assets (symbol, name, asset_type) VALUES ('AAPL', 'Apple', 'stock')")
    conn.execute(
        "INSERT INTO market_prices (asset_id, date, price, source) VALUES (1, '2025-01-10', 100.0, 'manual')"
    )
    conn.execute(
        "INSERT INTO market_prices (asset_id, date, price, source) VALUES (1, '2025-01-10', 105.0, 'manual')"
    )
    conn.execute(
        "INSERT INTO market_prices (asset_id, date, price, source) VALUES (1, '2025-01-10', 110.0, 'manual')"
    )
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM market_prices").fetchone()[0] == 3

    from src.storage.database import _migrate
    _migrate(conn)
    conn.commit()

    assert conn.execute("SELECT COUNT(*) FROM market_prices").fetchone()[0] == 1
    row = conn.execute("SELECT price FROM market_prices").fetchone()
    assert row[0] == 110.0  # newest row (highest id) kept
    conn.close()


def test_migration_upsert_works_after_index_created():
    conn = _create_old_market_prices_db()
    conn.execute("INSERT INTO assets (symbol, name, asset_type) VALUES ('AAPL', 'Apple', 'stock')")
    conn.commit()

    from src.storage.database import _migrate
    _migrate(conn)
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
    conn.execute("INSERT INTO assets (symbol, name, asset_type) VALUES ('AAPL', 'Apple', 'stock')")
    conn.commit()

    from src.storage.database import _migrate
    _migrate(conn)
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


def test_migration_leaves_old_property_cashflow_start_date_null():
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

    row = conn.execute("SELECT cashflow_start_date FROM properties WHERE asset_id = 1").fetchone()
    assert row[0] is None
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
        assert get_report_stats(db_conn) == {"total": 0, "monthly": 0, "annual": 0}
        _insert_report(db_conn, "monthly", "2025-01-01", "2025-02-01", "2025-01", "Jan")
        _insert_report(db_conn, "monthly", "2025-02-01", "2025-03-01", "2025-02", "Feb")
        _insert_report(db_conn, "annual", "2025-01-01", "2026-01-01", "2025", "Year 2025")
        stats = get_report_stats(db_conn)
        assert stats == {"total": 3, "monthly": 2, "annual": 1}


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
