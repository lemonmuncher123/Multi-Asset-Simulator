import csv
import io
import os
import sqlite3
import tempfile
import zipfile
import pytest
from pathlib import Path

from src.storage.database import init_db
from src.models.asset import Asset
from src.storage.asset_repo import create_asset, list_assets, get_asset
from src.storage.transaction_repo import create_transaction, list_transactions
from src.models.transaction import Transaction
from src.engines.full_data_io import (
    EXPORT_TABLES,
    IMPORT_ORDER,
    export_full_data,
    import_full_data,
    inspect_full_export,
    read_csv_table,
    _get_table_columns)


@pytest.fixture
def db_conn():
    conn = init_db(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def populated_db(db_conn):
    a1 = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    a2 = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto", region="Global"))

    create_transaction(db_conn, Transaction(
        date="2025-01-01", txn_type="deposit_cash",
        total_amount=100000.0, currency="USD"))
    create_transaction(db_conn, Transaction(
        date="2025-01-02", txn_type="buy", asset_id=a1.id,
        quantity=10, price=150.0, total_amount=-1500.0, currency="USD"))

    db_conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?)",
        ("theme", "dark"))
    db_conn.commit()
    return db_conn


# --- Export generates all expected CSV files ---

def test_export_to_folder_creates_all_files(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        result = export_full_data(populated_db, out)
        assert result.success
        for table in EXPORT_TABLES:
            assert (out / f"{table}.csv").exists(), f"{table}.csv missing"
        assert (out / "manifest.csv").exists()


def test_export_to_zip_creates_all_files(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "backup.zip"
        result = export_full_data(populated_db, out)
        assert result.success
        with zipfile.ZipFile(out, "r") as zf:
            names = zf.namelist()
            for table in EXPORT_TABLES:
                assert f"{table}.csv" in names
            assert "manifest.csv" in names


# --- CSV headers match table columns ---

def test_export_csv_headers_match_schema(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)

        for table in EXPORT_TABLES:
            expected_cols = _get_table_columns(populated_db, table)
            with open(out / f"{table}.csv") as f:
                reader = csv.reader(f)
                headers = next(reader)
            assert headers == expected_cols, f"{table} headers mismatch"


# --- Manifest row counts match ---

def test_manifest_row_counts_match(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)

        with open(out / "manifest.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                table = row["table_name"]
                expected_count = int(row["row_count"])
                with open(out / f"{table}.csv") as tf:
                    table_reader = csv.reader(tf)
                    next(table_reader)  # skip headers
                    actual_count = sum(1 for _ in table_reader)
                assert actual_count == expected_count, f"{table} count mismatch"


# --- Full import restores data ---

def test_import_restores_row_counts(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)

        asset_count = populated_db.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
        txn_count = populated_db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]

        conn2 = init_db(":memory:")
        result = import_full_data(conn2, out)
        assert result.success, result.message

        assert conn2.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == asset_count
        assert conn2.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == txn_count
        conn2.close()


def test_import_restores_key_values(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)

        conn2 = init_db(":memory:")
        import_full_data(conn2, out)

        aapl = conn2.execute(
            "SELECT * FROM assets WHERE symbol = ?", ("AAPL",)
        ).fetchone()
        assert aapl is not None
        assert aapl["name"] == "Apple"
        assert aapl["asset_type"] == "stock"

        setting = conn2.execute(
            "SELECT value FROM settings WHERE key = ?", ("theme",)
        ).fetchone()
        assert setting["value"] == "dark"
        conn2.close()


# --- Import preserves ids ---

def test_import_preserves_ids(populated_db):
    orig_assets = populated_db.execute("SELECT id, symbol FROM assets").fetchall()
    orig_txns = populated_db.execute("SELECT id, txn_type FROM transactions").fetchall()

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)

        conn2 = init_db(":memory:")
        import_full_data(conn2, out)

        for orig in orig_assets:
            row = conn2.execute(
                "SELECT symbol FROM assets WHERE id = ?", (orig["id"],)
            ).fetchone()
            assert row is not None
            assert row["symbol"] == orig["symbol"]

        for orig in orig_txns:
            row = conn2.execute(
                "SELECT txn_type FROM transactions WHERE id = ?", (orig["id"],)
            ).fetchone()
            assert row is not None
            assert row["txn_type"] == orig["txn_type"]

        conn2.close()


# --- Foreign keys pass after import ---

def test_import_foreign_keys_valid(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)

        conn2 = init_db(":memory:")
        result = import_full_data(conn2, out)
        assert result.success

        conn2.execute("PRAGMA foreign_keys=ON")
        fk_issues = conn2.execute("PRAGMA foreign_key_check").fetchall()
        assert len(fk_issues) == 0
        conn2.close()


# --- Import preserves the journal → transaction link ---

def test_import_preserves_journal_transaction_link(db_conn):
    # Schema v2 dropped the back-pointer transactions.journal_id, so the
    # only direction worth round-tripping is decision_journal.transaction_id.
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))

    create_transaction(db_conn, Transaction(
        date="2025-01-02", txn_type="buy", asset_id=asset.id,
        quantity=10, price=150.0, total_amount=-1500.0,
        currency="USD"))
    txn_id = db_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    db_conn.execute(
        "INSERT INTO decision_journal (transaction_id, date, title, reasoning) "
        "VALUES (?, ?, ?, ?)",
        (txn_id, "2025-01-02", "Buy Apple", "Growth thesis"))
    journal_id = db_conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    db_conn.commit()

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(db_conn, out)

        conn2 = init_db(":memory:")
        result = import_full_data(conn2, out)
        assert result.success, result.message

        j = conn2.execute(
            "SELECT transaction_id FROM decision_journal WHERE id = ?", (journal_id,)
        ).fetchone()
        assert j["transaction_id"] == txn_id

        conn2.close()


# --- Rollback on invalid CSV ---

def test_import_rollback_on_bad_csv(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    db_conn.commit()
    orig_count = db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0]

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(db_conn, out)

        # Corrupt a CSV by adding a bad column header
        txn_path = out / "transactions.csv"
        txn_path.write_text("bad,headers,only\n1,2,3\n")

        conn2 = init_db(":memory:")
        create_asset(conn2, Asset(symbol="MSFT", name="Microsoft", asset_type="stock"))
        conn2.commit()
        pre_import_count = conn2.execute("SELECT COUNT(*) FROM assets").fetchone()[0]

        result = import_full_data(conn2, out)
        assert not result.success

        post_import_count = conn2.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
        assert post_import_count == pre_import_count
        conn2.close()


def test_import_missing_manifest(db_conn):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        out.mkdir()
        result = import_full_data(db_conn, out)
        assert not result.success
        assert "manifest" in result.message.lower()


def test_import_missing_csv_file(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)
        os.remove(out / "assets.csv")

        conn2 = init_db(":memory:")
        result = import_full_data(conn2, out)
        assert not result.success
        assert "assets" in result.message.lower()
        conn2.close()


# --- inspect_full_export ---

def test_inspect_reads_metadata(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)

        manifest = inspect_full_export(out)
        assert manifest is not None
        assert manifest.schema_version == "1"
        assert manifest.exported_at != ""
        table_names = [t.name for t in manifest.tables]
        for table in EXPORT_TABLES:
            assert table in table_names


def test_inspect_returns_none_for_invalid(db_conn):
    with tempfile.TemporaryDirectory() as tmpdir:
        result = inspect_full_export(tmpdir)
        assert result is None


def test_inspect_from_zip(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "backup.zip"
        export_full_data(populated_db, out)

        manifest = inspect_full_export(out)
        assert manifest is not None
        assert len(manifest.tables) == len(EXPORT_TABLES)


def test_inspect_does_not_modify_db(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)

        count_before = populated_db.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
        inspect_full_export(out)
        count_after = populated_db.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
        assert count_before == count_after


# --- read_csv_table ---

def test_read_csv_table_from_folder(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)

        result = read_csv_table(out, "assets")
        assert result is not None
        headers, rows = result
        assert "symbol" in headers
        assert len(rows) == 2


def test_read_csv_table_from_zip(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "backup.zip"
        export_full_data(populated_db, out)

        result = read_csv_table(out, "assets")
        assert result is not None
        headers, rows = result
        assert len(rows) == 2


def test_read_csv_table_max_rows(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)

        result = read_csv_table(out, "assets", max_rows=1)
        assert result is not None
        _, rows = result
        assert len(rows) == 1


def test_read_csv_table_missing_returns_none(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)
        result = read_csv_table(out, "nonexistent_table")
        assert result is None


# --- Zip roundtrip ---

def test_zip_roundtrip(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "backup.zip"
        export_full_data(populated_db, out)

        conn2 = init_db(":memory:")
        result = import_full_data(conn2, out)
        assert result.success

        assert conn2.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 2
        assert conn2.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 2
        conn2.close()


# --- Unsupported mode ---

def test_import_unsupported_mode(db_conn):
    with tempfile.TemporaryDirectory() as tmpdir:
        result = import_full_data(db_conn, tmpdir, mode="merge")
        assert not result.success
        assert "unsupported" in result.message.lower()


# --- Property columns roundtrip ---

def test_full_export_import_preserves_property_columns(db_conn):
    from src.engines.ledger import deposit_cash, add_property, sell_property

    deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset, _, _ = add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0,
        down_payment=100000.0,
        rent_collection_frequency="annual")
    sell_property(db_conn, "2025-06-01", asset.id, 550000.0, fees=5000.0)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        result = export_full_data(db_conn, out)
        assert result.success

        conn2 = init_db(":memory:")
        result = import_full_data(conn2, out)
        assert result.success, result.message

        row = conn2.execute(
            "SELECT status, sold_date, sold_price, sale_fees, rent_collection_frequency "
            "FROM properties WHERE asset_id = ?",
            (asset.id,)).fetchone()
        assert row is not None
        assert row["status"] == "sold"
        assert row["sold_date"] == "2025-06-01"
        assert row["sold_price"] == 550000.0
        assert row["sale_fees"] == 5000.0
        assert row["rent_collection_frequency"] == "annual"

        conn2.close()


# --- Reports in full backup ---

def test_export_includes_reports_csv(populated_db):
    from src.engines.reports import generate_monthly_report
    generate_monthly_report(populated_db, 2025, 1)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        result = export_full_data(populated_db, out)
        assert result.success
        assert (out / "reports.csv").exists()

        import csv as _csv
        with open(out / "reports.csv") as f:
            reader = _csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["report_type"] == "monthly"
        assert rows[0]["period_label"] == "2025-01"


def test_import_restores_reports(populated_db):
    import json
    from src.engines.reports import generate_monthly_report
    from src.storage.report_repo import report_exists, get_report

    report = generate_monthly_report(populated_db, 2025, 1)
    orig_json = report.report_json

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "backup.zip"
        export_full_data(populated_db, out)

        conn2 = init_db(":memory:")
        result = import_full_data(conn2, out)
        assert result.success, result.message

        assert report_exists(conn2, "monthly", "2025-01")
        restored = get_report(conn2, "monthly", "2025-01")
        assert restored.report_json == orig_json
        data = json.loads(restored.report_json)
        assert "summary" in data

        conn2.close()


# --- EXPORT_TABLES schema coverage guard ---

def test_export_tables_covers_all_active_schema_tables(db_conn):
    schema_tables = {
        row[0] for row in db_conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }
    missing = schema_tables - set(EXPORT_TABLES)
    assert not missing, (
        f"EXPORT_TABLES is missing active schema tables: {sorted(missing)}. "
        "Full backup/restore must cover every table created by the schema."
    )


def test_export_tables_contains_no_nonexistent_tables(db_conn):
    """The reverse of the coverage guard: every entry in EXPORT_TABLES must
    correspond to a real table in the current schema, otherwise export
    will crash with `no such table: X`."""
    schema_tables = {
        row[0] for row in db_conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }
    extra = set(EXPORT_TABLES) - schema_tables
    assert not extra, (
        f"EXPORT_TABLES references tables that do not exist in the schema: "
        f"{sorted(extra)}. Remove them or add the matching CREATE TABLE."
    )


def test_import_order_covers_all_active_schema_tables(db_conn):
    """`import_full_data` only inserts tables listed in IMPORT_ORDER. If a
    new schema table is added without an IMPORT_ORDER entry, it would be
    silently skipped during restore, leaving an inconsistent DB."""
    schema_tables = {
        row[0] for row in db_conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }
    missing = schema_tables - set(IMPORT_ORDER)
    assert not missing, (
        f"IMPORT_ORDER is missing active schema tables: {sorted(missing)}. "
        "Every exported table must also be importable, in FK-safe order."
    )


def test_import_order_and_export_tables_have_same_set(db_conn):
    """Sanity: IMPORT_ORDER and EXPORT_TABLES must cover identical sets of
    tables. The orderings differ (FK-safe vs. arbitrary), but the sets
    must match — any divergence is a bug."""
    assert set(EXPORT_TABLES) == set(IMPORT_ORDER), (
        f"EXPORT_TABLES vs IMPORT_ORDER set mismatch: "
        f"in EXPORT only={sorted(set(EXPORT_TABLES) - set(IMPORT_ORDER))}, "
        f"in IMPORT only={sorted(set(IMPORT_ORDER) - set(EXPORT_TABLES))}"
    )


# --- payment_record round-trips ---------------------------------------------
#
# debt_payment_records and mortgage_payment_records both have FK
# dependencies on `transactions` (and on `debts` / `mortgages`). The
# round-trip tests verify the IMPORT_ORDER is correctly arranged so the
# parent rows exist when the children are imported.

def test_full_export_import_round_trips_debt_payment_records(db_conn):
    from src.engines.ledger import deposit_cash, add_debt, pay_debt
    from src.storage.debt_payment_record_repo import list_payment_records_for_debt

    deposit_cash(db_conn, "2025-01-01", 50_000.0)
    asset, debt, _ = add_debt(
        db_conn, date="2025-01-01", symbol="LOAN1", name="Test Loan",
        amount=10_000.0, interest_rate=0.05,
        schedule_frequency="monthly", payment_per_period=1_000.0,
    )
    pay_debt(db_conn, "2025-02-01", asset.id, 1_000.0)

    records_before = list_payment_records_for_debt(db_conn, debt.id)
    assert len(records_before) == 1, "test seed: payment record should exist"

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        result = export_full_data(db_conn, out)
        assert result.success
        assert (out / "debt_payment_records.csv").exists()

        conn2 = init_db(":memory:")
        try:
            res2 = import_full_data(conn2, out, mode="replace")
            assert res2.success, res2.message
            records_after = list_payment_records_for_debt(conn2, debt.id)
            assert len(records_after) == 1
            r_before = records_before[0]
            r_after = records_after[0]
            assert r_after.transaction_id == r_before.transaction_id
            assert r_after.payment_type == r_before.payment_type
            assert r_after.balance_before_payment == pytest.approx(r_before.balance_before_payment)
            assert r_after.balance_after_payment == pytest.approx(r_before.balance_after_payment)
        finally:
            conn2.close()


def test_full_export_import_round_trips_mortgage_payment_records(db_conn):
    from src.engines.ledger import deposit_cash, add_property, add_mortgage, pay_mortgage
    from src.storage.mortgage_payment_record_repo import list_payment_records_for_mortgage

    deposit_cash(db_conn, "2025-01-01", 250_000.0)
    asset, prop, _ = add_property(
        db_conn, "2025-01-01", symbol="HOUSE1", name="Test House",
        purchase_price=200_000.0, down_payment=50_000.0,
        acquisition_mode="new_purchase",
    )
    mortgage = add_mortgage(
        db_conn, property_id=prop.id, original_amount=150_000.0,
        interest_rate=0.04, payment_per_period=900.0,
        origination_date="2025-01-01",
    )
    pay_mortgage(db_conn, "2025-02-01", mortgage.id, 900.0)

    records_before = list_payment_records_for_mortgage(db_conn, mortgage.id)
    assert len(records_before) == 1, "test seed: mortgage payment record should exist"

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        result = export_full_data(db_conn, out)
        assert result.success
        assert (out / "mortgage_payment_records.csv").exists()

        conn2 = init_db(":memory:")
        try:
            res2 = import_full_data(conn2, out, mode="replace")
            assert res2.success, res2.message
            records_after = list_payment_records_for_mortgage(conn2, mortgage.id)
            assert len(records_after) == 1
            r_before = records_before[0]
            r_after = records_after[0]
            assert r_after.transaction_id == r_before.transaction_id
            assert r_after.payment_type == r_before.payment_type
            assert r_after.balance_before_payment == pytest.approx(r_before.balance_before_payment)
            assert r_after.balance_after_payment == pytest.approx(r_before.balance_after_payment)
        finally:
            conn2.close()


# --- db_schema_version in manifest -------------------------------------------
#
# The manifest's `schema_version` column has always tracked the export FILE
# format ("1"). It does NOT record the database schema version that produced
# the export. When debugging an old backup that fails to import, knowing the
# source DB's schema version (e.g. v9 vs v11) is essential. We capture it as
# a separate `db_schema_version` column so the file-format version can stay
# stable while the DB schema evolves.

def test_manifest_includes_db_schema_version_column(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)
        with open(out / "manifest.csv") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            assert "db_schema_version" in headers
            first = next(reader, None)
            assert first is not None
            assert first["db_schema_version"] == "12"


def test_inspect_full_export_returns_db_schema_version(populated_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)
        manifest = inspect_full_export(out)
        assert manifest is not None
        assert manifest.db_schema_version == "12"


def test_inspect_full_export_handles_old_manifest_without_db_schema_version(populated_db):
    """Backward-compat: a manifest written by an older version of the app
    won't have the `db_schema_version` column. inspect_full_export should
    return an empty string for that field rather than crash."""
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        export_full_data(populated_db, out)
        # Rewrite the manifest WITHOUT the new column to simulate an old export.
        manifest_path = out / "manifest.csv"
        rows = list(csv.DictReader(manifest_path.read_text().splitlines()))
        with open(manifest_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["schema_version", "exported_at", "table_name", "row_count"])
            for r in rows:
                writer.writerow([r["schema_version"], r["exported_at"],
                                 r["table_name"], r["row_count"]])

        manifest = inspect_full_export(out)
        assert manifest is not None
        assert manifest.db_schema_version == ""


# --- bankruptcy_events round-trips through export/import ---------------------

def test_full_export_import_round_trips_bankruptcy_events(db_conn):
    """A bankruptcy_event row must survive export + import.

    Verifies the new persistent table participates in the full backup/restore
    contract — same as any other user-data table.
    """
    from src.storage.bankruptcy_event_repo import (
        record_bankruptcy_event, list_active_bankruptcy_events)

    asset = create_asset(db_conn, Asset(symbol="L", name="Loan", asset_type="debt"))
    record_bankruptcy_event(
        db_conn,
        event_date="2025-02-01",
        trigger_kind="debt",
        asset_id=asset.id,
        due_date="2025-02-01",
        amount_due=200.0,
        cash_balance=-50.0,
        shortfall_amount=250.0,
        notes="auto-settle could not fund (test seed)")
    seeded = list_active_bankruptcy_events(db_conn)
    assert len(seeded) == 1

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "export"
        result = export_full_data(db_conn, out)
        assert result.success
        # CSV file must exist for bankruptcy_events.
        assert (out / "bankruptcy_events.csv").exists()

        # Re-import into a fresh DB and verify the row is restored exactly.
        conn2 = init_db(":memory:")
        try:
            res2 = import_full_data(conn2, out, mode="replace")
            assert res2.success, res2.message
            restored = list_active_bankruptcy_events(conn2)
            assert len(restored) == 1
            r = restored[0]
            assert r.event_date == "2025-02-01"
            assert r.trigger_kind == "debt"
            assert r.due_date == "2025-02-01"
            assert r.amount_due == pytest.approx(200.0)
            assert r.cash_balance == pytest.approx(-50.0)
            assert r.shortfall_amount == pytest.approx(250.0)
            assert r.status == "active"
            assert r.notes == "auto-settle could not fund (test seed)"
        finally:
            conn2.close()
