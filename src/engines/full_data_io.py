import csv
import io
import logging
import os
import sqlite3
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

_log = logging.getLogger(__name__)

SCHEMA_VERSION = "1"

EXPORT_TABLES = [
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

IMPORT_ORDER = [
    "settings",
    "securities_master",
    "assets",
    "portfolio_snapshots",
    "price_sync_log",
    "market_prices",
    "market_quotes",
    "properties",
    "debts",
    # mortgages depend on properties (FK) — must come after.
    "mortgages",
    "decision_journal",
    "transactions",
    "transaction_fee_breakdown",
    # debt_payment_records depends on transactions (FK) — must come after.
    "debt_payment_records",
    # mortgage_payment_records depends on transactions AND mortgages — must
    # come after both.
    "mortgage_payment_records",
    "reports",
    # Imported last so the assets they reference are guaranteed present.
    "bankruptcy_events",
]


# Drift guard: EXPORT_TABLES is the set of tables we serialize; IMPORT_ORDER
# is the FK-aware insertion order for restoring them. Any new persistent
# table must appear in BOTH (PROJECT_UNDERSTANDING.md §6 calls this out
# explicitly). Asserting set-equality at module load fails loudly the next
# time someone forgets one of the two.
assert set(EXPORT_TABLES) == set(IMPORT_ORDER), (
    "EXPORT_TABLES and IMPORT_ORDER drift: "
    f"only in EXPORT_TABLES={set(EXPORT_TABLES) - set(IMPORT_ORDER)}, "
    f"only in IMPORT_ORDER={set(IMPORT_ORDER) - set(EXPORT_TABLES)}"
)


# Per-table per-column validators for the Full Data Import path.
#
# The importer treats CSVs as authoritative and replays raw rows via
# `executemany`, bypassing every engine-layer guard in `ledger.*`. These
# rules mirror the engine's non-negative invariants so a hand-edited or
# corrupted CSV cannot smuggle a negative `monthly_rent` / `current_balance`
# / etc. into the DB.
#
# Columns NOT listed here are intentionally unconstrained:
#   - `transactions.total_amount` is signed by convention (buys negative,
#     sells positive); enforcing a sign here would break every export.
#   - `portfolio_snapshots.cash` and `.net_worth` may legitimately be
#     negative (overdraft, insolvency).
#   - `bankruptcy_events.cash_balance` may be negative (the event records
#     the cash level at the moment of bankruptcy, often negative).
#
# Each entry maps column name -> ("non_negative" | "positive" | "in_unit").
# `non_negative` means `value is None or value >= 0`. `positive` means
# `value > 0` (`None` allowed). `in_unit` means `value is None or 0 <= value <= 1`.
_NEGATIVE_GUARD_RULES: dict[str, dict[str, str]] = {
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
        # `price` is the canonical close — must be > 0. OHLCV fields are
        # nullable per-source and may legitimately be missing; reject only
        # negatives, not zeros (some illiquid sessions print 0 volume).
        "price": "positive",
        "open": "non_negative",
        "high": "non_negative",
        "low": "non_negative",
        "close": "non_negative",
        "adjusted_close": "non_negative",
        "volume": "non_negative",
    },
    "market_quotes": {
        "bid": "positive",
        "ask": "positive",
        "last": "positive",
    },
    "transactions": {
        # `total_amount` is signed by convention — NOT validated.
        # `quantity` is positive when set (buy/sell qty > 0; manual_adjustment
        # quantity must be > 0 per the new rule). Allow NULL.
        "quantity": "positive",
        "price": "positive",
        "fees": "non_negative",
    },
    "transaction_fee_breakdown": {
        # Fee items are conceptually >= 0 — see trading_costs.py and
        # `_V12_CHECK_RULES["transaction_fee_breakdown"]` in database.py.
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
        # cash_balance may be negative — that's the point of the event.
        "amount_due": "non_negative",
        "shortfall_amount": "non_negative",
    },
    "portfolio_snapshots": {
        # cash and net_worth may be negative (overdraft, insolvency).
        "total_assets": "non_negative",
        "total_liabilities": "non_negative",
    },
}


def _validate_row_for_table(
    table: str, row: dict, row_num: int,
) -> list[str]:
    """Apply `_NEGATIVE_GUARD_RULES[table]` to `row`. Returns a list of
    human-readable error strings (empty when the row is clean).

    Raw CSV string values are coerced via `float`; an unparseable cell is
    reported but does not abort the rule loop, so a single row can produce
    multiple violations in one pass.
    """
    rules = _NEGATIVE_GUARD_RULES.get(table)
    if not rules:
        return []
    errors: list[str] = []
    for col, rule in rules.items():
        raw = row.get(col, "")
        if raw is None or (isinstance(raw, str) and raw.strip() == ""):
            continue  # NULL is permitted by every rule
        try:
            val = float(raw)
        except (TypeError, ValueError):
            errors.append(
                f"{table} row {row_num} column {col}: cannot parse {raw!r} as a number."
            )
            continue
        if rule == "positive" and val <= 0:
            errors.append(
                f"{table} row {row_num} column {col}: must be positive (got {val})."
            )
        elif rule == "non_negative" and val < 0:
            errors.append(
                f"{table} row {row_num} column {col}: cannot be negative (got {val})."
            )
        elif rule == "in_unit" and (val < 0 or val > 1):
            errors.append(
                f"{table} row {row_num} column {col}: must be in [0, 1] (got {val})."
            )
    return errors


@dataclass
class TableInfo:
    name: str
    row_count: int
    columns: list[str]


@dataclass
class ExportManifest:
    schema_version: str
    exported_at: str
    tables: list[TableInfo]
    # Source database's `PRAGMA user_version` at export time. Recorded so an
    # old backup that fails to import can be diagnosed against the DB schema
    # it came from, rather than guessing. Empty string when reading an older
    # manifest that pre-dates this column (backward compat).
    db_schema_version: str = ""


@dataclass
class FullIOResult:
    success: bool
    message: str
    details: list[str] = field(default_factory=list)


def _get_table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def _order_clause(table: str) -> str:
    if table == "settings":
        return "ORDER BY key"
    return "ORDER BY id"


def _export_table_csv(conn: sqlite3.Connection, table: str) -> tuple[str, int]:
    columns = _get_table_columns(conn, table)
    rows = conn.execute(
        f"SELECT {', '.join(columns)} FROM {table} {_order_clause(table)}"
    ).fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    for row in rows:
        writer.writerow(list(row))
    return output.getvalue(), len(rows)


def _read_db_schema_version(conn: sqlite3.Connection) -> str:
    try:
        row = conn.execute("PRAGMA user_version").fetchone()
    except Exception:
        return ""
    if row is None:
        return ""
    return str(row[0])


def _write_manifest(
    tables_info: list[tuple[str, int]],
    exported_at: str,
    db_schema_version: str,
) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "schema_version", "exported_at", "db_schema_version",
        "table_name", "row_count",
    ])
    for table_name, row_count in tables_info:
        writer.writerow([
            SCHEMA_VERSION, exported_at, db_schema_version,
            table_name, row_count,
        ])
    return output.getvalue()


def export_full_data(conn: sqlite3.Connection, output_path: str | Path) -> FullIOResult:
    output_path = Path(output_path)
    exported_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    use_zip = str(output_path).endswith(".zip")

    tables_info: list[tuple[str, int]] = []
    csv_data: dict[str, str] = {}

    for table in EXPORT_TABLES:
        csv_text, row_count = _export_table_csv(conn, table)
        csv_data[f"{table}.csv"] = csv_text
        tables_info.append((table, row_count))

    db_schema_version = _read_db_schema_version(conn)
    manifest_csv = _write_manifest(tables_info, exported_at, db_schema_version)
    csv_data["manifest.csv"] = manifest_csv

    if use_zip:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for filename, content in csv_data.items():
                zf.writestr(filename, content)
    else:
        output_path.mkdir(parents=True, exist_ok=True)
        for filename, content in csv_data.items():
            with open(output_path / filename, "w", newline="") as f:
                f.write(content)

    total_rows = sum(rc for _, rc in tables_info)
    return FullIOResult(
        success=True,
        message=f"Exported {len(EXPORT_TABLES)} tables ({total_rows} rows) to {output_path}",
    )


def _read_csv_from_source(source_path: Path, filename: str) -> str | None:
    if source_path.suffix == ".zip":
        with zipfile.ZipFile(source_path, "r") as zf:
            if filename in zf.namelist():
                return zf.read(filename).decode("utf-8")
            return None
    else:
        file_path = source_path / filename
        if file_path.exists():
            return file_path.read_text()
        return None


def _list_csv_files(source_path: Path) -> list[str]:
    if source_path.suffix == ".zip":
        with zipfile.ZipFile(source_path, "r") as zf:
            return [n for n in zf.namelist() if n.endswith(".csv")]
    else:
        return [f.name for f in source_path.iterdir() if f.suffix == ".csv"]


def inspect_full_export(input_path: str | Path) -> ExportManifest | None:
    input_path = Path(input_path)
    manifest_text = _read_csv_from_source(input_path, "manifest.csv")
    if manifest_text is None:
        return None

    reader = csv.DictReader(io.StringIO(manifest_text))
    tables: list[TableInfo] = []
    schema_version = ""
    exported_at = ""
    db_schema_version = ""

    for row in reader:
        schema_version = row.get("schema_version", "")
        exported_at = row.get("exported_at", "")
        # Older manifests don't have this column; default to empty string.
        db_schema_version = row.get("db_schema_version", "") or ""
        table_name = row.get("table_name", "")
        row_count = int(row.get("row_count", 0))

        csv_text = _read_csv_from_source(input_path, f"{table_name}.csv")
        if csv_text:
            csv_reader = csv.reader(io.StringIO(csv_text))
            columns = next(csv_reader, [])
        else:
            columns = []

        tables.append(TableInfo(name=table_name, row_count=row_count, columns=columns))

    return ExportManifest(
        schema_version=schema_version,
        exported_at=exported_at,
        tables=tables,
        db_schema_version=db_schema_version,
    )


def read_csv_table(input_path: str | Path, table_name: str, max_rows: int = 1000) -> tuple[list[str], list[list[str]]] | None:
    input_path = Path(input_path)
    filename = table_name if table_name.endswith(".csv") else f"{table_name}.csv"
    csv_text = _read_csv_from_source(input_path, filename)
    if csv_text is None:
        return None

    reader = csv.reader(io.StringIO(csv_text))
    headers = next(reader, [])
    rows = []
    for i, row in enumerate(reader):
        if i >= max_rows:
            break
        rows.append(row)
    return headers, rows


def _make_pre_import_backup(conn: sqlite3.Connection) -> Path | None:
    try:
        row = conn.execute("PRAGMA database_list").fetchone()
    except Exception:
        _log.exception("Could not read PRAGMA database_list for pre-import backup")
        return None
    if row is None:
        return None
    db_file = row["file"] if "file" in row.keys() else (row[2] if len(row) > 2 else "")
    if not db_file:
        return None
    db_path = Path(db_file)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_name(f"{db_path.stem}.{timestamp}.pre-import.bak.db")
    try:
        # SQLite ≥ 3.27 supports parameter binding for VACUUM INTO. The
        # prior hand-rolled single-quote escape (`chr(39)+chr(39)`) handled
        # the common case but would mishandle paths containing backslashes,
        # NUL bytes, or other characters SQLite's literal parser treats
        # specially. Parameter binding sidesteps the escape entirely.
        conn.execute("VACUUM INTO ?", (str(backup_path),))
    except Exception:
        _log.exception("VACUUM INTO failed for pre-import backup at %s", backup_path)
        return None
    return backup_path


def import_full_data(
    conn: sqlite3.Connection,
    input_path: str | Path,
    mode: str = "replace",
) -> FullIOResult:
    """Replace every table's contents from the export CSVs at
    ``input_path``. The current DB state is wiped; the file is
    authoritative.

    **Bankruptcy state is part of the snapshot.** The `bankruptcy_events`
    table is included in `IMPORT_ORDER`, so a backup taken when the
    portfolio was solvent imports as solvent (no rows), and a backup
    taken when bankrupt restores the same `bankruptcy_events` rows.

    **Caveat: auto-settle runs after import** (driven by the GUI's
    `data_changed` signal in `main_window._handle_data_changed` →
    `_run_auto_settle`). If the imported snapshot has scheduled
    debt/mortgage obligations whose due dates are now in the past,
    auto-settle will try to fund them. With insufficient cash and
    sellable assets, it will declare bankruptcy via
    `record_bankruptcy_event` — even if the original snapshot was
    solvent. This is the same auto-settle pipeline that runs on every
    app launch; importing a snapshot is functionally equivalent to
    "user is starting the simulator with this state at today's date".
    The behavior is intentional in the simulator (overdue obligations
    have consequences) but worth knowing about when restoring older
    backups.
    """
    if mode != "replace":
        return FullIOResult(success=False, message=f"Unsupported import mode: {mode}")

    input_path = Path(input_path)
    details: list[str] = []

    manifest_text = _read_csv_from_source(input_path, "manifest.csv")
    if manifest_text is None:
        return FullIOResult(success=False, message="manifest.csv not found in export")

    manifest_reader = csv.DictReader(io.StringIO(manifest_text))
    manifest_tables = {}
    for row in manifest_reader:
        manifest_tables[row["table_name"]] = int(row.get("row_count", 0))

    missing_tables = []
    for table in EXPORT_TABLES:
        csv_text = _read_csv_from_source(input_path, f"{table}.csv")
        if csv_text is None:
            missing_tables.append(table)
    if missing_tables:
        return FullIOResult(
            success=False,
            message=f"Missing CSV files: {', '.join(missing_tables)}",
        )

    table_csv: dict[str, str] = {}
    for table in EXPORT_TABLES:
        table_csv[table] = _read_csv_from_source(input_path, f"{table}.csv")

    # Cache the live column list per table — both the header check
    # below and the INSERT loop later need it. Computing once avoids
    # the redundant `PRAGMA table_info(...)` round-trip per table.
    columns_by_table: dict[str, list[str]] = {
        table: _get_table_columns(conn, table) for table in EXPORT_TABLES
    }

    header_errors = []
    for table in EXPORT_TABLES:
        expected_cols = columns_by_table[table]
        reader = csv.reader(io.StringIO(table_csv[table]))
        csv_headers = next(reader, [])
        if csv_headers != expected_cols:
            header_errors.append(
                f"{table}: expected {expected_cols}, got {csv_headers}"
            )
    if header_errors:
        return FullIOResult(
            success=False,
            message="CSV header mismatch",
            details=header_errors,
        )

    # Per-field validation. The importer bypasses every engine guard, so
    # this is the only place that catches a hand-edited CSV with a
    # negative monthly_rent / current_balance / etc. Validation runs over
    # in-memory CSV rows BEFORE we touch the DB — a rejection short-
    # circuits without making the pre-import backup or modifying state.
    field_errors: list[str] = []
    for table in EXPORT_TABLES:
        if table not in _NEGATIVE_GUARD_RULES:
            continue
        reader = csv.DictReader(io.StringIO(table_csv[table]))
        for row_num, row in enumerate(reader, start=2):  # +1 for header, +1 for 1-indexed
            field_errors.extend(_validate_row_for_table(table, row, row_num))
    if field_errors:
        return FullIOResult(
            success=False,
            message="Field validation failed (negative or out-of-range values)",
            details=field_errors,
        )

    backup_path = _make_pre_import_backup(conn)
    if backup_path is not None:
        details.append(f"pre-import backup: {backup_path}")
        _log.info("Pre-import backup written to %s", backup_path)

    prev_fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    conn.execute("PRAGMA foreign_keys=OFF")

    try:
        conn.execute("BEGIN")

        for table in IMPORT_ORDER:
            conn.execute(f"DELETE FROM {table}")

        for table in IMPORT_ORDER:
            columns = columns_by_table[table]
            placeholders = ", ".join("?" * len(columns))
            col_names = ", ".join(columns)
            sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

            # Stage every row first, then push the whole batch with
            # executemany. SQLite drops most of its per-statement
            # overhead when multiple parameter sets share one
            # prepared statement, so this is meaningfully faster
            # than the prior per-row execute loop on real on-disk
            # databases. Behaviour is identical: we still run
            # inside the same BEGIN/COMMIT and the same
            # foreign_keys=OFF window the original code used.
            reader = csv.DictReader(io.StringIO(table_csv[table]))
            batch: list[list] = []
            for row in reader:
                values = []
                for col in columns:
                    val = row.get(col, "")
                    if val == "":
                        values.append(None)
                    else:
                        values.append(val)
                batch.append(values)

            if batch:
                conn.executemany(sql, batch)

            imported_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            details.append(f"{table}: {imported_count} rows imported")

        conn.execute("PRAGMA foreign_keys=ON")
        fk_issues = conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk_issues:
            conn.execute("ROLLBACK")
            issue_details = [
                f"Table {r[0]}, rowid {r[1]}, parent {r[2]}, fkid {r[3]}"
                for r in fk_issues
            ]
            return FullIOResult(
                success=False,
                message="Foreign key check failed after import",
                details=issue_details,
            )

        conn.execute("COMMIT")
        return FullIOResult(
            success=True,
            message="Full data import completed successfully",
            details=details,
        )

    except Exception as e:
        _log.exception("Full data import failed")
        try:
            conn.execute("ROLLBACK")
        except Exception:
            _log.exception("Rollback after failed import also failed")
        return FullIOResult(
            success=False,
            message=f"Import failed: {e}",
            details=details,
        )
    finally:
        conn.execute(f"PRAGMA foreign_keys={'ON' if prev_fk else 'OFF'}")
