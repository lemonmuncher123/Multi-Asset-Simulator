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
    "decision_journal",
    "portfolio_snapshots",
    "reports",
    "securities_master",
    "settings",
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
    "decision_journal",
    "transactions",
    "transaction_fee_breakdown",
    "reports",
]


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


def _write_manifest(tables_info: list[tuple[str, int]], exported_at: str) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["schema_version", "exported_at", "table_name", "row_count"])
    for table_name, row_count in tables_info:
        writer.writerow([SCHEMA_VERSION, exported_at, table_name, row_count])
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

    manifest_csv = _write_manifest(tables_info, exported_at)
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

    for row in reader:
        schema_version = row.get("schema_version", "")
        exported_at = row.get("exported_at", "")
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
        conn.execute(f"VACUUM INTO '{str(backup_path).replace(chr(39), chr(39) + chr(39))}'")
    except Exception:
        _log.exception("VACUUM INTO failed for pre-import backup at %s", backup_path)
        return None
    return backup_path


def import_full_data(
    conn: sqlite3.Connection,
    input_path: str | Path,
    mode: str = "replace",
) -> FullIOResult:
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

    header_errors = []
    for table in EXPORT_TABLES:
        expected_cols = _get_table_columns(conn, table)
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
            columns = _get_table_columns(conn, table)
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
