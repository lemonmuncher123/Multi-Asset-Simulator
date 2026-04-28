import json
import sqlite3
from dataclasses import dataclass
from src.models.report import Report


def create_or_replace_report(conn: sqlite3.Connection, report: Report) -> Report:
    # Extract list-friendly summary metrics from report_json once at save
    # time so the Reports list can render quickly without re-parsing JSON.
    ncf = 0.0
    oni = 0.0
    txn_count = 0
    nwc = None
    ff = 0.0
    arp = None
    try:
        data = json.loads(report.report_json)
        s = data.get("summary", {})
        ncf = s.get("net_cash_flow", 0.0)
        oni = s.get("operating_net_income", 0.0)
        txn_count = s.get("transaction_count", 0)
        # Phase 1+ performance fields. Pre-Phase-1 reports omit these,
        # in which case the columns keep their nullable / zero defaults.
        perf = data.get("performance") or {}
        nwc = perf.get("net_worth_change")
        ff = perf.get("funding_flow") or 0.0
        arp = perf.get("approximate_return_pct")
    except (json.JSONDecodeError, KeyError):
        pass

    report.net_cash_flow = ncf
    report.operating_net_income = oni
    report.transaction_count = txn_count
    report.net_worth_change = nwc
    report.funding_flow = ff
    report.approximate_return_pct = arp

    cursor = conn.execute(
        "INSERT OR REPLACE INTO reports "
        "(report_type, period_start, period_end, period_label, "
        "generated_at, title, report_json, notes, "
        "net_cash_flow, operating_net_income, transaction_count, "
        "net_worth_change, funding_flow, approximate_return_pct) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (report.report_type, report.period_start, report.period_end,
         report.period_label, report.generated_at, report.title,
         report.report_json, report.notes,
         ncf, oni, txn_count,
         nwc, ff, arp),
    )
    conn.commit()
    report.id = cursor.lastrowid
    return report


def get_report(conn: sqlite3.Connection, report_type: str, period_label: str) -> Report | None:
    row = conn.execute(
        "SELECT * FROM reports WHERE report_type = ? AND period_label = ?",
        (report_type, period_label),
    ).fetchone()
    if row is None:
        return None
    return _row_to_report(row)


def list_reports(conn: sqlite3.Connection, report_type: str | None = None) -> list[Report]:
    if report_type:
        rows = conn.execute(
            "SELECT * FROM reports WHERE report_type = ? ORDER BY period_label DESC",
            (report_type,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM reports ORDER BY report_type, period_label DESC"
        ).fetchall()
    return [_row_to_report(r) for r in rows]


@dataclass
class ReportSummaryRow:
    id: int
    report_type: str
    period_label: str
    generated_at: str
    title: str
    net_cash_flow: float
    operating_net_income: float
    transaction_count: int
    net_worth_change: float | None = None
    funding_flow: float = 0.0
    approximate_return_pct: float | None = None


def list_report_summaries(
    conn: sqlite3.Connection,
    report_type: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[ReportSummaryRow]:
    # Lightweight: select only the precomputed summary columns; never
    # SELECT report_json here, so the Reports list stays fast even with
    # many large reports.
    query = (
        "SELECT id, report_type, period_label, generated_at, title, "
        "net_cash_flow, operating_net_income, transaction_count, "
        "net_worth_change, funding_flow, approximate_return_pct "
        "FROM reports"
    )
    params: list = []
    if report_type:
        query += " WHERE report_type = ?"
        params.append(report_type)
    query += " ORDER BY period_label DESC"
    if limit is not None:
        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    rows = conn.execute(query, params).fetchall()
    return [
        ReportSummaryRow(
            id=r["id"],
            report_type=r["report_type"],
            period_label=r["period_label"],
            generated_at=r["generated_at"],
            title=r["title"],
            net_cash_flow=r["net_cash_flow"],
            operating_net_income=r["operating_net_income"],
            transaction_count=r["transaction_count"],
            net_worth_change=r["net_worth_change"],
            funding_flow=r["funding_flow"] if r["funding_flow"] is not None else 0.0,
            approximate_return_pct=r["approximate_return_pct"],
        )
        for r in rows
    ]


def report_count(conn: sqlite3.Connection, report_type: str | None = None) -> int:
    if report_type:
        row = conn.execute(
            "SELECT COUNT(*) FROM reports WHERE report_type = ?", (report_type,)
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) FROM reports").fetchone()
    return row[0]


def report_exists(conn: sqlite3.Connection, report_type: str, period_label: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM reports WHERE report_type = ? AND period_label = ?",
        (report_type, period_label),
    ).fetchone()
    return row is not None


def delete_report(conn: sqlite3.Connection, report_id: int) -> None:
    conn.execute("DELETE FROM reports WHERE id = ?", (report_id,))
    conn.commit()


def delete_reports_before_date(conn: sqlite3.Connection, cutoff_date: str) -> int:
    cursor = conn.execute(
        "DELETE FROM reports WHERE period_start < ?", (cutoff_date,)
    )
    conn.commit()
    return cursor.rowcount


def delete_reports_by_ids(conn: sqlite3.Connection, ids: list[int]) -> int:
    if not ids:
        return 0
    placeholders = ",".join("?" * len(ids))
    cursor = conn.execute(
        f"DELETE FROM reports WHERE id IN ({placeholders})", ids
    )
    conn.commit()
    return cursor.rowcount


def delete_reports_by_type(conn: sqlite3.Connection, report_type: str) -> int:
    cursor = conn.execute(
        "DELETE FROM reports WHERE report_type = ?", (report_type,)
    )
    conn.commit()
    return cursor.rowcount


def delete_reports_in_period_range(
    conn: sqlite3.Connection, start: str, end: str
) -> int:
    cursor = conn.execute(
        "DELETE FROM reports WHERE period_start >= ? AND period_start < ?",
        (start, end),
    )
    conn.commit()
    return cursor.rowcount


def delete_all_reports(conn: sqlite3.Connection) -> int:
    cursor = conn.execute("DELETE FROM reports")
    conn.commit()
    return cursor.rowcount


def get_report_stats(conn: sqlite3.Connection) -> dict:
    total = conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
    monthly = conn.execute(
        "SELECT COUNT(*) FROM reports WHERE report_type = 'monthly'"
    ).fetchone()[0]
    annual = conn.execute(
        "SELECT COUNT(*) FROM reports WHERE report_type = 'annual'"
    ).fetchone()[0]
    return {"total": total, "monthly": monthly, "annual": annual}


def _row_to_report(row: sqlite3.Row) -> Report:
    keys = row.keys()
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
        net_cash_flow=row["net_cash_flow"] if "net_cash_flow" in keys else 0.0,
        operating_net_income=row["operating_net_income"] if "operating_net_income" in keys else 0.0,
        transaction_count=row["transaction_count"] if "transaction_count" in keys else 0,
        net_worth_change=(
            row["net_worth_change"] if "net_worth_change" in keys else None
        ),
        funding_flow=(
            (row["funding_flow"] or 0.0) if "funding_flow" in keys else 0.0
        ),
        approximate_return_pct=(
            row["approximate_return_pct"] if "approximate_return_pct" in keys else None
        ),
    )
