import sqlite3
from datetime import datetime


def create_sync_log(
    conn: sqlite3.Connection,
    source: str | None = None,
) -> int:
    cursor = conn.execute(
        """INSERT INTO price_sync_log (started_at, status, source)
           VALUES (?, 'running', ?)""",
        (datetime.now().isoformat(), source),
    )
    conn.commit()
    return cursor.lastrowid


def finish_sync_log(
    conn: sqlite3.Connection,
    log_id: int,
    status: str,
    assets_attempted: int = 0,
    assets_succeeded: int = 0,
    assets_failed: int = 0,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """UPDATE price_sync_log SET
             finished_at = ?, status = ?,
             assets_attempted = ?, assets_succeeded = ?,
             assets_failed = ?, error_message = ?
           WHERE id = ?""",
        (datetime.now().isoformat(), status, assets_attempted,
         assets_succeeded, assets_failed, error_message, log_id),
    )
    conn.commit()


def list_sync_logs(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        """SELECT id, started_at, finished_at, status, source,
                  assets_attempted, assets_succeeded, assets_failed, error_message
           FROM price_sync_log ORDER BY id DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "started_at": r["started_at"],
            "finished_at": r["finished_at"],
            "status": r["status"],
            "source": r["source"],
            "assets_attempted": r["assets_attempted"],
            "assets_succeeded": r["assets_succeeded"],
            "assets_failed": r["assets_failed"],
            "error_message": r["error_message"],
        }
        for r in rows
    ]


def _row_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "status": row["status"],
        "source": row["source"],
        "assets_attempted": row["assets_attempted"],
        "assets_succeeded": row["assets_succeeded"],
        "assets_failed": row["assets_failed"],
        "error_message": row["error_message"],
    }


def get_latest_sync_log(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        "SELECT * FROM price_sync_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def get_last_successful_sync(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        """SELECT * FROM price_sync_log
           WHERE status IN ('success', 'partial')
           ORDER BY id DESC LIMIT 1"""
    ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)
