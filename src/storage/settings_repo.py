import sqlite3


def get_setting(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    if row is None:
        return default
    return row["value"]


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()


def list_settings(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute("SELECT key, value FROM settings ORDER BY key").fetchall()
    return {r["key"]: r["value"] for r in rows}


THRESHOLD_DEFAULTS: dict[str, float] = {
    "low_cash_threshold": 0.05,
    "concentration_threshold": 0.25,
    "crypto_threshold": 0.20,
    "debt_threshold": 0.50,
}


def parse_threshold(raw: str, default: float) -> float:
    raw = raw.strip().rstrip("%").strip()
    try:
        val = float(raw)
    except (ValueError, TypeError):
        return default
    if val < 0:
        return default
    if val >= 1:
        return val / 100.0
    return val


def get_threshold(conn: sqlite3.Connection, key: str) -> float:
    default = THRESHOLD_DEFAULTS.get(key)
    if default is None:
        raise ValueError(f"Unknown threshold key: {key}")
    raw = get_setting(conn, key)
    if raw is None:
        return default
    return parse_threshold(raw, default)
