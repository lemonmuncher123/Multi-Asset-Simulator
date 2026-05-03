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


# How many *months* the user is willing to take to pay off any single
# debt before the risk engine flags it. Default 60 (5 years). Editable
# from the Settings page.
DEFAULT_MAX_DEBT_PAYOFF_MONTHS = 60


def get_max_debt_payoff_months(conn: sqlite3.Connection) -> int:
    raw = get_setting(conn, "max_debt_payoff_months", None)
    if raw is None:
        return DEFAULT_MAX_DEBT_PAYOFF_MONTHS
    try:
        val = int(float(raw))
    except (ValueError, TypeError):
        return DEFAULT_MAX_DEBT_PAYOFF_MONTHS
    return val if val > 0 else DEFAULT_MAX_DEBT_PAYOFF_MONTHS


def set_max_debt_payoff_months(conn: sqlite3.Connection, months: int) -> None:
    if months <= 0:
        raise ValueError("Max debt payoff months must be positive.")
    set_setting(conn, "max_debt_payoff_months", str(int(months)))


# Default annual interest rate (in percent) pre-filled in the Add Debt
# form. Spec §3 #7 names 7% as the industry-average rate to use when
# the user leaves the field blank. Users adjust up for credit cards,
# down for promotional debt.
DEFAULT_DEBT_ANNUAL_RATE_PCT = 7.0


def get_default_debt_annual_rate_pct(conn: sqlite3.Connection) -> float:
    raw = get_setting(conn, "default_debt_annual_rate_pct", None)
    if raw is None:
        return DEFAULT_DEBT_ANNUAL_RATE_PCT
    try:
        val = float(raw)
    except (ValueError, TypeError):
        return DEFAULT_DEBT_ANNUAL_RATE_PCT
    return val if val >= 0 else DEFAULT_DEBT_ANNUAL_RATE_PCT


def set_default_debt_annual_rate_pct(
    conn: sqlite3.Connection, value: float,
) -> None:
    if value < 0:
        raise ValueError("Default debt annual rate cannot be negative.")
    set_setting(conn, "default_debt_annual_rate_pct", str(float(value)))
