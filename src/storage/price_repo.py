import sqlite3
from datetime import datetime


def upsert_price(conn: sqlite3.Connection, asset_id: int, date: str, price: float, source: str = "manual") -> None:
    conn.execute(
        "INSERT OR REPLACE INTO market_prices (asset_id, date, price, source) "
        "VALUES (?, ?, ?, ?)",
        (asset_id, date, price, source),
    )
    conn.commit()


def upsert_ohlcv(
    conn: sqlite3.Connection,
    asset_id: int,
    symbol: str,
    asset_type: str,
    date: str,
    open_: float | None,
    high: float | None,
    low: float | None,
    close: float | None,
    adjusted_close: float | None,
    volume: float | None,
    source: str,
) -> None:
    price = adjusted_close if adjusted_close is not None else (close or 0.0)
    conn.execute(
        """INSERT INTO market_prices
           (asset_id, symbol, asset_type, date, open, high, low, close,
            adjusted_close, volume, price, source, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(asset_id, date, source) DO UPDATE SET
             open=excluded.open, high=excluded.high, low=excluded.low,
             close=excluded.close, adjusted_close=excluded.adjusted_close,
             volume=excluded.volume, price=excluded.price,
             symbol=excluded.symbol, asset_type=excluded.asset_type""",
        (asset_id, symbol, asset_type, date, open_, high, low, close,
         adjusted_close, volume, price, source, datetime.now().isoformat()),
    )
    conn.commit()


def bulk_upsert_ohlcv(conn: sqlite3.Connection, rows: list[dict]) -> int:
    # Single executemany batches every parameter set into one
    # SQLite call instead of paying Python-loop + per-row execute
    # overhead. Same SQL (ON CONFLICT clause is preserved verbatim)
    # so behaviour at the table level is unchanged.
    #
    # ``created_at`` is computed once per batch rather than per row;
    # rows in the same batch are inserted at the same time, so a
    # shared timestamp is the more accurate reading anyway.
    if not rows:
        conn.commit()
        return 0
    now = datetime.now().isoformat()
    params = [
        (
            r["asset_id"], r["symbol"], r["asset_type"], r["date"],
            r.get("open"), r.get("high"), r.get("low"), r.get("close"),
            r.get("adjusted_close"), r.get("volume"),
            r.get("adjusted_close") or r.get("close") or 0.0,
            r["source"], now,
        )
        for r in rows
    ]
    conn.executemany(
        """INSERT INTO market_prices
           (asset_id, symbol, asset_type, date, open, high, low, close,
            adjusted_close, volume, price, source, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(asset_id, date, source) DO UPDATE SET
             open=excluded.open, high=excluded.high, low=excluded.low,
             close=excluded.close, adjusted_close=excluded.adjusted_close,
             volume=excluded.volume, price=excluded.price,
             symbol=excluded.symbol, asset_type=excluded.asset_type""",
        params,
    )
    conn.commit()
    return len(params)


def get_latest_price(conn: sqlite3.Connection, asset_id: int) -> float | None:
    row = conn.execute(
        "SELECT price FROM market_prices WHERE asset_id = ? ORDER BY date DESC LIMIT 1",
        (asset_id,),
    ).fetchone()
    if row is None:
        return None
    return row["price"]


def get_latest_price_record(conn: sqlite3.Connection, asset_id: int) -> dict | None:
    row = conn.execute(
        """SELECT date, price, source, close, adjusted_close
           FROM market_prices WHERE asset_id = ? ORDER BY date DESC LIMIT 1""",
        (asset_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "date": row["date"],
        "price": row["price"],
        "source": row["source"],
        "close": row["close"],
        "adjusted_close": row["adjusted_close"],
    }


def get_price_on_date(conn: sqlite3.Connection, asset_id: int, date: str) -> float | None:
    row = conn.execute(
        "SELECT price FROM market_prices WHERE asset_id = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (asset_id, date),
    ).fetchone()
    if row is None:
        return None
    return row["price"]


def list_prices(conn: sqlite3.Connection, asset_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT date, price, source FROM market_prices WHERE asset_id = ? ORDER BY date",
        (asset_id,),
    ).fetchall()
    return [{"date": r["date"], "price": r["price"], "source": r["source"]} for r in rows]


def list_latest_prices(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """SELECT mp.asset_id, a.symbol, a.name, a.asset_type,
                  mp.date, mp.price, mp.source
           FROM market_prices mp
           JOIN assets a ON mp.asset_id = a.id
           WHERE mp.date = (
               SELECT MAX(mp2.date) FROM market_prices mp2
               WHERE mp2.asset_id = mp.asset_id
           )
           ORDER BY a.symbol""",
    ).fetchall()
    return [
        {
            "asset_id": r["asset_id"],
            "symbol": r["symbol"],
            "name": r["name"],
            "asset_type": r["asset_type"],
            "date": r["date"],
            "price": r["price"],
            "source": r["source"],
        }
        for r in rows
    ]
