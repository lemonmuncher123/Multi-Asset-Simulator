import sqlite3
from datetime import datetime


def upsert_quote(
    conn: sqlite3.Connection,
    asset_id: int,
    symbol: str,
    asset_type: str,
    bid: float | None,
    ask: float | None,
    last: float | None,
    timestamp: str,
    source: str,
) -> None:
    conn.execute(
        """INSERT INTO market_quotes
           (asset_id, symbol, asset_type, bid, ask, last, timestamp, source, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(asset_id, source) DO UPDATE SET
             bid=excluded.bid, ask=excluded.ask, last=excluded.last,
             timestamp=excluded.timestamp, symbol=excluded.symbol,
             asset_type=excluded.asset_type, created_at=excluded.created_at""",
        (asset_id, symbol, asset_type, bid, ask, last, timestamp, source,
         datetime.now().isoformat()),
    )
    conn.commit()


def get_latest_quote_record(conn: sqlite3.Connection, asset_id: int) -> dict | None:
    row = conn.execute(
        """SELECT bid, ask, last, timestamp, source
           FROM market_quotes WHERE asset_id = ?
           ORDER BY created_at DESC LIMIT 1""",
        (asset_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "bid": row["bid"],
        "ask": row["ask"],
        "last": row["last"],
        "timestamp": row["timestamp"],
        "source": row["source"],
    }


def list_latest_quotes(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """SELECT mq.asset_id, a.symbol, a.name, a.asset_type,
                  mq.bid, mq.ask, mq.last, mq.timestamp, mq.source
           FROM market_quotes mq
           JOIN assets a ON mq.asset_id = a.id
           ORDER BY a.symbol""",
    ).fetchall()
    return [
        {
            "asset_id": r["asset_id"],
            "symbol": r["symbol"],
            "name": r["name"],
            "asset_type": r["asset_type"],
            "bid": r["bid"],
            "ask": r["ask"],
            "last": r["last"],
            "timestamp": r["timestamp"],
            "source": r["source"],
        }
        for r in rows
    ]


def list_latest_market_data(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """SELECT
                a.id AS asset_id,
                a.symbol,
                a.name,
                a.asset_type,
                (SELECT mq.bid FROM market_quotes mq
                 WHERE mq.asset_id = a.id ORDER BY mq.created_at DESC LIMIT 1) AS bid,
                (SELECT mq.ask FROM market_quotes mq
                 WHERE mq.asset_id = a.id ORDER BY mq.created_at DESC LIMIT 1) AS ask,
                (SELECT mq.last FROM market_quotes mq
                 WHERE mq.asset_id = a.id ORDER BY mq.created_at DESC LIMIT 1) AS last,
                (SELECT mq.timestamp FROM market_quotes mq
                 WHERE mq.asset_id = a.id ORDER BY mq.created_at DESC LIMIT 1) AS quote_time,
                (SELECT mq.source FROM market_quotes mq
                 WHERE mq.asset_id = a.id ORDER BY mq.created_at DESC LIMIT 1) AS quote_source,
                (SELECT mp.price FROM market_prices mp
                 WHERE mp.asset_id = a.id ORDER BY mp.date DESC LIMIT 1) AS valuation_price,
                (SELECT mp.date FROM market_prices mp
                 WHERE mp.asset_id = a.id ORDER BY mp.date DESC LIMIT 1) AS valuation_date
           FROM assets a
           WHERE a.asset_type IN ('stock', 'etf', 'crypto')
           ORDER BY a.symbol""",
    ).fetchall()
    return [
        {
            "asset_id": r["asset_id"],
            "symbol": r["symbol"],
            "name": r["name"],
            "asset_type": r["asset_type"],
            "bid": r["bid"],
            "ask": r["ask"],
            "last": r["last"],
            "quote_time": r["quote_time"],
            "quote_source": r["quote_source"],
            "valuation_price": r["valuation_price"],
            "valuation_date": r["valuation_date"],
        }
        for r in rows
    ]
