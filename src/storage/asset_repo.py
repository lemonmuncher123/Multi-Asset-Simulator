import sqlite3
from src.models.asset import Asset


def create_asset(conn: sqlite3.Connection, asset: Asset) -> Asset:
    cursor = conn.execute(
        "INSERT INTO assets (symbol, name, asset_type, currency, region, liquidity, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (asset.symbol, asset.name, asset.asset_type, asset.currency,
         asset.region, asset.liquidity, asset.notes),
    )
    conn.commit()
    asset.id = cursor.lastrowid
    return asset


def get_asset(conn: sqlite3.Connection, asset_id: int) -> Asset | None:
    row = conn.execute("SELECT * FROM assets WHERE id = ?", (asset_id,)).fetchone()
    if row is None:
        return None
    return _row_to_asset(row)


def get_asset_by_symbol(conn: sqlite3.Connection, symbol: str) -> Asset | None:
    row = conn.execute("SELECT * FROM assets WHERE symbol = ?", (symbol,)).fetchone()
    if row is None:
        return None
    return _row_to_asset(row)


def list_assets(conn: sqlite3.Connection, asset_type: str | None = None) -> list[Asset]:
    if asset_type:
        rows = conn.execute(
            "SELECT * FROM assets WHERE asset_type = ? ORDER BY symbol", (asset_type,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM assets ORDER BY symbol").fetchall()
    return [_row_to_asset(r) for r in rows]


def update_asset(conn: sqlite3.Connection, asset: Asset) -> None:
    conn.execute(
        "UPDATE assets SET symbol=?, name=?, asset_type=?, currency=?, region=?, "
        "liquidity=?, notes=? WHERE id=?",
        (asset.symbol, asset.name, asset.asset_type, asset.currency,
         asset.region, asset.liquidity, asset.notes, asset.id),
    )
    conn.commit()


def delete_asset(conn: sqlite3.Connection, asset_id: int) -> None:
    conn.execute("DELETE FROM assets WHERE id = ?", (asset_id,))
    conn.commit()


def _row_to_asset(row: sqlite3.Row) -> Asset:
    return Asset(
        id=row["id"],
        symbol=row["symbol"],
        name=row["name"],
        asset_type=row["asset_type"],
        currency=row["currency"],
        region=row["region"],
        liquidity=row["liquidity"],
        notes=row["notes"],
        created_at=row["created_at"],
    )
