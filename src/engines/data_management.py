import sqlite3


CASH_ONLY_TXN_TYPES = {"deposit_cash", "withdraw_cash"}

# Tables with INTEGER PRIMARY KEY AUTOINCREMENT whose sequence counter must be
# reset when all data is cleared, so new inserts start from id=1.
_AUTOINCREMENT_TABLES = (
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
)


def get_asset_usage_summary(conn: sqlite3.Connection, asset_id: int) -> dict:
    txn_count = conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE asset_id = ?", (asset_id,)
    ).fetchone()[0]

    price_count = conn.execute(
        "SELECT COUNT(*) FROM market_prices WHERE asset_id = ?", (asset_id,)
    ).fetchone()[0]

    has_property = conn.execute(
        "SELECT COUNT(*) FROM properties WHERE asset_id = ?", (asset_id,)
    ).fetchone()[0] > 0

    has_debt = conn.execute(
        "SELECT COUNT(*) FROM debts WHERE asset_id = ?", (asset_id,)
    ).fetchone()[0] > 0

    journal_count = conn.execute(
        """SELECT COUNT(*) FROM decision_journal dj
           JOIN transactions t ON dj.transaction_id = t.id
           WHERE t.asset_id = ?""",
        (asset_id,),
    ).fetchone()[0]

    return {
        "transactions": txn_count,
        "prices": price_count,
        "has_property": has_property,
        "has_debt": has_debt,
        "journal_entries": journal_count,
    }


def delete_asset_with_related_data(conn: sqlite3.Connection, asset_id: int) -> dict:
    # Delete journal entries linked via transactions for this asset.
    # The legacy `transactions.journal_id` back-pointer was dropped in
    # schema v2, so no FK-cycle dance is required.
    conn.execute(
        """DELETE FROM decision_journal WHERE transaction_id IN (
               SELECT id FROM transactions WHERE asset_id = ?
           )""",
        (asset_id,),
    )

    # Delete fee breakdown rows linked to those transactions before the
    # transactions themselves go away.
    conn.execute(
        """DELETE FROM transaction_fee_breakdown WHERE transaction_id IN (
               SELECT id FROM transactions WHERE asset_id = ?
           )""",
        (asset_id,),
    )

    deleted = {}

    deleted["transactions"] = conn.execute(
        "DELETE FROM transactions WHERE asset_id = ?", (asset_id,)
    ).rowcount

    deleted["prices"] = conn.execute(
        "DELETE FROM market_prices WHERE asset_id = ?", (asset_id,)
    ).rowcount

    deleted["quotes"] = conn.execute(
        "DELETE FROM market_quotes WHERE asset_id = ?", (asset_id,)
    ).rowcount

    deleted["properties"] = conn.execute(
        "DELETE FROM properties WHERE asset_id = ?", (asset_id,)
    ).rowcount

    deleted["debts"] = conn.execute(
        "DELETE FROM debts WHERE asset_id = ?", (asset_id,)
    ).rowcount

    conn.execute("DELETE FROM assets WHERE id = ?", (asset_id,))
    conn.commit()

    return deleted


def clear_all_assets(conn: sqlite3.Connection) -> dict:
    deleted = {}

    # Delete journal entries linked to asset-related transactions.
    conn.execute(
        """DELETE FROM decision_journal WHERE transaction_id IN (
               SELECT id FROM transactions WHERE asset_id IS NOT NULL
           )"""
    )

    deleted["properties"] = conn.execute("DELETE FROM properties").rowcount
    deleted["debts"] = conn.execute("DELETE FROM debts").rowcount
    deleted["market_prices"] = conn.execute("DELETE FROM market_prices").rowcount
    deleted["market_quotes"] = conn.execute("DELETE FROM market_quotes").rowcount
    deleted["price_sync_log"] = conn.execute("DELETE FROM price_sync_log").rowcount

    # Drop fee breakdowns for asset-linked transactions before removing them.
    conn.execute(
        """DELETE FROM transaction_fee_breakdown WHERE transaction_id IN (
               SELECT id FROM transactions WHERE asset_id IS NOT NULL
           )"""
    )

    # Delete non-cash transactions (those with asset_id)
    deleted["transactions"] = conn.execute(
        "DELETE FROM transactions WHERE asset_id IS NOT NULL"
    ).rowcount

    deleted["assets"] = conn.execute("DELETE FROM assets").rowcount
    deleted["portfolio_snapshots"] = conn.execute("DELETE FROM portfolio_snapshots").rowcount

    conn.commit()
    return deleted


def delete_property_with_related_data(conn: sqlite3.Connection, asset_id: int) -> dict:
    asset = conn.execute("SELECT * FROM assets WHERE id = ?", (asset_id,)).fetchone()
    if asset is None:
        raise ValueError(f"Asset id={asset_id} not found.")
    if asset["asset_type"] != "real_estate":
        raise ValueError(f"Asset id={asset_id} is not a real_estate asset.")
    prop = conn.execute("SELECT * FROM properties WHERE asset_id = ?", (asset_id,)).fetchone()
    if prop is None:
        raise ValueError(f"No property record for asset id={asset_id}.")

    conn.execute(
        """DELETE FROM decision_journal WHERE transaction_id IN (
               SELECT id FROM transactions WHERE asset_id = ?
           )""",
        (asset_id,),
    )
    conn.execute(
        """DELETE FROM transaction_fee_breakdown WHERE transaction_id IN (
               SELECT id FROM transactions WHERE asset_id = ?
           )""",
        (asset_id,),
    )

    deleted = {}
    deleted["transactions"] = conn.execute(
        "DELETE FROM transactions WHERE asset_id = ?", (asset_id,)
    ).rowcount
    deleted["prices"] = conn.execute(
        "DELETE FROM market_prices WHERE asset_id = ?", (asset_id,)
    ).rowcount
    deleted["quotes"] = conn.execute(
        "DELETE FROM market_quotes WHERE asset_id = ?", (asset_id,)
    ).rowcount
    deleted["properties"] = conn.execute(
        "DELETE FROM properties WHERE asset_id = ?", (asset_id,)
    ).rowcount
    conn.execute("DELETE FROM assets WHERE id = ?", (asset_id,))
    conn.commit()
    return deleted


def clear_all_properties(conn: sqlite3.Connection) -> dict:
    re_asset_ids = [
        row[0] for row in conn.execute(
            "SELECT id FROM assets WHERE asset_type = 'real_estate'"
        ).fetchall()
    ]
    if not re_asset_ids:
        return {"assets": 0, "properties": 0, "transactions": 0, "prices": 0, "quotes": 0}

    placeholders = ",".join("?" * len(re_asset_ids))

    conn.execute(
        f"""DELETE FROM decision_journal WHERE transaction_id IN (
                SELECT id FROM transactions WHERE asset_id IN ({placeholders})
            )""",
        re_asset_ids,
    )
    conn.execute(
        f"""DELETE FROM transaction_fee_breakdown WHERE transaction_id IN (
                SELECT id FROM transactions WHERE asset_id IN ({placeholders})
            )""",
        re_asset_ids,
    )

    deleted = {}
    deleted["transactions"] = conn.execute(
        f"DELETE FROM transactions WHERE asset_id IN ({placeholders})",
        re_asset_ids,
    ).rowcount
    deleted["prices"] = conn.execute(
        f"DELETE FROM market_prices WHERE asset_id IN ({placeholders})",
        re_asset_ids,
    ).rowcount
    deleted["quotes"] = conn.execute(
        f"DELETE FROM market_quotes WHERE asset_id IN ({placeholders})",
        re_asset_ids,
    ).rowcount
    deleted["properties"] = conn.execute("DELETE FROM properties").rowcount
    deleted["assets"] = conn.execute(
        f"DELETE FROM assets WHERE id IN ({placeholders})",
        re_asset_ids,
    ).rowcount
    conn.commit()
    return deleted


def clear_all_user_data(conn: sqlite3.Connection) -> dict:
    deleted = {}

    deleted["decision_journal"] = conn.execute("DELETE FROM decision_journal").rowcount
    deleted["properties"] = conn.execute("DELETE FROM properties").rowcount
    deleted["debts"] = conn.execute("DELETE FROM debts").rowcount
    deleted["market_prices"] = conn.execute("DELETE FROM market_prices").rowcount
    deleted["market_quotes"] = conn.execute("DELETE FROM market_quotes").rowcount
    deleted["price_sync_log"] = conn.execute("DELETE FROM price_sync_log").rowcount
    deleted["transaction_fee_breakdown"] = conn.execute("DELETE FROM transaction_fee_breakdown").rowcount
    deleted["transactions"] = conn.execute("DELETE FROM transactions").rowcount
    deleted["assets"] = conn.execute("DELETE FROM assets").rowcount
    deleted["portfolio_snapshots"] = conn.execute("DELETE FROM portfolio_snapshots").rowcount
    deleted["reports"] = conn.execute("DELETE FROM reports").rowcount
    deleted["settings"] = conn.execute("DELETE FROM settings").rowcount
    deleted["securities_master"] = conn.execute("DELETE FROM securities_master").rowcount

    # Drop the legacy option_contracts table if a previous build left it
    # behind. Options are not part of the active schema, so we don't preserve
    # rows or recreate the table.
    has_legacy_options = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='option_contracts'"
    ).fetchone()
    if has_legacy_options:
        conn.execute("DROP TABLE IF EXISTS option_contracts")

    # Reset AUTOINCREMENT counters so new data starts from id=1 after a full
    # clear. sqlite_sequence is created lazily by SQLite, so it may not exist
    # yet on a freshly initialized database.
    has_seq = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'"
    ).fetchone()
    if has_seq:
        placeholders = ",".join("?" * len(_AUTOINCREMENT_TABLES))
        conn.execute(
            f"DELETE FROM sqlite_sequence WHERE name IN ({placeholders})",
            _AUTOINCREMENT_TABLES,
        )

    conn.commit()
    return deleted
