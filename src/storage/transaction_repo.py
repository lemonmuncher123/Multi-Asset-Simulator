import sqlite3
from src.models.transaction import Transaction


def create_transaction(conn: sqlite3.Connection, txn: Transaction) -> Transaction:
    cursor = conn.execute(
        "INSERT INTO transactions (date, txn_type, asset_id, quantity, price, "
        "total_amount, currency, fees, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (txn.date, txn.txn_type, txn.asset_id, txn.quantity, txn.price,
         txn.total_amount, txn.currency, txn.fees, txn.notes),
    )
    conn.commit()
    txn.id = cursor.lastrowid
    return txn


def get_transaction(conn: sqlite3.Connection, txn_id: int) -> Transaction | None:
    row = conn.execute("SELECT * FROM transactions WHERE id = ?", (txn_id,)).fetchone()
    if row is None:
        return None
    return _row_to_txn(row)


def list_transactions(
    conn: sqlite3.Connection,
    asset_id: int | None = None,
    txn_type: str | None = None,
) -> list[Transaction]:
    query = "SELECT * FROM transactions WHERE 1=1"
    params: list = []
    if asset_id is not None:
        query += " AND asset_id = ?"
        params.append(asset_id)
    if txn_type is not None:
        query += " AND txn_type = ?"
        params.append(txn_type)
    query += " ORDER BY date, id"
    rows = conn.execute(query, params).fetchall()
    return [_row_to_txn(r) for r in rows]


def delete_transaction(conn: sqlite3.Connection, txn_id: int) -> None:
    conn.execute("DELETE FROM transactions WHERE id = ?", (txn_id,))
    conn.commit()


def _row_to_txn(row: sqlite3.Row) -> Transaction:
    return Transaction(
        id=row["id"],
        date=row["date"],
        txn_type=row["txn_type"],
        asset_id=row["asset_id"],
        quantity=row["quantity"],
        price=row["price"],
        total_amount=row["total_amount"],
        currency=row["currency"],
        fees=row["fees"],
        notes=row["notes"],
        created_at=row["created_at"],
    )
