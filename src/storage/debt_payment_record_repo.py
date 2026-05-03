"""Repository for the `debt_payment_records` table (schema v10).

Each `txn_type='pay_debt'` transaction has exactly one matching record
here, written atomically by `ledger._record_debt_payment`. The record
captures spec §5 fields plus a `transaction_id` link for navigation
between the two rows.

The 1:1 invariant between `transactions` and `debt_payment_records` is
enforced by:
- The UNIQUE(transaction_id) column constraint.
- The single atomic write helper in ledger (no other path produces a
  pay_debt transaction).
- An integrity invariant test in tests/test_storage.py that asserts
  the counts match.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass
class DebtPaymentRecord:
    id: int | None
    transaction_id: int
    debt_id: int
    debt_name: str
    payment_amount: float
    payment_date: str
    payment_type: str  # 'manual' | 'automatic'
    balance_before_payment: float
    balance_after_payment: float
    note: str | None


def create_payment_record(
    conn: sqlite3.Connection, *,
    transaction_id: int,
    debt_id: int,
    debt_name: str,
    payment_amount: float,
    payment_date: str,
    payment_type: str,
    balance_before_payment: float,
    balance_after_payment: float,
    note: str | None = None,
) -> DebtPaymentRecord:
    """INSERT one debt-payment record. Caller is responsible for the
    1:1 invariant against transactions (the table's UNIQUE constraint
    will reject a second insert with the same transaction_id)."""
    if payment_type not in ("manual", "automatic"):
        raise ValueError(
            f"Invalid payment_type: {payment_type!r}. Use 'manual' or 'automatic'."
        )
    cur = conn.execute(
        "INSERT INTO debt_payment_records "
        "(transaction_id, debt_id, debt_name, payment_amount, "
        "payment_date, payment_type, balance_before_payment, "
        "balance_after_payment, note) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (transaction_id, debt_id, debt_name,
         float(payment_amount), payment_date, payment_type,
         float(balance_before_payment), float(balance_after_payment), note),
    )
    return DebtPaymentRecord(
        id=cur.lastrowid,
        transaction_id=transaction_id, debt_id=debt_id, debt_name=debt_name,
        payment_amount=float(payment_amount), payment_date=payment_date,
        payment_type=payment_type,
        balance_before_payment=float(balance_before_payment),
        balance_after_payment=float(balance_after_payment),
        note=note,
    )


def list_payment_records_for_debt(
    conn: sqlite3.Connection, debt_id: int,
) -> list[DebtPaymentRecord]:
    """Chronological list of payment records for one debt."""
    rows = conn.execute(
        "SELECT id, transaction_id, debt_id, debt_name, payment_amount, "
        "payment_date, payment_type, balance_before_payment, "
        "balance_after_payment, note "
        "FROM debt_payment_records WHERE debt_id=? "
        "ORDER BY payment_date, id",
        (debt_id,),
    ).fetchall()
    return [
        DebtPaymentRecord(
            id=r["id"], transaction_id=r["transaction_id"],
            debt_id=r["debt_id"], debt_name=r["debt_name"],
            payment_amount=float(r["payment_amount"]),
            payment_date=r["payment_date"],
            payment_type=r["payment_type"],
            balance_before_payment=float(r["balance_before_payment"]),
            balance_after_payment=float(r["balance_after_payment"]),
            note=r["note"],
        )
        for r in rows
    ]
