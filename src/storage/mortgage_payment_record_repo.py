"""Repository for the `mortgage_payment_records` table (schema v11).

Mirror of `debt_payment_records`. Every `txn_type='pay_mortgage'`
transaction has exactly one matching record here, written atomically
by `ledger._record_mortgage_payment`. The 1:1 invariant is enforced
by the UNIQUE(transaction_id) column constraint plus the single
atomic write helper in ledger.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass
class MortgagePaymentRecord:
    id: int | None
    transaction_id: int
    mortgage_id: int
    mortgage_name: str
    payment_amount: float
    payment_date: str
    payment_type: str  # 'manual' | 'automatic'
    balance_before_payment: float
    balance_after_payment: float
    note: str | None


def create_payment_record(
    conn: sqlite3.Connection, *,
    transaction_id: int,
    mortgage_id: int,
    mortgage_name: str,
    payment_amount: float,
    payment_date: str,
    payment_type: str,
    balance_before_payment: float,
    balance_after_payment: float,
    note: str | None = None,
) -> MortgagePaymentRecord:
    """INSERT one mortgage-payment record. Caller is responsible for the
    1:1 invariant against transactions (the table's UNIQUE constraint
    will reject a second insert with the same transaction_id)."""
    if payment_type not in ("manual", "automatic"):
        raise ValueError(
            f"Invalid payment_type: {payment_type!r}. Use 'manual' or 'automatic'."
        )
    cur = conn.execute(
        "INSERT INTO mortgage_payment_records "
        "(transaction_id, mortgage_id, mortgage_name, payment_amount, "
        "payment_date, payment_type, balance_before_payment, "
        "balance_after_payment, note) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (transaction_id, mortgage_id, mortgage_name,
         float(payment_amount), payment_date, payment_type,
         float(balance_before_payment), float(balance_after_payment), note),
    )
    return MortgagePaymentRecord(
        id=cur.lastrowid,
        transaction_id=transaction_id, mortgage_id=mortgage_id,
        mortgage_name=mortgage_name,
        payment_amount=float(payment_amount), payment_date=payment_date,
        payment_type=payment_type,
        balance_before_payment=float(balance_before_payment),
        balance_after_payment=float(balance_after_payment),
        note=note,
    )


def list_payment_records_for_mortgage(
    conn: sqlite3.Connection, mortgage_id: int,
) -> list[MortgagePaymentRecord]:
    """Chronological list of payment records for one mortgage."""
    rows = conn.execute(
        "SELECT id, transaction_id, mortgage_id, mortgage_name, payment_amount, "
        "payment_date, payment_type, balance_before_payment, "
        "balance_after_payment, note "
        "FROM mortgage_payment_records WHERE mortgage_id=? "
        "ORDER BY payment_date, id",
        (mortgage_id,),
    ).fetchall()
    return [
        MortgagePaymentRecord(
            id=r["id"], transaction_id=r["transaction_id"],
            mortgage_id=r["mortgage_id"], mortgage_name=r["mortgage_name"],
            payment_amount=float(r["payment_amount"]),
            payment_date=r["payment_date"],
            payment_type=r["payment_type"],
            balance_before_payment=float(r["balance_before_payment"]),
            balance_after_payment=float(r["balance_after_payment"]),
            note=r["note"],
        )
        for r in rows
    ]
