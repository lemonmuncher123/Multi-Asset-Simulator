import contextlib
import contextvars
import functools
import logging
import math
import sqlite3
from datetime import date as date_type
from dateutil.relativedelta import relativedelta

_log = logging.getLogger(__name__)


# Phase 6.4: engine-level bankruptcy lock with an auto-settle bypass.
# `_BANKRUPTCY_BYPASS` is a ContextVar so the flag is naturally
# scoped to the call (no cleanup leaks if an exception propagates).
# `_auto_settle_bypass()` is the only intended way to set it.
_BANKRUPTCY_BYPASS: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "_BANKRUPTCY_BYPASS", default=False,
)


class BankruptcyLockedError(Exception):
    """Raised when a public ledger write is attempted while the
    simulator is in bankruptcy and the auto-settle bypass is not
    active. Spec §6 #25 — every user-initiated transaction is banned
    when bankrupt; auto-settle internals (settle_due_*, force_sell_*,
    retry_deferred) enter `_auto_settle_bypass()` so they keep
    running.
    """


@contextlib.contextmanager
def _auto_settle_bypass():
    """Context manager that lets ledger writes proceed during
    bankruptcy. Used by every auto-settle helper that needs to
    process scheduled obligations or recover cash."""
    token = _BANKRUPTCY_BYPASS.set(True)
    try:
        yield
    finally:
        _BANKRUPTCY_BYPASS.reset(token)


def _autosettle_bypass(fn):
    """Decorator wrapping `_auto_settle_bypass` for every public
    auto-settle helper (settle_due_*, force_sell_*, retry_deferred).
    Eliminates the per-call try/finally boilerplate without re-
    indenting each function's body.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        token = _BANKRUPTCY_BYPASS.set(True)
        try:
            return fn(*args, **kwargs)
        finally:
            _BANKRUPTCY_BYPASS.reset(token)
    return wrapper


def _assert_not_bankrupt(conn: sqlite3.Connection) -> None:
    """Raise `BankruptcyLockedError` when the portfolio is bankrupt
    and the auto-settle bypass is not active. No-op otherwise."""
    if _BANKRUPTCY_BYPASS.get():
        return
    # Late import to avoid a circular dependency at module load time
    # (risk imports from the storage layer; we don't want to drag in
    # the risk module as part of `import ledger`).
    from src.engines.risk import is_bankrupt
    if is_bankrupt(conn):
        raise BankruptcyLockedError(
            "Account is bankrupt — transactions are disabled. "
            "See Dashboard for details."
        )


from src.models.asset import Asset
from src.models.transaction import Transaction
from src.models.property_asset import PropertyAsset
from src.models.debt import Debt
from src.models.mortgage import Mortgage
from src.storage.asset_repo import create_asset, get_asset, list_assets
from src.storage.transaction_repo import create_transaction, list_transactions
from src.storage.property_repo import (
    create_property, get_property_by_asset, get_property,
    update_property, list_active_properties,
)
from src.storage.debt_repo import create_debt, get_debt_by_asset, update_debt, list_debts
from src.storage.debt_payment_record_repo import create_payment_record
from src.storage.mortgage_repo import (
    create_mortgage, get_mortgage_by_property, update_mortgage, list_mortgages,
    list_active_mortgages,
)
from src.storage.mortgage_payment_record_repo import (
    create_payment_record as create_mortgage_payment_record,
)
from src.storage.price_repo import get_latest_price
from src.engines.debt_math import (
    compute_debt_schedule, DebtSchedule, period_interest,
    recompute_after_payment, PAID_OFF_REASON,
    compute_preview_values,
    simulate_amortization_balance, compute_periods_elapsed,
)
from src.engines.holdings import SELLABLE_ASSET_TYPES, get_asset_quantity, _EPSILON
from src.engines.portfolio import calc_cash_balance
from src.utils.dates import next_month_start


def first_day_next_month(today: date_type | None = None) -> str:
    return next_month_start(today or date_type.today()).isoformat()


def _assert_sufficient_cash(
    conn: sqlite3.Connection, cost: float, action: str,
) -> None:
    """Reject the operation if the user lacks cash to cover `cost`.

    `cost` is the magnitude of cash leaving the account (always positive
    or zero). The simulator's source-of-truth contract treats negative
    cash as a bug — the user can't spend money they don't have. The one
    documented escape hatch is `manual_adjustment`, which deliberately
    skips this check.
    """
    if cost <= 0:
        return
    current = calc_cash_balance(conn)
    if current + _EPSILON < cost:
        raise ValueError(
            f"Insufficient cash for {action}: need {cost:,.2f}, have {current:,.2f}."
        )


def _assert_positive(value: float | None, label: str) -> None:
    """Reject non-positive user-supplied numerics (`<= 0` or `None`).

    The simulator's domain has no legitimate negative or zero amount,
    quantity, or price input. Signs are an internal engine concern (e.g.
    `buy.total_amount = -(qty*price + fees)`); the user always types a
    positive magnitude. The single documented exception is
    `manual_adjustment.amount`, which deliberately accepts both
    directions and therefore does NOT call this helper.
    """
    if value is None or value <= 0:
        raise ValueError(f"{label} must be positive (got {value!r}).")


def _assert_non_negative(value: float | None, label: str) -> None:
    """Reject negative user-supplied numerics (`< 0`). `None` and `0`
    are permitted — used for fields like fees, expenses, and rates where
    zero is a meaningful "no charge" / "no rate" value.
    """
    if value is None:
        return
    if value < 0:
        raise ValueError(f"{label} cannot be negative (got {value!r}).")


# Half-cent tolerance for "is this payment within the payoff amount"
# checks. Money is displayed to 2 decimals via fmt_money, so a user
# typing the displayed payoff (e.g. "1006.67") may exceed the actual
# stored payoff (e.g. 1006.6666...) by up to half a cent. _EPSILON
# (1e-9) is too tight to absorb that rounding gap and would reject the
# user's exact-payoff input. Matches the same constant used by
# recompute_after_payment for "essentially zero balance" detection.
# Hoisted near the top of the file so both pay_debt and pay_mortgage
# can reference it.
_PAYMENT_DISPLAY_TOLERANCE = 0.005


def deposit_cash(conn: sqlite3.Connection, date: str, amount: float, notes: str | None = None) -> Transaction:
    _assert_not_bankrupt(conn)
    # A "deposit" of zero is meaningless; a *negative* deposit was a
    # back-door withdrawal — it bypassed `_assert_sufficient_cash` and
    # could drive the cash balance arbitrarily negative. Both rejected.
    if amount is None or amount <= 0:
        raise ValueError(
            f"Deposit amount must be positive (got {amount!r}). "
            f"Use 'Withdraw Cash' to take money out."
        )
    return create_transaction(conn, Transaction(
        date=date, txn_type="deposit_cash", total_amount=amount, notes=notes,
    ))


def withdraw_cash(conn: sqlite3.Connection, date: str, amount: float, notes: str | None = None) -> Transaction:
    _assert_not_bankrupt(conn)
    # Symmetrically reject non-positive withdrawals: zero is a no-op and
    # negative is a sneaky deposit. Forces the user to be explicit.
    if amount is None or amount <= 0:
        raise ValueError(
            f"Withdraw amount must be positive (got {amount!r}). "
            f"Use 'Deposit Cash' to add money."
        )
    _assert_sufficient_cash(conn, amount, "withdraw")
    return create_transaction(conn, Transaction(
        date=date, txn_type="withdraw_cash", total_amount=-amount, notes=notes,
    ))


def buy(
    conn: sqlite3.Connection, date: str, asset_id: int,
    quantity: float, price: float, fees: float = 0.0, notes: str | None = None,
) -> Transaction:
    _assert_not_bankrupt(conn)
    if quantity <= 0:
        raise ValueError("Buy quantity must be positive.")
    if price <= 0:
        raise ValueError("Buy price must be positive.")
    if fees < 0:
        raise ValueError("Fees cannot be negative.")

    asset = get_asset(conn, asset_id)
    if asset is None:
        raise ValueError(f"Asset id={asset_id} not found.")
    if asset.asset_type not in SELLABLE_ASSET_TYPES:
        raise ValueError(
            f"Cannot buy asset type '{asset.asset_type}' via buy(). "
            f"Use the appropriate function for {asset.asset_type} assets."
        )

    cost = quantity * price + fees
    _assert_sufficient_cash(conn, cost, f"buy {quantity} {asset.symbol}")
    return create_transaction(conn, Transaction(
        date=date, txn_type="buy", asset_id=asset_id,
        quantity=quantity, price=price, total_amount=-cost, fees=fees, notes=notes,
    ))


def sell(
    conn: sqlite3.Connection, date: str, asset_id: int,
    quantity: float, price: float, fees: float = 0.0, notes: str | None = None,
) -> Transaction:
    _assert_not_bankrupt(conn)
    if quantity <= 0:
        raise ValueError("Sell quantity must be positive.")
    if price <= 0:
        raise ValueError("Sell price must be positive.")
    if fees < 0:
        raise ValueError("Fees cannot be negative.")

    asset = get_asset(conn, asset_id)
    if asset is None:
        raise ValueError(f"Asset id={asset_id} not found.")
    if asset.asset_type not in SELLABLE_ASSET_TYPES:
        raise ValueError(
            f"Cannot sell asset type '{asset.asset_type}' via sell(). "
            f"Use the appropriate function for {asset.asset_type} assets."
        )

    available = get_asset_quantity(conn, asset_id, as_of_date=date)
    if available - quantity < -_EPSILON:
        if available <= _EPSILON:
            raise ValueError(f"Cannot sell {asset.symbol}: no position is currently held.")
        raise ValueError(
            f"Insufficient quantity for {asset.symbol}. "
            f"Requested {quantity}, available {available}."
        )

    total = quantity * price - fees
    return create_transaction(conn, Transaction(
        date=date, txn_type="sell", asset_id=asset_id,
        quantity=quantity, price=price, total_amount=total, fees=fees, notes=notes,
    ))


def add_property(
    conn: sqlite3.Connection, date: str, symbol: str, name: str,
    purchase_price: float, current_value: float | None = None,
    monthly_rent: float = 0.0,
    monthly_expense: float = 0.0, address: str | None = None,
    down_payment: float | None = None,
    monthly_property_tax: float = 0.0,
    monthly_insurance: float = 0.0,
    monthly_hoa: float = 0.0,
    monthly_maintenance_reserve: float = 0.0,
    monthly_property_management: float = 0.0,
    vacancy_rate: float = 0.0,
    rent_collection_frequency: str = "monthly",
    acquisition_mode: str = "new_purchase",
    cashflow_start_date: str | None = None,
    transaction_date: str | None = None,
    notes: str | None = None,
) -> tuple[Asset, PropertyAsset, Transaction]:
    """Create a real estate property. As of schema v11, mortgage info
    no longer lives on this row — call `add_mortgage(property_id=prop.id, ...)`
    as a follow-up to attach a mortgage. Pure cash purchases (no
    mortgage) skip the follow-up call.
    """
    _assert_not_bankrupt(conn)
    if acquisition_mode not in ("new_purchase", "existing_property", "planned_purchase"):
        raise ValueError(f"Unknown acquisition_mode: {acquisition_mode!r}")
    if not name or not name.strip():
        raise ValueError("Property name is required.")
    # All numeric property fields must be non-negative. Note: for
    # `existing_property` mode the user may legitimately leave
    # purchase_price blank (the GUI submits 0); current_value carries the
    # property's worth in that case. For `new_purchase` and
    # `planned_purchase`, the GUI form enforces purchase_price > 0
    # before this engine call.
    _assert_non_negative(purchase_price, "Purchase price")
    _assert_non_negative(current_value, "Current value")
    _assert_non_negative(down_payment, "Down payment")
    _assert_non_negative(monthly_rent, "Monthly rent")
    _assert_non_negative(monthly_expense, "Monthly expense")
    _assert_non_negative(monthly_property_tax, "Monthly property tax")
    _assert_non_negative(monthly_insurance, "Monthly insurance")
    _assert_non_negative(monthly_hoa, "Monthly HOA")
    _assert_non_negative(monthly_maintenance_reserve, "Monthly maintenance reserve")
    _assert_non_negative(monthly_property_management, "Monthly property management")
    if vacancy_rate is None or vacancy_rate < 0 or vacancy_rate > 1:
        raise ValueError(
            f"Vacancy rate must be between 0 and 1 (got {vacancy_rate!r})."
        )
    # Hard-validate down payment (spec: "down payment cannot exceed the
    # value of the property when it was bought"). Only meaningful for
    # purchases — existing-property entries have no cash flow.
    if (
        down_payment is not None
        and purchase_price is not None
        and purchase_price > 0
        and down_payment > purchase_price
    ):
        raise ValueError(
            f"Down payment ({down_payment:,.2f}) cannot exceed "
            f"purchase price ({purchase_price:,.2f})."
        )
    # Reject collisions with any *active* property. Sold properties keep
    # their row (status='sold') but their name is free to reuse.
    normalized = name.strip().casefold()
    existing_assets = {a.id: a for a in list_assets(conn)}
    for prop in list_active_properties(conn):
        a = existing_assets.get(prop.asset_id)
        if a is None:
            continue
        if (a.name or "").strip().casefold() == normalized:
            raise ValueError(
                f"A property named '{a.name}' already exists. "
                f"Pick a different name."
            )

    resolved_cashflow_start = cashflow_start_date or first_day_next_month()

    asset = create_asset(conn, Asset(
        symbol=symbol, name=name, asset_type="real_estate",
        liquidity="illiquid",
    ))
    prop_status = "planned" if acquisition_mode == "planned_purchase" else "active"

    prop = create_property(conn, PropertyAsset(
        asset_id=asset.id, address=address,
        purchase_date=date,
        purchase_price=purchase_price,
        current_value=current_value or purchase_price,
        down_payment=down_payment,
        monthly_rent=monthly_rent,
        monthly_property_tax=monthly_property_tax,
        monthly_insurance=monthly_insurance,
        monthly_hoa=monthly_hoa,
        monthly_maintenance_reserve=monthly_maintenance_reserve,
        monthly_property_management=monthly_property_management,
        monthly_expense=monthly_expense,
        vacancy_rate=vacancy_rate,
        rent_collection_frequency=rent_collection_frequency,
        cashflow_start_date=resolved_cashflow_start,
        status=prop_status,
        entry_type=acquisition_mode,
    ))

    # Schema v12 rejects `transactions.price = 0` (CHECK price IS NULL OR
    # price > 0), so when the user leaves purchase_price blank for an
    # existing-property entry we record the marker transaction with NULL
    # price. The asset's price-history is not derived from add_property
    # rows, so this only affects the historical record.
    txn_price = purchase_price if purchase_price and purchase_price > 0 else None
    if acquisition_mode == "existing_property":
        record_date = transaction_date or date_type.today().isoformat()
        txn_notes = notes or ""
        if txn_notes:
            txn_notes += " | "
        txn_notes += "Existing property entry - no purchase cash impact."
        txn = create_transaction(conn, Transaction(
            date=record_date, txn_type="add_property", asset_id=asset.id,
            quantity=1, price=txn_price, total_amount=0.0,
            notes=txn_notes,
        ))
    elif acquisition_mode == "planned_purchase":
        txn_notes = notes or ""
        if txn_notes:
            txn_notes += " | "
        txn_notes += "Planned purchase scenario - no cash impact."
        txn = create_transaction(conn, Transaction(
            date=date, txn_type="add_property", asset_id=asset.id,
            quantity=1, price=txn_price, total_amount=0.0,
            notes=txn_notes,
        ))
    else:
        # New-purchase: cash leaving the account = down_payment. The
        # mortgage's principal is the gap between purchase price and
        # down payment; that liability is recorded by the follow-up
        # add_mortgage call (no cash impact for the borrowed portion).
        cash_out = down_payment if down_payment is not None else purchase_price
        _assert_sufficient_cash(conn, abs(cash_out), f"add_property {symbol}")
        txn = create_transaction(conn, Transaction(
            date=transaction_date or date, txn_type="add_property", asset_id=asset.id,
            quantity=1, price=txn_price, total_amount=-abs(cash_out),
            notes=notes,
        ))
    return asset, prop, txn


def update_property_value(
    conn: sqlite3.Connection, date: str, asset_id: int,
    new_value: float, notes: str | None = None,
) -> Transaction:
    _assert_not_bankrupt(conn)
    _assert_non_negative(new_value, "Property value")
    prop = get_property_by_asset(conn, asset_id)
    prop.current_value = new_value
    update_property(conn, prop)
    return create_transaction(conn, Transaction(
        date=date, txn_type="update_property_value", asset_id=asset_id,
        total_amount=0.0, notes=notes,
    ))


def receive_rent(
    conn: sqlite3.Connection, date: str, asset_id: int,
    amount: float, notes: str | None = None,
) -> Transaction:
    _assert_not_bankrupt(conn)
    _assert_positive(amount, "Rent amount")
    return create_transaction(conn, Transaction(
        date=date, txn_type="receive_rent", asset_id=asset_id,
        total_amount=amount, notes=notes,
    ))


def pay_property_expense(
    conn: sqlite3.Connection, date: str, asset_id: int,
    amount: float, notes: str | None = None,
) -> Transaction:
    _assert_not_bankrupt(conn)
    _assert_positive(amount, "Property expense amount")
    _assert_sufficient_cash(conn, amount, f"pay property expense (asset {asset_id})")
    return create_transaction(conn, Transaction(
        date=date, txn_type="pay_property_expense", asset_id=asset_id,
        total_amount=-amount, notes=notes,
    ))


# Note prefix written by `settle_due_mortgage_payments` for every
# scheduled auto-deduction. Counted by `count_scheduled_mortgage_payments`
# to determine how many term periods a fixed-term mortgage has consumed.
SCHEDULED_MORTGAGE_PAYMENT_NOTE_PREFIX = "Scheduled mortgage payment"


def _payoff_amount_for_mortgage(mortgage: Mortgage | None) -> float:
    """Pure form of ``compute_mortgage_payoff_amount`` — takes a
    Mortgage object instead of (conn, asset_id) so ``pay_mortgage``
    can reuse the math without a redundant DB lookup. Mirrors
    ``_payoff_amount_for_debt``.
    """
    if mortgage is None or mortgage.current_balance <= 0:
        return 0.0
    rate = mortgage.interest_rate or 0.0
    if rate <= 0:
        return mortgage.current_balance
    return mortgage.current_balance + period_interest(
        mortgage.current_balance, rate, mortgage.schedule_frequency,
    )


def compute_mortgage_payoff_amount(
    conn: sqlite3.Connection, asset_id: int,
) -> float:
    """Cash needed to extinguish a mortgage today — principal plus this
    period's interest. Mirror of ``compute_payoff_amount`` for debts.
    Looks up the mortgage by the property's asset_id.
    """
    prop = get_property_by_asset(conn, asset_id)
    if prop is None:
        return 0.0
    return _payoff_amount_for_mortgage(get_mortgage_by_property(conn, prop.id))


def count_scheduled_mortgage_payments(
    conn: sqlite3.Connection, asset_id: int,
) -> int:
    """How many scheduled auto-payments have already fired for the
    mortgage on the property identified by `asset_id`. Mirror of
    ``count_scheduled_debt_payments``."""
    row = conn.execute(
        "SELECT COUNT(*) FROM transactions "
        "WHERE asset_id = ? AND txn_type = 'pay_mortgage' AND notes LIKE ?",
        (asset_id, f"{SCHEDULED_MORTGAGE_PAYMENT_NOTE_PREFIX}%"),
    ).fetchone()
    return int(row[0]) if row else 0


def total_paid_for_mortgage(
    conn: sqlite3.Connection, asset_id: int,
) -> float:
    """Sum of every `pay_mortgage` transaction's cash outflow for the
    mortgage on the property identified by `asset_id`. Mirror of
    ``total_paid_for_debt``."""
    row = conn.execute(
        "SELECT COALESCE(SUM(ABS(total_amount)), 0) FROM transactions "
        "WHERE asset_id = ? AND txn_type = 'pay_mortgage'",
        (asset_id,),
    ).fetchone()
    return float(row[0]) if row else 0.0


def _refresh_mortgage_preview_values(
    conn: sqlite3.Connection, mortgage: Mortgage,
) -> Mortgage:
    """Refresh the 5 stored preview values on the mortgage row. Mirror
    of ``_refresh_debt_preview_values``."""
    consumed = count_scheduled_mortgage_payments(
        conn, _asset_id_for_mortgage(conn, mortgage),
    )
    values = compute_preview_values(
        current_balance=float(mortgage.current_balance or 0.0),
        annual_rate=float(mortgage.interest_rate or 0.0),
        schedule="monthly",
        plan_type=mortgage.plan_type or "fixed_payment",
        monthly_payment_amount=float(mortgage.monthly_payment_amount or 0.0),
        original_term_periods=mortgage.original_term_periods,
        scheduled_payments_so_far=consumed,
    )
    if values is None:
        return mortgage
    mortgage.preview_regular_payment = values["regular_payment"]
    mortgage.preview_period_count = values["period_count"]
    mortgage.preview_final_payment = values["final_payment"]
    mortgage.preview_total_paid = values["total_paid"]
    mortgage.preview_total_interest = values["total_interest"]
    update_mortgage(conn, mortgage)
    return mortgage


def _asset_id_for_mortgage(
    conn: sqlite3.Connection, mortgage: Mortgage,
) -> int:
    """Resolve a mortgage's owning property's asset_id. Used wherever
    the mortgage layer needs to write a transaction (transactions are
    keyed by asset_id, not property_id)."""
    prop = get_property(conn, mortgage.property_id)
    if prop is None:
        raise ValueError(
            f"Mortgage {mortgage.id} references missing property "
            f"{mortgage.property_id}."
        )
    return prop.asset_id


def add_mortgage(
    conn: sqlite3.Connection,
    property_id: int,
    original_amount: float,
    interest_rate: float = 0.0,
    *,
    payment_per_period: float | None = None,
    term_periods: int | None = None,
    cashflow_start_date: str | None = None,
    origination_date: str | None = None,
    name: str | None = None,
    minimum_payment: float = 0.0,
    due_date: str | None = None,
    notes: str | None = None,
) -> Mortgage:
    """Create a mortgage attached to a property.

    Mirror of ``add_debt`` adapted for mortgage semantics:
      - Linked to a property via ``property_id`` (not an Asset row).
      - Monthly schedule only.
      - Validates ``original_amount <= property.purchase_price`` (LTV
        check).
      - When ``origination_date`` is in the past, ``current_balance`` is
        computed via ``simulate_amortization_balance`` from the
        ``original_amount`` + payment plan + periods elapsed. When
        omitted or set to today, ``current_balance == original_amount``
        (fresh loan).

    Provide exactly one of ``payment_per_period`` or ``term_periods``.
    """
    _assert_not_bankrupt(conn)
    # Direct rejections produce a clearer error than the indirect "schedule
    # is infeasible" path through `compute_debt_schedule` below.
    _assert_non_negative(interest_rate, "Interest rate")
    _assert_non_negative(minimum_payment, "Minimum payment")

    prop = get_property(conn, property_id)
    if prop is None:
        raise ValueError(f"Property id={property_id} not found.")
    # One mortgage per property (UNIQUE(property_id) at the schema level
    # also enforces this; raise a clear message before SQLite does).
    if get_mortgage_by_property(conn, property_id) is not None:
        raise ValueError(
            f"Property already has a mortgage. Pay it off first or "
            f"delete it before adding another."
        )

    if (payment_per_period is None) == (term_periods is None):
        raise ValueError(
            "add_mortgage requires exactly one of `payment_per_period` "
            "or `term_periods`."
        )
    if original_amount <= 0:
        raise ValueError("Mortgage original_amount must be positive.")
    # Spec: "mortgage amount cannot exceed the value of the property
    # when it was bought." purchase_price is the at-purchase value.
    if (
        prop.purchase_price is not None
        and prop.purchase_price > 0
        and original_amount > prop.purchase_price + _EPSILON
    ):
        raise ValueError(
            f"Mortgage original amount ({original_amount:,.2f}) cannot "
            f"exceed property purchase price ({prop.purchase_price:,.2f})."
        )

    # Resolve the per-period payment from the user's plan choice. The
    # at-origination schedule (computed from original_amount + rate)
    # determines the per-period payment; we then walk forward to find
    # today's balance.
    origination_schedule = compute_debt_schedule(
        principal=original_amount, annual_rate=interest_rate,
        schedule="monthly",
        payment=payment_per_period, term_periods=term_periods,
    )
    if not origination_schedule.feasible:
        raise ValueError(
            f"Mortgage schedule is infeasible: "
            f"{origination_schedule.infeasibility_reason}"
        )
    plan_type = "fixed_term" if term_periods is not None else "fixed_payment"
    persisted_term = (
        origination_schedule.num_periods if plan_type == "fixed_term" else None
    )
    per_period_payment = origination_schedule.per_period_payment

    # Walk forward from origination to today for an existing loan.
    today = date_type.today().isoformat()
    if origination_date and origination_date < today:
        periods_elapsed = compute_periods_elapsed(
            origination_date, today, "monthly",
        )
        current_balance = simulate_amortization_balance(
            principal=original_amount, annual_rate=interest_rate,
            schedule="monthly", payment=per_period_payment,
            periods_elapsed=periods_elapsed,
        )
        if current_balance <= _EPSILON:
            raise ValueError(
                f"Computed current balance is zero — the loan would "
                f"already be paid off given origination_date "
                f"{origination_date} and the supplied payment plan. "
                f"Pick a more recent origination_date or a different plan."
            )
    else:
        current_balance = original_amount

    if cashflow_start_date is None:
        cashflow_start_date = first_day_next_month()

    mortgage_name = name or (
        get_asset(conn, prop.asset_id).name if prop.asset_id else "Mortgage"
    )

    mortgage = create_mortgage(conn, Mortgage(
        property_id=property_id,
        name=mortgage_name,
        original_amount=original_amount,
        current_balance=current_balance,
        interest_rate=interest_rate,
        minimum_payment=minimum_payment,
        due_date=due_date,
        notes=notes,
        monthly_payment_amount=per_period_payment,
        cashflow_start_date=cashflow_start_date,
        plan_type=plan_type,
        original_term_periods=persisted_term,
    ))
    # Refresh preview values from the *current* state (post-walk).
    _refresh_mortgage_preview_values(conn, mortgage)
    return mortgage


def pay_mortgage(
    conn: sqlite3.Connection, date: str, asset_id: int,
    amount: float, principal: float | None = None, notes: str | None = None,
) -> Transaction:
    """Apply a mortgage payment. Mirror of ``pay_debt``.

    Looks up the mortgage by the property's asset_id (the GUI's
    selection key). Splits one period of interest off the cash payment
    and reduces principal by the remainder, mirroring debt's interest-
    split math. Upper bound is the payoff amount (balance + one
    period's interest), absorbing display-rounding via the half-cent
    ``_PAYMENT_DISPLAY_TOLERANCE``.

    The ``principal`` override is used by the auto-settle final-payment
    path to land the balance at exactly 0 even when the cash payment
    legitimately exceeds the balance by one period's interest.
    """
    _assert_not_bankrupt(conn)
    _assert_positive(amount, "Mortgage payment amount")
    _assert_non_negative(principal, "Principal portion")
    prop = get_property_by_asset(conn, asset_id)
    if prop is None:
        raise ValueError(f"No property for asset id={asset_id}.")
    mortgage = get_mortgage_by_property(conn, prop.id)
    if mortgage is None:
        raise ValueError(
            f"Property '{prop.address or asset_id}' has no mortgage."
        )

    if principal is not None:
        if principal > mortgage.current_balance + _EPSILON:
            raise ValueError(
                f"Mortgage principal reduction ({principal:,.2f}) exceeds "
                f"remaining balance ({mortgage.current_balance:,.2f})."
            )
        max_allowed = principal + (
            period_interest(
                mortgage.current_balance, mortgage.interest_rate,
                mortgage.schedule_frequency,
            ) if mortgage.interest_rate and mortgage.current_balance > 0
            else 0.0
        )
    else:
        max_allowed = _payoff_amount_for_mortgage(mortgage)
    if amount > max_allowed + _PAYMENT_DISPLAY_TOLERANCE:
        raise ValueError(
            f"Mortgage payment ({amount:,.2f}) exceeds payoff amount "
            f"for {mortgage.name} ({max_allowed:,.2f})."
        )
    _assert_sufficient_cash(conn, amount, f"pay mortgage (asset {asset_id})")

    if principal is not None:
        reduction = principal
    elif mortgage.interest_rate and mortgage.current_balance > 0:
        accrued_interest = period_interest(
            mortgage.current_balance, mortgage.interest_rate,
            mortgage.schedule_frequency,
        )
        reduction = max(0.0, amount - accrued_interest)
    else:
        reduction = amount

    balance_before = mortgage.current_balance
    mortgage.current_balance = max(0.0, mortgage.current_balance - reduction)
    update_mortgage(conn, mortgage)
    txn = create_transaction(conn, Transaction(
        date=date, txn_type="pay_mortgage", asset_id=asset_id,
        total_amount=-amount, notes=notes,
    ))
    payment_type = (
        "automatic"
        if (notes or "").startswith(SCHEDULED_MORTGAGE_PAYMENT_NOTE_PREFIX)
        else "manual"
    )
    create_mortgage_payment_record(
        conn, transaction_id=txn.id, mortgage_id=mortgage.id,
        mortgage_name=mortgage.name or "",
        payment_amount=amount, payment_date=date,
        payment_type=payment_type,
        balance_before_payment=balance_before,
        balance_after_payment=mortgage.current_balance,
        note=notes,
    )
    conn.commit()
    _refresh_mortgage_preview_values(
        conn, get_mortgage_by_property(conn, prop.id),
    )
    return txn


def pay_mortgage_in_full(
    conn: sqlite3.Connection, date: str, asset_id: int,
    notes: str | None = None,
) -> Transaction:
    """Fully extinguish a mortgage — including this period's interest.
    Mirror of ``pay_debt_in_full``."""
    _assert_not_bankrupt(conn)
    prop = get_property_by_asset(conn, asset_id)
    if prop is None:
        raise ValueError(f"No property for asset id={asset_id}.")
    mortgage = get_mortgage_by_property(conn, prop.id)
    if mortgage is None:
        raise ValueError(
            f"Property '{prop.address or asset_id}' has no mortgage."
        )
    if mortgage.current_balance <= 0:
        raise ValueError(
            f"Mortgage '{mortgage.name or 'unnamed'}' is already paid off."
        )
    principal_at_payoff = mortgage.current_balance
    payoff = _payoff_amount_for_mortgage(mortgage)
    accrued_interest = max(0.0, payoff - principal_at_payoff)
    _assert_sufficient_cash(
        conn, payoff,
        f"pay off mortgage '{mortgage.name or 'unnamed'}'",
    )
    mortgage.current_balance = 0.0
    mortgage.last_payment_date = date
    update_mortgage(conn, mortgage)
    _refresh_mortgage_preview_values(conn, mortgage)
    marker = (
        f"Pay-off in full for mortgage '{mortgage.name or 'unnamed'}' "
        f"(principal {principal_at_payoff:,.2f} + accrued interest "
        f"{accrued_interest:,.2f})"
    )
    full_note = f"{marker} — {notes}" if notes else marker
    txn = create_transaction(conn, Transaction(
        date=date, txn_type="pay_mortgage", asset_id=asset_id,
        total_amount=-payoff, notes=full_note,
    ))
    payment_type = (
        "automatic"
        if (notes or "").startswith(SCHEDULED_MORTGAGE_PAYMENT_NOTE_PREFIX)
        else "manual"
    )
    create_mortgage_payment_record(
        conn, transaction_id=txn.id, mortgage_id=mortgage.id,
        mortgage_name=mortgage.name or "",
        payment_amount=payoff, payment_date=date,
        payment_type=payment_type,
        balance_before_payment=principal_at_payoff,
        balance_after_payment=0.0,
        note=full_note,
    )
    conn.commit()
    return txn


def update_mortgage_plan_after_manual_payment(
    conn: sqlite3.Connection, asset_id: int,
) -> None:
    """Refresh `monthly_payment_amount` on a fixed_term mortgage after
    a manual partial payment. Mirror of
    ``update_plan_after_manual_payment`` for debts."""
    prop = get_property_by_asset(conn, asset_id)
    if prop is None:
        return
    mortgage = get_mortgage_by_property(conn, prop.id)
    if mortgage is None or mortgage.current_balance <= 0:
        return
    if mortgage.plan_type != "fixed_term":
        return
    consumed = count_scheduled_mortgage_payments(conn, asset_id)
    original = int(mortgage.original_term_periods or 0)
    remaining = max(1, original - consumed)
    sched = compute_debt_schedule(
        principal=mortgage.current_balance,
        annual_rate=mortgage.interest_rate or 0.0,
        schedule="monthly", term_periods=remaining,
    )
    if sched.feasible:
        mortgage.monthly_payment_amount = sched.per_period_payment
        update_mortgage(conn, mortgage)
        _refresh_mortgage_preview_values(conn, mortgage)


def add_debt(
    conn: sqlite3.Connection, date: str, symbol: str, name: str,
    amount: float, interest_rate: float = 0.0, minimum_payment: float = 0.0,
    due_date: str | None = None, cash_received: bool = True,
    notes: str | None = None,
    schedule_frequency: str = "monthly",
    payment_per_period: float | None = None,
    term_periods: int | None = None,
    cashflow_start_date: str | None = None,
    original_amount: float | None = None,
    origination_date: str | None = None,
    # Legacy alias — older callers passed `monthly_payment_amount` even
    # for yearly schedules. New code should use `payment_per_period`.
    monthly_payment_amount: float | None = None,
    # Legacy kwarg — rate is now always annual, so anything other than
    # 'annual' is rejected (kept in the signature so old callers either
    # silently keep working or fail loudly with a clear message).
    interest_period: str = "annual",
) -> tuple[Asset, Debt, Transaction]:
    """Register a new debt liability.

    Interest is always **annual** — pass the yearly decimal as
    `interest_rate`. `schedule_frequency` is `'monthly'` or `'yearly'`
    and sets when the auto-deduction fires (always on the 1st of the
    period).

    Two creation modes:

    1. **Fresh loan** (default): `amount` is both the principal and the
       starting `current_balance`. `original_amount` defaults to
       `amount`. Use this for newly-borrowed debt.

    2. **Existing loan with payment history** (when ``origination_date``
       is in the past): `amount` is the *original* principal at
       origination, and `current_balance` is computed by walking the
       amortization schedule forward from ``origination_date`` to today.
       In this mode, `original_amount` is also set to `amount` (the
       borrowed principal). Use this when the user is entering a loan
       they've been paying for some time before adding it to the
       simulator.

    Provide **exactly one** of:
      - `payment_per_period`: how much to pay each period.
      - `term_periods`: how many periods the loan should take.
    """
    _assert_not_bankrupt(conn)
    if not name or not name.strip():
        raise ValueError("Debt name is required.")
    # Direct rejections produce a clearer error than the indirect "schedule
    # is infeasible" path through `compute_debt_schedule` below.
    _assert_positive(amount, "Debt principal amount")
    _assert_non_negative(interest_rate, "Interest rate")
    _assert_non_negative(minimum_payment, "Minimum payment")
    normalized = name.strip().casefold()
    for existing in list_debts(conn):
        if (existing.current_balance or 0) <= 0:
            continue
        if (existing.name or "").strip().casefold() == normalized:
            raise ValueError(
                f"A debt named '{existing.name}' already exists. "
                f"Pick a different name."
            )
    if schedule_frequency not in ("monthly", "yearly"):
        raise ValueError(f"Invalid schedule_frequency: {schedule_frequency!r}")
    if interest_period != "annual":
        raise ValueError(
            "Interest rate is always annual; `interest_period` other than "
            "'annual' is no longer supported."
        )

    if payment_per_period is None and monthly_payment_amount is not None:
        payment_per_period = monthly_payment_amount

    if payment_per_period is not None and term_periods is not None:
        raise ValueError(
            "Provide `payment_per_period` OR `term_periods`, not both."
        )

    if payment_per_period is None and term_periods is None:
        raise ValueError(
            "add_debt requires exactly one of `payment_per_period` or "
            "`term_periods` (spec §6 #6)."
        )
    plan_type = "fixed_term" if term_periods is not None else "fixed_payment"

    # Compute the at-origination schedule from `amount` (the principal at
    # origination). This drives the per-period payment regardless of
    # whether we're walking forward to today.
    schedule = compute_debt_schedule(
        principal=amount, annual_rate=interest_rate,
        schedule=schedule_frequency,
        payment=payment_per_period, term_periods=term_periods,
    )
    if not schedule.feasible:
        raise ValueError(
            f"Debt schedule is infeasible: {schedule.infeasibility_reason}"
        )

    persisted_term = schedule.num_periods if plan_type == "fixed_term" else None

    # Forward-walk the amortization for an existing loan (origination in
    # the past). Otherwise current_balance == amount (fresh loan today).
    today_iso = date_type.today().isoformat()
    if origination_date and origination_date < today_iso:
        periods_elapsed = compute_periods_elapsed(
            origination_date, today_iso, schedule_frequency,
        )
        current_balance = simulate_amortization_balance(
            principal=amount, annual_rate=interest_rate,
            schedule=schedule_frequency,
            payment=schedule.per_period_payment,
            periods_elapsed=periods_elapsed,
        )
        if current_balance <= _EPSILON:
            raise ValueError(
                f"Computed current balance is zero — the loan would "
                f"already be paid off given origination_date "
                f"{origination_date} and the supplied plan. Pick a more "
                f"recent origination_date or a different plan."
            )
        # original_amount on an existing loan IS the input `amount`
        # (the borrowed principal). The legacy `original_amount` arg is
        # ignored in this mode since `amount` already plays that role.
        resolved_original = amount
    else:
        current_balance = amount
        if original_amount is None:
            resolved_original = amount
        elif original_amount < amount - 1e-6:
            raise ValueError(
                f"original_amount ({original_amount:,.2f}) cannot be less than "
                f"current balance ({amount:,.2f})."
            )
        else:
            resolved_original = original_amount

    if cashflow_start_date is None:
        if schedule_frequency == "yearly":
            today = date_type.today()
            cashflow_start_date = date_type(today.year + 1, 1, 1).isoformat()
        else:
            cashflow_start_date = first_day_next_month()

    asset = create_asset(conn, Asset(
        symbol=symbol, name=name, asset_type="debt",
    ))
    debt = create_debt(conn, Debt(
        asset_id=asset.id, name=name,
        original_amount=resolved_original, current_balance=current_balance,
        interest_rate=interest_rate, minimum_payment=minimum_payment,
        due_date=due_date,
        schedule_frequency=schedule_frequency,
        interest_period="annual",
        monthly_payment_amount=schedule.per_period_payment,
        cashflow_start_date=cashflow_start_date,
        plan_type=plan_type,
        original_term_periods=persisted_term,
    ))
    # Refresh preview values from the *current* state (post-walk for an
    # existing loan; same as origination for a fresh loan).
    _refresh_debt_preview_values(conn, debt)
    # Cash impact: a fresh loan brings cash in (if cash_received).
    # An existing-loan entry never adds cash — the borrowing happened
    # in the past.
    cash_impact = (
        amount if (cash_received and not (origination_date and origination_date < today_iso))
        else 0.0
    )
    txn = create_transaction(conn, Transaction(
        date=date, txn_type="add_debt", asset_id=asset.id,
        total_amount=cash_impact, notes=notes,
    ))
    return asset, debt, txn


def _refresh_debt_preview_values(
    conn: sqlite3.Connection, debt: Debt,
) -> Debt:
    """Recompute and persist the 5 preview values on the debt row.

    Called after any balance-mutating event (manual partial pay, full
    payoff, scheduled auto-pay). Idempotent — rerunning produces the
    same write. Skips the persist when the schedule is now infeasible
    (e.g. fixed_payment debt whose payment no longer covers periodic
    interest); the previously stored preview values stay in place so
    the UI doesn't blank out.

    Returns the (mutated) `debt` object so callers can chain reads.
    """
    consumed = count_scheduled_debt_payments(conn, debt.asset_id)
    values = compute_preview_values(
        current_balance=float(debt.current_balance or 0.0),
        annual_rate=float(debt.interest_rate or 0.0),
        schedule=debt.schedule_frequency or "monthly",
        plan_type=debt.plan_type or "fixed_payment",
        monthly_payment_amount=float(debt.monthly_payment_amount or 0.0),
        original_term_periods=debt.original_term_periods,
        scheduled_payments_so_far=consumed,
    )
    if values is None:
        return debt
    debt.preview_regular_payment = values["regular_payment"]
    debt.preview_period_count = values["period_count"]
    debt.preview_final_payment = values["final_payment"]
    debt.preview_total_paid = values["total_paid"]
    debt.preview_total_interest = values["total_interest"]
    update_debt(conn, debt)
    return debt


def pay_debt(
    conn: sqlite3.Connection, date: str, asset_id: int,
    amount: float, principal_portion: float | None = None,
    notes: str | None = None,
) -> Transaction:
    """Apply a debt payment.

    If `principal_portion` is omitted and the debt has a non-zero
    interest rate, one period of interest is computed via
    ``period_interest`` (using the debt's ``schedule_frequency``) and
    the principal reduction is ``amount - interest`` (clamped). When
    rate is unknown or zero, the full payment is treated as principal —
    preserving legacy behavior.

    For a true "clear the debt to zero" path (which must also pay this
    period's interest on top of principal), use ``pay_debt_in_full`` —
    paying exactly ``current_balance`` here is *not* enough to extinguish
    interest-bearing debt, since one period's interest is taken off the
    top of the cash payment first.
    """
    _assert_not_bankrupt(conn)
    _assert_positive(amount, "Debt payment amount")
    debt = get_debt_by_asset(conn, asset_id)
    # Upper bound is the *payoff amount* (balance + this period's interest),
    # not just the current balance. The interest-split math below correctly
    # lands the balance at exactly 0 when the user pays the full payoff;
    # rejecting that input was an artificial restriction that forced users
    # to click "Pay Off in Full" even when they typed the same number.
    max_allowed = _payoff_amount_for_debt(debt)
    if amount > max_allowed + _PAYMENT_DISPLAY_TOLERANCE:
        raise ValueError(
            f"Debt payment ({amount:,.2f}) exceeds payoff amount "
            f"for {debt.name} ({max_allowed:,.2f})."
        )
    _assert_sufficient_cash(conn, amount, f"pay debt (asset {asset_id})")
    if principal_portion is not None:
        # Parity with `pay_mortgage`: principal_portion is the
        # auto-settle final-payment override; bound it to the current
        # balance so a buggy caller can't drive the recorded balance
        # below zero or invert the cost-basis ledger.
        _assert_non_negative(principal_portion, "Principal portion")
        if principal_portion > debt.current_balance + _EPSILON:
            raise ValueError(
                f"Debt principal reduction ({principal_portion:,.2f}) exceeds "
                f"remaining balance ({debt.current_balance:,.2f})."
            )
        reduction = principal_portion
    elif debt.interest_rate and debt.current_balance > 0:
        accrued_interest = period_interest(
            debt.current_balance, debt.interest_rate, debt.schedule_frequency,
        )
        reduction = max(0.0, amount - accrued_interest)
    else:
        reduction = amount
    balance_before = debt.current_balance
    debt.current_balance = max(0, debt.current_balance - reduction)
    update_debt(conn, debt)
    txn = create_transaction(conn, Transaction(
        date=date, txn_type="pay_debt", asset_id=asset_id,
        total_amount=-amount, notes=notes,
    ))
    # Sync-disciplined payment record write (spec §5 / Phase 6.3): every
    # pay_debt transaction has exactly one matching debt_payment_records
    # row. payment_type is derived from the auto-settle note prefix —
    # see SCHEDULED_DEBT_PAYMENT_NOTE_PREFIX.
    payment_type = (
        "automatic"
        if (notes or "").startswith(SCHEDULED_DEBT_PAYMENT_NOTE_PREFIX)
        else "manual"
    )
    create_payment_record(
        conn, transaction_id=txn.id, debt_id=debt.id,
        debt_name=debt.name or "",
        payment_amount=amount, payment_date=date,
        payment_type=payment_type,
        balance_before_payment=balance_before,
        balance_after_payment=debt.current_balance,
        note=notes,
    )
    conn.commit()
    # Refresh the 5 stored preview values to mirror the post-payment plan.
    # Done after the transaction insert so count_scheduled_debt_payments
    # sees this row when it runs (only relevant if `notes` carries the
    # auto-settle prefix; manual pays don't affect the count). For
    # fixed-term debts where the per-period payment also needs an update,
    # update_plan_after_manual_payment overwrites monthly_payment_amount
    # before this refresh runs.
    _refresh_debt_preview_values(conn, get_debt_by_asset(conn, asset_id))
    return txn


def _payoff_amount_for_debt(debt: Debt | None) -> float:
    """Pure form of ``compute_payoff_amount`` that takes a debt object
    instead of a (conn, asset_id) pair — lets ``pay_debt`` reuse the
    payoff math without a redundant DB lookup.
    """
    if debt is None or debt.current_balance <= 0:
        return 0.0
    rate = debt.interest_rate or 0.0
    if rate <= 0:
        return debt.current_balance
    return debt.current_balance + period_interest(
        debt.current_balance, rate, debt.schedule_frequency,
    )


def compute_payoff_amount(
    conn: sqlite3.Connection, asset_id: int,
) -> float:
    """Cash needed to extinguish a debt today — principal plus this
    period's interest at the stored rate and schedule.

    For a zero-rate debt this equals ``current_balance``. For an interest-
    bearing debt it equals ``current_balance + one period's interest``,
    where the period is the debt's ``schedule_frequency`` (monthly or
    yearly). Paying *only* ``current_balance`` via ``pay_debt`` would
    leave a small interest residue, so this helper is the canonical
    amount to charge when the user really wants the debt cleared.
    """
    return _payoff_amount_for_debt(get_debt_by_asset(conn, asset_id))


def pay_debt_in_full(
    conn: sqlite3.Connection, date: str, asset_id: int,
    notes: str | None = None,
) -> Transaction:
    """Fully extinguish a debt — including this period's interest.

    Charges ``compute_payoff_amount(...)`` against cash and drives
    ``debt.current_balance`` to exactly zero. One ``pay_debt``
    transaction is recorded for the full payoff amount. Rejected if
    cash is insufficient.

    The transaction note carries an explicit "Pay-off in full
    (principal X + accrued interest Y)" marker so the audit trail
    reflects what the payoff actually covered (spec §3 #3): the
    principal balance plus this payment period's accrued interest.
    Any user-supplied ``notes`` is appended after the marker.
    """
    _assert_not_bankrupt(conn)
    debt = get_debt_by_asset(conn, asset_id)
    if debt is None:
        raise ValueError(f"No debt for asset id={asset_id}.")
    if debt.current_balance <= 0:
        raise ValueError(
            f"Debt '{debt.name or 'unnamed'}' is already paid off."
        )
    principal_at_payoff = debt.current_balance
    payoff = compute_payoff_amount(conn, asset_id)
    accrued_interest = max(0.0, payoff - principal_at_payoff)
    _assert_sufficient_cash(
        conn, payoff,
        f"pay off debt '{debt.name or 'unnamed'}'",
    )
    debt.current_balance = 0.0
    debt.last_payment_date = date
    # Balance is now 0; preview values become zero via the helper.
    update_debt(conn, debt)
    _refresh_debt_preview_values(conn, debt)
    marker = (
        f"Pay-off in full for debt '{debt.name or 'unnamed'}' "
        f"(principal {principal_at_payoff:,.2f} + accrued interest "
        f"{accrued_interest:,.2f})"
    )
    full_note = f"{marker} — {notes}" if notes else marker
    txn = create_transaction(conn, Transaction(
        date=date, txn_type="pay_debt", asset_id=asset_id,
        total_amount=-payoff, notes=full_note,
    ))
    # Phase 6.3 sync: every pay_debt transaction has exactly one matching
    # debt_payment_records row. The final-payment path of auto-settle
    # routes through pay_debt_in_full (because pay_debt would leave a
    # one-period interest residue on interest-bearing debts), and it
    # passes the canonical "Scheduled debt payment ..." note prefix.
    # Derive payment_type from that prefix so an auto final payoff is
    # correctly classified as 'automatic'.
    payment_type = (
        "automatic"
        if (notes or "").startswith(SCHEDULED_DEBT_PAYMENT_NOTE_PREFIX)
        else "manual"
    )
    create_payment_record(
        conn, transaction_id=txn.id, debt_id=debt.id,
        debt_name=debt.name or "",
        payment_amount=payoff, payment_date=date,
        payment_type=payment_type,
        balance_before_payment=principal_at_payoff,
        balance_after_payment=0.0,
        note=full_note,
    )
    conn.commit()
    return txn


# Note prefix written by `settle_due_debt_payments` for every scheduled
# auto-deduction. Counted by `count_scheduled_debt_payments` to determine
# how many term periods a fixed-term debt has consumed.
SCHEDULED_DEBT_PAYMENT_NOTE_PREFIX = "Scheduled debt payment"


def count_scheduled_debt_payments(
    conn: sqlite3.Connection, asset_id: int,
) -> int:
    """How many scheduled auto-payments have already fired for a debt.

    Used by Pay Debt's recompute path on fixed-term debts: the remaining
    term equals ``original_term_periods - count_scheduled_debt_payments(...)``.
    Manual partial payments do not count toward the consumed periods.
    """
    row = conn.execute(
        "SELECT COUNT(*) FROM transactions "
        "WHERE asset_id = ? AND txn_type = 'pay_debt' AND notes LIKE ?",
        (asset_id, f"{SCHEDULED_DEBT_PAYMENT_NOTE_PREFIX}%"),
    ).fetchone()
    return int(row[0]) if row else 0


def total_paid_for_debt(
    conn: sqlite3.Connection, asset_id: int,
) -> float:
    """Sum of every `pay_debt` transaction's cash outflow for this debt.

    Used by the Pay Debt preview to render the spec's "total paid" line,
    which combines past payments + the proposed current payment + the
    future payments in the recalculated plan. Stored as negative
    `total_amount` so we sum the absolute values.
    """
    row = conn.execute(
        "SELECT COALESCE(SUM(ABS(total_amount)), 0) FROM transactions "
        "WHERE asset_id = ? AND txn_type = 'pay_debt'",
        (asset_id,),
    ).fetchone()
    return float(row[0]) if row else 0.0


def update_plan_after_manual_payment(
    conn: sqlite3.Connection, asset_id: int,
) -> None:
    """Refresh `monthly_payment_amount` after a manual partial payment.

    Only `fixed_term` debts are affected: their per-period payment is
    derived from the post-payment balance and the *remaining* term, so a
    manual extra payment must drop the per-period amount. `fixed_payment`
    debts keep their per-period amount constant by definition (the user
    just shortens the schedule), so this is a no-op for them.

    Safe to call after `pay_debt(...)` succeeds. If the debt is paid off
    or no longer exists, this returns without modifying anything.
    """
    debt = get_debt_by_asset(conn, asset_id)
    if debt is None or debt.current_balance <= 0:
        return
    if debt.plan_type != "fixed_term":
        return
    consumed = count_scheduled_debt_payments(conn, asset_id)
    original = int(debt.original_term_periods or 0)
    remaining = max(1, original - consumed)
    sched = compute_debt_schedule(
        principal=debt.current_balance, annual_rate=debt.interest_rate or 0.0,
        schedule=debt.schedule_frequency or "monthly",
        term_periods=remaining,
    )
    if sched.feasible:
        debt.monthly_payment_amount = sched.per_period_payment
        update_debt(conn, debt)
        # The per-period payment changed → refresh the 5 preview values
        # so they reflect the new live plan.
        _refresh_debt_preview_values(conn, debt)


def manual_adjustment(
    conn: sqlite3.Connection, date: str, amount: float,
    asset_id: int | None = None, quantity: float | None = None,
    price: float | None = None, notes: str | None = None,
) -> Transaction:
    """Cash- and/or position-correcting entry.

    A pure cash adjustment leaves quantity and price unset. A position
    adjustment requires asset_id, quantity, AND price so the resulting
    average cost basis stays well-defined. Without all three, a bare
    quantity would silently fail to flow into derived positions
    (AUDIT-CB-001).
    """
    _assert_not_bankrupt(conn)
    if quantity is not None:
        # Position-bearing adjustments must be positive — same convention
        # as buy/sell. To remove shares, use `sell` (or a follow-up buy
        # at $0 isn't supported either). Negative quantity here corrupts
        # `calc_positions`'s avg-cost denominator and silently zeroes the
        # cost basis on the remaining shares (verified bug, May 2026).
        # `amount` (the cash side) keeps its signed semantics — that is
        # the documented escape hatch for cash corrections.
        _assert_positive(quantity, "Quantity")
        if asset_id is None:
            raise ValueError(
                "manual_adjustment with a quantity requires asset_id."
            )
        if price is None or price <= 0:
            raise ValueError(
                "manual_adjustment with a quantity requires a positive price "
                "so cost basis remains correct."
            )
        asset = get_asset(conn, asset_id)
        if asset is None:
            raise ValueError(f"Asset id={asset_id} not found.")
        if asset.asset_type not in SELLABLE_ASSET_TYPES:
            raise ValueError(
                f"manual_adjustment cannot change quantity for asset type "
                f"'{asset.asset_type}'."
            )
    return create_transaction(conn, Transaction(
        date=date, txn_type="manual_adjustment", asset_id=asset_id,
        quantity=quantity, price=price, total_amount=amount, notes=notes,
    ))


def sell_property(
    conn: sqlite3.Connection, date: str, asset_id: int,
    sale_price: float, fees: float = 0.0, notes: str | None = None,
) -> Transaction:
    """Sell a property. If the property has an active mortgage, the
    sale settles it via ``pay_mortgage_in_full`` first (writing a
    separate ``pay_mortgage`` transaction), then records the
    ``sell_property`` transaction with proceeds = sale_price - fees.

    Net cash to the seller after both transactions is
    ``sale_price - payoff - fees``, where payoff = mortgage balance +
    one period's accrued interest. Behavior change vs schema v10: the
    accrued-interest portion is now correctly charged at closing
    (previously the sell_property net_proceeds silently used just
    `mortgage_balance`, omitting the period's interest).
    """
    _assert_not_bankrupt(conn)
    if sale_price <= 0:
        raise ValueError("Sale price must be positive.")
    if fees < 0:
        raise ValueError("Fees cannot be negative.")

    asset = get_asset(conn, asset_id)
    if asset is None:
        raise ValueError(f"Asset id={asset_id} not found.")
    if asset.asset_type != "real_estate":
        raise ValueError(f"Asset '{asset.symbol}' is not a real estate asset.")

    prop = get_property_by_asset(conn, asset_id)
    if prop is None:
        raise ValueError(f"No property record for asset id={asset_id}.")
    if prop.status != "active":
        raise ValueError(f"Property '{asset.name}' is already sold.")

    mortgage = get_mortgage_by_property(conn, prop.id)
    payoff = (
        _payoff_amount_for_mortgage(mortgage)
        if mortgage is not None and mortgage.current_balance > 0 else 0.0
    )

    # If the mortgage payoff plus fees exceeds the sale price, the seller
    # must bring cash to closing — verify they have it before any writes.
    proceeds_to_seller = sale_price - fees - payoff
    if proceeds_to_seller < 0:
        _assert_sufficient_cash(
            conn, -proceeds_to_seller,
            f"settle mortgage on sale of {asset.symbol}",
        )

    # Record the sell_property transaction FIRST so the sale proceeds
    # land in cash before pay_mortgage_in_full's cash check runs. Without
    # this ordering, an above-water sale where the user has zero cash
    # would falsely fail "Insufficient cash for pay off mortgage" — the
    # sale itself is supposed to fund the mortgage settlement.
    sale_proceeds = sale_price - fees
    txn = create_transaction(conn, Transaction(
        date=date, txn_type="sell_property", asset_id=asset_id,
        quantity=1, price=sale_price, total_amount=sale_proceeds,
        fees=fees, notes=notes,
    ))

    # Now settle the mortgage (if any). pay_mortgage_in_full writes one
    # pay_mortgage transaction and zeroes the mortgage balance.
    if mortgage is not None and mortgage.current_balance > 0:
        sale_marker = f"Settled at sale of '{asset.name}' on {date}"
        pay_mortgage_in_full(conn, date, asset_id, notes=sale_marker)

    prop.status = "sold"
    prop.sold_date = date
    prop.sold_price = sale_price
    prop.sale_fees = fees
    prop.current_value = 0
    update_property(conn, prop)

    return txn


@_autosettle_bypass
def settle_due_rent(
    conn: sqlite3.Connection, through_date: str,
    property_asset_id: int | None = None,
) -> list[Transaction]:
    """Auto-credit *effective* rent — `monthly_rent * (1 - vacancy_rate)`
    — for each due period. Crediting full rent would diverge from the
    analysis pages (`real_estate.calc_effective_rent`), so the cash
    ledger and the projection always agree.
    """
    through = date_type.fromisoformat(through_date)
    created: list[Transaction] = []

    if property_asset_id is not None:
        prop = get_property_by_asset(conn, property_asset_id)
        if prop is None:
            raise ValueError(f"No property for asset id={property_asset_id}.")
        props = [prop] if prop.status == "active" else []
    else:
        props = list_active_properties(conn)

    for prop in props:
        if not prop.monthly_rent or prop.monthly_rent <= 0:
            continue
        effective_monthly_rent = prop.monthly_rent * (
            1.0 - (prop.vacancy_rate or 0.0)
        )
        if effective_monthly_rent <= 0:
            continue

        if prop.cashflow_start_date:
            anchor = date_type.fromisoformat(prop.cashflow_start_date)
        else:
            anchor = date_type.fromisoformat(first_day_next_month())

        stop = through
        if prop.status == "sold" and prop.sold_date:
            sold = date_type.fromisoformat(prop.sold_date)
            if sold < stop:
                stop = sold

        existing = list_transactions(conn, asset_id=prop.asset_id, txn_type="receive_rent")
        existing_notes = {t.notes for t in existing if t.notes}
        prop_asset = get_asset(conn, prop.asset_id)
        prop_name = prop_asset.name if prop_asset else f"property #{prop.id}"

        if prop.rent_collection_frequency == "annual":
            if anchor.month == 1 and anchor.day == 1:
                d = anchor
            else:
                d = date_type(anchor.year + 1, 1, 1)
            while d <= stop:
                # Stable dedupe prefix; the human-readable suffix is appended
                # after, so old rows that wrote only the prefix still match.
                prefix = f"Scheduled rent {d.year}"
                if not any(n and n.startswith(prefix) for n in existing_notes):
                    annual_rent = effective_monthly_rent * 12
                    note = f"{prefix} — auto-credited rent for '{prop_name}'"
                    txn = create_transaction(conn, Transaction(
                        date=d.isoformat(), txn_type="receive_rent",
                        asset_id=prop.asset_id, total_amount=annual_rent,
                        notes=note,
                    ))
                    created.append(txn)
                d = date_type(d.year + 1, 1, 1)
        else:
            if anchor.day == 1:
                d = anchor
            else:
                d = date_type(anchor.year, anchor.month, 1) + relativedelta(months=1)
            while d <= stop:
                prefix = f"Scheduled rent {d.strftime('%Y-%m')}"
                if not any(n and n.startswith(prefix) for n in existing_notes):
                    note = f"{prefix} — auto-credited rent for '{prop_name}'"
                    txn = create_transaction(conn, Transaction(
                        date=d.isoformat(), txn_type="receive_rent",
                        asset_id=prop.asset_id,
                        total_amount=effective_monthly_rent,
                        notes=note,
                    ))
                    created.append(txn)
                d += relativedelta(months=1)

    return created


# Note prefix written by `settle_due_property_expenses` for every
# scheduled monthly opex deduction. Used by the dedupe check so the
# function is idempotent under re-runs.
SCHEDULED_PROPERTY_EXPENSE_NOTE_PREFIX = "Scheduled property expense"


@_autosettle_bypass
def settle_due_property_expenses(
    conn: sqlite3.Connection, through_date: str,
    property_asset_id: int | None = None,
) -> list[Transaction]:
    """Auto-deduct monthly operating expenses (property tax, insurance,
    HOA, maintenance reserve, property management) for each active
    property. One `pay_property_expense` per property per month
    aggregating the five fields. Skips properties whose total opex is
    zero. Like rent, expenses fire on the 1st of each month from the
    property's `cashflow_start_date` (or the engine default) forward.

    Cash is *not* pre-checked — these are scheduled obligations; the
    auto-settle pipeline's mop-up `force_sell_to_cover_negative_cash`
    handles any resulting negative cash.
    """
    through = date_type.fromisoformat(through_date)
    created: list[Transaction] = []

    if property_asset_id is not None:
        prop = get_property_by_asset(conn, property_asset_id)
        if prop is None:
            raise ValueError(f"No property for asset id={property_asset_id}.")
        props = [prop] if prop.status == "active" else []
    else:
        props = list_active_properties(conn)

    for prop in props:
        opex_total = (
            (prop.monthly_property_tax or 0.0)
            + (prop.monthly_insurance or 0.0)
            + (prop.monthly_hoa or 0.0)
            + (prop.monthly_maintenance_reserve or 0.0)
            + (prop.monthly_property_management or 0.0)
        )
        if opex_total <= 0:
            continue

        if prop.cashflow_start_date:
            anchor = date_type.fromisoformat(prop.cashflow_start_date)
        else:
            anchor = date_type.fromisoformat(first_day_next_month())

        stop = through
        if prop.status == "sold" and prop.sold_date:
            sold = date_type.fromisoformat(prop.sold_date)
            if sold < stop:
                stop = sold

        existing = list_transactions(
            conn, asset_id=prop.asset_id, txn_type="pay_property_expense",
        )
        existing_notes = {t.notes for t in existing if t.notes}
        prop_asset = get_asset(conn, prop.asset_id)
        prop_name = prop_asset.name if prop_asset else f"property #{prop.id}"

        if anchor.day == 1:
            d = anchor
        else:
            d = date_type(anchor.year, anchor.month, 1) + relativedelta(months=1)
        while d <= stop:
            prefix = (
                f"{SCHEDULED_PROPERTY_EXPENSE_NOTE_PREFIX} "
                f"{d.strftime('%Y-%m')}"
            )
            if not any(n and n.startswith(prefix) for n in existing_notes):
                note = (
                    f"{prefix} — tax/insurance/HOA/maintenance/management "
                    f"for '{prop_name}'"
                )
                txn = create_transaction(conn, Transaction(
                    date=d.isoformat(), txn_type="pay_property_expense",
                    asset_id=prop.asset_id,
                    total_amount=-abs(opex_total),
                    notes=note,
                ))
                created.append(txn)
            d += relativedelta(months=1)

    return created


@_autosettle_bypass
def settle_due_debt_payments(
    conn: sqlite3.Connection, through_date: str,
) -> tuple[list[Transaction], list[dict]]:
    """Auto-deduct scheduled debt payments through `through_date`.

    Walks each debt's payment schedule (monthly or yearly) from
    `cashflow_start_date` forward, creating a `pay_debt` txn for each
    due date that hasn't already been processed. If cash is insufficient
    for a particular due date, the item is appended to `deferred` and
    the loop continues; the caller can retry after force-selling.

    Returns ``(created, deferred)``. Each deferred entry is a dict with
    keys ``kind``, ``asset_id``, ``amount``, ``date``, ``label``.
    """
    through = date_type.fromisoformat(through_date)
    created: list[Transaction] = []
    deferred: list[dict] = []

    for debt in list_debts(conn):
        if debt.current_balance <= 0:
            continue
        if debt.monthly_payment_amount <= 0:
            continue

        if debt.cashflow_start_date:
            anchor = date_type.fromisoformat(debt.cashflow_start_date)
        else:
            anchor = next_month_start(date_type.today())

        # Auto-deductions always fire on the 1st of the period (1st of
        # the month for monthly schedules, January 1st for yearly). If the
        # anchor isn't already aligned, snap it forward to the next 1st.
        if debt.schedule_frequency == "yearly":
            if anchor.month != 1 or anchor.day != 1:
                anchor = date_type(anchor.year + 1, 1, 1)
        else:
            if anchor.day != 1:
                anchor = (date_type(anchor.year, anchor.month, 1)
                          + relativedelta(months=1))

        existing = list_transactions(conn, asset_id=debt.asset_id, txn_type="pay_debt")
        existing_notes = {t.notes for t in existing if t.notes}
        last_paid = (
            date_type.fromisoformat(debt.last_payment_date)
            if debt.last_payment_date else None
        )

        step = (
            relativedelta(years=1) if debt.schedule_frequency == "yearly"
            else relativedelta(months=1)
        )

        d = anchor
        while d <= through:
            # Date-only prefix is the dedupe key (stable across restarts and
            # against legacy rows). The note we actually write also names the
            # debt for the user.
            prefix = f"Scheduled debt payment {d.isoformat()}"
            already = (
                (last_paid and last_paid >= d)
                or any(n and n.startswith(prefix) for n in existing_notes)
            )
            if not already:
                fresh = get_debt_by_asset(conn, debt.asset_id)
                if fresh.current_balance <= 0:
                    break
                # Re-derive the schedule from the *current* balance each
                # iteration. When the next payment is the final one we use
                # `sched.final_payment` (= principal + this period's interest)
                # routed through pay_debt_in_full so the balance lands on
                # zero — paying just min(payment, balance) would leave
                # interest residue on interest-bearing debts.
                sched = compute_debt_schedule(
                    principal=fresh.current_balance,
                    annual_rate=fresh.interest_rate or 0.0,
                    schedule=fresh.schedule_frequency,
                    payment=debt.monthly_payment_amount,
                )
                is_final = sched.feasible and sched.num_periods == 1
                if is_final:
                    pay_amt = sched.final_payment
                else:
                    pay_amt = min(
                        debt.monthly_payment_amount,
                        fresh.current_balance,
                    )
                note = (
                    f"{prefix} — auto-deducted from cash to pay off "
                    f"debt '{debt.name or 'unnamed'}'"
                )
                try:
                    if is_final:
                        txn = pay_debt_in_full(
                            conn, d.isoformat(), debt.asset_id, notes=note,
                        )
                    else:
                        txn = pay_debt(
                            conn, d.isoformat(), debt.asset_id, pay_amt,
                            notes=note,
                        )
                    created.append(txn)
                    last_paid = d
                    fresh = get_debt_by_asset(conn, debt.asset_id)
                    fresh.last_payment_date = d.isoformat()
                    update_debt(conn, fresh)
                except ValueError as exc:
                    if "Insufficient cash" in str(exc):
                        deferred.append({
                            "kind": "debt",
                            "asset_id": debt.asset_id,
                            "amount": pay_amt,
                            "date": d.isoformat(),
                            "label": note,
                            "is_final": is_final,
                        })
                    else:
                        raise
            d = d + step

    return created, deferred


@_autosettle_bypass
def settle_due_mortgage_payments(
    conn: sqlite3.Connection, through_date: str,
) -> tuple[list[Transaction], list[dict]]:
    """Auto-deduct scheduled mortgage payments through `through_date`.

    Walks each active mortgage's monthly schedule from
    ``cashflow_start_date`` forward, creating a `pay_mortgage` txn
    for each due date that hasn't already been processed. Final
    payment routes through ``pay_mortgage_in_full`` so the balance
    lands at exactly 0 (no interest residue). Mirror of
    ``settle_due_debt_payments``.
    """
    through = date_type.fromisoformat(through_date)
    created: list[Transaction] = []
    deferred: list[dict] = []

    for mortgage in list_active_mortgages(conn):
        if mortgage.monthly_payment_amount <= 0:
            continue
        prop = get_property(conn, mortgage.property_id)
        if prop is None or prop.status != "active":
            continue
        asset_id = prop.asset_id

        if mortgage.cashflow_start_date:
            anchor = date_type.fromisoformat(mortgage.cashflow_start_date)
        else:
            anchor = next_month_start(date_type.today())

        # Auto-deductions fire on the 1st of the month. Snap forward
        # if the anchor isn't already aligned.
        if anchor.day != 1:
            anchor = (date_type(anchor.year, anchor.month, 1)
                      + relativedelta(months=1))

        existing = list_transactions(
            conn, asset_id=asset_id, txn_type="pay_mortgage",
        )
        existing_notes = {t.notes for t in existing if t.notes}
        last_paid = (
            date_type.fromisoformat(mortgage.last_payment_date)
            if mortgage.last_payment_date else None
        )

        prop_asset = get_asset(conn, asset_id)
        prop_name = prop_asset.name if prop_asset else f"property #{prop.id}"

        d = anchor
        step = relativedelta(months=1)
        while d <= through:
            prefix = f"{SCHEDULED_MORTGAGE_PAYMENT_NOTE_PREFIX} {d.isoformat()}"
            already = (
                (last_paid and last_paid >= d)
                or any(n and n.startswith(prefix) for n in existing_notes)
            )
            if not already:
                fresh = get_mortgage_by_property(conn, prop.id)
                if fresh is None or fresh.current_balance <= 0:
                    break
                # Re-derive the schedule from the *current* balance each
                # iteration. When the next payment is the final one we
                # use pay_mortgage_in_full so the balance lands on zero
                # without an interest residue (mirror debt's path).
                sched = compute_debt_schedule(
                    principal=fresh.current_balance,
                    annual_rate=fresh.interest_rate or 0.0,
                    schedule="monthly",
                    payment=mortgage.monthly_payment_amount,
                )
                is_final = sched.feasible and sched.num_periods == 1
                if is_final:
                    pay_amt = sched.final_payment
                else:
                    pay_amt = min(
                        mortgage.monthly_payment_amount,
                        fresh.current_balance,
                    )
                note = (
                    f"{prefix} — auto-deducted from cash for "
                    f"property '{prop_name}'"
                )
                try:
                    if is_final:
                        txn = pay_mortgage_in_full(
                            conn, d.isoformat(), asset_id, notes=note,
                        )
                    else:
                        txn = pay_mortgage(
                            conn, d.isoformat(), asset_id, pay_amt,
                            notes=note,
                        )
                    created.append(txn)
                    last_paid = d
                    fresh = get_mortgage_by_property(conn, prop.id)
                    if fresh is not None:
                        fresh.last_payment_date = d.isoformat()
                        update_mortgage(conn, fresh)
                except ValueError as exc:
                    if "Insufficient cash" in str(exc):
                        deferred.append({
                            "kind": "mortgage",
                            "asset_id": asset_id,
                            "amount": pay_amt,
                            "date": d.isoformat(),
                            "label": note,
                            "is_final": is_final,
                        })
                    else:
                        raise
            d = d + step

    return created, deferred


@_autosettle_bypass
def force_sell_to_raise_cash(
    conn: sqlite3.Connection, today: str, target_cash: float,
    reason: str = "scheduled debt/mortgage payment",
    debt_id: int | None = None,
    debt_name: str | None = None,
    required_payment: float | None = None,
) -> list[Transaction]:
    """Force-sell sellable assets until cash >= ``target_cash``.

    Spec §11 ordering: stock < etf < other < real_estate, cheapest
    first within each bucket. Prices for tradeable assets are refreshed
    before pricing the plan (best-effort — failures degrade to cached
    quotes). Stock/ETF round up to whole shares; crypto/custom allow
    8-decimal fractional. Real estate sells whole properties; sale
    price uses ``properties.current_value`` if set, else
    ``purchase_price``.

    The function builds a ``ForceSellPlan`` and only writes
    transactions when the plan can fully cover the gap. If the plan
    cannot cover, no sales are written — the caller is expected to
    record a ``bankruptcy_event``.

    ``debt_id`` and ``debt_name`` (when provided) are threaded into
    each transaction's note so the user can audit *why* the asset was
    sold; ``reason`` is the legacy free-text fallback used when the
    caller doesn't have a debt name handy.

    ``required_payment`` is the actual obligation amount (or aggregate
    shortfall) the force-sell is funding — it goes into the plan's
    audit metadata. ``target_cash`` remains the cash level the force-
    sell loop must reach (typically ``current_cash + required_payment``
    for an obligation-driven sell). When ``required_payment`` is None
    we fall back to ``target_cash`` for back-compat with callers that
    pre-date the split.
    """
    from src.engines.force_sell import (
        build_force_sell_plan, execute_force_sell_plan,
    )
    obligation_amount = (
        target_cash if required_payment is None else float(required_payment)
    )
    plan = build_force_sell_plan(
        conn, target_cash=target_cash, required_payment=obligation_amount,
        debt_id=debt_id,
        debt_name=debt_name or (reason if reason and debt_name is None else None),
    )
    if plan.bankruptcy_triggered:
        # Spec: no partial state. Caller (auto-settle) records bankruptcy.
        return []
    return execute_force_sell_plan(conn, plan, today, strict=True)


@_autosettle_bypass
def force_sell_to_cover_negative_cash(
    conn: sqlite3.Connection, today: str,
    reason: str = "scheduled debt/mortgage payment",
) -> list[Transaction]:
    """Recover negative cash by liquidating sellable assets.

    Unlike ``force_sell_to_raise_cash``, this path executes whatever
    items the plan can muster *even if they can't fully restore cash to
    zero*. This is the legacy mop-up semantic: an unrelated
    ``manual_adjustment`` or some other path drove cash below zero, and
    we want to recover what we can. Bankruptcy in this scenario is
    detected separately by ``risk.check_bankruptcy`` (cash<0 + no
    sellable holdings remain).

    ``reason`` is threaded into each transaction's note (e.g.
    ``"…(auto debt deduction)"``) so legacy callers that pass a custom
    reason still see it in the audit trail.
    """
    from src.engines.force_sell import (
        build_force_sell_plan, execute_force_sell_plan,
    )
    plan = build_force_sell_plan(
        conn, target_cash=0.0, required_payment=0.0,
        debt_id=None, debt_name=reason or None,
    )
    if not plan.assets_to_sell:
        return []
    return execute_force_sell_plan(conn, plan, today)


@_autosettle_bypass
def retry_deferred(
    conn: sqlite3.Connection, deferred: list[dict],
) -> tuple[list[Transaction], list[dict]]:
    """One retry pass over auto-payments that previously hit insufficient
    cash. Returns ``(created, still_deferred)`` so the caller can persist
    the still-unfundable items as missed payments. Items that have since
    become irrelevant (debt cleared elsewhere, mortgage paid off) are
    silently skipped — they appear in neither return list.
    """
    created: list[Transaction] = []
    still_deferred: list[dict] = []
    for item in deferred:
        kind = item.get("kind")
        try:
            if kind == "debt":
                fresh = get_debt_by_asset(conn, item["asset_id"])
                if fresh is None or fresh.current_balance <= 0:
                    continue
                if item.get("is_final"):
                    txn = pay_debt_in_full(
                        conn, item["date"], item["asset_id"],
                        notes=item["label"],
                    )
                else:
                    pay_amt = min(item["amount"], fresh.current_balance)
                    txn = pay_debt(
                        conn, item["date"], item["asset_id"], pay_amt,
                        notes=item["label"],
                    )
                fresh = get_debt_by_asset(conn, item["asset_id"])
                fresh.last_payment_date = item["date"]
                update_debt(conn, fresh)
                created.append(txn)
            elif kind == "mortgage":
                prop = get_property_by_asset(conn, item["asset_id"])
                if prop is None:
                    continue
                fresh = get_mortgage_by_property(conn, prop.id)
                if fresh is None or fresh.current_balance <= 0:
                    continue
                if item.get("is_final"):
                    txn = pay_mortgage_in_full(
                        conn, item["date"], item["asset_id"],
                        notes=item["label"],
                    )
                else:
                    pay_amt = min(item["amount"], fresh.current_balance)
                    txn = pay_mortgage(
                        conn, item["date"], item["asset_id"], pay_amt,
                        notes=item["label"],
                    )
                fresh = get_mortgage_by_property(conn, prop.id)
                if fresh is not None:
                    fresh.last_payment_date = item["date"]
                    update_mortgage(conn, fresh)
                created.append(txn)
        except ValueError as exc:
            if "Insufficient cash" in str(exc):
                still_deferred.append(item)
            else:
                # Anything else is a programmer error — surface it.
                raise
    return created, still_deferred


# Legacy `record_missed_payment` / `list_unresolved_missed_payments` /
# `record_deferred_as_missed` were removed in schema v10. Auto-settle's
# unfundable-obligation path goes straight to `bankruptcy_events` via
# `MainWindow._run_auto_settle` → `record_bankruptcy_event`.
