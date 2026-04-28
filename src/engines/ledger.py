import logging
import sqlite3
from datetime import date as date_type
from dateutil.relativedelta import relativedelta

_log = logging.getLogger(__name__)
from src.models.asset import Asset
from src.models.transaction import Transaction
from src.models.property_asset import PropertyAsset
from src.models.debt import Debt
from src.storage.asset_repo import create_asset, get_asset
from src.storage.transaction_repo import create_transaction, list_transactions
from src.storage.property_repo import create_property, get_property_by_asset, update_property, list_active_properties
from src.storage.debt_repo import create_debt, get_debt_by_asset, update_debt
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


def deposit_cash(conn: sqlite3.Connection, date: str, amount: float, notes: str | None = None) -> Transaction:
    return create_transaction(conn, Transaction(
        date=date, txn_type="deposit_cash", total_amount=amount, notes=notes,
    ))


def withdraw_cash(conn: sqlite3.Connection, date: str, amount: float, notes: str | None = None) -> Transaction:
    _assert_sufficient_cash(conn, abs(amount), "withdraw")
    return create_transaction(conn, Transaction(
        date=date, txn_type="withdraw_cash", total_amount=-abs(amount), notes=notes,
    ))


def buy(
    conn: sqlite3.Connection, date: str, asset_id: int,
    quantity: float, price: float, fees: float = 0.0, notes: str | None = None,
) -> Transaction:
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
    mortgage_balance: float = 0.0, monthly_rent: float = 0.0,
    monthly_expense: float = 0.0, address: str | None = None,
    down_payment: float | None = None,
    mortgage_interest_rate: float = 0.0,
    monthly_mortgage_payment: float = 0.0,
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
    if acquisition_mode not in ("new_purchase", "existing_property", "planned_purchase"):
        raise ValueError(f"Unknown acquisition_mode: {acquisition_mode!r}")

    if (
        acquisition_mode == "new_purchase"
        and down_payment is not None
        and purchase_price > 0
        and abs(down_payment + mortgage_balance - purchase_price) > 1.0
    ):
        # Inconsistent breakdown — could be intentional (gift money, partial
        # capital later, seller financing). The simulator allows it but logs
        # it so the discrepancy is visible.
        _log.warning(
            "add_property: down_payment (%s) + mortgage_balance (%s) != purchase_price (%s) for %s",
            down_payment, mortgage_balance, purchase_price, symbol,
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
        mortgage_balance=mortgage_balance,
        mortgage_interest_rate=mortgage_interest_rate,
        monthly_mortgage_payment=monthly_mortgage_payment,
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

    if acquisition_mode == "existing_property":
        record_date = transaction_date or date_type.today().isoformat()
        txn_notes = notes or ""
        if txn_notes:
            txn_notes += " | "
        txn_notes += "Existing property entry - no purchase cash impact."
        txn = create_transaction(conn, Transaction(
            date=record_date, txn_type="add_property", asset_id=asset.id,
            quantity=1, price=purchase_price, total_amount=0.0,
            notes=txn_notes,
        ))
    elif acquisition_mode == "planned_purchase":
        txn_notes = notes or ""
        if txn_notes:
            txn_notes += " | "
        txn_notes += "Planned purchase scenario - no cash impact."
        txn = create_transaction(conn, Transaction(
            date=date, txn_type="add_property", asset_id=asset.id,
            quantity=1, price=purchase_price, total_amount=0.0,
            notes=txn_notes,
        ))
    else:
        cash_out = down_payment if down_payment is not None else (purchase_price - mortgage_balance)
        # New-purchase real estate must be paid for: reject if the user
        # lacks the cash for the down payment / out-of-pocket portion.
        _assert_sufficient_cash(conn, abs(cash_out), f"add_property {symbol}")
        txn = create_transaction(conn, Transaction(
            date=transaction_date or date, txn_type="add_property", asset_id=asset.id,
            quantity=1, price=purchase_price, total_amount=-abs(cash_out),
            notes=notes,
        ))
    return asset, prop, txn


def update_property_value(
    conn: sqlite3.Connection, date: str, asset_id: int,
    new_value: float, notes: str | None = None,
) -> Transaction:
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
    return create_transaction(conn, Transaction(
        date=date, txn_type="receive_rent", asset_id=asset_id,
        total_amount=amount, notes=notes,
    ))


def pay_property_expense(
    conn: sqlite3.Connection, date: str, asset_id: int,
    amount: float, notes: str | None = None,
) -> Transaction:
    _assert_sufficient_cash(conn, abs(amount), f"pay property expense (asset {asset_id})")
    return create_transaction(conn, Transaction(
        date=date, txn_type="pay_property_expense", asset_id=asset_id,
        total_amount=-abs(amount), notes=notes,
    ))


def pay_mortgage(
    conn: sqlite3.Connection, date: str, asset_id: int,
    amount: float, principal: float | None = None, notes: str | None = None,
) -> Transaction:
    """Apply a mortgage payment.

    If `principal` is omitted and the property has a non-zero
    `mortgage_interest_rate`, the interest portion for one month is
    estimated as `mortgage_balance * rate / 12` and the principal is
    reduced by `amount - interest` (clamped to >= 0). When the rate is
    unknown or zero, the full payment is treated as principal — preserving
    the legacy behavior.
    """
    prop = get_property_by_asset(conn, asset_id)
    _assert_sufficient_cash(conn, abs(amount), f"pay mortgage (asset {asset_id})")
    if principal is not None:
        reduction = principal
    elif prop.mortgage_interest_rate and prop.mortgage_balance > 0:
        monthly_interest = prop.mortgage_balance * prop.mortgage_interest_rate / 12
        reduction = max(0.0, amount - monthly_interest)
    else:
        reduction = amount
    prop.mortgage_balance = max(0, prop.mortgage_balance - reduction)
    update_property(conn, prop)
    return create_transaction(conn, Transaction(
        date=date, txn_type="pay_mortgage", asset_id=asset_id,
        total_amount=-abs(amount), notes=notes,
    ))


def add_debt(
    conn: sqlite3.Connection, date: str, symbol: str, name: str,
    amount: float, interest_rate: float = 0.0, minimum_payment: float = 0.0,
    due_date: str | None = None, cash_received: bool = True,
    notes: str | None = None,
) -> tuple[Asset, Debt, Transaction]:
    """Register a new debt liability.

    `cash_received=True` (default, used by the Transactions UI): the loan
    proceeds appear as a +amount cash inflow, the debt appears as a
    +amount liability, net worth is unchanged. This is the normal
    "took out a loan" case.

    `cash_received=False` (API/CSV only): no cash movement is recorded,
    only the liability. This models existing debt whose proceeds were
    already spent before tracking began. Net worth drops by `amount`,
    which is the accurate accounting outcome — the user *does* owe more
    than they hold an offsetting asset for. To recognize the offsetting
    asset (e.g., a property bought before tracking started), record it
    as an existing-property entry separately.
    """
    asset = create_asset(conn, Asset(
        symbol=symbol, name=name, asset_type="debt",
    ))
    debt = create_debt(conn, Debt(
        asset_id=asset.id, name=name,
        original_amount=amount, current_balance=amount,
        interest_rate=interest_rate, minimum_payment=minimum_payment,
        due_date=due_date,
    ))
    cash_impact = amount if cash_received else 0.0
    txn = create_transaction(conn, Transaction(
        date=date, txn_type="add_debt", asset_id=asset.id,
        total_amount=cash_impact, notes=notes,
    ))
    return asset, debt, txn


def pay_debt(
    conn: sqlite3.Connection, date: str, asset_id: int,
    amount: float, principal_portion: float | None = None,
    notes: str | None = None,
) -> Transaction:
    """Apply a debt payment.

    If `principal_portion` is omitted and the debt has a non-zero
    interest rate, one month of interest is computed
    (`current_balance * rate / 12`) and the principal reduction is
    `amount - interest` (clamped). When rate is unknown or zero, the
    full payment is treated as principal — preserving legacy behavior.
    """
    debt = get_debt_by_asset(conn, asset_id)
    _assert_sufficient_cash(conn, abs(amount), f"pay debt (asset {asset_id})")
    if principal_portion is not None:
        reduction = principal_portion
    elif debt.interest_rate and debt.current_balance > 0:
        monthly_interest = debt.current_balance * debt.interest_rate / 12
        reduction = max(0.0, amount - monthly_interest)
    else:
        reduction = amount
    debt.current_balance = max(0, debt.current_balance - reduction)
    update_debt(conn, debt)
    return create_transaction(conn, Transaction(
        date=date, txn_type="pay_debt", asset_id=asset_id,
        total_amount=-abs(amount), notes=notes,
    ))


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
    if quantity is not None:
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

    net_proceeds = sale_price - prop.mortgage_balance - fees

    # If the mortgage payoff plus fees exceeds the sale price, the seller
    # has to bring cash to closing. Reject if they don't have it.
    if net_proceeds < 0:
        _assert_sufficient_cash(
            conn, -net_proceeds,
            f"settle mortgage on sale of {asset.symbol}",
        )

    txn = create_transaction(conn, Transaction(
        date=date, txn_type="sell_property", asset_id=asset_id,
        quantity=1, price=sale_price, total_amount=net_proceeds,
        fees=fees, notes=notes,
    ))

    prop.status = "sold"
    prop.sold_date = date
    prop.sold_price = sale_price
    prop.sale_fees = fees
    prop.current_value = 0
    prop.mortgage_balance = 0
    update_property(conn, prop)

    return txn


def settle_due_rent(
    conn: sqlite3.Connection, through_date: str,
    property_asset_id: int | None = None,
) -> list[Transaction]:
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

        if prop.rent_collection_frequency == "annual":
            if anchor.month == 1 and anchor.day == 1:
                d = anchor
            else:
                d = date_type(anchor.year + 1, 1, 1)
            while d <= stop:
                label = f"Scheduled rent {d.year}"
                if label not in existing_notes:
                    annual_rent = prop.monthly_rent * 12
                    txn = create_transaction(conn, Transaction(
                        date=d.isoformat(), txn_type="receive_rent",
                        asset_id=prop.asset_id, total_amount=annual_rent,
                        notes=label,
                    ))
                    created.append(txn)
                d = date_type(d.year + 1, 1, 1)
        else:
            if anchor.day == 1:
                d = anchor
            else:
                d = date_type(anchor.year, anchor.month, 1) + relativedelta(months=1)
            while d <= stop:
                label = f"Scheduled rent {d.strftime('%Y-%m')}"
                if label not in existing_notes:
                    txn = create_transaction(conn, Transaction(
                        date=d.isoformat(), txn_type="receive_rent",
                        asset_id=prop.asset_id, total_amount=prop.monthly_rent,
                        notes=label,
                    ))
                    created.append(txn)
                d += relativedelta(months=1)

    return created
