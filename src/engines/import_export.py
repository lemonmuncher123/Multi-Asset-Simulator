import csv
import io
import logging
import sqlite3
from dataclasses import dataclass
from src.models.asset import Asset
from src.models.transaction import Transaction
from src.storage.asset_repo import create_asset, list_assets, get_asset_by_symbol, get_asset
from src.storage.transaction_repo import create_transaction, list_transactions
from src.engines.portfolio import get_portfolio_summary, calc_cash_balance
from src.engines.holdings import SELLABLE_ASSET_TYPES, get_asset_quantity, _EPSILON

_log = logging.getLogger(__name__)


from src.models.asset_types import (
    ALL_ASSET_TYPES as VALID_ASSET_TYPES,
    VALID_LIQUIDITY,
)

VALID_TXN_TYPES = {
    "deposit_cash", "withdraw_cash", "buy", "sell",
    "add_property", "update_property_value", "receive_rent",
    "pay_property_expense", "pay_mortgage", "sell_property",
    "add_debt", "pay_debt", "manual_adjustment",
}

# Subset of VALID_TXN_TYPES that the simple-CSV importer can handle safely.
# The blocked types (add_property, update_property_value, pay_mortgage,
# sell_property, add_debt, pay_debt) all require coordinated writes to
# sibling tables (`debts`, `mortgages`, `properties`,
# `debt_payment_records`, `mortgage_payment_records`) and/or balance
# recomputation that the simple CSV format cannot express. Inserting
# those rows directly into `transactions` would leave the sibling tables
# out of sync. Users who need to restore those types should use the Full
# Data Import (full_data_io.py), which round-trips every table.
CSV_IMPORTABLE_TXN_TYPES = {
    "deposit_cash", "withdraw_cash", "buy", "sell",
    "receive_rent", "pay_property_expense", "manual_adjustment",
}

ASSET_CSV_HEADERS = ["symbol", "name", "asset_type", "currency", "region", "liquidity", "notes"]
TXN_CSV_HEADERS = [
    "date", "txn_type", "asset_symbol", "quantity", "price",
    "total_amount", "currency", "fees", "notes",
]


@dataclass
class ImportResult:
    imported: int = 0
    skipped: int = 0
    errors: list[str] | None = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


def export_assets_csv(conn: sqlite3.Connection) -> str:
    assets = list_assets(conn)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(ASSET_CSV_HEADERS)
    for a in assets:
        writer.writerow([
            a.symbol, a.name, a.asset_type, a.currency,
            a.region, a.liquidity, a.notes or "",
        ])
    return output.getvalue()


def export_transactions_csv(conn: sqlite3.Connection) -> str:
    txns = list_transactions(conn)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(TXN_CSV_HEADERS)
    for t in txns:
        asset = None
        if t.asset_id:
            from src.storage.asset_repo import get_asset
            asset = get_asset(conn, t.asset_id)
        symbol = asset.symbol if asset else ""
        writer.writerow([
            t.date, t.txn_type, symbol,
            t.quantity if t.quantity is not None else "",
            t.price if t.price is not None else "",
            t.total_amount, t.currency, t.fees, t.notes or "",
        ])
    return output.getvalue()


def export_summary_csv(conn: sqlite3.Connection) -> str:
    summary = get_portfolio_summary(conn)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["metric", "value"])
    for key in ["cash", "positions_value", "property_value", "total_assets",
                 "mortgage", "debt", "total_liabilities", "net_worth",
                 "real_estate_equity"]:
        writer.writerow([key, f"{summary[key]:.2f}"])

    positions = summary.get("positions", [])
    if positions:
        writer.writerow([])
        writer.writerow(["symbol", "name", "asset_type", "quantity",
                         "cost_basis", "market_value", "unrealized_pnl"])
        for p in positions:
            writer.writerow([
                p.symbol, p.name, p.asset_type, p.quantity,
                f"{p.cost_basis:.2f}",
                f"{p.market_value:.2f}" if p.market_value is not None else "",
                f"{p.unrealized_pnl:.2f}" if p.unrealized_pnl is not None else "",
            ])
    return output.getvalue()


def _validate_asset_row(row: dict, row_num: int) -> list[str]:
    # csv.DictReader fills short rows with None (no restval set), so use
    # `(row.get(...) or "")` rather than the `default` arg of `get` —
    # the default only kicks in for missing keys, not present-but-None.
    errors = []
    if not (row.get("symbol") or "").strip():
        errors.append(f"Row {row_num}: missing symbol")
    if not (row.get("name") or "").strip():
        errors.append(f"Row {row_num}: missing name")
    asset_type = (row.get("asset_type") or "").strip()
    if not asset_type:
        errors.append(f"Row {row_num}: missing asset_type")
    elif asset_type not in VALID_ASSET_TYPES:
        errors.append(f"Row {row_num}: invalid asset_type '{asset_type}'")
    liquidity = (row.get("liquidity") or "liquid").strip()
    if liquidity and liquidity not in VALID_LIQUIDITY:
        errors.append(f"Row {row_num}: invalid liquidity '{liquidity}'")
    return errors


def import_assets_csv(conn: sqlite3.Connection, csv_text: str) -> ImportResult:
    result = ImportResult()
    rows = list(csv.DictReader(io.StringIO(csv_text)))

    # Pass 1: validate, dedupe within CSV, skip pre-existing.
    valid: list[tuple[int, dict, str]] = []
    seen_symbols: set[str] = set()
    for i, row in enumerate(rows, start=2):
        row_errors = _validate_asset_row(row, i)
        if row_errors:
            result.errors.extend(row_errors)
            continue

        symbol = row["symbol"].strip()
        if symbol in seen_symbols:
            result.errors.append(f"Row {i}: duplicate symbol '{symbol}' in CSV")
            continue
        seen_symbols.add(symbol)

        existing = get_asset_by_symbol(conn, symbol)
        if existing:
            result.skipped += 1
            result.errors.append(f"Row {i}: asset '{symbol}' already exists, skipped")
            continue
        valid.append((i, row, symbol))

    if not valid:
        return result

    # Pass 2: atomic insertion.
    try:
        conn.execute("BEGIN")
        for _, row, symbol in valid:
            conn.execute(
                "INSERT INTO assets (symbol, name, asset_type, currency, region, liquidity, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    symbol,
                    row["name"].strip(),
                    row["asset_type"].strip(),
                    (row.get("currency") or "USD").strip() or "USD",
                    (row.get("region") or "US").strip() or "US",
                    (row.get("liquidity") or "liquid").strip() or "liquid",
                    (row.get("notes") or "").strip() or None,
                ),
            )
            result.imported += 1
        conn.execute("COMMIT")
    except Exception as e:
        _log.exception("CSV asset import failed; rolling back")
        try:
            conn.execute("ROLLBACK")
        except Exception:
            _log.exception("Rollback after CSV asset import failure also failed")
        result.imported = 0
        result.errors.append(f"Insert failed: {e}; all rows rolled back.")

    return result


# Cash-impact sign expected for each importable txn_type. The CSV's
# `total_amount` is the signed cash impact (the same convention the engine
# stores). Rows whose sign disagrees with the type would let an attacker
# (or a buggy export) write a "buy" with positive total_amount, inflating
# both cash and position simultaneously — straight violation of "transactions
# are the source of truth". `manual_adjustment` deliberately stays open so
# it can be used as the cash-correcting back-door it's documented to be.
_REQUIRED_SIGN: dict[str, str] = {
    "deposit_cash": "positive",
    "withdraw_cash": "negative",
    "buy": "negative",
    "sell": "positive",
    "receive_rent": "positive",
    "pay_property_expense": "negative",
}


def _validate_txn_row(row: dict, row_num: int, conn: sqlite3.Connection) -> list[str]:
    # See _validate_asset_row for why we coalesce None via `(get(...) or "")`.
    errors = []
    date = (row.get("date") or "").strip()
    if not date:
        errors.append(f"Row {row_num}: missing date")
    elif len(date) != 10 or date[4] != "-" or date[7] != "-":
        errors.append(f"Row {row_num}: invalid date format '{date}' (expected YYYY-MM-DD)")

    txn_type = (row.get("txn_type") or "").strip()
    if not txn_type:
        errors.append(f"Row {row_num}: missing txn_type")
    elif txn_type not in VALID_TXN_TYPES:
        errors.append(f"Row {row_num}: invalid txn_type '{txn_type}'")
    elif txn_type not in CSV_IMPORTABLE_TXN_TYPES:
        errors.append(
            f"Row {row_num}: txn_type '{txn_type}' cannot be imported via "
            "simple CSV — it requires sibling-table updates (debts, mortgages, "
            "properties, payment records). Use Import → Full Data instead."
        )

    total_str = (row.get("total_amount") or "").strip()
    total_value: float | None = None
    if not total_str:
        errors.append(f"Row {row_num}: missing total_amount")
    else:
        try:
            total_value = float(total_str)
        except ValueError:
            errors.append(f"Row {row_num}: invalid total_amount '{total_str}'")

    # Sign check: ledger.* enforces the cash-impact direction by construction
    # (e.g. ledger.buy stores `total_amount = -(qty*price + fees)`). The CSV
    # path bypasses ledger.* and writes total_amount verbatim, so the sign
    # rule has to be enforced here. `manual_adjustment` is exempt because it
    # is the documented escape hatch for cash corrections in either direction.
    if (
        txn_type in _REQUIRED_SIGN
        and total_value is not None
        and total_value != 0
    ):
        required = _REQUIRED_SIGN[txn_type]
        if required == "positive" and total_value < 0:
            errors.append(
                f"Row {row_num}: total_amount for '{txn_type}' must be "
                f"positive (cash inflow); got {total_value}."
            )
        elif required == "negative" and total_value > 0:
            errors.append(
                f"Row {row_num}: total_amount for '{txn_type}' must be "
                f"negative (cash outflow); got {total_value}."
            )

    asset_symbol = (row.get("asset_symbol") or "").strip()
    asset_obj = None
    if asset_symbol:
        asset_obj = get_asset_by_symbol(conn, asset_symbol)
        if not asset_obj:
            errors.append(f"Row {row_num}: asset '{asset_symbol}' not found")

    qty_str = (row.get("quantity") or "").strip()
    qty_value: float | None = None
    if qty_str:
        try:
            qty_value = float(qty_str)
        except ValueError:
            errors.append(f"Row {row_num}: invalid quantity '{qty_str}'")

    price_str = (row.get("price") or "").strip()
    price_value: float | None = None
    if price_str:
        try:
            price_value = float(price_str)
        except ValueError:
            errors.append(f"Row {row_num}: invalid price '{price_str}'")

    fees_str = (row.get("fees") or "").strip()
    if fees_str:
        try:
            fees_val = float(fees_str)
        except ValueError:
            errors.append(f"Row {row_num}: invalid fees '{fees_str}'")
        else:
            # Fees are always a positive cost; ledger.* rejects negative
            # `fees` arguments (`if fees < 0: raise`) at every public
            # write function. The CSV path bypasses ledger and inserts
            # directly, so the same rule has to be enforced here.
            if fees_val < 0:
                errors.append(
                    f"Row {row_num}: fees cannot be negative (got {fees_val})."
                )

    # buy / sell shape rules: ledger.buy and ledger.sell both reject
    # non-positive quantity and non-positive price. The CSV path inserts
    # directly into transactions, so without these checks a buy-with-zero-
    # price or buy-without-quantity would land in the table and produce a
    # misleading position. asset_id presence is part of the ledger contract.
    if txn_type in ("buy", "sell"):
        if asset_obj is None:
            errors.append(
                f"Row {row_num}: '{txn_type}' requires an asset_symbol."
            )
        if qty_value is None or qty_value <= 0:
            errors.append(
                f"Row {row_num}: '{txn_type}' requires a positive quantity."
            )
        if price_value is None or price_value <= 0:
            errors.append(
                f"Row {row_num}: '{txn_type}' requires a positive price."
            )

    # `receive_rent` and `pay_property_expense` are linked to a property
    # via asset_id at the engine layer — `ledger.receive_rent(asset_id, ...)`
    # and `ledger.pay_property_expense(asset_id, ...)` both require it.
    # The CSV path inserts directly, so without this check the row would
    # land with `asset_id IS NULL`, decoupling rent/expense from the
    # property and breaking the cashflow / RE-equity attributions.
    if txn_type in ("receive_rent", "pay_property_expense") and asset_obj is None:
        errors.append(
            f"Row {row_num}: '{txn_type}' requires an asset_symbol "
            f"identifying the property."
        )

    # manual_adjustment quantity rules mirror ledger.manual_adjustment: a
    # quantity-bearing row needs an asset_id, a positive price (so cost basis
    # stays defined), and a sellable asset type. Without these the row would
    # corrupt position SQL — calc_positions counts the qty but contributes 0
    # to total_cost_buy when price is NULL, producing a misleading $0
    # cost-basis position.
    if txn_type == "manual_adjustment" and qty_value is not None:
        if asset_obj is None:
            errors.append(
                f"Row {row_num}: manual_adjustment with a quantity requires "
                "an asset_symbol."
            )
        elif asset_obj.asset_type not in SELLABLE_ASSET_TYPES:
            errors.append(
                f"Row {row_num}: manual_adjustment cannot change quantity "
                f"for asset type '{asset_obj.asset_type}'."
            )
        if price_value is None or price_value <= 0:
            errors.append(
                f"Row {row_num}: manual_adjustment with a quantity requires "
                "a positive price so cost basis stays defined."
            )

    return errors


def import_transactions_csv(conn: sqlite3.Connection, csv_text: str) -> ImportResult:
    result = ImportResult()
    rows = list(csv.DictReader(io.StringIO(csv_text)))

    # Pass 1: per-row syntactic validation.
    parsed: list[tuple[int, dict]] = []
    for i, row in enumerate(rows, start=2):
        row_errors = _validate_txn_row(row, i, conn)
        if row_errors:
            result.errors.extend(row_errors)
            continue
        parsed.append((i, row))

    # Sort chronologically (ISO date strings sort lexicographically).
    # Stable sort preserves CSV order within the same date.
    parsed.sort(key=lambda item: item[1].get("date", "").strip())

    # Pass 2: semantic validation against running per-asset qty AND running
    # cash, both seeded from the DB's current state. The cash check mirrors
    # `ledger._assert_sufficient_cash`: we reject a row whose total_amount
    # would push the cumulative balance below zero, so a CSV can't sneak in
    # an overdraft that ledger.* would refuse.
    qty_by_asset: dict[int, float] = {}
    running_cash = calc_cash_balance(conn)

    def _running_qty(aid: int) -> float:
        if aid not in qty_by_asset:
            qty_by_asset[aid] = get_asset_quantity(conn, aid)
        return qty_by_asset[aid]

    valid: list[tuple[int, dict, int | None, float | None]] = []
    for i, row in parsed:
        txn_type = row["txn_type"].strip()
        asset_symbol = (row.get("asset_symbol") or "").strip()
        asset_id = None
        asset_obj = None
        if asset_symbol:
            asset_obj = get_asset_by_symbol(conn, asset_symbol)
            asset_id = asset_obj.id if asset_obj else None

        qty_str = (row.get("quantity") or "").strip()
        qty = float(qty_str) if qty_str else None

        if txn_type == "buy" and asset_id is not None:
            if asset_obj and asset_obj.asset_type not in SELLABLE_ASSET_TYPES:
                result.errors.append(
                    f"Row {i}: cannot buy asset type '{asset_obj.asset_type}' via CSV import"
                )
                continue

        if txn_type == "sell" and asset_id is not None and qty is not None:
            if asset_obj and asset_obj.asset_type not in SELLABLE_ASSET_TYPES:
                result.errors.append(
                    f"Row {i}: cannot sell asset type '{asset_obj.asset_type}'"
                )
                continue
            available = _running_qty(asset_id)
            if available - qty < -_EPSILON:
                if available <= _EPSILON:
                    result.errors.append(
                        f"Row {i}: cannot sell {asset_symbol}: no position held"
                    )
                else:
                    result.errors.append(
                        f"Row {i}: insufficient quantity for {asset_symbol}. "
                        f"Requested {qty}, available {available}"
                    )
                continue
            qty_by_asset[asset_id] = available - qty

        if txn_type == "buy" and asset_id is not None and qty is not None:
            qty_by_asset[asset_id] = _running_qty(asset_id) + qty

        # Cash sufficiency. The CSV's `total_amount` is the signed cash
        # impact (positive for inflow, negative for outflow). We track a
        # running balance and reject rows that would drive it negative,
        # matching `ledger._assert_sufficient_cash` for direct API calls.
        try:
            row_amount = float(row["total_amount"].strip())
        except (ValueError, AttributeError):
            row_amount = 0.0
        prospective_cash = running_cash + row_amount
        if prospective_cash + _EPSILON < 0:
            result.errors.append(
                f"Row {i}: insufficient cash for {txn_type}: "
                f"need {-row_amount:,.2f}, have {running_cash:,.2f}"
            )
            continue
        running_cash = prospective_cash

        valid.append((i, row, asset_id, qty))

    if not valid:
        return result

    # Pass 3: atomic insertion.
    try:
        conn.execute("BEGIN")
        for _, row, asset_id, qty in valid:
            price_str = (row.get("price") or "").strip()
            fees_str = (row.get("fees") or "").strip()
            conn.execute(
                "INSERT INTO transactions (date, txn_type, asset_id, quantity, price, "
                "total_amount, currency, fees, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row["date"].strip(),
                    row["txn_type"].strip(),
                    asset_id,
                    qty,
                    float(price_str) if price_str else None,
                    float(row["total_amount"].strip()),
                    row.get("currency", "USD").strip() or "USD",
                    float(fees_str) if fees_str else 0.0,
                    row.get("notes", "").strip() or None,
                ),
            )
            result.imported += 1
        conn.execute("COMMIT")
    except Exception as e:
        _log.exception("CSV transaction import failed; rolling back")
        try:
            conn.execute("ROLLBACK")
        except Exception:
            _log.exception("Rollback after CSV transaction import failure also failed")
        result.imported = 0
        result.errors.append(f"Insert failed: {e}; all rows rolled back.")

    return result
