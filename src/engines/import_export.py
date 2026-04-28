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
    errors = []
    if not row.get("symbol", "").strip():
        errors.append(f"Row {row_num}: missing symbol")
    if not row.get("name", "").strip():
        errors.append(f"Row {row_num}: missing name")
    asset_type = row.get("asset_type", "").strip()
    if not asset_type:
        errors.append(f"Row {row_num}: missing asset_type")
    elif asset_type not in VALID_ASSET_TYPES:
        errors.append(f"Row {row_num}: invalid asset_type '{asset_type}'")
    liquidity = row.get("liquidity", "liquid").strip()
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
                    row.get("currency", "USD").strip() or "USD",
                    row.get("region", "US").strip() or "US",
                    row.get("liquidity", "liquid").strip() or "liquid",
                    row.get("notes", "").strip() or None,
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


def _validate_txn_row(row: dict, row_num: int, conn: sqlite3.Connection) -> list[str]:
    errors = []
    date = row.get("date", "").strip()
    if not date:
        errors.append(f"Row {row_num}: missing date")
    elif len(date) != 10 or date[4] != "-" or date[7] != "-":
        errors.append(f"Row {row_num}: invalid date format '{date}' (expected YYYY-MM-DD)")

    txn_type = row.get("txn_type", "").strip()
    if not txn_type:
        errors.append(f"Row {row_num}: missing txn_type")
    elif txn_type not in VALID_TXN_TYPES:
        errors.append(f"Row {row_num}: invalid txn_type '{txn_type}'")

    total_str = row.get("total_amount", "").strip()
    if not total_str:
        errors.append(f"Row {row_num}: missing total_amount")
    else:
        try:
            float(total_str)
        except ValueError:
            errors.append(f"Row {row_num}: invalid total_amount '{total_str}'")

    asset_symbol = row.get("asset_symbol", "").strip()
    if asset_symbol:
        asset = get_asset_by_symbol(conn, asset_symbol)
        if not asset:
            errors.append(f"Row {row_num}: asset '{asset_symbol}' not found")

    qty_str = row.get("quantity", "").strip()
    if qty_str:
        try:
            float(qty_str)
        except ValueError:
            errors.append(f"Row {row_num}: invalid quantity '{qty_str}'")

    price_str = row.get("price", "").strip()
    if price_str:
        try:
            float(price_str)
        except ValueError:
            errors.append(f"Row {row_num}: invalid price '{price_str}'")

    fees_str = row.get("fees", "").strip()
    if fees_str:
        try:
            float(fees_str)
        except ValueError:
            errors.append(f"Row {row_num}: invalid fees '{fees_str}'")

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
        asset_symbol = row.get("asset_symbol", "").strip()
        asset_id = None
        asset_obj = None
        if asset_symbol:
            asset_obj = get_asset_by_symbol(conn, asset_symbol)
            asset_id = asset_obj.id if asset_obj else None

        qty_str = row.get("quantity", "").strip()
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
            price_str = row.get("price", "").strip()
            fees_str = row.get("fees", "").strip()
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
