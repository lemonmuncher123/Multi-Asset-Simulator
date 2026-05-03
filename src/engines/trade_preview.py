import logging
import math
import sqlite3
from dataclasses import dataclass, field

from src.data_sources.price_provider import PriceProvider, QuoteRecord

_log = logging.getLogger(__name__)
from src.models.asset import Asset
from src.storage.asset_repo import get_asset
from src.storage.price_repo import get_latest_price_record
from src.storage.quote_repo import upsert_quote, get_latest_quote_record
from src.storage.fee_breakdown_repo import FeeBreakdownRow, create_fee_breakdown
from src.engines.pricing_engine import sync_asset_price, SYNCABLE_TYPES, get_provider
from src.engines.portfolio import calc_cash_balance, calc_total_assets, calc_net_worth
from src.engines.allocation import calc_allocation_by_asset_type
from src.engines.risk import get_all_warnings
from src.engines.holdings import SELLABLE_ASSET_TYPES, get_asset_quantity, _EPSILON
from src.engines.trading_costs import compute_trading_costs, FeeItem
from src.engines import ledger


@dataclass
class TradeDraft:
    action: str = ""
    asset_id: int = 0
    quantity: float = 0.0
    manual_price: float | None = None
    fee: float = 0.0
    note: str | None = None
    target_amount: float | None = None


@dataclass
class TradePreview:
    action: str = ""
    asset_id: int = 0
    symbol: str = ""
    asset_type: str = ""
    quantity: float = 0.0
    trade_price: float = 0.0
    price_source: str = ""
    price_date: str | None = None
    execution_side: str = ""
    bid_price: float | None = None
    ask_price: float | None = None
    last_price: float | None = None
    quote_time: str | None = None
    quote_source: str = ""
    estimated_trade_value: float = 0.0
    fee: float = 0.0
    additional_fee: float = 0.0
    fee_breakdown: list[FeeItem] = field(default_factory=list)
    cash_before: float = 0.0
    cash_after: float = 0.0
    total_assets_before: float = 0.0
    total_assets_after: float = 0.0
    net_worth_before: float = 0.0
    net_worth_after: float = 0.0
    allocation_before: dict = field(default_factory=dict)
    allocation_after: dict = field(default_factory=dict)
    risk_warnings_before: list = field(default_factory=list)
    risk_warnings_after: list = field(default_factory=list)
    risk_changes_summary: list[str] = field(default_factory=list)
    note: str | None = None
    can_confirm: bool = False
    blocking_errors: list[str] = field(default_factory=list)
    target_amount: float | None = None
    quantity_source: str = "quantity"
    uninvested_amount: float = 0.0
    simulation_failed: bool = False


def prepare_trade_preview(
    conn: sqlite3.Connection,
    draft: TradeDraft,
    date: str = "",
    providers: dict[str, PriceProvider] | None = None,
) -> TradePreview:
    asset = get_asset(conn, draft.asset_id)
    if asset is None:
        preview = TradePreview()
        preview.blocking_errors.append("Asset not found")
        return preview

    is_amount_mode = draft.target_amount is not None

    preview = TradePreview(
        action=draft.action,
        asset_id=asset.id,
        symbol=asset.symbol,
        asset_type=asset.asset_type,
        quantity=draft.quantity,
        additional_fee=draft.fee,
        note=draft.note,
    )

    if draft.action == "sell":
        if not is_amount_mode and draft.quantity <= 0:
            preview.blocking_errors.append("Sell quantity must be positive.")
        elif asset.asset_type not in SELLABLE_ASSET_TYPES:
            preview.blocking_errors.append(
                f"Cannot sell asset type '{asset.asset_type}' via sell."
            )
        else:
            available = get_asset_quantity(conn, asset.id, as_of_date=date or None)
            if available <= _EPSILON:
                preview.blocking_errors.append(
                    f"Cannot sell {asset.symbol}: no position is currently held."
                )
            elif not is_amount_mode and available - draft.quantity < -_EPSILON:
                preview.blocking_errors.append(
                    f"Insufficient quantity. Requested {draft.quantity}, available {available}."
                )

    if preview.blocking_errors:
        preview.can_confirm = False
        return preview

    _resolve_price(conn, asset, draft, preview, providers)

    if preview.trade_price <= 0 and not preview.blocking_errors:
        if asset.asset_type in SYNCABLE_TYPES:
            preview.blocking_errors.append(
                "No executable quote available. Run Data Sync to fetch latest market quotes."
            )
        else:
            preview.blocking_errors.append("No price available. Enter a manual price.")

    if is_amount_mode and not preview.blocking_errors:
        if draft.target_amount <= 0:
            preview.blocking_errors.append("Target amount must be positive.")
        else:
            # Stock/ETF round down to whole shares. Crypto and custom
            # accept 8-decimal fractional units (matching force_sell's
            # quantization rules so the two paths agree on what's
            # tradeable). The previous unconditional `math.floor` left
            # crypto target_amount mode broken — a $1000 buy at $30k/BTC
            # would derive 0 units and block the trade.
            raw_qty = draft.target_amount / preview.trade_price
            if asset.asset_type in ("stock", "etf"):
                derived_qty = math.floor(raw_qty)
            else:
                derived_qty = math.floor(raw_qty * 1e8) / 1e8
            if derived_qty <= 0:
                preview.blocking_errors.append(
                    "Trade amount is too small to buy/sell at least 1 unit at the execution price."
                )
            else:
                preview.quantity = derived_qty
                preview.target_amount = draft.target_amount
                preview.quantity_source = "amount"
                preview.uninvested_amount = draft.target_amount - derived_qty * preview.trade_price

                if draft.action == "sell":
                    available = get_asset_quantity(conn, asset.id, as_of_date=date or None)
                    if available - preview.quantity < -_EPSILON:
                        preview.blocking_errors.append(
                            f"Insufficient quantity. Requested {preview.quantity}, available {available}."
                        )

    if not preview.blocking_errors:
        preview.estimated_trade_value = preview.quantity * preview.trade_price

        trade_year = _parse_trade_year(date)
        cost_result = compute_trading_costs(
            conn,
            action=draft.action,
            asset_type=asset.asset_type,
            quantity=preview.quantity,
            trade_value=preview.estimated_trade_value,
            trade_year=trade_year,
            additional_fee=preview.additional_fee,
        )
        preview.fee = cost_result.total
        preview.fee_breakdown = cost_result.items

        preview.cash_before = calc_cash_balance(conn)
        preview.total_assets_before = calc_total_assets(conn)
        preview.net_worth_before = calc_net_worth(conn)
        preview.allocation_before = calc_allocation_by_asset_type(conn)
        preview.risk_warnings_before = [
            w.message for w in get_all_warnings(conn)
        ]

        # After-state totals are computed against the simulated DB in
        # _simulate_after_state; default them to before-state here so the
        # blocked-trade path (which short-circuits the simulation) still has
        # sensible values.
        preview.total_assets_after = preview.total_assets_before
        preview.net_worth_after = preview.net_worth_before
        if draft.action == "buy":
            trade_total = preview.estimated_trade_value + preview.fee
            preview.cash_after = preview.cash_before - trade_total
        elif draft.action == "sell":
            trade_total = preview.estimated_trade_value - preview.fee
            preview.cash_after = preview.cash_before + trade_total

        if draft.action == "buy" and preview.cash_after < 0:
            preview.blocking_errors.append(
                f"Insufficient cash. Need {preview.estimated_trade_value + preview.fee:,.2f}, "
                f"have {preview.cash_before:,.2f}."
            )

        _simulate_after_state(conn, draft, preview, date)

    preview.can_confirm = len(preview.blocking_errors) == 0
    return preview


def _resolve_price(
    conn: sqlite3.Connection,
    asset: Asset,
    draft: TradeDraft,
    preview: TradePreview,
    providers: dict[str, PriceProvider] | None,
) -> None:
    if asset.asset_type not in SYNCABLE_TYPES:
        _resolve_non_syncable_price(conn, asset, draft, preview)
        return
    _resolve_syncable_price(conn, asset, draft, preview, providers)


def _resolve_non_syncable_price(
    conn: sqlite3.Connection,
    asset: Asset,
    draft: TradeDraft,
    preview: TradePreview,
) -> None:
    if draft.manual_price is not None and draft.manual_price > 0:
        preview.trade_price = draft.manual_price
        preview.price_source = "manual"
        return

    record = get_latest_price_record(conn, asset.id)
    if record:
        preview.trade_price = record["price"]
        preview.price_source = "last_available"
        preview.price_date = record["date"]
        return

    preview.price_source = "missing"
    preview.trade_price = 0.0


def _resolve_syncable_price(
    conn: sqlite3.Connection,
    asset: Asset,
    draft: TradeDraft,
    preview: TradePreview,
    providers: dict[str, PriceProvider] | None,
) -> None:
    # Sync daily prices for portfolio valuation
    try:
        sync_asset_price(conn, asset, providers=providers)
    except Exception:
        _log.exception("sync_asset_price failed for %s during trade preview", asset.symbol)

    # Try live quote from provider
    quote = _fetch_live_quote(asset, providers)
    if quote:
        try:
            upsert_quote(
                conn, asset.id, asset.symbol, asset.asset_type,
                quote.bid, quote.ask, quote.last, quote.timestamp, quote.source,
            )
        except Exception:
            _log.exception("upsert_quote failed for %s during trade preview", asset.symbol)

    # Fall back to stored quote
    if quote is None:
        stored = get_latest_quote_record(conn, asset.id)
        if stored:
            quote = QuoteRecord(
                symbol=asset.symbol,
                bid=stored["bid"],
                ask=stored["ask"],
                last=stored["last"],
                timestamp=stored.get("timestamp", ""),
                source=stored.get("source", "stored"),
            )

    if quote:
        preview.bid_price = quote.bid
        preview.ask_price = quote.ask
        preview.last_price = quote.last
        preview.quote_time = quote.timestamp
        preview.quote_source = quote.source

        exec_price = _pick_execution_price(draft.action, quote)
        if exec_price and exec_price > 0:
            preview.trade_price = exec_price
            if draft.action == "buy":
                preview.price_source = "quote_ask"
                preview.execution_side = "ask"
            else:
                preview.price_source = "quote_bid"
                preview.execution_side = "bid"
            return

        side = "ask" if draft.action == "buy" else "bid"
        preview.blocking_errors.append(
            f"Quote available but {side} price is missing. Cannot determine execution price."
        )
        preview.price_source = "missing"
        preview.trade_price = 0.0
        return

    preview.price_source = "missing"
    preview.trade_price = 0.0
    preview.blocking_errors.append(
        "No executable quote available. Run Data Sync to fetch latest market quotes."
    )


def _fetch_live_quote(
    asset: Asset,
    providers: dict[str, PriceProvider] | None,
) -> QuoteRecord | None:
    try:
        provider = get_provider(asset.asset_type, providers)
        if provider:
            return provider.fetch_latest_quote(asset.symbol)
    except Exception:
        _log.exception("fetch_latest_quote failed for %s", asset.symbol)
    return None


def _pick_execution_price(action: str, quote: QuoteRecord) -> float | None:
    if action == "buy":
        return quote.ask
    elif action == "sell":
        return quote.bid
    return None


def _simulate_after_state(
    conn: sqlite3.Connection,
    draft: TradeDraft,
    preview: TradePreview,
    date: str,
) -> None:
    # If the trade is already blocked (e.g. insufficient cash), the
    # underlying ledger function would refuse the simulated buy/sell as
    # well. Skip the simulation and reuse before-state — the user's main
    # blocker message already explains why.
    if preview.blocking_errors:
        preview.allocation_after = preview.allocation_before
        preview.risk_warnings_after = preview.risk_warnings_before
        return
    try:
        from src.storage.database import init_db

        sim = init_db(":memory:")

        for row in conn.execute("SELECT * FROM assets").fetchall():
            sim.execute(
                "INSERT INTO assets (id, symbol, name, asset_type, currency, region, liquidity, notes, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (row["id"], row["symbol"], row["name"], row["asset_type"],
                 row["currency"], row["region"], row["liquidity"], row["notes"], row["created_at"]),
            )

        for row in conn.execute("SELECT * FROM transactions").fetchall():
            sim.execute(
                "INSERT INTO transactions (id, date, txn_type, asset_id, quantity, price, total_amount, currency, fees, notes, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (row["id"], row["date"], row["txn_type"], row["asset_id"],
                 row["quantity"], row["price"], row["total_amount"], row["currency"],
                 row["fees"], row["notes"], row["created_at"]),
            )

        for row in conn.execute("SELECT * FROM market_prices").fetchall():
            sim.execute(
                "INSERT INTO market_prices (id, asset_id, symbol, asset_type, date, open, high, low, close, adjusted_close, volume, price, source, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (row["id"], row["asset_id"], row["symbol"], row["asset_type"],
                 row["date"], row["open"], row["high"], row["low"], row["close"],
                 row["adjusted_close"], row["volume"], row["price"], row["source"], row["created_at"]),
            )

        # Tables that risk/allocation engines read. `settings` is the
        # critical one: get_threshold() falls back to defaults if missing,
        # which makes the preview's "Risk After" panel use package defaults
        # instead of the user's customized thresholds. `decision_journal`
        # is needed so `check_missing_journal` sees the user's existing
        # journal entries — without it, every preview would report all
        # trades as unjournaled and the after-state would always show a
        # "RESOLVED/NEW" pair with different counts.
        for table in ("properties", "debts", "mortgages", "settings", "decision_journal"):
            cols_info = conn.execute(f"PRAGMA table_info({table})").fetchall()
            col_names = [c[1] for c in cols_info]
            for row in conn.execute(f"SELECT * FROM {table}").fetchall():
                placeholders = ", ".join("?" * len(col_names))
                col_list = ", ".join(col_names)
                sim.execute(
                    f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})",
                    tuple(row[c] for c in col_names),
                )

        sim.commit()

        trade_date = date or "2099-01-01"
        if draft.action == "buy":
            ledger.buy(sim, trade_date, draft.asset_id, preview.quantity,
                       preview.trade_price, fees=preview.fee, notes=draft.note)
        elif draft.action == "sell":
            ledger.sell(sim, trade_date, draft.asset_id, preview.quantity,
                        preview.trade_price, fees=preview.fee, notes=draft.note)

        preview.allocation_after = calc_allocation_by_asset_type(sim)
        preview.risk_warnings_after = [
            w.message for w in get_all_warnings(sim)
        ]
        preview.total_assets_after = calc_total_assets(sim)
        preview.net_worth_after = calc_net_worth(sim)

        _build_risk_changes(preview)
        sim.close()

    except Exception:
        _log.exception("Trade simulation failed; falling back to before-state")
        preview.allocation_after = preview.allocation_before
        preview.risk_warnings_after = preview.risk_warnings_before
        preview.simulation_failed = True


def _build_risk_changes(preview: TradePreview) -> None:
    before_set = set(preview.risk_warnings_before)
    after_set = set(preview.risk_warnings_after)

    new_warnings = after_set - before_set
    resolved = before_set - after_set

    for w in sorted(new_warnings):
        preview.risk_changes_summary.append(f"NEW: {w}")
    for w in sorted(resolved):
        preview.risk_changes_summary.append(f"RESOLVED: {w}")


def _parse_trade_year(date: str) -> int:
    if date and len(date) >= 4:
        try:
            return int(date[:4])
        except ValueError:
            pass
    from datetime import date as date_cls
    return date_cls.today().year


def confirm_trade(
    conn: sqlite3.Connection,
    preview: TradePreview,
    date: str,
) -> bool:
    """Execute the previewed trade.

    Returns ``True`` on success, ``False`` on any rejection. The engine's
    write functions (``ledger.buy`` / ``ledger.sell``) re-validate cash
    and quantity at write time, which closes the preview→confirm window
    where an auto-deduction might have moved the balance.
    """
    if not preview.can_confirm:
        return False

    try:
        if preview.action == "buy":
            txn = ledger.buy(conn, date, preview.asset_id, preview.quantity,
                             preview.trade_price, fees=preview.fee, notes=preview.note)
        elif preview.action == "sell":
            txn = ledger.sell(conn, date, preview.asset_id, preview.quantity,
                              preview.trade_price, fees=preview.fee, notes=preview.note)
        else:
            return False
    except ValueError:
        return False

    if txn and txn.id and preview.fee_breakdown:
        for item in preview.fee_breakdown:
            create_fee_breakdown(conn, FeeBreakdownRow(
                transaction_id=txn.id,
                fee_type=item.fee_type,
                amount=item.amount,
                rate=item.rate,
                notes=item.notes,
            ))

    return True
