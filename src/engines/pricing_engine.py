import math
import sqlite3
from datetime import date, timedelta

from src.data_sources.price_provider import PriceProvider, ProviderUnavailableError
from src.models.asset import Asset
from src.models.asset_types import SYNCABLE_ASSET_TYPES as SYNCABLE_TYPES
from src.storage.asset_repo import list_assets
from src.storage.price_repo import bulk_upsert_ohlcv, get_latest_price
from src.storage.quote_repo import upsert_quote
from src.storage.sync_log_repo import create_sync_log, finish_sync_log


DEFAULT_LOOKBACK_DAYS = 30


def _is_valid_price(value) -> bool:
    if value is None:
        return False
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    if math.isnan(v) or math.isinf(v):
        return False
    return v > 0


def get_provider(asset_type: str, providers: dict[str, PriceProvider] | None = None) -> PriceProvider | None:
    if providers and asset_type in providers:
        return providers[asset_type]
    if providers:
        return None
    if asset_type in ("stock", "etf"):
        from src.data_sources.stock_data import YFinanceProvider
        return YFinanceProvider()
    elif asset_type == "crypto":
        from src.data_sources.crypto_data import YFinanceCryptoProvider
        return YFinanceCryptoProvider()
    return None


def sync_asset_price(
    conn: sqlite3.Connection,
    asset: Asset,
    start_date: str | None = None,
    end_date: str | None = None,
    providers: dict[str, PriceProvider] | None = None,
) -> int:
    if asset.asset_type not in SYNCABLE_TYPES:
        return 0

    provider = get_provider(asset.asset_type, providers)
    if provider is None:
        raise RuntimeError(f"No provider for asset type '{asset.asset_type}'")

    if end_date is None:
        end_date = date.today().isoformat()
    if start_date is None:
        start_date = (date.fromisoformat(end_date) - timedelta(days=DEFAULT_LOOKBACK_DAYS)).isoformat()

    records = provider.fetch_daily_prices(asset.symbol, start_date, end_date)

    rows = [
        {
            "asset_id": asset.id,
            "symbol": asset.symbol,
            "asset_type": asset.asset_type,
            "date": r.date,
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close,
            "adjusted_close": r.adjusted_close,
            "volume": r.volume,
            "source": r.source,
        }
        for r in records
    ]

    return bulk_upsert_ohlcv(conn, rows)


def sync_asset_quote(
    conn: sqlite3.Connection,
    asset: Asset,
    providers: dict[str, PriceProvider] | None = None,
) -> bool:
    if asset.asset_type not in SYNCABLE_TYPES:
        return False

    provider = get_provider(asset.asset_type, providers)
    if provider is None:
        raise RuntimeError(f"No provider for asset type '{asset.asset_type}'")

    quote = provider.fetch_latest_quote(asset.symbol)
    if quote is None:
        return False

    bid = quote.bid if _is_valid_price(quote.bid) else None
    ask = quote.ask if _is_valid_price(quote.ask) else None
    last = quote.last if _is_valid_price(quote.last) else None

    upsert_quote(
        conn, asset.id, asset.symbol, asset.asset_type,
        bid, ask, last, quote.timestamp, quote.source,
    )

    return bid is not None and ask is not None


def sync_asset_market_data(
    conn: sqlite3.Connection,
    asset: Asset,
    start_date: str | None = None,
    end_date: str | None = None,
    providers: dict[str, PriceProvider] | None = None,
) -> dict:
    price_ok = False
    quote_ok = False
    errors: list[str] = []

    try:
        sync_asset_price(conn, asset, start_date, end_date, providers)
        price_ok = True
    except Exception as e:
        errors.append(str(e))

    try:
        quote_ok = sync_asset_quote(conn, asset, providers)
    except Exception as e:
        errors.append(f"Quote: {e}")

    return {
        "price_synced": price_ok,
        "quote_synced": quote_ok,
        "errors": errors,
    }


def sync_all_market_assets(
    conn: sqlite3.Connection,
    start_date: str | None = None,
    end_date: str | None = None,
    providers: dict[str, PriceProvider] | None = None,
    cancelled: callable = None,
) -> dict:
    all_assets = list_assets(conn)
    syncable = [a for a in all_assets if a.asset_type in SYNCABLE_TYPES]

    source_name = "mixed"
    if providers:
        sources = set()
        for p in providers.values():
            sources.add(p.source_name())
        if len(sources) == 1:
            source_name = sources.pop()

    yfinance_blocked = False
    if providers is None and syncable:
        from src.utils.deps import is_yfinance_available
        if not is_yfinance_available():
            yfinance_blocked = True

    log_id = create_sync_log(conn, source=source_name)

    attempted = 0
    succeeded = 0
    failed = 0
    errors = []

    if yfinance_blocked:
        from src.utils.deps import yfinance_missing_message
        attempted = len(syncable)
        failed = len(syncable)
        errors.append(yfinance_missing_message())
    else:
        for asset in syncable:
            if cancelled and cancelled():
                break
            attempted += 1
            try:
                result = sync_asset_market_data(conn, asset, start_date, end_date, providers)
                if result["quote_synced"]:
                    succeeded += 1
                elif result["price_synced"]:
                    failed += 1
                    errors.append(
                        f"{asset.symbol}: No executable quote (missing bid/ask). "
                        "Daily prices synced for valuation."
                    )
                else:
                    failed += 1
                    for e in result["errors"]:
                        errors.append(f"{asset.symbol}: {e}")
            except Exception as e:
                failed += 1
                errors.append(f"{asset.symbol}: {e}")

    status = "success"
    if failed > 0 and succeeded > 0:
        status = "partial"
    elif failed > 0 and succeeded == 0:
        status = "failed"

    error_msg = "\n".join(errors) if errors else None
    finish_sync_log(conn, log_id, status, attempted, succeeded, failed, error_msg)

    return {
        "log_id": log_id,
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": failed,
        "errors": errors,
        "status": status,
    }


def get_latest_market_price(conn: sqlite3.Connection, asset_id: int) -> float | None:
    return get_latest_price(conn, asset_id)
