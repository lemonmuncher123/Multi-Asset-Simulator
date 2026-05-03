from src.data_sources.price_provider import PriceProvider, PriceRecord, QuoteRecord, ProviderUnavailableError
from src.utils.deps import yfinance_missing_message


class YFinanceProvider(PriceProvider):
    def source_name(self) -> str:
        return "yfinance"

    def fetch_latest_quote(self, symbol: str) -> QuoteRecord | None:
        try:
            import yfinance as yf
        except ImportError:
            raise ProviderUnavailableError(yfinance_missing_message())

        from datetime import datetime

        ticker = yf.Ticker(symbol)
        info = ticker.info

        bid = info.get("bid")
        ask = info.get("ask")
        last = info.get("regularMarketPrice") or info.get("currentPrice")

        if bid is None and ask is None and last is None:
            return None

        return QuoteRecord(
            symbol=symbol,
            bid=float(bid) if bid is not None else None,
            ask=float(ask) if ask is not None else None,
            last=float(last) if last is not None else None,
            timestamp=datetime.now().isoformat(),
            source=self.source_name(),
        )

    def fetch_daily_prices(
        self, symbol: str, start_date: str, end_date: str
    ) -> list[PriceRecord]:
        try:
            import yfinance as yf
        except ImportError:
            raise ProviderUnavailableError(yfinance_missing_message())

        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start_date, end=end_date, auto_adjust=False)

        if df.empty:
            raise ValueError(f"No price data returned for '{symbol}'")

        records = []
        for idx, row in df.iterrows():
            date_str = idx.strftime("%Y-%m-%d")
            records.append(PriceRecord(
                symbol=symbol,
                date=date_str,
                open=float(row.get("Open")) if row.get("Open") is not None else None,
                high=float(row.get("High")) if row.get("High") is not None else None,
                low=float(row.get("Low")) if row.get("Low") is not None else None,
                close=float(row.get("Close")) if row.get("Close") is not None else None,
                adjusted_close=float(row.get("Adj Close")) if row.get("Adj Close") is not None else None,
                volume=float(row.get("Volume")) if row.get("Volume") is not None else None,
                source=self.source_name(),
            ))
        return records
