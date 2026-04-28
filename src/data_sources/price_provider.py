from abc import ABC, abstractmethod
from dataclasses import dataclass


class ProviderUnavailableError(RuntimeError):
    pass


@dataclass
class PriceRecord:
    symbol: str
    date: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    adjusted_close: float | None = None
    volume: float | None = None
    source: str = ""


@dataclass
class QuoteRecord:
    symbol: str
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    timestamp: str = ""
    source: str = ""


class PriceProvider(ABC):
    @abstractmethod
    def fetch_daily_prices(
        self, symbol: str, start_date: str, end_date: str
    ) -> list[PriceRecord]:
        ...

    @abstractmethod
    def source_name(self) -> str:
        ...

    def fetch_latest_quote(self, symbol: str) -> QuoteRecord | None:
        return None
