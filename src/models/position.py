from dataclasses import dataclass


@dataclass
class Position:
    asset_id: int = 0
    symbol: str = ""
    name: str = ""
    asset_type: str = ""
    quantity: float = 0.0
    cost_basis: float = 0.0
    average_price: float = 0.0
    current_price: float | None = None
    market_value: float | None = None
    unrealized_pnl: float | None = None
    currency: str = "USD"

    def effective_value(self) -> float:
        """Market value when a recent price is available, otherwise cost basis."""
        return self.market_value if self.market_value is not None else self.cost_basis
