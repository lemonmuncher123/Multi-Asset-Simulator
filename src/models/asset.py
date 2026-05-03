from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Asset:
    id: int | None = None
    symbol: str = ""
    name: str = ""
    asset_type: str = ""  # stock | etf | crypto | option | real_estate | cash | debt | custom
    currency: str = "USD"
    region: str = "US"  # US | EU | Asia | Global | Other
    liquidity: str = "liquid"  # liquid | illiquid
    notes: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
