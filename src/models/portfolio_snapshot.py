from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class PortfolioSnapshot:
    id: int | None = None
    date: str = ""
    cash: float = 0.0
    total_assets: float = 0.0
    total_liabilities: float = 0.0
    net_worth: float = 0.0
    allocation_json: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
