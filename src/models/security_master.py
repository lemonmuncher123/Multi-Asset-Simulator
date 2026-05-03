from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class SecurityMasterRecord:
    id: int | None = None
    symbol: str = ""
    name: str = ""
    asset_type: str = ""
    exchange: str | None = None
    sector: str | None = None
    industry: str | None = None
    etf_category: str | None = None
    is_common_etf: bool = False
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
