from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Transaction:
    id: int | None = None
    date: str = ""
    txn_type: str = ""
    asset_id: int | None = None
    quantity: float | None = None
    price: float | None = None
    total_amount: float = 0.0
    currency: str = "USD"
    fees: float = 0.0
    notes: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
