from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Debt:
    id: int | None = None
    asset_id: int = 0
    name: str = ""
    original_amount: float = 0.0
    current_balance: float = 0.0
    interest_rate: float = 0.0
    minimum_payment: float = 0.0
    due_date: str | None = None
    notes: str | None = None
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
