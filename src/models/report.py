from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Report:
    id: int | None = None
    report_type: str = ""
    period_start: str = ""
    period_end: str = ""
    period_label: str = ""
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    title: str = ""
    report_json: str = ""
    notes: str | None = None
    net_cash_flow: float = 0.0
    operating_net_income: float = 0.0
    transaction_count: int = 0
    net_worth_change: float | None = None
    funding_flow: float = 0.0
    approximate_return_pct: float | None = None
