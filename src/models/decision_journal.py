from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class DecisionJournalEntry:
    id: int | None = None
    transaction_id: int | None = None
    date: str = ""
    title: str = ""
    thesis: str | None = None
    intended_role: str | None = None
    risk_reasoning: str | None = None
    exit_plan: str | None = None
    confidence_level: int | None = None
    expected_holding_period: str | None = None
    pre_trade_notes: str | None = None
    post_trade_review: str | None = None
    mistake_tags: str | None = None
    lesson_learned: str | None = None
    snapshot_before: str | None = None
    snapshot_after: str | None = None
    # Legacy fields kept for backward compatibility
    reasoning: str | None = None
    expected: str | None = None
    actual: str | None = None
    score: int | None = None
    tags: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
