from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Mortgage:
    """Mortgage on a real estate property.

    Cloned from `Debt` so the shared `debt_math` helpers work
    polymorphically (they read attributes by name, not by type).
    Differences from `Debt`:
      - Links to a property via `property_id` instead of `asset_id` —
        mortgages do not have their own `Asset` row.
      - Monthly-only schedule (no `schedule_frequency` field). Yearly
        mortgages are not modeled per spec.
      - Interest is always annual (no `interest_period` field).

    The `schedule_frequency` attribute below is a constant property
    rather than a field so call sites that pass a Mortgage to
    `debt_math` (which reads `.schedule_frequency`) work without a
    branch.
    """
    id: int | None = None
    property_id: int = 0
    name: str = ""
    original_amount: float = 0.0
    current_balance: float = 0.0
    interest_rate: float = 0.0
    minimum_payment: float = 0.0
    due_date: str | None = None
    notes: str | None = None
    monthly_payment_amount: float = 0.0
    cashflow_start_date: str | None = None
    last_payment_date: str | None = None
    plan_type: str = "fixed_payment"
    original_term_periods: int | None = None
    preview_regular_payment: float = 0.0
    preview_period_count: int = 0
    preview_final_payment: float = 0.0
    preview_total_paid: float = 0.0
    preview_total_interest: float = 0.0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    @property
    def schedule_frequency(self) -> str:
        """Always 'monthly' — mortgages have no yearly variant. Lets
        shared `debt_math` helpers (which read `.schedule_frequency`
        on a debt-shaped object) work on Mortgage without branching."""
        return "monthly"
