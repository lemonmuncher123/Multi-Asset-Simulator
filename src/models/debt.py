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
    schedule_frequency: str = "monthly"
    interest_period: str = "annual"
    monthly_payment_amount: float = 0.0
    cashflow_start_date: str | None = None
    last_payment_date: str | None = None
    # 'fixed_payment' (user supplied a per-period amount; engine derived
    # the term) or 'fixed_term' (user supplied a number of periods; engine
    # derived the per-period amount). Drives Pay Debt's
    # recompute-after-payment policy.
    plan_type: str = "fixed_payment"
    # Populated only when plan_type == 'fixed_term'. The original term
    # the user committed to at creation; lets recompute-after-payment
    # derive how many periods remain by counting scheduled pay_debt
    # transactions.
    original_term_periods: int | None = None
    # Spec §5: the 5 preview values are the live current official
    # payment plan, refreshed by `ledger._refresh_debt_preview_values`
    # on every Add Debt / pay_debt / pay_debt_in_full / scheduled
    # auto-pay event. Zero on a paid-off debt.
    preview_regular_payment: float = 0.0
    preview_period_count: int = 0
    preview_final_payment: float = 0.0
    preview_total_paid: float = 0.0
    preview_total_interest: float = 0.0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
