"""Pure amortization math for debts.

Given a principal, an annual interest rate, and a schedule (monthly or
yearly), the user fixes EXACTLY ONE of:
  - the per-period payment, OR
  - the number of periods to take to pay it off
and this module computes the other plus the total paid, interest, and
final-period payment (which may differ slightly from the standard payment
to leave the balance at exactly zero).

The simulator stores rates as annualized decimals; this module derives the
periodic rate internally (monthly = annual/12, yearly = annual).
"""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class DebtSchedule:
    principal: float
    annual_rate: float
    schedule: str
    per_period_payment: float
    num_periods: int
    final_payment: float
    total_paid: float
    total_interest: float
    feasible: bool
    infeasibility_reason: str | None = None


def _periodic_rate(annual_rate: float, schedule: str) -> float:
    if schedule == "monthly":
        return annual_rate / 12.0
    if schedule == "yearly":
        return annual_rate
    raise ValueError(f"Invalid schedule: {schedule!r}")


def period_interest(balance: float, annual_rate: float, schedule: str) -> float:
    """Interest accrued on `balance` for one period of the given schedule.

    Canonical helper for the "balance × rate / 12" (monthly) and
    "balance × rate" (yearly) calculation that ledger code repeats. Use
    this anywhere a single period's interest charge is needed — partial
    payments, payoff amounts, mortgage interest splits.
    """
    return _periodic_rate(annual_rate, schedule) * balance


def _infeasible(
    principal: float, annual_rate: float, schedule: str, reason: str,
) -> DebtSchedule:
    return DebtSchedule(
        principal=principal, annual_rate=annual_rate, schedule=schedule,
        per_period_payment=0.0, num_periods=0, final_payment=0.0,
        total_paid=0.0, total_interest=0.0,
        feasible=False, infeasibility_reason=reason,
    )


def _amortize(
    principal: float, periodic_rate: float, payment: float,
    max_periods: int = 10_000,
) -> tuple[int, float, float]:
    """Walk an amortization schedule period-by-period.

    Returns (num_periods, total_paid, final_payment). Caps at
    `max_periods` to avoid pathological loops; the caller has already
    rejected payments that don't cover interest.
    """
    balance = principal
    total = 0.0
    n = 0
    final = payment
    while balance > 1e-9 and n < max_periods:
        interest = balance * periodic_rate
        principal_part = payment - interest
        if balance - principal_part <= 1e-9:
            # Last period: pay only what's owed plus this period's interest.
            final = balance + interest
            total += final
            balance = 0.0
            n += 1
            break
        balance -= principal_part
        total += payment
        n += 1
    return n, total, final


def compute_debt_schedule(
    principal: float,
    annual_rate: float,
    schedule: str,
    *,
    payment: float | None = None,
    term_periods: int | None = None,
) -> DebtSchedule:
    """Compute a debt repayment schedule.

    Provide exactly one of `payment` (per-period) or `term_periods`. The
    other is derived from the standard annuity formula and then verified
    by walking an amortization schedule so the totals are exact.
    """
    if (payment is None) == (term_periods is None):
        raise ValueError(
            "Provide exactly one of `payment` or `term_periods`."
        )
    if schedule not in ("monthly", "yearly"):
        raise ValueError(f"Invalid schedule: {schedule!r}")
    if principal <= 0:
        return _infeasible(principal, annual_rate, schedule,
                           "Principal must be positive.")
    if annual_rate < 0:
        return _infeasible(principal, annual_rate, schedule,
                           "Annual rate cannot be negative.")

    r = _periodic_rate(annual_rate, schedule)

    if payment is not None:
        if payment <= 0:
            return _infeasible(principal, annual_rate, schedule,
                               "Payment must be positive.")
        if r > 0 and payment <= r * principal + 1e-9:
            return _infeasible(
                principal, annual_rate, schedule,
                f"Payment ({payment:.2f}) does not cover the periodic "
                f"interest ({r * principal:.2f}); the debt would grow.",
            )
        n, total_paid, final = _amortize(principal, r, payment)
        return DebtSchedule(
            principal=principal, annual_rate=annual_rate, schedule=schedule,
            per_period_payment=payment, num_periods=n, final_payment=final,
            total_paid=total_paid, total_interest=total_paid - principal,
            feasible=True,
        )

    # term_periods branch
    n_req = int(term_periods)  # type: ignore[arg-type]
    if n_req <= 0:
        return _infeasible(principal, annual_rate, schedule,
                           "Term must be at least one period.")
    if r == 0:
        payment = principal / n_req
    else:
        # Annuity payment formula: P = L * r * (1+r)^n / ((1+r)^n - 1)
        factor = (1 + r) ** n_req
        payment = principal * r * factor / (factor - 1)
    n, total_paid, final = _amortize(principal, r, payment)
    return DebtSchedule(
        principal=principal, annual_rate=annual_rate, schedule=schedule,
        per_period_payment=payment, num_periods=n, final_payment=final,
        total_paid=total_paid, total_interest=total_paid - principal,
        feasible=True,
    )


def normalize_period_to_months(num_periods: int, schedule: str) -> int:
    """Express a period count in months for cross-schedule comparisons."""
    if schedule == "monthly":
        return num_periods
    if schedule == "yearly":
        return num_periods * 12
    raise ValueError(f"Invalid schedule: {schedule!r}")


# Sentinel preview returned when the debt is paid off (or about to be).
# The caller writes these zeros to the 5 preview columns.
_PAID_OFF_PREVIEW = {
    "regular_payment": 0.0,
    "period_count": 0,
    "final_payment": 0.0,
    "total_paid": 0.0,
    "total_interest": 0.0,
}


def compute_preview_values(
    *,
    current_balance: float,
    annual_rate: float,
    schedule: str,
    plan_type: str,
    monthly_payment_amount: float,
    original_term_periods: int | None,
    scheduled_payments_so_far: int,
) -> dict | None:
    """Compute the 5 preview values that mirror the live debt plan.

    Returns a dict with ``regular_payment``, ``period_count``,
    ``final_payment``, ``total_paid``, ``total_interest`` — the spec §5
    "preview" set, computed for the debt's *current* state.

    For a paid-off debt (balance ≤ 0), all five values are zero. For a
    `fixed_payment` debt, the per-period amount is held constant and
    `compute_debt_schedule` derives the rest from the current balance.
    For a `fixed_term` debt, the remaining term is
    ``original_term_periods - scheduled_payments_so_far`` (clamped at
    1) and `compute_debt_schedule` derives the per-period amount.

    Returns ``None`` when the schedule is infeasible (e.g. a
    fixed_payment debt whose payment no longer covers the periodic
    interest after a balance change). The caller leaves the previously
    stored preview values in place.

    Pure function — no DB access. The caller computes
    ``scheduled_payments_so_far`` (typically via
    ``ledger.count_scheduled_debt_payments``).
    """
    if current_balance <= 0:
        return dict(_PAID_OFF_PREVIEW)
    if plan_type == "fixed_term" and original_term_periods:
        remaining = max(
            1, int(original_term_periods) - int(scheduled_payments_so_far),
        )
        sched = compute_debt_schedule(
            principal=current_balance, annual_rate=annual_rate,
            schedule=schedule, term_periods=remaining,
        )
    else:
        if monthly_payment_amount <= 0:
            return None
        sched = compute_debt_schedule(
            principal=current_balance, annual_rate=annual_rate,
            schedule=schedule, payment=monthly_payment_amount,
        )
    if not sched.feasible:
        return None
    return {
        "regular_payment": sched.per_period_payment,
        "period_count": sched.num_periods,
        "final_payment": sched.final_payment,
        "total_paid": sched.total_paid,
        "total_interest": sched.total_interest,
    }


# Sentinel returned when a manual payment fully clears the debt. The
# caller distinguishes this from a normal feasible schedule via
# `infeasibility_reason == 'paid_off'`.
PAID_OFF_REASON = "paid_off"


def recompute_after_payment(
    debt,
    payment_amount: float,
    scheduled_payments_so_far: int,
) -> DebtSchedule:
    """Compute the post-payment debt plan for the Pay Debt preview.

    Mirrors `ledger.pay_debt`'s interest split: one period of interest
    is taken off the cash payment first, the remainder reduces principal.
    Then the schedule is recomputed according to `debt.plan_type`:

    - ``fixed_payment`` — keep the original per-period amount; derive
      new term and final-payment from the post-payment balance.
    - ``fixed_term`` — keep the *remaining* term (the original term
      minus how many scheduled auto-payments have already fired); derive
      new per-period payment and final payment from the post-payment
      balance.

    Returns a ``DebtSchedule`` with ``feasible=True`` and
    ``infeasibility_reason='paid_off'`` (zero fields) when the payment
    fully clears the debt — the caller can render a "this will pay off
    the debt" preview without resolving an annuity formula.

    The function does not write to the DB; the caller is responsible
    for persisting changes after the user confirms.
    """
    balance = float(debt.current_balance or 0.0)
    rate = float(debt.interest_rate or 0.0)
    schedule = debt.schedule_frequency or "monthly"
    plan_type = debt.plan_type or "fixed_payment"

    if balance <= 0:
        return _paid_off(rate, schedule)

    if rate > 0:
        accrued = period_interest(balance, rate, schedule)
    else:
        accrued = 0.0
    reduction = max(0.0, payment_amount - accrued)
    new_balance = max(0.0, balance - reduction)

    # Tolerance: anything less than half a cent counts as paid off so
    # rounding noise from the annuity walk doesn't produce a 1¢ residue
    # that triggers an extra micro-period in the recomputed schedule.
    if new_balance <= 0.005:
        return _paid_off(rate, schedule)

    if plan_type == "fixed_term":
        original = int(debt.original_term_periods or 0)
        # Manual extra payments do NOT consume a period; the spec
        # requires keeping the existing remaining-term and dropping the
        # per-period amount. Auto-settle scheduled payments DO consume
        # periods (counted by the caller via transaction notes).
        remaining = max(1, original - int(scheduled_payments_so_far or 0))
        return compute_debt_schedule(
            principal=new_balance, annual_rate=rate,
            schedule=schedule, term_periods=remaining,
        )

    # fixed_payment: keep the per-period amount.
    return compute_debt_schedule(
        principal=new_balance, annual_rate=rate,
        schedule=schedule, payment=float(debt.monthly_payment_amount or 0.0),
    )


def _paid_off(annual_rate: float, schedule: str) -> DebtSchedule:
    return DebtSchedule(
        principal=0.0, annual_rate=annual_rate, schedule=schedule,
        per_period_payment=0.0, num_periods=0, final_payment=0.0,
        total_paid=0.0, total_interest=0.0,
        feasible=True, infeasibility_reason=PAID_OFF_REASON,
    )


def simulate_amortization_balance(
    principal: float,
    annual_rate: float,
    schedule: str,
    payment: float,
    periods_elapsed: int,
) -> float:
    """Remaining balance after `periods_elapsed` full payments.

    Used to model an existing loan that's been partially paid down before
    being entered into the simulator. Given the original principal, the
    payment plan, and how many periods have already passed, returns
    today's balance.

    Closed-form annuity formula:
        B_n = P*(1+r)^n - PMT*((1+r)^n - 1)/r        (rate > 0)
        B_n = max(0, P - n*PMT)                      (rate == 0)
    where ``r`` is the periodic rate (annual/12 for monthly, annual for
    yearly). Saturates at 0 if the schedule pays off before
    ``periods_elapsed``. Edge case: a payment that doesn't cover periodic
    interest causes the balance to grow over time — this function
    returns whatever the formula yields without clamping above
    ``principal``; the caller is responsible for rejecting infeasible
    plans before persisting.
    """
    if periods_elapsed <= 0:
        return float(principal)
    if principal <= 0:
        return 0.0
    r = _periodic_rate(annual_rate, schedule)
    n = int(periods_elapsed)
    if r <= 0:
        return max(0.0, float(principal) - n * float(payment))
    growth = (1.0 + r) ** n
    balance = principal * growth - payment * (growth - 1.0) / r
    return max(0.0, balance)


def compute_periods_elapsed(
    start_date: str, end_date: str, schedule: str,
) -> int:
    """Count whole periods between two ISO dates (inclusive of start,
    exclusive-of-incomplete-period). For ``monthly``, returns whole
    calendar months; for ``yearly``, whole calendar years. A partial
    period at the end does not count — a loan started 2020-01-15 with
    today=2020-02-10 has 0 monthly periods elapsed (only ~26 days, not
    a full month).

    Returns 0 if `end_date <= start_date`.
    """
    from datetime import date as _date
    from dateutil.relativedelta import relativedelta

    if schedule not in ("monthly", "yearly"):
        raise ValueError(f"Invalid schedule: {schedule!r}")
    start = _date.fromisoformat(start_date)
    end = _date.fromisoformat(end_date)
    if end <= start:
        return 0
    delta = relativedelta(end, start)
    if schedule == "yearly":
        return max(0, delta.years)
    return max(0, delta.years * 12 + delta.months)
