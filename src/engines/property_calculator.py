from __future__ import annotations


def parse_percent(value: float) -> float:
    """Heuristic percent parser kept for back-compat.

    Returns the value unchanged when ``value <= 1`` (assumes already a
    fraction) and divides by 100 otherwise (assumes a percent literal).
    The heuristic round-trips a stored fraction like 0.005 (= 0.5%)
    incorrectly: display * 100 = 0.5 → re-parsed as 0.5 (= 50%) since
    0.5 is below the threshold. Use ``parse_percent_literal`` in any
    code path where the form field is documented as a percent literal —
    the unambiguous parser avoids this footgun.
    """
    if value > 1:
        return value / 100.0
    return value


def parse_percent_literal(value: float) -> float:
    """Unambiguous percent parser: always divides by 100.

    Use for form fields whose label and placeholder document the input
    as a percent literal (e.g. "Vacancy Rate (%)" with placeholder
    "e.g. 5 for 5%"). Round-trips with display logic of the form
    ``round(stored_fraction * 100, n)`` cleanly for any magnitude,
    including sub-1% rates that ``parse_percent`` mishandles.
    """
    return value / 100.0


def normalize_rent_to_monthly(amount: float, frequency: str) -> float:
    if frequency == "annual":
        return amount / 12.0
    if frequency == "weekly":
        return amount * 52.0 / 12.0
    if frequency == "biweekly":
        return amount * 26.0 / 12.0
    return amount


def calc_down_payment(price: float, dp_type: str, dp_value: float) -> float:
    if dp_type == "percent":
        result = price * parse_percent(dp_value)
    else:
        result = dp_value
    if result > price:
        raise ValueError("Down payment cannot exceed purchase price.")
    return result


def calc_monthly_mortgage(principal: float, annual_rate: float, term_years: int) -> float:
    if principal <= 0 or annual_rate <= 0 or term_years <= 0:
        return 0.0
    r = annual_rate / 12.0
    n = term_years * 12
    return principal * (r * (1 + r) ** n) / ((1 + r) ** n - 1)


def normalize_expense(
    input_type: str,
    input_value: float,
    property_value: float = 0.0,
    reference_rent: float = 0.0,
) -> float:
    if input_type == "annual":
        return input_value / 12.0
    if input_type == "pct_value":
        return property_value * parse_percent(input_value) / 12.0
    if input_type == "pct_rent":
        return reference_rent * parse_percent(input_value)
    return input_value


def calc_equity(current_value: float, mortgage_balance: float) -> float:
    return current_value - mortgage_balance


def calc_ltv(mortgage_balance: float, current_value: float) -> float | None:
    if current_value <= 0:
        return None
    return mortgage_balance / current_value


def calc_effective_monthly_rent(monthly_rent: float, vacancy_rate: float) -> float:
    return monthly_rent * (1.0 - vacancy_rate)


def calc_monthly_operating_expenses(
    monthly_property_tax: float = 0.0,
    monthly_insurance: float = 0.0,
    monthly_hoa: float = 0.0,
    monthly_maintenance: float = 0.0,
    monthly_management: float = 0.0,
) -> float:
    return (
        monthly_property_tax
        + monthly_insurance
        + monthly_hoa
        + monthly_maintenance
        + monthly_management
    )


def calc_total_monthly_expenses(
    monthly_operating: float,
    monthly_mortgage_payment: float,
) -> float:
    return monthly_operating + monthly_mortgage_payment


def calc_monthly_cash_flow(
    effective_rent: float,
    total_monthly_expenses: float,
) -> float:
    return effective_rent - total_monthly_expenses


def calc_annual_noi(
    effective_rent: float,
    monthly_operating_expenses: float,
) -> float:
    return (effective_rent - monthly_operating_expenses) * 12.0


def calc_cap_rate(annual_noi: float, current_value: float) -> float | None:
    if current_value <= 0:
        return None
    return annual_noi / current_value


def calc_cash_on_cash(annual_cash_flow: float, cash_invested: float) -> float | None:
    if cash_invested <= 0:
        return None
    return annual_cash_flow / cash_invested


def calc_property_summary(
    purchase_price: float,
    current_value: float,
    mortgage_balance: float,
    down_payment: float,
    monthly_mortgage_payment: float,
    monthly_rent: float,
    vacancy_rate: float,
    monthly_property_tax: float = 0.0,
    monthly_insurance: float = 0.0,
    monthly_hoa: float = 0.0,
    monthly_maintenance: float = 0.0,
    monthly_management: float = 0.0,
) -> dict:
    equity = calc_equity(current_value, mortgage_balance)
    ltv = calc_ltv(mortgage_balance, current_value)
    eff_rent = calc_effective_monthly_rent(monthly_rent, vacancy_rate)
    operating = calc_monthly_operating_expenses(
        monthly_property_tax, monthly_insurance, monthly_hoa,
        monthly_maintenance, monthly_management,
    )
    total_expenses = calc_total_monthly_expenses(operating, monthly_mortgage_payment)
    monthly_cf = calc_monthly_cash_flow(eff_rent, total_expenses)
    annual_cf = monthly_cf * 12.0
    noi = calc_annual_noi(eff_rent, operating)
    cap_rate = calc_cap_rate(noi, current_value)
    coc = calc_cash_on_cash(annual_cf, down_payment)

    return {
        "equity": equity,
        "ltv": ltv,
        "effective_monthly_rent": eff_rent,
        "monthly_operating_expenses": operating,
        "total_monthly_expenses": total_expenses,
        "monthly_cash_flow": monthly_cf,
        "annual_cash_flow": annual_cf,
        "annual_noi": noi,
        "cap_rate": cap_rate,
        "cash_on_cash_return": coc,
    }
