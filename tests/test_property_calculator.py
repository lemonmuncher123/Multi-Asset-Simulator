import pytest
from src.engines.property_calculator import (
    parse_percent,
    parse_percent_literal,
    normalize_rent_to_monthly,
    calc_down_payment,
    calc_monthly_mortgage,
    normalize_expense,
    calc_equity,
    calc_ltv,
    calc_effective_monthly_rent,
    calc_monthly_operating_expenses,
    calc_total_monthly_expenses,
    calc_monthly_cash_flow,
    calc_annual_noi,
    calc_cap_rate,
    calc_cash_on_cash,
    calc_property_summary)


# --- 1. parse_percent ---


class TestParsePercent:
    def test_5_becomes_005(self):
        assert parse_percent(5) == pytest.approx(0.05)

    def test_005_stays_005(self):
        assert parse_percent(0.05) == pytest.approx(0.05)

    def test_6_5_becomes_0065(self):
        assert parse_percent(6.5) == pytest.approx(0.065)

    def test_100_becomes_1(self):
        assert parse_percent(100) == pytest.approx(1.0)

    def test_boundary_exactly_1_stays_1(self):
        assert parse_percent(1) == pytest.approx(1.0)

    def test_zero(self):
        assert parse_percent(0) == pytest.approx(0.0)

    def test_small_decimal_unchanged(self):
        assert parse_percent(0.065) == pytest.approx(0.065)

    def test_20_becomes_020(self):
        assert parse_percent(20) == pytest.approx(0.20)

    def test_0_5_stays_0_5(self):
        assert parse_percent(0.5) == pytest.approx(0.5)

    def test_negative_value_passes_through(self):
        assert parse_percent(-5) == pytest.approx(-5)

    def test_negative_decimal_passes_through(self):
        assert parse_percent(-0.05) == pytest.approx(-0.05)


class TestParsePercentLiteral:
    """Unambiguous percent parser used by GUI form fields whose label
    documents the input as a percent literal. Always divides by 100,
    so round-tripping a stored fraction back through the form returns
    the same fraction.
    """

    def test_5_becomes_005(self):
        assert parse_percent_literal(5) == pytest.approx(0.05)

    def test_0_5_becomes_0_005(self):
        # The bug fix: a stored fraction of 0.005 (= 0.5%) displayed as
        # "0.5" must re-parse to 0.005, not 0.5.
        assert parse_percent_literal(0.5) == pytest.approx(0.005)

    def test_100_becomes_1(self):
        assert parse_percent_literal(100) == pytest.approx(1.0)

    def test_zero(self):
        assert parse_percent_literal(0) == pytest.approx(0.0)

    @pytest.mark.parametrize("stored_fraction", [
        0.0,        # 0%
        0.005,      # 0.5% — round-trip footgun
        0.0125,     # 1.25%
        0.05,       # 5%
        0.25,       # 25%
        0.50,       # 50%
        1.0,        # 100%
    ])
    def test_round_trip_from_stored_fraction(self, stored_fraction):
        """Display logic does ``round(stored * 100, 4)`` to produce the
        form value; re-parsing must recover the stored fraction."""
        displayed = round(stored_fraction * 100.0, 4)
        recovered = parse_percent_literal(displayed)
        assert recovered == pytest.approx(stored_fraction)


# --- 2. normalize_rent_to_monthly ---


class TestNormalizeRentToMonthly:
    def test_monthly_unchanged(self):
        assert normalize_rent_to_monthly(2000, "monthly") == pytest.approx(2000)

    def test_annual_divided_by_12(self):
        assert normalize_rent_to_monthly(24000, "annual") == pytest.approx(2000)

    def test_weekly_times_52_over_12(self):
        assert normalize_rent_to_monthly(500, "weekly") == pytest.approx(500 * 52 / 12)

    def test_biweekly_times_26_over_12(self):
        assert normalize_rent_to_monthly(1000, "biweekly") == pytest.approx(1000 * 26 / 12)

    def test_unknown_frequency_treated_as_monthly(self):
        assert normalize_rent_to_monthly(2000, "unknown") == pytest.approx(2000)

    def test_zero_amount_any_frequency(self):
        assert normalize_rent_to_monthly(0, "annual") == pytest.approx(0)
        assert normalize_rent_to_monthly(0, "weekly") == pytest.approx(0)

    def test_annual_odd_amount(self):
        assert normalize_rent_to_monthly(13000, "annual") == pytest.approx(13000 / 12)


# --- 3. calc_down_payment ---


class TestCalcDownPayment:
    def test_amount_mode_passthrough(self):
        assert calc_down_payment(500000, "amount", 100000) == pytest.approx(100000)

    def test_percent_mode_whole_number(self):
        assert calc_down_payment(500000, "percent", 20) == pytest.approx(100000)

    def test_percent_mode_decimal(self):
        assert calc_down_payment(500000, "percent", 0.20) == pytest.approx(100000)

    def test_zero_down(self):
        assert calc_down_payment(500000, "amount", 0) == pytest.approx(0)

    def test_percent_mode_3_5(self):
        assert calc_down_payment(400000, "percent", 3.5) == pytest.approx(14000)

    def test_down_payment_exceeds_price_raises(self):
        with pytest.raises(ValueError, match="cannot exceed"):
            calc_down_payment(500000, "amount", 600000)

    def test_percent_over_100_raises(self):
        with pytest.raises(ValueError, match="cannot exceed"):
            calc_down_payment(500000, "percent", 120)

    def test_full_cash_purchase(self):
        assert calc_down_payment(500000, "percent", 100) == pytest.approx(500000)


# --- 4. calc_monthly_mortgage ---


class TestCalcMonthlyMortgage:
    def test_standard_30yr_amortization(self):
        payment = calc_monthly_mortgage(400000, 0.065, 30)
        assert payment == pytest.approx(2528.27, abs=1.0)

    def test_15yr_loan(self):
        payment = calc_monthly_mortgage(400000, 0.065, 15)
        assert payment == pytest.approx(3484.39, abs=1.0)

    def test_zero_rate_returns_zero(self):
        assert calc_monthly_mortgage(400000, 0.0, 30) == pytest.approx(0)

    def test_zero_principal_returns_zero(self):
        assert calc_monthly_mortgage(0, 0.065, 30) == pytest.approx(0)

    def test_zero_term_returns_zero(self):
        assert calc_monthly_mortgage(400000, 0.065, 0) == pytest.approx(0)

    def test_negative_term_returns_zero(self):
        assert calc_monthly_mortgage(400000, 0.065, -5) == pytest.approx(0)

    def test_negative_principal_returns_zero(self):
        assert calc_monthly_mortgage(-100000, 0.065, 30) == pytest.approx(0)

    def test_low_rate_loan(self):
        payment = calc_monthly_mortgage(300000, 0.03, 30)
        assert payment == pytest.approx(1264.81, abs=1.0)

    def test_high_rate_short_term(self):
        payment = calc_monthly_mortgage(200000, 0.10, 10)
        assert payment > 0


# --- 5. normalize_expense (tax / insurance / maintenance / management) ---


class TestNormalizeExpenseTaxInsurance:
    def test_monthly_passthrough(self):
        assert normalize_expense("monthly", 500) == pytest.approx(500)

    def test_annual_to_monthly(self):
        assert normalize_expense("annual", 6000) == pytest.approx(500)

    def test_pct_of_value(self):
        result = normalize_expense("pct_value", 1.2, property_value=500000)
        assert result == pytest.approx(500000 * 0.012 / 12)

    def test_pct_of_value_whole_number(self):
        result = normalize_expense("pct_value", 2, property_value=600000)
        assert result == pytest.approx(600000 * 0.02 / 12)

    def test_zero_value_pct_value(self):
        result = normalize_expense("pct_value", 1.2, property_value=0)
        assert result == pytest.approx(0)


class TestNormalizeExpenseMaintenanceManagement:
    def test_fixed_monthly(self):
        assert normalize_expense("monthly", 200) == pytest.approx(200)

    def test_pct_of_value_maintenance(self):
        result = normalize_expense("pct_value", 1.5, property_value=500000)
        assert result == pytest.approx(500000 * 0.015 / 12)

    def test_pct_of_rent(self):
        result = normalize_expense("pct_rent", 10, reference_rent=2000)
        assert result == pytest.approx(200)

    def test_pct_of_rent_decimal(self):
        result = normalize_expense("pct_rent", 0.10, reference_rent=2000)
        assert result == pytest.approx(200)

    def test_pct_of_rent_zero_rent(self):
        result = normalize_expense("pct_rent", 10, reference_rent=0)
        assert result == pytest.approx(0)

    def test_unknown_type_treated_as_monthly(self):
        assert normalize_expense("garbage", 300) == pytest.approx(300)


# --- 6. Summary calculations ---


class TestCalcEquity:
    def test_positive_equity(self):
        assert calc_equity(500000, 300000) == pytest.approx(200000)

    def test_zero_mortgage_full_equity(self):
        assert calc_equity(500000, 0) == pytest.approx(500000)

    def test_underwater(self):
        assert calc_equity(300000, 400000) == pytest.approx(-100000)

    def test_zero_value_zero_mortgage(self):
        assert calc_equity(0, 0) == pytest.approx(0)


class TestCalcLtv:
    def test_80_percent(self):
        assert calc_ltv(400000, 500000) == pytest.approx(0.8)

    def test_zero_value_returns_none(self):
        assert calc_ltv(400000, 0) is None

    def test_negative_value_returns_none(self):
        assert calc_ltv(400000, -100) is None

    def test_no_mortgage_returns_zero(self):
        assert calc_ltv(0, 500000) == pytest.approx(0)

    def test_over_100_percent(self):
        assert calc_ltv(600000, 500000) == pytest.approx(1.2)


class TestCalcEffectiveMonthlyRent:
    def test_no_vacancy(self):
        assert calc_effective_monthly_rent(2000, 0) == pytest.approx(2000)

    def test_5_pct_vacancy(self):
        assert calc_effective_monthly_rent(2000, 0.05) == pytest.approx(1900)

    def test_full_vacancy(self):
        assert calc_effective_monthly_rent(2000, 1.0) == pytest.approx(0)

    def test_zero_rent(self):
        assert calc_effective_monthly_rent(0, 0.05) == pytest.approx(0)


class TestCalcMonthlyOperatingExpenses:
    def test_sum_of_all_components(self):
        result = calc_monthly_operating_expenses(200, 100, 50, 150, 100)
        assert result == pytest.approx(600)

    def test_all_zeros(self):
        assert calc_monthly_operating_expenses() == pytest.approx(0)

    def test_excludes_mortgage(self):
        result = calc_monthly_operating_expenses(200, 100, 50, 150, 100)
        assert result == pytest.approx(600)


class TestCalcTotalMonthlyExpenses:
    def test_includes_mortgage(self):
        assert calc_total_monthly_expenses(600, 2500) == pytest.approx(3100)

    def test_zero_mortgage(self):
        assert calc_total_monthly_expenses(600, 0) == pytest.approx(600)


class TestCalcMonthlyCashFlow:
    def test_positive(self):
        assert calc_monthly_cash_flow(2000, 1500) == pytest.approx(500)

    def test_negative(self):
        assert calc_monthly_cash_flow(1000, 1500) == pytest.approx(-500)

    def test_breakeven(self):
        assert calc_monthly_cash_flow(1500, 1500) == pytest.approx(0)


class TestCalcAnnualNoi:
    def test_positive_noi(self):
        result = calc_annual_noi(2000, 600)
        assert result == pytest.approx(16800)

    def test_negative_noi(self):
        result = calc_annual_noi(500, 800)
        assert result == pytest.approx(-3600)

    def test_noi_excludes_mortgage(self):
        eff_rent = 3000
        operating = 800
        noi = calc_annual_noi(eff_rent, operating)
        assert noi == pytest.approx((3000 - 800) * 12)


class TestCalcCapRate:
    def test_positive(self):
        assert calc_cap_rate(16800, 500000) == pytest.approx(0.0336)

    def test_zero_value_returns_none(self):
        assert calc_cap_rate(16800, 0) is None

    def test_negative_value_returns_none(self):
        assert calc_cap_rate(16800, -100) is None

    def test_negative_noi(self):
        assert calc_cap_rate(-5000, 500000) == pytest.approx(-0.01)


class TestCalcCashOnCash:
    def test_positive(self):
        assert calc_cash_on_cash(6000, 100000) == pytest.approx(0.06)

    def test_zero_investment_returns_none(self):
        assert calc_cash_on_cash(6000, 0) is None

    def test_negative_investment_returns_none(self):
        assert calc_cash_on_cash(6000, -100) is None

    def test_negative_cash_flow(self):
        assert calc_cash_on_cash(-3000, 100000) == pytest.approx(-0.03)


# --- 7. calc_property_summary (integration of all calculators) ---


class TestCalcPropertySummary:
    def test_full_summary_all_fields(self):
        s = calc_property_summary(
            purchase_price=500000,
            current_value=550000,
            mortgage_balance=400000,
            down_payment=100000,
            monthly_mortgage_payment=2500,
            monthly_rent=3000,
            vacancy_rate=0.05,
            monthly_property_tax=300,
            monthly_insurance=100,
            monthly_hoa=200,
            monthly_maintenance=150,
            monthly_management=100)
        assert s["equity"] == pytest.approx(150000)
        assert s["ltv"] == pytest.approx(400000 / 550000)
        eff_rent = 3000 * 0.95
        assert s["effective_monthly_rent"] == pytest.approx(eff_rent)
        operating = 300 + 100 + 200 + 150 + 100
        assert s["monthly_operating_expenses"] == pytest.approx(operating)
        total_exp = operating + 2500
        assert s["total_monthly_expenses"] == pytest.approx(total_exp)
        monthly_cf = eff_rent - total_exp
        assert s["monthly_cash_flow"] == pytest.approx(monthly_cf)
        assert s["annual_cash_flow"] == pytest.approx(monthly_cf * 12)
        noi = (eff_rent - operating) * 12
        assert s["annual_noi"] == pytest.approx(noi)
        assert s["cap_rate"] == pytest.approx(noi / 550000)
        assert s["cash_on_cash_return"] == pytest.approx(monthly_cf * 12 / 100000)

    def test_summary_no_rent_negative_cash_flow(self):
        s = calc_property_summary(
            purchase_price=500000,
            current_value=500000,
            mortgage_balance=400000,
            down_payment=100000,
            monthly_mortgage_payment=2500,
            monthly_rent=0,
            vacancy_rate=0)
        assert s["effective_monthly_rent"] == pytest.approx(0)
        assert s["monthly_cash_flow"] == pytest.approx(-2500)
        assert s["annual_cash_flow"] == pytest.approx(-30000)

    def test_summary_no_mortgage_free_and_clear(self):
        s = calc_property_summary(
            purchase_price=300000,
            current_value=400000,
            mortgage_balance=0,
            down_payment=300000,
            monthly_mortgage_payment=0,
            monthly_rent=2000,
            vacancy_rate=0.05)
        assert s["equity"] == pytest.approx(400000)
        assert s["ltv"] == pytest.approx(0)
        assert s["monthly_cash_flow"] == pytest.approx(2000 * 0.95)
        assert s["cap_rate"] is not None

    def test_summary_zero_down_coc_is_none(self):
        s = calc_property_summary(
            purchase_price=500000,
            current_value=500000,
            mortgage_balance=500000,
            down_payment=0,
            monthly_mortgage_payment=2500,
            monthly_rent=2500,
            vacancy_rate=0)
        assert s["cash_on_cash_return"] is None

    def test_summary_ltv_uses_current_value_not_purchase(self):
        s = calc_property_summary(
            purchase_price=400000,
            current_value=600000,
            mortgage_balance=300000,
            down_payment=100000,
            monthly_mortgage_payment=1500,
            monthly_rent=2500,
            vacancy_rate=0)
        assert s["ltv"] == pytest.approx(300000 / 600000)

    def test_summary_annual_cash_flow_is_12x_monthly(self):
        s = calc_property_summary(
            purchase_price=500000,
            current_value=500000,
            mortgage_balance=400000,
            down_payment=100000,
            monthly_mortgage_payment=2500,
            monthly_rent=3500,
            vacancy_rate=0)
        assert s["annual_cash_flow"] == pytest.approx(s["monthly_cash_flow"] * 12)

    def test_summary_noi_excludes_mortgage(self):
        s = calc_property_summary(
            purchase_price=500000,
            current_value=500000,
            mortgage_balance=400000,
            down_payment=100000,
            monthly_mortgage_payment=2500,
            monthly_rent=3000,
            vacancy_rate=0,
            monthly_property_tax=200,
            monthly_insurance=100)
        eff_rent = 3000
        operating = 200 + 100
        expected_noi = (eff_rent - operating) * 12
        assert s["annual_noi"] == pytest.approx(expected_noi)
