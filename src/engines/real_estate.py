import sqlite3
from dataclasses import dataclass
from src.models.property_asset import PropertyAsset
from src.models.risk_warning import RiskWarning
from src.storage.property_repo import list_properties, list_active_properties
from src.storage.asset_repo import get_asset
from src.engines.portfolio import calc_net_worth, calc_total_assets


@dataclass
class PropertyAnalysis:
    prop: PropertyAsset
    name: str
    equity: float
    ltv: float | None
    effective_rent: float
    monthly_expenses: float
    net_monthly_cash_flow: float
    annual_net_cash_flow: float
    cap_rate: float | None
    cash_on_cash_return: float | None


def calc_equity(prop: PropertyAsset) -> float:
    return (prop.current_value or 0) - prop.mortgage_balance


def calc_ltv(prop: PropertyAsset) -> float | None:
    value = prop.current_value or 0
    if value <= 0:
        return None
    return prop.mortgage_balance / value


def calc_effective_rent(prop: PropertyAsset) -> float:
    return prop.monthly_rent * (1 - prop.vacancy_rate)


def calc_monthly_expenses(prop: PropertyAsset) -> float:
    return (
        prop.monthly_mortgage_payment
        + prop.monthly_property_tax
        + prop.monthly_insurance
        + prop.monthly_hoa
        + prop.monthly_maintenance_reserve
        + prop.monthly_property_management
    )


def calc_net_monthly_cash_flow(prop: PropertyAsset) -> float:
    return calc_effective_rent(prop) - calc_monthly_expenses(prop)


def calc_annual_net_cash_flow(prop: PropertyAsset) -> float:
    return calc_net_monthly_cash_flow(prop) * 12


def calc_cap_rate(prop: PropertyAsset) -> float | None:
    value = prop.current_value or 0
    if value <= 0:
        return None
    noi = (calc_effective_rent(prop) - (
        prop.monthly_property_tax
        + prop.monthly_insurance
        + prop.monthly_hoa
        + prop.monthly_maintenance_reserve
        + prop.monthly_property_management
    )) * 12
    return noi / value


def calc_cash_on_cash_return(prop: PropertyAsset) -> float | None:
    cash_invested = prop.down_payment
    if not cash_invested or cash_invested <= 0:
        return None
    return calc_annual_net_cash_flow(prop) / cash_invested


def analyze_property(prop: PropertyAsset, conn: sqlite3.Connection) -> PropertyAnalysis:
    asset = get_asset(conn, prop.asset_id)
    name = asset.name if asset else f"Property {prop.id}"
    return PropertyAnalysis(
        prop=prop,
        name=name,
        equity=calc_equity(prop),
        ltv=calc_ltv(prop),
        effective_rent=calc_effective_rent(prop),
        monthly_expenses=calc_monthly_expenses(prop),
        net_monthly_cash_flow=calc_net_monthly_cash_flow(prop),
        annual_net_cash_flow=calc_annual_net_cash_flow(prop),
        cap_rate=calc_cap_rate(prop),
        cash_on_cash_return=calc_cash_on_cash_return(prop),
    )


def analyze_all_properties(conn: sqlite3.Connection) -> list[PropertyAnalysis]:
    props = list_active_properties(conn)
    return [analyze_property(p, conn) for p in props]


def calc_re_share_of_net_worth(conn: sqlite3.Connection) -> float | None:
    nw = calc_net_worth(conn)
    if nw <= 0:
        return None
    props = list_active_properties(conn)
    total_value = sum(p.current_value or 0 for p in props)
    return total_value / nw


def calc_illiquid_share(conn: sqlite3.Connection) -> float | None:
    total = calc_total_assets(conn)
    if total <= 0:
        return None
    props = list_active_properties(conn)
    illiquid = sum(p.current_value or 0 for p in props)
    return illiquid / total


def get_real_estate_warnings(conn: sqlite3.Connection) -> list[RiskWarning]:
    warnings = []
    props = list_active_properties(conn)

    for prop in props:
        asset = get_asset(conn, prop.asset_id)
        label = asset.name if asset else f"Property {prop.id}"

        ncf = calc_net_monthly_cash_flow(prop)
        if ncf < 0:
            warnings.append(RiskWarning(
                severity="high",
                category="real_estate",
                message=f"{label} has negative monthly cash flow (${ncf:,.2f}/mo).",
                metric_value=ncf,
                threshold=0.0,
                related_asset_id=prop.asset_id,
            ))

        ltv = calc_ltv(prop)
        if ltv is not None and ltv > 0.80:
            warnings.append(RiskWarning(
                severity="high",
                category="real_estate",
                message=f"{label} LTV is {ltv:.0%}. Mortgage balance is high relative to property value.",
                metric_value=ltv,
                threshold=0.80,
                related_asset_id=prop.asset_id,
            ))

        if prop.vacancy_rate > 0.10:
            warnings.append(RiskWarning(
                severity="medium",
                category="real_estate",
                message=f"{label} vacancy rate is {prop.vacancy_rate:.0%}.",
                metric_value=prop.vacancy_rate,
                threshold=0.10,
                related_asset_id=prop.asset_id,
            ))

        if prop.current_value is None or prop.current_value == 0:
            warnings.append(RiskWarning(
                severity="info",
                category="real_estate",
                message=f"{label} has no current property value set.",
                related_asset_id=prop.asset_id,
            ))

    re_share = calc_re_share_of_net_worth(conn)
    if re_share is not None and re_share > 0.50:
        warnings.append(RiskWarning(
            severity="medium",
            category="real_estate",
            message=f"Real estate is {re_share:.0%} of net worth.",
            metric_value=re_share,
            threshold=0.50,
        ))

    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
    warnings.sort(key=lambda w: severity_order.get(w.severity, 5))
    return warnings
