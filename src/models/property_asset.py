from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class PropertyAsset:
    id: int | None = None
    asset_id: int = 0
    address: str | None = None
    purchase_date: str | None = None
    purchase_price: float | None = None
    current_value: float | None = None
    down_payment: float | None = None
    mortgage_balance: float = 0.0
    mortgage_interest_rate: float = 0.0
    monthly_mortgage_payment: float = 0.0
    monthly_rent: float = 0.0
    monthly_property_tax: float = 0.0
    monthly_insurance: float = 0.0
    monthly_hoa: float = 0.0
    monthly_maintenance_reserve: float = 0.0
    monthly_property_management: float = 0.0
    monthly_expense: float = 0.0
    vacancy_rate: float = 0.0
    status: str = "active"
    sold_date: str | None = None
    sold_price: float | None = None
    sale_fees: float = 0.0
    rent_collection_frequency: str = "monthly"
    cashflow_start_date: str | None = None
    notes: str | None = None
    entry_type: str = "existing_property"
    loan_term_years: int | None = None
    down_payment_type: str = "amount"
    down_payment_input_value: float | None = None
    monthly_mortgage_override_enabled: int = 0
    monthly_mortgage_override: float = 0.0
    rent_input_amount: float = 0.0
    rent_input_frequency: str = "monthly"
    property_tax_input_type: str = "monthly"
    property_tax_input_value: float = 0.0
    insurance_input_type: str = "monthly"
    insurance_input_value: float = 0.0
    maintenance_input_type: str = "monthly"
    maintenance_input_value: float = 0.0
    management_input_type: str = "monthly"
    management_input_value: float = 0.0
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
