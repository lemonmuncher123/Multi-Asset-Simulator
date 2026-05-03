from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class PropertyAsset:
    """Real estate property. As of schema v11, mortgage info no longer
    lives on this row — see `Mortgage` (linked via `mortgages.property_id`).
    """
    id: int | None = None
    asset_id: int = 0
    address: str | None = None
    purchase_date: str | None = None
    purchase_price: float | None = None
    current_value: float | None = None
    down_payment: float | None = None
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
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
