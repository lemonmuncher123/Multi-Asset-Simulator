from dataclasses import dataclass


@dataclass
class RiskWarning:
    severity: str  # info | low | medium | high | critical
    category: str
    message: str
    metric_value: float | None = None
    threshold: float | None = None
    related_asset_id: int | None = None
