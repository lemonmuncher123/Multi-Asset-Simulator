"""Centralized asset-type enumerations.

The same set of asset-type strings is referenced from several engines
(positions, holdings, allocation, risk, import/export, pricing). Defining
them once here keeps a future "add a new asset type" task to a single
review surface.
"""

# All persisted asset_type strings, including non-tradeable ones.
ALL_ASSET_TYPES: frozenset[str] = frozenset({
    "stock",
    "etf",
    "crypto",
    "real_estate",
    "cash",
    "debt",
    "custom",
})

# Asset types that flow through ledger.buy / ledger.sell and contribute
# to derived "positions" (cost basis, market value). Excludes real_estate
# (handled via property_repo) and debt (handled via debt_repo).
SELLABLE_ASSET_TYPES: frozenset[str] = frozenset({
    "stock",
    "etf",
    "crypto",
    "custom",
})

# Asset types whose latest market data can be synced from external
# providers (yfinance today). "custom" assets accept manual prices only.
SYNCABLE_ASSET_TYPES: frozenset[str] = frozenset({
    "stock",
    "etf",
    "crypto",
})

# Liquidity classifications used by allocation breakdowns and the asset
# CSV import validator.
VALID_LIQUIDITY: frozenset[str] = frozenset({"liquid", "illiquid"})
