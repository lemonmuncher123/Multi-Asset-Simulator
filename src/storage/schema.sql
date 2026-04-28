CREATE TABLE IF NOT EXISTS assets (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol      TEXT NOT NULL,
    name        TEXT NOT NULL,
    asset_type  TEXT NOT NULL,
    currency    TEXT NOT NULL DEFAULT 'USD',
    region      TEXT NOT NULL DEFAULT 'US',
    liquidity   TEXT NOT NULL DEFAULT 'liquid',
    notes       TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS transactions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL,
    txn_type        TEXT NOT NULL,
    asset_id        INTEGER,
    quantity        REAL,
    price           REAL,
    total_amount    REAL NOT NULL,
    currency        TEXT NOT NULL DEFAULT 'USD',
    fees            REAL NOT NULL DEFAULT 0,
    notes           TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (asset_id) REFERENCES assets(id)
);

CREATE TABLE IF NOT EXISTS market_prices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id        INTEGER NOT NULL,
    symbol          TEXT NOT NULL DEFAULT '',
    asset_type      TEXT NOT NULL DEFAULT '',
    date            TEXT NOT NULL,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    adjusted_close  REAL,
    volume          REAL,
    price           REAL NOT NULL,
    source          TEXT NOT NULL DEFAULT 'manual',
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (asset_id) REFERENCES assets(id),
    UNIQUE(asset_id, date, source)
);

CREATE TABLE IF NOT EXISTS price_sync_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    status              TEXT NOT NULL,
    source              TEXT,
    assets_attempted    INTEGER DEFAULT 0,
    assets_succeeded    INTEGER DEFAULT 0,
    assets_failed       INTEGER DEFAULT 0,
    error_message       TEXT
);

CREATE TABLE IF NOT EXISTS properties (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id        INTEGER NOT NULL UNIQUE,
    address         TEXT,
    purchase_date   TEXT,
    purchase_price  REAL,
    current_value   REAL,
    down_payment    REAL,
    mortgage_balance REAL NOT NULL DEFAULT 0,
    mortgage_interest_rate REAL NOT NULL DEFAULT 0,
    monthly_mortgage_payment REAL NOT NULL DEFAULT 0,
    monthly_rent    REAL NOT NULL DEFAULT 0,
    monthly_property_tax REAL NOT NULL DEFAULT 0,
    monthly_insurance REAL NOT NULL DEFAULT 0,
    monthly_hoa     REAL NOT NULL DEFAULT 0,
    monthly_maintenance_reserve REAL NOT NULL DEFAULT 0,
    monthly_property_management REAL NOT NULL DEFAULT 0,
    monthly_expense REAL NOT NULL DEFAULT 0,
    vacancy_rate    REAL NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'active',
    sold_date       TEXT,
    sold_price      REAL,
    sale_fees       REAL NOT NULL DEFAULT 0,
    rent_collection_frequency TEXT NOT NULL DEFAULT 'monthly',
    cashflow_start_date TEXT,
    notes           TEXT,
    entry_type      TEXT NOT NULL DEFAULT 'existing_property',
    loan_term_years INTEGER,
    down_payment_type TEXT NOT NULL DEFAULT 'amount',
    down_payment_input_value REAL,
    monthly_mortgage_override_enabled INTEGER NOT NULL DEFAULT 0,
    monthly_mortgage_override REAL NOT NULL DEFAULT 0,
    rent_input_amount REAL NOT NULL DEFAULT 0,
    rent_input_frequency TEXT NOT NULL DEFAULT 'monthly',
    property_tax_input_type TEXT NOT NULL DEFAULT 'monthly',
    property_tax_input_value REAL NOT NULL DEFAULT 0,
    insurance_input_type TEXT NOT NULL DEFAULT 'monthly',
    insurance_input_value REAL NOT NULL DEFAULT 0,
    maintenance_input_type TEXT NOT NULL DEFAULT 'monthly',
    maintenance_input_value REAL NOT NULL DEFAULT 0,
    management_input_type TEXT NOT NULL DEFAULT 'monthly',
    management_input_value REAL NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (asset_id) REFERENCES assets(id)
);

CREATE TABLE IF NOT EXISTS debts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id        INTEGER NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    original_amount REAL NOT NULL,
    current_balance REAL NOT NULL,
    interest_rate   REAL NOT NULL DEFAULT 0,
    minimum_payment REAL NOT NULL DEFAULT 0,
    due_date        TEXT,
    notes           TEXT,
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (asset_id) REFERENCES assets(id)
);

CREATE TABLE IF NOT EXISTS decision_journal (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id  INTEGER,
    date            TEXT NOT NULL,
    title           TEXT NOT NULL,
    thesis          TEXT,
    intended_role   TEXT,
    risk_reasoning  TEXT,
    exit_plan       TEXT,
    confidence_level INTEGER,
    expected_holding_period TEXT,
    pre_trade_notes TEXT,
    post_trade_review TEXT,
    mistake_tags    TEXT,
    lesson_learned  TEXT,
    snapshot_before TEXT,
    snapshot_after  TEXT,
    reasoning       TEXT,
    expected        TEXT,
    actual          TEXT,
    score           INTEGER,
    tags            TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (transaction_id) REFERENCES transactions(id)
);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL UNIQUE,
    cash            REAL NOT NULL,
    total_assets    REAL NOT NULL,
    total_liabilities REAL NOT NULL,
    net_worth       REAL NOT NULL,
    allocation_json TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS securities_master (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    name            TEXT NOT NULL,
    asset_type      TEXT NOT NULL,
    exchange        TEXT,
    sector          TEXT,
    industry        TEXT,
    etf_category    TEXT,
    is_common_etf   INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(symbol, asset_type)
);

CREATE TABLE IF NOT EXISTS market_quotes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id    INTEGER NOT NULL,
    symbol      TEXT NOT NULL,
    asset_type  TEXT NOT NULL,
    bid         REAL,
    ask         REAL,
    last        REAL,
    timestamp   TEXT,
    source      TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (asset_id) REFERENCES assets(id),
    UNIQUE(asset_id, source)
);

CREATE TABLE IF NOT EXISTS reports (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    report_type     TEXT NOT NULL,
    period_start    TEXT NOT NULL,
    period_end      TEXT NOT NULL,
    period_label    TEXT NOT NULL,
    generated_at    TEXT NOT NULL,
    title           TEXT NOT NULL,
    report_json     TEXT NOT NULL,
    notes           TEXT,
    net_cash_flow       REAL NOT NULL DEFAULT 0,
    operating_net_income REAL NOT NULL DEFAULT 0,
    transaction_count   INTEGER NOT NULL DEFAULT 0,
    net_worth_change       REAL,
    funding_flow           REAL NOT NULL DEFAULT 0,
    approximate_return_pct REAL,
    UNIQUE(report_type, period_label)
);

CREATE TABLE IF NOT EXISTS transaction_fee_breakdown (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id  INTEGER NOT NULL,
    fee_type        TEXT NOT NULL,
    amount          REAL NOT NULL,
    rate            REAL,
    notes           TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (transaction_id) REFERENCES transactions(id)
);

CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL
);
