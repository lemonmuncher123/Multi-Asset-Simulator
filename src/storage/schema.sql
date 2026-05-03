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
    -- `quantity` is positive when set: buy/sell qty > 0; manual_adjustment
    -- with quantity is a position adjustment that the engine now requires
    -- to be > 0 (cost-basis denominator parity). Cash-only transactions
    -- (deposit/withdraw, pay_*) leave quantity NULL.
    quantity        REAL CHECK (quantity IS NULL OR quantity > 0),
    -- Same for price: meaningful values are > 0.
    price           REAL CHECK (price IS NULL OR price > 0),
    -- `total_amount` is signed by convention (buys negative, sells
    -- positive, manual_adjustment either) — intentionally NOT constrained.
    total_amount    REAL NOT NULL,
    currency        TEXT NOT NULL DEFAULT 'USD',
    fees            REAL NOT NULL DEFAULT 0 CHECK (fees >= 0),
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
    open            REAL CHECK (open IS NULL OR open >= 0),
    high            REAL CHECK (high IS NULL OR high >= 0),
    low             REAL CHECK (low IS NULL OR low >= 0),
    close           REAL CHECK (close IS NULL OR close >= 0),
    adjusted_close  REAL CHECK (adjusted_close IS NULL OR adjusted_close >= 0),
    volume          REAL CHECK (volume IS NULL OR volume >= 0),
    -- `price` is the canonical close used by the position valuation engine;
    -- a non-positive market price is meaningless.
    price           REAL NOT NULL CHECK (price > 0),
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
    -- All monetary fields are non-negative. `purchase_price` may be 0 for
    -- existing-property entries where the user no longer remembers what
    -- they paid (current_value carries the value in that case).
    purchase_price  REAL CHECK (purchase_price IS NULL OR purchase_price >= 0),
    current_value   REAL CHECK (current_value IS NULL OR current_value >= 0),
    down_payment    REAL CHECK (down_payment IS NULL OR down_payment >= 0),
    monthly_rent    REAL NOT NULL DEFAULT 0 CHECK (monthly_rent >= 0),
    monthly_property_tax REAL NOT NULL DEFAULT 0 CHECK (monthly_property_tax >= 0),
    monthly_insurance REAL NOT NULL DEFAULT 0 CHECK (monthly_insurance >= 0),
    monthly_hoa     REAL NOT NULL DEFAULT 0 CHECK (monthly_hoa >= 0),
    monthly_maintenance_reserve REAL NOT NULL DEFAULT 0 CHECK (monthly_maintenance_reserve >= 0),
    monthly_property_management REAL NOT NULL DEFAULT 0 CHECK (monthly_property_management >= 0),
    monthly_expense REAL NOT NULL DEFAULT 0 CHECK (monthly_expense >= 0),
    vacancy_rate    REAL NOT NULL DEFAULT 0 CHECK (vacancy_rate >= 0 AND vacancy_rate <= 1),
    status          TEXT NOT NULL DEFAULT 'active',
    sold_date       TEXT,
    sold_price      REAL CHECK (sold_price IS NULL OR sold_price >= 0),
    sale_fees       REAL NOT NULL DEFAULT 0 CHECK (sale_fees >= 0),
    rent_collection_frequency TEXT NOT NULL DEFAULT 'monthly',
    cashflow_start_date TEXT,
    notes           TEXT,
    entry_type      TEXT NOT NULL DEFAULT 'existing_property',
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (asset_id) REFERENCES assets(id)
);

-- Mortgage subsystem (schema v11). Cloned from `debts` / `debt_payment_records`
-- so mortgages get full feature parity: 5 stored preview values, plan_type
-- (fixed_payment vs fixed_term), per-payment audit records, Pay Off in Full
-- support. Differences from debts:
--   1. Links to a property via `property_id NOT NULL UNIQUE` instead of
--      `asset_id` — mortgages do not have their own Asset row; they are
--      always 1:1 with a real estate property.
--   2. Monthly-only schedule (no `schedule_frequency` column). Yearly
--      mortgages are not modeled.
--   3. Interest is always annual (no `interest_period` column).
-- See PROJECT_UNDERSTANDING.md §3 / §5 for the design rationale.
CREATE TABLE IF NOT EXISTS mortgages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id     INTEGER NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    original_amount REAL NOT NULL CHECK (original_amount > 0),
    current_balance REAL NOT NULL CHECK (current_balance >= 0),
    interest_rate   REAL NOT NULL DEFAULT 0 CHECK (interest_rate >= 0),
    minimum_payment REAL NOT NULL DEFAULT 0 CHECK (minimum_payment >= 0),
    due_date        TEXT,
    notes           TEXT,
    monthly_payment_amount  REAL NOT NULL DEFAULT 0 CHECK (monthly_payment_amount >= 0),
    cashflow_start_date     TEXT,
    last_payment_date       TEXT,
    plan_type               TEXT NOT NULL DEFAULT 'fixed_payment',
    original_term_periods   INTEGER CHECK (original_term_periods IS NULL OR original_term_periods > 0),
    -- 5 preview values mirror debts.preview_*. They are the live current
    -- official payment plan, refreshed on every mortgage state change.
    preview_regular_payment REAL NOT NULL DEFAULT 0 CHECK (preview_regular_payment >= 0),
    preview_period_count    INTEGER NOT NULL DEFAULT 0 CHECK (preview_period_count >= 0),
    preview_final_payment   REAL NOT NULL DEFAULT 0 CHECK (preview_final_payment >= 0),
    preview_total_paid      REAL NOT NULL DEFAULT 0 CHECK (preview_total_paid >= 0),
    preview_total_interest  REAL NOT NULL DEFAULT 0 CHECK (preview_total_interest >= 0),
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (property_id) REFERENCES properties(id)
);

-- Per-payment audit trail for mortgage payments (manual or automatic).
-- Mirror of debt_payment_records: every txn_type='pay_mortgage' row produces
-- exactly one matching record here, written atomically by
-- ledger._record_mortgage_payment. UNIQUE(transaction_id) is the structural
-- 1:1 backstop.
CREATE TABLE IF NOT EXISTS mortgage_payment_records (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id           INTEGER NOT NULL UNIQUE,
    mortgage_id              INTEGER NOT NULL,
    mortgage_name            TEXT NOT NULL,
    payment_amount           REAL NOT NULL CHECK (payment_amount >= 0),
    payment_date             TEXT NOT NULL,
    payment_type             TEXT NOT NULL,  -- 'manual' | 'automatic'
    balance_before_payment   REAL NOT NULL CHECK (balance_before_payment >= 0),
    balance_after_payment    REAL NOT NULL CHECK (balance_after_payment >= 0),
    note                     TEXT,
    created_at               TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (transaction_id) REFERENCES transactions(id),
    FOREIGN KEY (mortgage_id) REFERENCES mortgages(id)
);

CREATE TABLE IF NOT EXISTS debts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id        INTEGER NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    original_amount REAL NOT NULL CHECK (original_amount > 0),
    current_balance REAL NOT NULL CHECK (current_balance >= 0),
    interest_rate   REAL NOT NULL DEFAULT 0 CHECK (interest_rate >= 0),
    minimum_payment REAL NOT NULL DEFAULT 0 CHECK (minimum_payment >= 0),
    due_date        TEXT,
    notes           TEXT,
    schedule_frequency      TEXT NOT NULL DEFAULT 'monthly',
    interest_period         TEXT NOT NULL DEFAULT 'annual',
    monthly_payment_amount  REAL NOT NULL DEFAULT 0 CHECK (monthly_payment_amount >= 0),
    cashflow_start_date     TEXT,
    last_payment_date       TEXT,
    plan_type               TEXT NOT NULL DEFAULT 'fixed_payment',
    original_term_periods   INTEGER CHECK (original_term_periods IS NULL OR original_term_periods > 0),
    -- Spec §5: the 5 preview values are persisted as the live current
    -- official payment plan. Refreshed on Add Debt, every manual or
    -- automatic pay_debt, and Pay Off in Full (zeroed). See
    -- debt_math.write_preview_values_for + ledger._refresh_debt_preview.
    preview_regular_payment REAL NOT NULL DEFAULT 0 CHECK (preview_regular_payment >= 0),
    preview_period_count    INTEGER NOT NULL DEFAULT 0 CHECK (preview_period_count >= 0),
    preview_final_payment   REAL NOT NULL DEFAULT 0 CHECK (preview_final_payment >= 0),
    preview_total_paid      REAL NOT NULL DEFAULT 0 CHECK (preview_total_paid >= 0),
    preview_total_interest  REAL NOT NULL DEFAULT 0 CHECK (preview_total_interest >= 0),
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (asset_id) REFERENCES assets(id)
);

-- Per-payment audit trail for debt_payment events (manual or automatic).
-- Sibling of `transactions` — every txn_type='pay_debt' row produces
-- exactly one matching record here, written atomically by
-- ledger._record_debt_payment. Carries the spec §5 fields plus a
-- transaction_id link so callers can navigate between the two.
-- Idempotency is enforced by the unique transaction_id (one record per
-- transaction; never more, never less).
CREATE TABLE IF NOT EXISTS debt_payment_records (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id           INTEGER NOT NULL UNIQUE,
    debt_id                  INTEGER NOT NULL,
    debt_name                TEXT NOT NULL,
    payment_amount           REAL NOT NULL CHECK (payment_amount >= 0),
    payment_date             TEXT NOT NULL,
    payment_type             TEXT NOT NULL,  -- 'manual' | 'automatic'
    balance_before_payment   REAL NOT NULL CHECK (balance_before_payment >= 0),
    balance_after_payment    REAL NOT NULL CHECK (balance_after_payment >= 0),
    note                     TEXT,
    created_at               TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (transaction_id) REFERENCES transactions(id),
    FOREIGN KEY (debt_id) REFERENCES debts(id)
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
    -- `cash` and `net_worth` may legitimately be negative (overdraft,
    -- insolvency); only the totals are bounded.
    cash            REAL NOT NULL,
    total_assets    REAL NOT NULL CHECK (total_assets >= 0),
    total_liabilities REAL NOT NULL CHECK (total_liabilities >= 0),
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
    -- Bid/ask/last must be > 0 when set; NULLs represent unavailable sides.
    bid         REAL CHECK (bid IS NULL OR bid > 0),
    ask         REAL CHECK (ask IS NULL OR ask > 0),
    last        REAL CHECK (last IS NULL OR last > 0),
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
    -- Fee items are always >= 0; the engine never emits negative fee
    -- amounts. Schema-level CHECK so hand-crafted imports or direct DB
    -- writes can't smuggle a negative through.
    amount          REAL NOT NULL CHECK (amount >= 0),
    rate            REAL,
    notes           TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (transaction_id) REFERENCES transactions(id)
);

CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL
);

-- Bankruptcy events: persistent record of the moment a scheduled debt or
-- mortgage payment could not be funded even after all sellable assets
-- were liquidated. (The legacy `missed_payments` table was dropped in
-- schema v10; v10 migration converted any unresolved missed rows to
-- bankruptcy_events via the (trigger_kind, asset_id, due_date) idempotency
-- triple, then DROPped the table.) A bankruptcy_event is not a
-- "recoverable overdue payment" — it's the terminal failure state that
-- the simulator treats as game-over for the portfolio.
CREATE TABLE IF NOT EXISTS bankruptcy_events (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    event_date        TEXT NOT NULL,
    trigger_kind      TEXT NOT NULL,
    asset_id          INTEGER,
    due_date          TEXT,
    amount_due        REAL NOT NULL DEFAULT 0 CHECK (amount_due >= 0),
    -- `cash_balance` is the cash level at the moment of bankruptcy and
    -- is typically negative for the overdraft path — intentionally NOT
    -- constrained.
    cash_balance      REAL NOT NULL DEFAULT 0,
    shortfall_amount  REAL NOT NULL DEFAULT 0 CHECK (shortfall_amount >= 0),
    status            TEXT NOT NULL DEFAULT 'active',
    notes             TEXT,
    created_at        TEXT NOT NULL DEFAULT (datetime('now'))
);
