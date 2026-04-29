# Asset Trainer Local

A local-only desktop application for practicing multi-asset portfolio thinking — stocks, ETFs, crypto, real estate, cash, debt, and custom assets — built with Python and PySide6.

This is a **training simulator**, not a trading platform.

> ⚠️ **This project is for personal use only.**
> It is **not** a price predictor, **not** a trading recommendation engine, **not** a real-money trading bot, **not** a cloud app, and **not** financial advice. All data stays on your machine.

---

## Table of Contents

- [Features](#features)
- [Screens](#screens)
- [Tech Stack](#tech-stack)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Running the App](#running-the-app)
- [Troubleshooting](#troubleshooting)
- [Running the Tests](#running-the-tests)
- [Project Structure](#project-structure)
- [Data & Privacy](#data--privacy)
- [Supported Asset & Transaction Types](#supported-asset--transaction-types)
- [Risk Engine](#risk-engine)
- [Reports](#reports)
- [Market Data](#market-data)
- [Configuration](#configuration)
- [Limitations & Caveats](#limitations--caveats)
- [Contributing](#contributing)
- [License](#license)
- [Disclaimer](#disclaimer)

---

## Features

### Portfolio bookkeeping
- Single-source-of-truth ledger: cash balance, positions, cost basis, total assets, total liabilities, and net worth are all derived from the transaction history.
- Supports stocks, ETFs, crypto, real estate, cash, debt, and custom assets.
- Daily portfolio snapshots feed the net-worth trend chart and the period-end report data.

### Trade preview & confirmation
- Buy/Sell flow fetches a live bid/ask quote (via `yfinance`), falls back to the latest stored quote, and refuses to confirm without a usable execution price.
- Configurable broker commission (per-trade and basis points), optional SEC §31 and FINRA TAF regulatory fees on sell-side stock/ETF.
- A `:memory:` simulation shows allocation and risk **before vs after** the trade, so you can see what would change before clicking confirm.
- "Trade Amount" input mode auto-derives quantity from a target dollar value (whole shares only).

### Real estate
- Track existing properties (no cash impact) or new purchases (down payment deducted from cash).
- Auto-computes monthly mortgage from principal + rate + term.
- Per-property KPIs: equity, LTV, effective rent, operating expenses, net cash flow, annual NOI, cap rate, cash-on-cash return.
- Edit, sell, and delete; one-click "Settle Due Rent" creates monthly (or annual) `receive_rent` transactions from a configurable cashflow start date.

### Risk warnings
Observation-style warnings (never recommendations) for concentration, crypto exposure, low/negative cash, leverage, illiquidity, real-estate LTV, missing market prices, and unjournaled trades. Thresholds are user-tunable.

### Decision journal
- Capture pre-trade snapshot, thesis, intended role, risk reasoning, exit plan, confidence (1–5), and expected holding period.
- Add a post-trade review with mistake tags and lessons learned.
- Auto-computed five-component **training score** (diversification, liquidity, concentration, leverage, journal quality) plus an overall score.

### Reports
- Auto-generated monthly and annual reports for every completed period on startup.
- Sections: summary, cash-flow breakdown (funding / trade / real-estate / debt), approximate performance (snapshot-based), allocation, risk summary, operations, transactions, trades, real estate, debt, journal, current snapshot.
- Export selected report to **`.xlsx`** (multi-sheet workbook) or **`.txt`** (plain text with a "How To Read" preface).

### Import / Export
- **Simple CSV:** assets, transactions, summary.
- **Full backup:** zip or folder containing one CSV per table plus `manifest.csv`. Re-importing requires you to type `REPLACE DATA` and writes a pre-import `.bak.db` next to the existing database.
- **Export Reader** tab: open any prior full export and inspect each CSV in a table view (capped at 1000 rows for responsiveness).

### Market data sync
- Background sync of daily OHLCV and live bid/ask/last quotes for stocks, ETFs, and crypto using `yfinance`.
- Sync history log with success / partial / failed status per run.
- Per-asset and "sync all" controls; one-click installer for the optional `yfinance` dependency.

---

## Screens

The app has nine pages, accessible from the left-hand sidebar:

1. **Dashboard** — net worth + 30D change, cash, risk status, asset mix pie, return drivers, cash-flow snapshot, real-estate snapshot, recent activity.
2. **Transactions** — type-aware form for deposits, withdrawals, buy/sell preview-and-confirm, debt, rent income, and property expenses; built-in securities universe search.
3. **Asset Analysis** — balance sheet breakdown, positions table with unrealized P&L, allocation by type / asset / liquidity.
4. **Risk** — sortable, color-coded list of every active warning.
5. **Real Estate** — add / edit / sell / delete properties, settle due rent, view per-property analytics and warnings.
6. **Decision Journal** — pre-trade entry, post-trade review, before/after structure changes, training score, lessons learned.
7. **Import / Export** — CSV and full backup; reports tab; export reader tab.
8. **Data Sync** — yfinance dependency status, sync controls, market data table, sync history.
9. **Settings** — base currency (display only), risk thresholds, trading-cost configuration, FINRA TAF year customization, and a Data Management panel for asset deletion / bulk clear.

---

## Tech Stack

- **Python 3** with **PySide6** (Qt 6) for the GUI
- **SQLite** for persistent local storage (WAL mode, foreign keys ON)
- **pandas** + **openpyxl** for tabular data and Excel report export
- **matplotlib** for dashboard and allocation charts
- **plotly** (in the dependency list; matplotlib does the heavy lifting in the current code)
- **yfinance** for stock / ETF / crypto market data (optional — the app runs without it, but the trade-preview Buy/Sell flow needs a quote)
- **pytest** for the test suite

---

## Requirements

From [`requirements.txt`](requirements.txt):

```
PySide6>=6.6,<7
pandas>=2.0,<3
numpy>=1.24,<3
matplotlib>=3.7,<4
plotly>=5.15,<7
pytest>=7.0,<10
yfinance>=0.2,<0.3
openpyxl>=3.1,<4
python-dateutil>=2.8,<3
```

A reasonably recent Python 3 is required (PySide6 6.6+ is supported on Python 3.9+). The source-tree double-click launchers prefer **Python 3.12 or 3.13**, which is the range whose PySide6 / yfinance / pandas wheels are tested.

---

## Quick Start

There are two supported ways to run the app. Pick whichever matches your situation.

### 1. Recommended for normal users — download a packaged release

If you don't want to install Python or run anything from a terminal, download the pre-built release for your OS from the project's GitHub Releases page:

- **macOS:** download `PortfolioTrainer-macOS.zip`, unzip it, and double-click `Portfolio Trainer.app`.
- **Windows:** download `PortfolioTrainer-Windows.zip`, unzip it, and double-click `Portfolio Trainer.exe` inside the unzipped folder.

These are unsigned builds. The first time you launch them you'll have to confirm the OS warning:

- **macOS:** the system may say *"Portfolio Trainer cannot be opened because the developer cannot be verified."* Right-click the `.app` and choose **Open**, then click **Open** in the dialog. After the first launch, double-click works normally.
- **Windows:** SmartScreen may show *"Windows protected your PC."* Click **More info** → **Run anyway**.

The release build has every dependency bundled inside, so no Python install or `pip` is needed. The Data Sync page hides the in-app *Install Dependencies* button when running from a packaged release.

### 2. Source users — double-click launchers

If you cloned this repository from GitHub and want to run from source, you can still skip the manual `venv` / `pip` dance:

- **macOS:** double-click `Launch Portfolio Trainer.command` at the repo root.
- **Windows:** double-click `Launch Portfolio Trainer.bat` at the repo root.

Each launcher delegates to [`scripts/bootstrap_launcher.py`](scripts/bootstrap_launcher.py), which:

1. Picks a usable interpreter (preferring **Python 3.12 or 3.13**).
2. Creates `.venv/` in the project root if it doesn't exist.
3. Installs `requirements.txt` into that venv on first launch (and re-installs only when `requirements.txt` changes — a marker file caches the last-installed hash).
4. Starts `main.py` using the venv interpreter.
5. Writes a launcher log to `.launcher/launcher.log`.

**First-launch requirements:**
- **Python 3.12 or 3.13** must already be installed and on `PATH`. (Get it from [python.org/downloads](https://www.python.org/downloads/).)
- An **internet connection** so `pip` can download dependencies.
- The first launch can take several minutes while wheels download. Subsequent launches are fast.

**macOS Gatekeeper note:** if the system blocks `Launch Portfolio Trainer.command` because the file came from the internet, right-click it in Finder and choose **Open** the first time.

---

## Installation

If you'd rather wire up the venv yourself instead of using `Launch Portfolio Trainer.*`:

```bash
git clone https://github.com/<your-username>/asset-trainer-local.git
cd asset-trainer-local
python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

If you skip `yfinance` at first, the Data Sync page provides a one-click "Install Dependencies" button that runs `pip install -r requirements.txt` for you. (This button is hidden in packaged release builds, since dependencies are bundled there.)

---

## Running the App

```bash
python main.py
```

The window is titled **"Multi-Asset Portfolio Trainer"**. The sidebar exposes the nine pages described above.

On the first launch the app will:

1. Initialize (or migrate) the SQLite database at the per-user location below.
2. If `yfinance` is installed and any syncable asset already exists, run an initial market-data sync in the background.
3. Generate any missing monthly / annual reports for completed periods.
4. Record a daily portfolio snapshot for today.

---

## Troubleshooting

### "Python not found" / launcher closes immediately

The double-click launchers need a Python 3 interpreter on `PATH`.

- Install **Python 3.12** or **3.13** from [python.org/downloads](https://www.python.org/downloads/).
- On macOS, after installing, run the bundled `Install Certificates.command` once so `pip` can fetch packages over HTTPS.
- On Windows, when running the python.org installer **tick "Add python.exe to PATH"** on the first screen.
- Re-run `Launch Portfolio Trainer.command` / `.bat`.

If you'd rather not install Python at all, use the packaged GitHub Release instead — it ships its own Python and dependencies inside the bundle.

### Dependency install failed on first launch

If the bootstrap launcher errors out during `pip install`:

1. Check that you have an active internet connection.
2. Re-run the launcher — `pip` reuses already-downloaded wheels, so retries are cheap.
3. If a specific package keeps failing, delete `.venv/` at the repo root and try again with Python 3.12 or 3.13.
4. As a last resort, use the packaged release build instead of the source launcher.

### Where are the logs?

| Log                 | Path                                                                  |
| ------------------- | --------------------------------------------------------------------- |
| Launcher (bootstrap) | `<repo>/.launcher/launcher.log` (rotates at ~512 KB)                  |
| App (runtime)       | macOS: `~/Library/Logs/asset-trainer/app.log`                         |
|                     | Windows: `%LOCALAPPDATA%\asset-trainer\Logs\app.log`                  |
|                     | Linux: `$XDG_STATE_HOME/asset-trainer/logs/app.log`                   |

The launcher log is the right place to look for setup / venv / pip errors. The app log is the right place to look for crashes or sync errors **after** the GUI has come up.

### When should I use the Release build instead of the source launcher?

Use the packaged Release if any of these apply:

- You don't have Python 3.12 or 3.13 and don't want to install it.
- You don't want to wait through a one-time `pip install` on the first run.
- You want a self-contained `.app` / `.exe` you can move to another folder and double-click.
- You're sharing the app with someone non-technical.

Use the source launcher if you want to tweak the code, run tests, or stay on the bleeding edge of the repo.

---

## Running the Tests

```bash
pytest
```

The repository ships with a comprehensive test suite (~35k lines including tests) covering engines, repositories, GUI lifecycle, and stress scenarios. Custom pytest markers are defined in [`pytest.ini`](pytest.ini) (`stress_phase0` … `stress_phase4`, `stress_gui`, `stress_extreme`) so you can run subsets:

```bash
pytest -m "not stress_extreme"      # skip the extreme-scale stress tests
pytest -m stress_phase1             # only structure & integrity checks
```

---

## Project Structure

```
.
├── main.py                          # Entry point
├── requirements.txt
├── pytest.ini
├── README.md                        # ← this file
├── USER_GUIDE_FROM_CODE.txt         # Detailed feature-by-feature user guide
├── CLAUDE.md                        # Project conventions
├── PortfolioTrainer.spec            # PyInstaller build spec
├── Mac Launch Portfolio Trainer.command     # macOS double-click source launcher
├── Windows Launch Portfolio Trainer.bat     # Windows double-click source launcher
├── scripts/
│   └── bootstrap_launcher.py        # Cross-platform venv + launch logic
├── .github/workflows/
│   └── build-desktop.yml            # macOS + Windows release builds on tag
├── src/
│   ├── models/              # Dataclasses (Asset, Transaction, Property, etc.)
│   ├── storage/             # SQLite schema + per-table repositories
│   ├── engines/             # Business logic (no UI):
│   │   ├── portfolio.py        # Cash, positions, summary
│   │   ├── allocation.py       # Allocation breakdowns
│   │   ├── risk.py             # Warning checks
│   │   ├── real_estate.py      # Per-property analytics
│   │   ├── property_calculator.py
│   │   ├── ledger.py           # Write-side transaction creators
│   │   ├── trade_preview.py    # Preview / confirm pipeline
│   │   ├── trading_costs.py    # Broker / SEC / FINRA fees
│   │   ├── pricing_engine.py   # Sync orchestration
│   │   ├── price_sync_worker.py
│   │   ├── reports.py          # Monthly / annual report builder
│   │   ├── report_export.py    # .xlsx / .txt writers
│   │   ├── snapshots.py        # Daily portfolio snapshots
│   │   ├── journal.py          # Snapshots, structure changes, score
│   │   ├── data_management.py  # Delete / clear flows
│   │   ├── import_export.py    # Simple CSV
│   │   ├── full_data_io.py     # Full-backup zip / folder
│   │   ├── dashboard.py        # Dashboard summary helpers
│   │   ├── holdings.py         # Per-asset quantity SQL
│   │   └── security_universe_engine.py
│   ├── gui/
│   │   ├── main_window.py      # Sidebar + page stack
│   │   ├── pages/              # One file per nav page
│   │   └── widgets/common.py   # Shared formatters / table helpers
│   ├── charts/                 # matplotlib figure builders
│   ├── data_sources/           # yfinance providers + static security universe
│   └── utils/                  # logging, dates, deps, display labels
└── tests/                      # pytest suite (~40 files)
```

The GUI must not contain financial calculations — all derivation logic lives in `src/engines/`.

---

## Data & Privacy

All user data stays on your machine. The only network calls are made by `yfinance` when you explicitly sync market data or preview a buy/sell of a syncable asset.

User data lives in a per-user OS directory **outside** the app folder, so deleting / re-extracting / upgrading the release `.app` or `.exe` does not touch your portfolio. The exact paths are below.

### Database location

Resolved by `src/storage/database.py:_resolve_default_db_path`:

| OS      | Path                                                                 |
| ------- | -------------------------------------------------------------------- |
| macOS   | `~/Library/Application Support/asset-trainer/portfolio_simulator.db` |
| Windows | `%APPDATA%\asset-trainer\portfolio_simulator.db`                     |
| Linux   | `$XDG_DATA_HOME/asset-trainer/portfolio_simulator.db`<br>(falls back to `~/.local/share/asset-trainer/portfolio_simulator.db`) |

The database uses SQLite WAL mode, so you'll see `-wal` and `-shm` sidecar files while the app is open.

A legacy database at `./data/portfolio_simulator.db` (used by older builds) is automatically **moved** to the new location on first startup.

### Log file location

Resolved by `src/utils/app_logging.py:get_log_dir` — rotating at 2 MB with up to 4 files retained:

| OS      | Path                                              |
| ------- | ------------------------------------------------- |
| macOS   | `~/Library/Logs/asset-trainer/app.log`            |
| Windows | `%LOCALAPPDATA%\asset-trainer\Logs\app.log`       |
| Linux   | `$XDG_STATE_HOME/asset-trainer/logs/app.log`      |

### Backup

Use **Import / Export → Full Backup → Export Full Data** to dump every table to a `.zip` or folder. The "Import Full Data" path requires you to type `REPLACE DATA` and writes a `<dbname>.<timestamp>.pre-import.bak.db` alongside your existing database before replacing rows.

---

## Supported Asset & Transaction Types

### Asset types
`stock`, `etf`, `crypto`, `real_estate`, `cash`, `debt`, `custom`

Of these, `stock`, `etf`, `crypto`, and `custom` are tradeable through the buy/sell ledger; `real_estate` and `debt` have their own dedicated flows.

### Transaction types
`deposit_cash`, `withdraw_cash`, `buy`, `sell`,
`add_property`, `update_property_value`, `receive_rent`, `pay_property_expense`, `pay_mortgage`, `sell_property`,
`add_debt`, `pay_debt`, `manual_adjustment`

A few of these (`manual_adjustment`, `update_property_value`, `pay_mortgage`, and the `planned_purchase` property entry mode) are accepted by the ledger and CSV importer but are not exposed in the GUI's transaction-type dropdown — they can only be created via CSV import or the full-data importer.

---

## Risk Engine

Risk warnings are observations, not recommendations. The wording in `src/engines/risk.py` is intentionally non-prescriptive — for example:

- *"This portfolio is concentrated in {asset}. It represents {pct}% of net worth."*
- *"Cash balance is below the selected threshold ({threshold}%). Cash is {pct}% of total assets."*
- *"Debt ratio is above the selected threshold ({threshold}%)."*

You will never see things like *"You should sell..."* or *"This is a good investment."*

| Check                  | Severity         | Trigger                                                       |
| ---------------------- | ---------------- | ------------------------------------------------------------- |
| Concentration          | medium / high    | Any non-cash item > `concentration_threshold` / > 40%         |
| Crypto exposure        | high             | Crypto % of total assets > `crypto_threshold`                 |
| Cash                   | critical         | Cash balance is negative                                      |
| Low cash               | medium           | Cash % of total assets < `low_cash_threshold`                 |
| Leverage               | high             | Liabilities / total assets > `debt_threshold`                 |
| Illiquidity            | medium           | Illiquid assets / net worth > 60%                             |
| Real estate LTV        | high             | Per-property LTV > 80%                                        |
| Missing prices         | info             | Held syncable position with no `market_prices` row            |
| Missing journal        | info             | Buy/sell with no linked decision journal entry                |
| Real estate cash flow  | high / medium    | Negative monthly cash flow / vacancy > 10%                    |
| Real estate weight     | medium           | Real estate > 50% of net worth                                |

---

## Reports

Monthly and annual reports include 12 sections: summary, cash-flow breakdown, performance (approximate), allocation, risk summary, operations, transactions, trades, real estate, debt, journal, current snapshot.

> **Important caveats** (from the report's "How To Read" preface):
>
> - **Net Cash Flow is cash movement, not profit.** Depositing money increases Net Cash Flow without producing any gain.
> - **Performance metrics are approximate** — snapshot-based, not time-weighted, and they don't separate realized vs unrealized P&L.
> - **The Risk Summary always reflects current state at generation time**, not period-end state, because risk warnings aren't stored historically.

Export to `.xlsx` (multi-sheet) or `.txt` (with the same "How To Read" text up top).

---

## Market Data

`yfinance` is the only external data source. Two providers are wired up:

- `YFinanceProvider` for stocks and ETFs (`ticker.history` for daily OHLCV; `ticker.info` for bid/ask/last).
- `YFinanceCryptoProvider` for crypto (auto-appends `-USD` to symbols, then uses the same yfinance API).

**When sync runs:**
- On app startup, if yfinance is installed and at least one stock/etf/crypto asset exists.
- When you click **Sync All Market Data** or **Sync Selected Asset** on the Data Sync page.
- When you preview a Buy/Sell on a syncable asset (a fresh sync is attempted; failures fall back to stored data).

**If sync fails** the trade preview surfaces a blocking error and the trade cannot be confirmed. A row is appended to `price_sync_log` with the result.

---

## Configuration

Settings are stored in the `settings` table (key/value rows) and edited from the Settings page.

| Key                              | Default | Notes                                                            |
| -------------------------------- | ------- | ---------------------------------------------------------------- |
| `base_currency`                  | `USD`   | Display only — **no FX conversion is performed anywhere**        |
| `low_cash_threshold`             | `0.05`  | Cash % below this fires a "medium" warning                       |
| `concentration_threshold`        | `0.25`  | Single-asset % above this fires "medium"; > 40% fires "high"     |
| `crypto_threshold`               | `0.20`  | Crypto % above this fires "high"                                 |
| `debt_threshold`                 | `0.50`  | Liabilities/assets above this fires "high"                       |
| `broker_commission_per_trade`    | `0`     | USD per buy/sell                                                 |
| `broker_commission_rate_bps`     | `0`     | Basis points of trade value                                      |
| `auto_apply_regulatory_fees`     | `0`     | Toggles SEC §31 + FINRA TAF on sell-side stock/ETF only          |
| `sec_section31_rate_per_million` | `0`     | USD per $1M of proceeds                                          |
| `finra_taf_custom_json`          | `{}`    | Optional per-year overrides for FINRA TAF rates (2024–2029 presets shipped) |

Threshold values are stored as fractions (`0.05` = 5%); the GUI reads/writes them as percentages.

---

## Limitations & Caveats

- **No FX conversion.** The `currency` and `region` fields on assets and transactions are stored, but values are summed without conversion. Mixing currencies will produce misleading totals. Allocation by currency / region is computed by the engine but not surfaced in the GUI.
- **Trade preview requires a quote.** Without `yfinance` and a recent sync, you cannot record a buy or sell of a stock / ETF / crypto through the GUI. `custom` assets accept a manual price.
- **Trade Amount mode floors to whole shares.** Even crypto can only be bought / sold in whole units through this path.
- **Edit Property dialog** uses raw monthly values and a 0–1 vacancy fraction; it does not re-run frequency normalization the way the Add form does.
- **Bulk-delete actions are permanent** — "Clear All Properties", "Clear All Assets", "Clear All Data", and full-data import all destroy data. Always export first.
- **Reports are not P&L statements.** See the caveats above.
- **`manual_adjustment`, `update_property_value`, `pay_mortgage`, `planned_purchase`** can only be created via CSV / full-data import (not from the GUI's transaction-type dropdown).
- This project includes investigation/audit notes (`crash_investigation.md`, `STRESS_TEST_BUG_REPORT.md`, etc.) reflecting ongoing hardening work — running `pytest` before relying on critical features is recommended.

For an exhaustive feature-by-feature walkthrough see [`USER_GUIDE_FROM_CODE.txt`](USER_GUIDE_FROM_CODE.txt).

---

## Contributing

Contributions are welcome. A few project conventions worth knowing (also in [`CLAUDE.md`](CLAUDE.md)):

- **Transactions are the source of truth.** Don't store derived values; compute them.
- **Keep the GUI calculation-free.** Financial logic lives in `src/engines/`.
- **Risk warnings are observations**, never recommendations.
- **All data stays local.** No cloud calls beyond optional `yfinance` market data.
- After changes, run:
  ```bash
  pytest
  python -c "from src.storage.database import init_db; init_db(':memory:')"
  python main.py            # quick smoke test that the window opens
  ```

If you find a bug, please open an issue with the relevant log file (`app.log`, see paths above).

---

## License

No license file is currently included in the repository. If you intend to fork or redistribute, please contact the author or open an issue requesting a license to be added.

---

## Disclaimer

This software is provided **for educational and personal-training purposes only**. It does not provide financial, investment, tax, or legal advice. Nothing in the application or its output is a recommendation to buy or sell any security or asset. The authors and contributors accept no liability for decisions made on the basis of this software. Always consult a licensed financial professional before making real investment decisions.
