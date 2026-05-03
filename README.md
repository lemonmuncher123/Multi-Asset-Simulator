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
- [Bankruptcy & Auto-Settle Pipeline](#bankruptcy--auto-settle-pipeline)
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
- Single-source-of-truth ledger: cash balance, positions, cost basis, total assets, total liabilities, and net worth are all derived from the transaction history. No engine-level caches.
- Supports stocks, ETFs, crypto, real estate, cash, debt, and custom assets.
- Daily portfolio snapshots feed the net-worth trend chart and the period-end report data.

### Trade preview & confirmation
- Buy/Sell flow fetches a live bid/ask quote (via `yfinance`), falls back to the latest stored quote, and refuses to confirm without a usable execution price.
- Configurable broker commission (per-trade and basis points), optional SEC §31 and FINRA TAF regulatory fees on sell-side stock/ETF.
- A `:memory:` simulation shows allocation and risk **before vs after** the trade, so you can see what would change before clicking confirm. The after-state copies the user's `settings`, `decision_journal`, properties, debts, and mortgages so the preview matches reality.
- "Trade Amount" input mode auto-derives quantity from a target dollar value. Stock/ETF floor to whole shares; crypto and custom assets allow 8-decimal fractional units.

### Real estate & mortgages
- Track existing properties (no cash impact), new purchases (down payment deducted from cash), or planned scenarios.
- Mortgages live in their own `mortgages` table linked 1:1 to a property. Each mortgage has the same plan-type semantics as a debt — `fixed_payment` (fix the per-period payment, derive the term) or `fixed_term` (fix the term, derive the payment) — plus 5 stored preview values, "Pay Off in Full" support, and per-payment audit records.
- Down payment auto-derives from `purchase_price - original_loan_amount`. The mortgage origination date always equals the property purchase date.
- Per-property KPIs: equity, LTV, effective rent, operating expenses, net cash flow, annual NOI, cap rate, cash-on-cash return.
- Edit, sell, and delete; one-click "Settle Due Rent" creates monthly (or annual) `receive_rent` transactions from a configurable cashflow start date. `sell_property` settles the active mortgage in full first (writing a separate `pay_mortgage` transaction), then writes the `sell_property` row.

### Debt
- Add Debt requires exactly one of `payment_per_period` or `term_periods`; the engine derives the other via the standard annuity formula.
- Two creation modes: a **fresh loan** (cash inflow at recorded date) or an **existing loan** (origination date in the past — the engine walks the amortization forward to today and persists today's balance).
- Live "Schedule summary" preview as you fill the form; the 5 stored `preview_*` values mirror the live current plan and refresh on every Add Debt confirm, partial pay, full payoff, and scheduled auto-pay.
- Pay Debt supports partial payments and a "Pay Off in Full" button that charges principal + this period's accrued interest and stamps `last_payment_date`.
- Interest is always annual; schedules are monthly or yearly.

### Risk warnings
Observation-style warnings (never recommendations) for concentration, crypto exposure, low/negative cash, leverage, illiquidity, real-estate LTV, missing market prices, unjournaled trades, debt payoff horizon, debt affordability runway, and bankruptcy. Thresholds are user-tunable. The Real Estate page surfaces additional per-property warnings (negative cash flow, high vacancy, missing value, real-estate share of net worth) alongside its analytics.

### Bankruptcy & auto-settle
- An auto-settle pipeline runs on every app launch, on every data change, and on a 30-minute day-boundary timer. It credits effective rent, deducts monthly property opex, fires scheduled debt and mortgage payments, force-sells assets when cash falls short, and records a **bankruptcy event** when an obligation cannot be funded after force-selling everything sellable.
- **Bankruptcy is terminal.** Every public ledger write checks an engine-level lock; auto-settle internals carry a bypass so scheduled obligations keep firing even after bankruptcy is declared. Depositing more cash will not "un-bankrupt" the simulator.
- A red top-level banner sits above every page when bankruptcy is active and names the obligation that triggered it.

### Decision journal
- Capture pre-trade snapshot, thesis, intended role, risk reasoning, exit plan, confidence (1–5), and expected holding period.
- Add a post-trade review with mistake tags and lessons learned.
- Auto-computed five-component **training score** (diversification, liquidity, concentration, leverage, journal quality) plus an overall score.

### Reports
- Auto-generated monthly and annual reports for every completed period on startup.
- Sections: summary, cash-flow breakdown (funding / trade / real-estate / debt / fees / other), approximate performance (snapshot-based, period-end snapshot informed), allocation, risk summary, operations, transactions, trades, real estate, debt, journal, current snapshot, beginning + ending snapshots.
- Export selected report to **`.xlsx`** (multi-sheet workbook) or **`.txt`** (plain text with a "How To Read" preface).

### Cashflow analytics
- Stacked-bar chart: 12-month cashflow on the Dashboard; selectable Monthly/Yearly granularity on the Asset Analysis page (chart + table). Bars stack the same five categories the report uses; the black net line traces each period's total. Cash basis — lumpy payments stay in the month they occurred.

### Import / Export
- **Simple CSV:** assets, transactions, summary. The CSV importer is restricted to `deposit_cash`, `withdraw_cash`, `buy`, `sell`, `receive_rent`, `pay_property_expense`, and `manual_adjustment` — types that need coordinated writes to debts/mortgages/properties have to use Full Data Import instead.
- **Full backup:** zip or folder containing one CSV per table plus `manifest.csv`. The manifest carries both the file-format `schema_version` and the source DB's `db_schema_version`. Re-importing requires you to type `REPLACE DATA` and writes a pre-import `.bak.db` next to the existing database. Field-level validators reject negative amounts before the import touches the DB.
- **Export Reader** tab: open any prior full export and inspect each CSV in a table view (capped at 1000 rows for responsiveness).

### Market data sync
- Background sync of daily OHLCV and live bid/ask/last quotes for stocks, ETFs, and crypto using `yfinance` (a `QThread` worker with its own DB connection).
- Sync history log with success / partial / failed status per run.
- Per-asset and "sync all" controls; one-click installer for the optional `yfinance` dependency in source builds.

### Defense in depth
- **Single-instance guard.** A `QLockFile` plus a per-user `QLocalServer` socket prevent two app instances from racing the same database. A second launch activates the existing window and exits.
- **Engine-level negative-input rejection** (schema v12). Every numeric user input is validated at five layers: GUI submit handlers, engine assertions, CSV row validators, full-data import validators, and SQLite `CHECK` constraints. Negative balances, payments, rates, and quantities are rejected before they can corrupt derived calculations.

---

## Screens

The app has nine pages, accessible from the left-hand sidebar:

1. **Dashboard** — net worth + 30D change, cash, risk status, asset mix pie, net-worth trend, return drivers, 12-month cashflow stacked bar, cash-flow snapshot, real-estate snapshot, recent activity. Layout reflows for narrow widths.
2. **Transactions** — type-aware form for deposits, withdrawals, buy/sell preview-and-confirm, debt operations (Add Debt, Pay Debt with extra-payment or "Pay Off in Full"), mortgage payment ("Pay Mortgage" with "Pay Off in Full"), and property expense; built-in securities universe search.
3. **Asset Analysis** — balance sheet breakdown, positions table with unrealized P&L, allocation by type / asset / liquidity, debts and mortgages tables, monthly/yearly cashflow chart and table.
4. **Risk** — sortable, color-coded list of every active warning.
5. **Real Estate** — add / edit / sell / delete properties, settle due rent, view per-property analytics and warnings.
6. **Decision Journal** — pre-trade entry, post-trade review, before/after structure changes, training score, lessons learned.
7. **Import / Export** — CSV and full backup tab; Reports tab (generate, export, delete); Export Reader tab.
8. **Data Sync** — yfinance dependency status, sync controls, market data + execution-quote table, sync history.
9. **Settings** — base currency (display only), risk thresholds, max debt payoff cap, default debt annual rate, trading-cost configuration, FINRA TAF year customization, and a Data Management panel for asset deletion / bulk clear.

---

## Tech Stack

- **Python 3** with **PySide6** (Qt 6) for the GUI
- **SQLite** for persistent local storage (WAL mode, foreign keys ON, schema version 12)
- **pandas** + **openpyxl** for tabular data and Excel report export
- **matplotlib** for dashboard, allocation, and cashflow charts
- **plotly** (in the dependency list; matplotlib does the rendering in the current code)
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

These are unsigned (and un-notarized) builds. The first time you launch them you'll have to confirm the OS warning:

- **macOS** — the steps depend on your macOS version:
  - **macOS 15 (Sequoia) and newer:** the right-click → Open bypass no longer exists in this version. The dialog you see only has *"Done"* and *"Move to Trash"* buttons — that is expected, the app has not been damaged.
    1. Click **Done** to dismiss the dialog.
    2. Open **System Settings → Privacy & Security**, scroll down to the *Security* section, and you should see *"Portfolio Trainer was blocked to protect your Mac."* — click **Open Anyway** next to it.
    3. Re-launch the app from Finder; macOS will now show a confirmation dialog with **Open Anyway** — click it and enter your password.
    4. *Terminal alternative:* run `xattr -dr com.apple.quarantine "/path/to/Portfolio Trainer.app"` once. After that, double-click works normally with no further prompts.
  - **macOS 14 (Sonoma) and older:** right-click the `.app` and choose **Open**, then click **Open** in the confirmation dialog. After the first launch, double-click works normally.
- **Windows:** SmartScreen may show *"Windows protected your PC."* Click **More info** → **Run anyway**.

The release build has every dependency bundled inside, so no Python install or `pip` is needed. The Data Sync page hides the in-app *Install Dependencies* button when running from a packaged release.

### 2. Source users — double-click launchers

If you cloned this repository from GitHub and want to run from source, you can still skip the manual `venv` / `pip` dance:

- **macOS:** double-click `Mac Launch Portfolio Trainer.command` at the repo root.
- **Windows:** double-click `Windows Launch Portfolio Trainer.bat` at the repo root.

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

**macOS Gatekeeper note:** if the system blocks `Mac Launch Portfolio Trainer.command` because the file came from the internet, right-click it in Finder and choose **Open** the first time.

---

## Installation

If you'd rather wire up the venv yourself instead of using the `Mac Launch Portfolio Trainer.command` / `Windows Launch Portfolio Trainer.bat` double-click launchers:

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

1. Acquire a single-instance `QLockFile` so two copies can't race the same database.
2. Initialize (or migrate) the SQLite database at the per-user location below.
3. If `yfinance` is installed and any syncable asset already exists, run an initial market-data sync in the background.
4. Run the auto-settle pipeline (rent → property opex → scheduled debt → scheduled mortgage → force-sell mop-up → bankruptcy events).
5. Generate any missing monthly / annual reports for completed periods.
6. Record a daily portfolio snapshot for today.

A 30-minute timer keeps the auto-settle pipeline current if the app stays open across midnight.

---

## Troubleshooting

### macOS: *"Apple could not verify 'Portfolio Trainer' is free of malware"* (Sequoia / macOS 15+)

The release builds are not signed with an Apple Developer ID and not notarized. On macOS 15 (Sequoia) the old **right-click → Open** bypass has been removed — the dialog now only offers *Done* and *Move to Trash*.

To launch the app:

1. Click **Done** on the warning dialog.
2. Open **System Settings → Privacy & Security**, scroll to the *Security* section, find *"Portfolio Trainer was blocked to protect your Mac"*, and click **Open Anyway**.
3. Launch the app again — confirm with **Open Anyway** and your password.

Or, equivalently, from Terminal:

```bash
xattr -dr com.apple.quarantine "/path/to/Portfolio Trainer.app"
```

This removes the download-quarantine attribute that triggers Gatekeeper. After it's gone, double-click works normally.

The app is already ad-hoc code-signed by PyInstaller; the warning is purely about notarization, which requires a paid Apple Developer account. The source-launcher route (`Mac Launch Portfolio Trainer.command`) avoids the issue entirely because nothing inside the repo is quarantined.

### "Python not found" / launcher closes immediately

The double-click launchers need a Python 3 interpreter on `PATH`.

- Install **Python 3.12** or **3.13** from [python.org/downloads](https://www.python.org/downloads/).
- On macOS, after installing, run the bundled `Install Certificates.command` once so `pip` can fetch packages over HTTPS.
- On Windows, when running the python.org installer **tick "Add python.exe to PATH"** on the first screen.
- Re-run `Mac Launch Portfolio Trainer.command` / `Windows Launch Portfolio Trainer.bat`.

If you'd rather not install Python at all, use the packaged GitHub Release instead — it ships its own Python and dependencies inside the bundle.

### "I moved the folder and now `Mac Launch Portfolio Trainer.command` doesn't open"

Three things bite users after moving the project folder around (especially after extracting a freshly-downloaded zip from GitHub into a new location):

1. **macOS Privacy & Security blocks file access in protected folders** *(most common, especially since macOS Catalina)*. macOS automatically protects:
    - `~/Desktop`
    - `~/Documents`
    - `~/Downloads`
    - iCloud Drive (`~/Library/Mobile Documents/...`)

    Terminal can `cd` into these locations, but it can't *read files* in them until you grant permission per app. Python launched from the launcher counts as a different "responsible app" from Terminal for permission purposes — so even if Terminal works, Python may fail with `[Errno 1] Operation not permitted` and the launcher exits with status 2.

    The launcher now detects this and prints a clear message. The fix is one of:
    - **Move the folder out of the protected location** (easiest). Anywhere not in the list above works — for example `~/Projects/multi-asset-simulator`, `~/Applications/multi-asset-simulator`, or just `~/multi-asset-simulator`.
    - **Grant Terminal access**: System Settings → Privacy & Security → Files and Folders → find Terminal → enable the relevant folder (Desktop / Documents / Downloads). Then re-run the launcher.

2. **macOS quarantine.** Files from a download stay quarantined. Finder will refuse to run a quarantined `.command` file even after you've moved it. The launcher self-clears quarantine on the rest of the folder *after* it starts, so subsequent moves and double-clicks work seamlessly — but the **first** double-click in any new copy still needs the user to bypass Gatekeeper once. Right-click the `.command` → **Open** → confirm in the dialog. Or, equivalently from Terminal:
    ```bash
    xattr -cr "/path/to/multi-asset-simulator"
    ```
    After the first successful run, the launcher de-quarantines everything for you.

3. **Lost executable bit.** Some unzip tools (especially cross-platform ones) and cloud-sync clients (iCloud Drive, Dropbox, OneDrive) strip the `+x` bit when they touch the file. Then double-click opens the `.command` in TextEdit instead of running it. Repair from Terminal:
    ```bash
    chmod +x "/path/to/Mac Launch Portfolio Trainer.command"
    ```

If you're inside an iCloud-synced location, also check that `.venv/`'s symlinks haven't been evicted to the cloud — moving to a non-synced location like `~/Projects/` or `~/Applications/` avoids that whole class of failure.

The bootstrap launcher also detects when `.venv/` was built in a different location and **rebuilds it automatically** for the current folder. The first launch from the new location will take a few minutes (one-time pip re-install); subsequent launches are fast again.

**Windows equivalent.** macOS-specific TCC permissions (the EPERM issue above) don't exist on Windows — file reads in `~\Desktop`, `~\Documents`, `~\Downloads` are unrestricted. But Windows has its own analogous failure mode: **OneDrive Files On-Demand**. On Windows 10/11, Documents and Desktop are OneDrive-synced by default, and individual files can be cloud-only placeholders. When `Windows Launch Portfolio Trainer.bat` tries to read a file in `.venv/`, OneDrive should auto-download it — but during partial sync, network drops, or first-extract scenarios, the file can be missing entirely. The launcher detects this case and prints an actionable error pointing at three repair options (open the folder in Explorer to force a full sync, re-extract the zip, or move the folder to a non-synced location like `C:\Users\<you>\Projects\`). It also warns when the cwd path contains "OneDrive" so you're not surprised if pip-install fails partway through.

### Dependency install failed on first launch

If the bootstrap launcher errors out during `pip install`:

1. Check that you have an active internet connection.
2. Re-run the launcher — `pip` reuses already-downloaded wheels, so retries are cheap.
3. If a specific package keeps failing, delete `.venv/` at the repo root and try again with Python 3.12 or 3.13.
4. As a last resort, use the packaged release build instead of the source launcher.

### Where are the logs?

| Log                  | Path                                                                  |
| -------------------- | --------------------------------------------------------------------- |
| Launcher (bootstrap) | `<repo>/.launcher/launcher.log` (rotates at ~512 KB)                  |
| App (runtime)        | macOS: `~/Library/Logs/asset-trainer/app.log`                         |
|                      | Windows: `%LOCALAPPDATA%\asset-trainer\Logs\app.log`                  |
|                      | Linux: `$XDG_STATE_HOME/asset-trainer/logs/app.log`                   |

The launcher log is the right place to look for setup / venv / pip errors. The app log is the right place to look for crashes or sync errors **after** the GUI has come up. App logs rotate at 2 MB with up to 4 files retained.

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

The test suite ships with a `conftest.py` that sets `QT_QPA_PLATFORM=offscreen`, provides a `:memory:` `db_conn` per test, and patches Qt's unsafe synchronous teardown. Custom pytest markers are defined in [`pytest.ini`](pytest.ini):

| Marker            | Scope                                                                |
| ----------------- | -------------------------------------------------------------------- |
| `stress_phase0`   | Sanity / harness validation                                          |
| `stress_phase1`   | Base dataset structure & integrity                                   |
| `stress_phase2`   | Base dataset read-side engines                                       |
| `stress_phase3`   | Base dataset reports + per-report export                             |
| `stress_phase4`   | Base dataset full-data export/import roundtrip                       |
| `stress_gui`      | Offscreen GUI navigation against the base dataset                    |
| `stress_extreme`  | Stress variants run against the extreme-scale dataset                |
| `stress_cashflow` | Auto-settle, force-sell, and bankruptcy scenarios                    |

Run a subset with `-m`, e.g.:

```bash
pytest -m "not stress_extreme"      # skip the extreme-scale stress tests
pytest -m stress_phase1             # only structure & integrity checks
pytest -m stress_cashflow           # cashflow / bankruptcy scenarios
```

---

## Project Structure

```
.
├── main.py                          # Entry point (single-instance lock + window activation)
├── requirements.txt
├── pytest.ini
├── README.md                        # ← this file
├── PortfolioTrainer.spec            # PyInstaller build spec
├── Mac Launch Portfolio Trainer.command     # macOS double-click source launcher
├── Windows Launch Portfolio Trainer.bat     # Windows double-click source launcher
├── scripts/
│   └── bootstrap_launcher.py        # Cross-platform venv + launch logic
├── .github/workflows/
│   └── build-desktop.yml            # macOS + Windows release builds on tag
├── src/
│   ├── models/                 # Dataclasses (Asset, Transaction, Property, Debt, Mortgage, etc.)
│   ├── storage/                # SQLite schema + per-table repositories
│   │   ├── schema.sql
│   │   ├── database.py             # init_db, migration runner (schema v1 → v12)
│   │   └── *_repo.py               # one repo module per table
│   ├── engines/                # Business logic (no UI):
│   │   ├── portfolio.py            # Cash, positions, summary
│   │   ├── allocation.py           # Allocation breakdowns
│   │   ├── risk.py                 # Warning checks + bankruptcy predicate
│   │   ├── real_estate.py          # Per-property analytics
│   │   ├── property_calculator.py
│   │   ├── ledger.py               # All write paths + auto-settle helpers + bankruptcy lock
│   │   ├── debt_math.py            # Pure amortization (debts and mortgages share)
│   │   ├── force_sell.py           # Plan-then-execute force-sell engine (Spec §11 ordering)
│   │   ├── trade_preview.py        # Preview / confirm pipeline
│   │   ├── trading_costs.py        # Broker / SEC §31 / FINRA TAF fees
│   │   ├── pricing_engine.py       # Sync orchestration
│   │   ├── price_sync_worker.py    # QThread for background syncs
│   │   ├── reports.py              # Monthly / annual report builder
│   │   ├── report_export.py        # .xlsx / .txt writers
│   │   ├── snapshots.py            # Daily portfolio snapshots
│   │   ├── journal.py              # Snapshots, structure changes, training score
│   │   ├── data_management.py     # Delete / clear flows (FK-aware)
│   │   ├── import_export.py        # Simple CSV
│   │   ├── full_data_io.py         # Full-backup zip / folder
│   │   ├── cashflow.py             # Multi-period cashflow series
│   │   ├── dashboard.py            # Dashboard summary helpers
│   │   ├── holdings.py             # Per-asset quantity SQL
│   │   └── security_universe_engine.py
│   ├── gui/
│   │   ├── main_window.py          # Sidebar + page stack + auto-settle wiring
│   │   ├── pages/                  # One file per nav page
│   │   └── widgets/                # Bankruptcy banner, common formatters/tables
│   ├── charts/                     # matplotlib figure builders
│   ├── data_sources/               # yfinance providers + static security universe
│   └── utils/                      # logging, dates, deps, display labels
└── tests/                          # pytest suite (~50 files)
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

The database uses SQLite WAL mode, so you'll see `-wal` and `-shm` sidecar files while the app is open. The single-instance `QLockFile` lives in the same directory.

A legacy database at `./data/portfolio_simulator.db` (used by older builds) is automatically **moved** to the new location on first startup.

### Log file location

Resolved by `src/utils/app_logging.py:get_log_dir` — rotating at 2 MB with up to 4 files retained:

| OS      | Path                                              |
| ------- | ------------------------------------------------- |
| macOS   | `~/Library/Logs/asset-trainer/app.log`            |
| Windows | `%LOCALAPPDATA%\asset-trainer\Logs\app.log`       |
| Linux   | `$XDG_STATE_HOME/asset-trainer/logs/app.log`      |

### Backup

Use **Import / Export → Full Backup → Export Full Data** to dump every table to a `.zip` or folder. The "Import Full Data" path requires you to type `REPLACE DATA` and writes a `<dbname>.<timestamp>.pre-import.bak.db` alongside your existing database before replacing rows. Field validators reject negative amounts row-by-row before the import begins.

---

## Supported Asset & Transaction Types

### Asset types
`stock`, `etf`, `crypto`, `real_estate`, `cash`, `debt`, `custom`

Of these, `stock`, `etf`, `crypto`, and `custom` are tradeable through the buy/sell ledger; `real_estate` and `debt` have their own dedicated flows. Mortgages are not represented as Asset rows — they live in the `mortgages` table linked 1:1 to a property.

### Transaction types (13)
`deposit_cash`, `withdraw_cash`, `buy`, `sell`,
`add_property`, `update_property_value`, `receive_rent`, `pay_property_expense`, `pay_mortgage`, `sell_property`,
`add_debt`, `pay_debt`, `manual_adjustment`

### Surfacing in the GUI
- The **Transactions** page exposes: `deposit_cash`, `withdraw_cash`, `buy`, `sell`, `add_debt`, `pay_debt`, `pay_mortgage`, `pay_property_expense`.
- `add_property`, `update_property_value`, `sell_property`, `receive_rent` are written through the **Real Estate** page (Add / Edit / Sell / Settle Due Rent) or by the auto-settle pipeline.
- `manual_adjustment` and `update_property_value` can also be created via Full Data Import for backups and tests.

### CSV importable subset
The simple-CSV importer accepts only the types whose effect is fully captured by a row in `transactions`: `deposit_cash`, `withdraw_cash`, `buy`, `sell`, `receive_rent`, `pay_property_expense`, `manual_adjustment`. Types that need coordinated writes to sibling tables (`debts`, `mortgages`, `properties`, `*_payment_records`) must use Full Data Import.

---

## Risk Engine

Risk warnings are observations, not recommendations. The wording in `src/engines/risk.py` is intentionally non-prescriptive — for example:

- *"This portfolio is concentrated in {asset}. It represents {pct}% of net worth."*
- *"Cash balance is below the selected threshold ({threshold}%). Cash is {pct}% of total assets."*
- *"Debt ratio is above the selected threshold ({threshold}%)."*

You will never see things like *"You should sell..."* or *"This is a good investment."*

The Risk page (and the Dashboard's risk card) shows the warnings produced by `risk.get_all_warnings`:

| Check                  | Severity         | Trigger                                                       |
| ---------------------- | ---------------- | ------------------------------------------------------------- |
| Concentration          | medium / high    | Any non-cash item > `concentration_threshold` / > 40%         |
| Crypto exposure        | high             | Crypto % of total assets > `crypto_threshold`                 |
| Negative cash          | critical         | Cash balance is negative                                      |
| Low cash               | medium           | Cash % of total assets < `low_cash_threshold`                 |
| Leverage               | high             | Liabilities / total assets > `debt_threshold`                 |
| Illiquidity            | medium           | Illiquid assets / net worth > 60%                             |
| Real estate LTV        | high             | Per-property LTV > 80%                                        |
| Missing prices         | info             | Held syncable position with no `market_prices` row            |
| Missing journal        | info             | Buy/sell with no linked decision journal entry                |
| Debt payoff horizon    | high             | Projected payoff > `max_debt_payoff_months` (default 60)      |
| Debt affordability     | high / critical  | Cash covers < 6 months of scheduled debt+mortgage obligation  |
| Bankruptcy             | critical         | See [Bankruptcy & Auto-Settle Pipeline](#bankruptcy--auto-settle-pipeline) |

The Real Estate page surfaces additional per-property observations via `real_estate.get_real_estate_warnings` (negative monthly cash flow, vacancy > 10%, missing current value, real-estate share of net worth > 50%). Those run alongside the page's analytics rather than feeding the global Risk page.

---

## Bankruptcy & Auto-Settle Pipeline

A scheduled debt or mortgage payment is **never** allowed to remain "overdue". On every app launch, every data change, and a 30-minute day-boundary timer, `MainWindow._run_auto_settle` runs this pipeline (order matters):

1. Credit effective rent (`monthly_rent * (1 - vacancy_rate)`) for each due period.
2. Deduct each property's monthly opex (tax + insurance + HOA + maintenance reserve + property management).
3. Fire scheduled `pay_debt` and `pay_mortgage` for any due dates that haven't been processed. Items with insufficient cash are deferred.
4. If anything was deferred, **force-sell** assets to raise the total shortfall. Spec §11 ordering — `stock < etf < other (crypto/custom) < real_estate`, cheapest-first within each bucket. Real-estate uses `current_value` if set else `purchase_price`. The plan must fully cover the obligation before any sale is written; otherwise, the obligation is recorded as a **bankruptcy event** without partial sales.
5. Retry the deferred items after the force-sell.
6. Anything still deferred is recorded as a `bankruptcy_events` row — terminal, not a recoverable "missed payment".
7. Final mop-up: if cash is still negative (e.g., from `manual_adjustment`), force-sell to recover what's possible.

When the simulator is bankrupt:
- Every public ledger write raises `BankruptcyLockedError`. Auto-settle internals carry an explicit bypass via a `contextvars.ContextVar` so scheduled obligations and recovery sales keep firing.
- A red banner appears above every page naming the obligation that triggered bankruptcy.
- The Transactions, Real Estate, and Decision Journal submit handlers all call a shared `guard_transaction_or_warn` helper that opens a modal warning before aborting the action.

Bankruptcy is **terminal**. Depositing more cash will not unlock writes — you must clear the bankruptcy state via Data Management or restore a non-bankrupt backup.

---

## Reports

Monthly and annual reports include 13 sections: summary, cash-flow breakdown, performance (approximate), allocation, risk summary, operations, transactions, trades, real estate, debt, journal, current snapshot, beginning + ending snapshots.

> **Important caveats** (from the report's "How To Read" preface):
>
> - **Net Cash Flow is cash movement, not profit.** Depositing money increases Net Cash Flow without producing any gain.
> - **Performance metrics are approximate** — snapshot-based, not time-weighted, and they don't separate realized vs unrealized P&L.
> - **The Risk Summary always reflects current state at generation time**, not period-end state, because risk warnings aren't stored historically.
> - The Allocation section prefers the period-end stored snapshot's `allocation_json` and falls back to the live portfolio when no snapshot exists; the data quality note makes the source explicit.

Export to `.xlsx` (multi-sheet workbook) or `.txt` (with the same "How To Read" text up top).

---

## Market Data

`yfinance` is the only external data source. Two providers are wired up:

- `YFinanceProvider` for stocks and ETFs (`ticker.history` for daily OHLCV; `ticker.info` for bid/ask/last).
- `YFinanceCryptoProvider` for crypto (auto-appends `-USD` to symbols, then uses the same yfinance API).

**When sync runs:**
- On app startup, if yfinance is installed and at least one stock/etf/crypto asset exists.
- When you click **Sync All Market Data** or **Sync Selected Asset** on the Data Sync page.
- When you preview a Buy/Sell on a syncable asset (a fresh sync is attempted; failures fall back to stored data).
- Best-effort, before the force-sell engine prices the plan (failures degrade to cached quotes).

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
| `max_debt_payoff_months`         | `60`    | Debts whose projected payoff exceeds this fire `debt_horizon`    |
| `default_debt_annual_rate_pct`   | `7.0`   | Pre-fills the rate field on Add Debt and Add Property mortgage   |
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
- **Trade Amount mode** floors stock/ETF to whole shares; crypto and custom assets accept up to 8 decimal places.
- **Edit Property dialog** uses raw monthly values and a percent-literal vacancy input. Mortgage terms are not editable post-creation — use the Pay Mortgage / Pay Off in Full flows on the Transactions page to change the balance.
- **Force-sell skips the trading-cost engine.** Transactions written by the force-sell pipeline use `fees=0`. Manual sells through `confirm_trade` apply the configured commission and regulatory fees.
- **`pay_debt(amount=current_balance)` does NOT clear interest-bearing debt.** One period's interest is taken off the cash payment first, leaving a small principal residue. Use **Pay Off in Full** (which charges balance + this period's interest) to extinguish the debt cleanly.
- **Bulk-delete actions are permanent** — "Clear All Properties", "Clear All Assets", "Clear All Data", and full-data import all destroy data. Always export first.
- **Reports are not P&L statements.** See the caveats above.
- **`manual_adjustment`, `update_property_value`** can only be created via CSV / full-data import (not from the GUI's transaction-type dropdown).
- **Cashflow uses cash basis, not run-rate normalization.** A lumpy payment (e.g. an annual property tax in March) shows as a single tall negative segment in March's bar — not retroactively spread across the prior 12 months.
- **Bankruptcy is terminal.** Depositing more cash will not unlock the ledger; you must clear the `bankruptcy_events` rows via a destructive Data Management action or restore a non-bankrupt backup.
- This project reflects ongoing hardening work — running `pytest` before relying on critical features is recommended.

---

## Contributing

Contributions are welcome. A few project conventions worth knowing:

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
