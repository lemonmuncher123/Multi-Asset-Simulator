import sqlite3
import pytest
from src.storage.database import init_db
from src.engines.data_management import (
    get_asset_usage_summary,
    delete_asset_with_related_data,
    delete_property_with_related_data,
    clear_all_assets,
    clear_all_properties,
    clear_all_user_data,
)


def _insert_asset(conn, symbol, name, asset_type):
    cursor = conn.execute(
        "INSERT INTO assets (symbol, name, asset_type) VALUES (?, ?, ?)",
        (symbol, name, asset_type),
    )
    conn.commit()
    return cursor.lastrowid


def _insert_transaction(conn, date, txn_type, asset_id, total_amount):
    cursor = conn.execute(
        "INSERT INTO transactions (date, txn_type, asset_id, total_amount) "
        "VALUES (?, ?, ?, ?)",
        (date, txn_type, asset_id, total_amount),
    )
    conn.commit()
    return cursor.lastrowid


def _insert_cash_txn(conn, date, txn_type, amount):
    cursor = conn.execute(
        "INSERT INTO transactions (date, txn_type, total_amount) "
        "VALUES (?, ?, ?)",
        (date, txn_type, amount),
    )
    conn.commit()
    return cursor.lastrowid


def _insert_price(conn, asset_id, date, price, source="manual"):
    conn.execute(
        "INSERT INTO market_prices (asset_id, date, price, source) "
        "VALUES (?, ?, ?, ?)",
        (asset_id, date, price, source),
    )
    conn.commit()


def _insert_property(conn, asset_id):
    conn.execute(
        "INSERT INTO properties (asset_id, purchase_price, current_value) "
        "VALUES (?, 100000, 110000)",
        (asset_id,),
    )
    conn.commit()


def _insert_debt(conn, asset_id):
    conn.execute(
        "INSERT INTO debts (asset_id, name, original_amount, current_balance) "
        "VALUES (?, 'Test Debt', 10000, 8000)",
        (asset_id,),
    )
    conn.commit()


def _insert_journal(conn, transaction_id=None):
    cursor = conn.execute(
        "INSERT INTO decision_journal (date, title, transaction_id) "
        "VALUES ('2025-01-01', 'Test Entry', ?)",
        (transaction_id,),
    )
    conn.commit()
    return cursor.lastrowid


# --- get_asset_usage_summary ---

def test_summary_empty_asset(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    summary = get_asset_usage_summary(db_conn, aid)
    assert summary["transactions"] == 0
    assert summary["prices"] == 0
    assert summary["has_property"] is False
    assert summary["has_debt"] is False
    assert summary["journal_entries"] == 0


def test_summary_with_data(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    _insert_transaction(db_conn, "2025-01-02", "buy", aid, -2000)
    _insert_price(db_conn, aid, "2025-01-01", 150.0)
    summary = get_asset_usage_summary(db_conn, aid)
    assert summary["transactions"] == 2
    assert summary["prices"] == 1


def test_summary_with_property_and_debt(db_conn):
    aid = _insert_asset(db_conn, "HOUSE", "My House", "real_estate")
    _insert_property(db_conn, aid)
    debt_asset = _insert_asset(db_conn, "MORT", "Mortgage", "debt")
    _insert_debt(db_conn, debt_asset)
    summary = get_asset_usage_summary(db_conn, aid)
    assert summary["has_property"] is True
    debt_summary = get_asset_usage_summary(db_conn, debt_asset)
    assert debt_summary["has_debt"] is True


def test_summary_journal_entries_via_transaction(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    txn_id = _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    _insert_journal(db_conn, transaction_id=txn_id)
    summary = get_asset_usage_summary(db_conn, aid)
    assert summary["journal_entries"] == 1


# --- delete_asset_with_related_data ---

def test_delete_asset_basic(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    _insert_price(db_conn, aid, "2025-01-01", 150.0)
    deleted = delete_asset_with_related_data(db_conn, aid)
    assert deleted["transactions"] == 1
    assert deleted["prices"] == 1
    assert db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0


def test_delete_asset_with_property(db_conn):
    aid = _insert_asset(db_conn, "HOUSE", "My House", "real_estate")
    _insert_property(db_conn, aid)
    deleted = delete_asset_with_related_data(db_conn, aid)
    assert deleted["properties"] == 1


def test_delete_asset_with_debt(db_conn):
    aid = _insert_asset(db_conn, "LOAN", "Car Loan", "debt")
    _insert_debt(db_conn, aid)
    deleted = delete_asset_with_related_data(db_conn, aid)
    assert deleted["debts"] == 1


def test_delete_asset_cascades_journal_via_transaction(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    txn_id = _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    _insert_journal(db_conn, transaction_id=txn_id)
    db_conn.commit()

    delete_asset_with_related_data(db_conn, aid)
    assert db_conn.execute("SELECT COUNT(*) FROM decision_journal").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0


def test_delete_asset_removes_fee_breakdowns(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    txn_id = _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    db_conn.execute(
        "INSERT INTO transaction_fee_breakdown (transaction_id, fee_type, amount) "
        "VALUES (?, 'commission', 4.95)",
        (txn_id,),
    )
    db_conn.commit()

    delete_asset_with_related_data(db_conn, aid)

    assert db_conn.execute(
        "SELECT COUNT(*) FROM transaction_fee_breakdown"
    ).fetchone()[0] == 0


def test_delete_asset_does_not_delete_unrelated_journal(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    _insert_journal(db_conn)  # unrelated journal (no transaction_id)
    delete_asset_with_related_data(db_conn, aid)
    assert db_conn.execute("SELECT COUNT(*) FROM decision_journal").fetchone()[0] == 1


# --- clear_all_assets ---

def test_clear_all_assets_removes_assets_and_related(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    _insert_price(db_conn, aid, "2025-01-01", 150.0)

    _insert_cash_txn(db_conn, "2025-01-01", "deposit_cash", 10000)

    deleted = clear_all_assets(db_conn)
    assert deleted["assets"] == 1
    assert deleted["transactions"] == 1
    assert deleted["market_prices"] == 1
    assert db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0


def test_clear_all_assets_preserves_cash_transactions(db_conn):
    _insert_cash_txn(db_conn, "2025-01-01", "deposit_cash", 10000)
    _insert_cash_txn(db_conn, "2025-01-02", "withdraw_cash", -500)
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    _insert_transaction(db_conn, "2025-01-03", "buy", aid, -1500)

    clear_all_assets(db_conn)
    assert db_conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 2


def test_clear_all_assets_preserves_settings(db_conn):
    db_conn.execute("INSERT INTO settings (key, value) VALUES ('theme', 'dark')")
    db_conn.commit()
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)

    clear_all_assets(db_conn)
    row = db_conn.execute("SELECT value FROM settings WHERE key='theme'").fetchone()
    assert row[0] == "dark"


def test_clear_all_assets_preserves_securities_master(db_conn):
    db_conn.execute(
        "INSERT INTO securities_master (symbol, name, asset_type) "
        "VALUES ('SPY', 'SPDR S&P 500', 'etf')"
    )
    db_conn.commit()
    aid = _insert_asset(db_conn, "SPY", "SPY ETF", "etf")
    _insert_price(db_conn, aid, "2025-01-01", 500.0)

    clear_all_assets(db_conn)
    assert db_conn.execute("SELECT COUNT(*) FROM securities_master").fetchone()[0] == 1


def test_clear_all_assets_removes_fee_breakdowns(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    txn_id = _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    db_conn.execute(
        "INSERT INTO transaction_fee_breakdown (transaction_id, fee_type, amount) "
        "VALUES (?, 'commission', 4.95)",
        (txn_id,),
    )
    db_conn.commit()

    clear_all_assets(db_conn)

    assert db_conn.execute(
        "SELECT COUNT(*) FROM transaction_fee_breakdown"
    ).fetchone()[0] == 0


def test_clear_all_assets_cascades_to_journal(db_conn):
    # The legacy back-pointer (transactions.journal_id) was dropped in
    # schema v2, so there's no FK cycle to break — clear_all_assets just
    # needs to delete journal entries linked via decision_journal.transaction_id.
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    txn_id = _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    _insert_journal(db_conn, transaction_id=txn_id)
    db_conn.commit()

    clear_all_assets(db_conn)
    assert db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM decision_journal").fetchone()[0] == 0


# --- clear_all_user_data ---

def test_clear_all_data_removes_everything(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    _insert_cash_txn(db_conn, "2025-01-01", "deposit_cash", 10000)
    _insert_price(db_conn, aid, "2025-01-01", 150.0)
    db_conn.execute("INSERT INTO settings (key, value) VALUES ('theme', 'dark')")
    db_conn.commit()

    deleted = clear_all_user_data(db_conn)
    assert deleted["assets"] == 1
    assert deleted["transactions"] == 2
    assert deleted["settings"] == 1
    assert db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM settings").fetchone()[0] == 0


def test_clear_all_data_succeeds_with_debt_payment_records_present(db_conn):
    """Regression for Issue #2: `debt_payment_records` has FOREIGN KEY
    references to both `debts(id)` and `transactions(id)`. With
    `PRAGMA foreign_keys=ON`, deleting the parents first violates the
    constraint. Pre-fix, this test failed with
    "FOREIGN KEY constraint failed" on the debts DELETE."""
    from src.engines import ledger
    ledger.deposit_cash(db_conn, "2025-01-01", 5000.0)
    _, debt, _ = ledger.add_debt(
        db_conn, "2025-01-02", symbol="L", name="Loan",
        amount=1000.0, interest_rate=0.0,
        payment_per_period=100.0, cash_received=False,
    )
    ledger.pay_debt(db_conn, "2025-02-01", debt.asset_id, 100.0)
    # Sanity: there's at least one debt_payment_records row before
    # clearing, so the FK ordering matters.
    pre = db_conn.execute(
        "SELECT COUNT(*) FROM debt_payment_records"
    ).fetchone()[0]
    assert pre > 0
    deleted = clear_all_user_data(db_conn)
    assert deleted["debt_payment_records"] == pre
    assert db_conn.execute(
        "SELECT COUNT(*) FROM debt_payment_records"
    ).fetchone()[0] == 0
    assert db_conn.execute(
        "SELECT COUNT(*) FROM debts"
    ).fetchone()[0] == 0
    assert db_conn.execute(
        "SELECT COUNT(*) FROM transactions"
    ).fetchone()[0] == 0


def test_clear_all_data_deletes_securities_master(db_conn):
    db_conn.execute(
        "INSERT INTO securities_master (symbol, name, asset_type) "
        "VALUES ('SPY', 'SPDR S&P 500', 'etf')"
    )
    db_conn.commit()
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)

    deleted = clear_all_user_data(db_conn)
    assert deleted["securities_master"] == 1
    assert db_conn.execute("SELECT COUNT(*) FROM securities_master").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0


def test_clear_all_data_preserves_table_structure(db_conn):
    db_conn.execute(
        "INSERT INTO securities_master (symbol, name, asset_type) "
        "VALUES ('SPY', 'SPDR S&P 500', 'etf')"
    )
    db_conn.commit()
    expected = {
        "assets", "transactions", "transaction_fee_breakdown",
        "market_prices", "market_quotes", "price_sync_log",
        "properties", "debts", "decision_journal",
        "portfolio_snapshots", "reports", "securities_master", "settings",
    }

    clear_all_user_data(db_conn)

    tables = {
        row[0] for row in db_conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    }
    assert expected.issubset(tables)


def test_clear_all_data_resets_autoincrement_sequences(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    assert aid >= 1

    clear_all_user_data(db_conn)

    new_aid = _insert_asset(db_conn, "MSFT", "Microsoft", "stock")
    new_txn = _insert_transaction(db_conn, "2025-02-01", "buy", new_aid, -100)
    assert new_aid == 1
    assert new_txn == 1


def test_clear_all_data_drops_legacy_option_contracts(db_conn):
    db_conn.execute(
        "CREATE TABLE option_contracts ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT)"
    )
    db_conn.execute("INSERT INTO option_contracts (symbol) VALUES ('AAPL_C')")
    db_conn.commit()

    clear_all_user_data(db_conn)

    has_table = db_conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name='option_contracts'"
    ).fetchone()
    assert has_table is None


def test_clear_all_data_safe_when_no_legacy_options(db_conn):
    has_table = db_conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name='option_contracts'"
    ).fetchone()
    assert has_table is None
    clear_all_user_data(db_conn)  # must not raise


def test_clear_all_data_cascades_to_journal(db_conn):
    # As above — the legacy back-pointer column was dropped in schema v2
    # so clear_all_user_data simply DELETEs both tables in dependency order.
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    txn_id = _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    _insert_journal(db_conn, transaction_id=txn_id)
    db_conn.commit()

    clear_all_user_data(db_conn)
    assert db_conn.execute("SELECT COUNT(*) FROM decision_journal").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0


def test_clear_all_data_clears_portfolio_snapshots(db_conn):
    db_conn.execute(
        "INSERT INTO portfolio_snapshots (date, cash, total_assets, total_liabilities, net_worth) "
        "VALUES ('2025-01-01', 10000, 15000, 5000, 10000)"
    )
    db_conn.commit()

    deleted = clear_all_user_data(db_conn)
    assert deleted["portfolio_snapshots"] == 1
    assert db_conn.execute("SELECT COUNT(*) FROM portfolio_snapshots").fetchone()[0] == 0


def test_clear_all_data_deletes_reports(db_conn):
    db_conn.execute(
        "INSERT INTO reports (report_type, period_start, period_end, period_label, "
        "generated_at, title, report_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("monthly", "2025-01-01", "2025-02-01", "2025-01", "now", "Jan 2025", "{}"),
    )
    db_conn.execute(
        "INSERT INTO reports (report_type, period_start, period_end, period_label, "
        "generated_at, title, report_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("annual", "2025-01-01", "2026-01-01", "2025", "now", "Year 2025", "{}"),
    )
    db_conn.commit()

    deleted = clear_all_user_data(db_conn)
    assert deleted["reports"] == 2
    assert db_conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 0


def test_clear_all_data_deletes_transaction_fee_breakdown(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    txn_id = _insert_transaction(db_conn, "2025-01-01", "buy", aid, -1500)
    db_conn.execute(
        "INSERT INTO transaction_fee_breakdown (transaction_id, fee_type, amount) "
        "VALUES (?, ?, ?)",
        (txn_id, "commission", 4.95),
    )
    db_conn.commit()

    deleted = clear_all_user_data(db_conn)
    assert deleted["transaction_fee_breakdown"] == 1
    assert db_conn.execute("SELECT COUNT(*) FROM transaction_fee_breakdown").fetchone()[0] == 0


# --- delete_property_with_related_data ---

def test_delete_property_removes_all_related(db_conn):
    aid = _insert_asset(db_conn, "HOUSE", "My House", "real_estate")
    _insert_property(db_conn, aid)
    txn_id = _insert_transaction(db_conn, "2025-01-01", "add_property", aid, -100000)
    _insert_price(db_conn, aid, "2025-01-01", 500000.0)
    _insert_journal(db_conn, transaction_id=txn_id)
    db_conn.commit()

    deleted = delete_property_with_related_data(db_conn, aid)
    assert deleted["transactions"] >= 1
    assert deleted["properties"] == 1
    assert deleted["prices"] == 1
    assert db_conn.execute("SELECT COUNT(*) FROM assets WHERE id = ?", (aid,)).fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM decision_journal").fetchone()[0] == 0


def test_delete_property_removes_fee_breakdowns(db_conn):
    aid = _insert_asset(db_conn, "HOUSE", "My House", "real_estate")
    _insert_property(db_conn, aid)
    txn_id = _insert_transaction(db_conn, "2025-01-01", "add_property", aid, -100000)
    db_conn.execute(
        "INSERT INTO transaction_fee_breakdown (transaction_id, fee_type, amount) "
        "VALUES (?, 'commission', 250.0)",
        (txn_id,),
    )
    db_conn.commit()

    delete_property_with_related_data(db_conn, aid)

    assert db_conn.execute(
        "SELECT COUNT(*) FROM transaction_fee_breakdown"
    ).fetchone()[0] == 0


def test_delete_property_creates_no_cash_transaction(db_conn):
    _insert_cash_txn(db_conn, "2025-01-01", "deposit_cash", 100000)
    aid = _insert_asset(db_conn, "HOUSE", "My House", "real_estate")
    _insert_property(db_conn, aid)
    _insert_transaction(db_conn, "2025-02-01", "add_property", aid, -50000)

    cash_before = db_conn.execute(
        "SELECT COALESCE(SUM(total_amount), 0) FROM transactions WHERE asset_id IS NULL"
    ).fetchone()[0]

    delete_property_with_related_data(db_conn, aid)

    cash_after = db_conn.execute(
        "SELECT COALESCE(SUM(total_amount), 0) FROM transactions WHERE asset_id IS NULL"
    ).fetchone()[0]
    assert cash_after == cash_before


def test_delete_property_rejects_non_real_estate(db_conn):
    aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    with pytest.raises(ValueError, match="not a real_estate"):
        delete_property_with_related_data(db_conn, aid)


def test_delete_property_rejects_missing_property_record(db_conn):
    aid = _insert_asset(db_conn, "FAKE_RE", "Fake RE", "real_estate")
    with pytest.raises(ValueError, match="No property record"):
        delete_property_with_related_data(db_conn, aid)


def test_delete_property_does_not_touch_other_assets(db_conn):
    re_aid = _insert_asset(db_conn, "HOUSE", "My House", "real_estate")
    _insert_property(db_conn, re_aid)
    stock_aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    _insert_transaction(db_conn, "2025-01-01", "buy", stock_aid, -1500)

    delete_property_with_related_data(db_conn, re_aid)
    assert db_conn.execute("SELECT COUNT(*) FROM assets WHERE id = ?", (stock_aid,)).fetchone()[0] == 1
    assert db_conn.execute("SELECT COUNT(*) FROM transactions WHERE asset_id = ?", (stock_aid,)).fetchone()[0] == 1


# --- clear_all_properties ---

def test_clear_all_properties_deletes_re_assets(db_conn):
    re1 = _insert_asset(db_conn, "H1", "House 1", "real_estate")
    re2 = _insert_asset(db_conn, "H2", "House 2", "real_estate")
    _insert_property(db_conn, re1)
    _insert_property(db_conn, re2)
    _insert_transaction(db_conn, "2025-01-01", "add_property", re1, -50000)
    _insert_transaction(db_conn, "2025-01-01", "add_property", re2, -80000)

    deleted = clear_all_properties(db_conn)
    assert deleted["assets"] == 2
    assert deleted["properties"] == 2
    assert deleted["transactions"] == 2
    assert db_conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0] == 0
    assert db_conn.execute(
        "SELECT COUNT(*) FROM assets WHERE asset_type = 'real_estate'"
    ).fetchone()[0] == 0


def test_clear_all_properties_preserves_non_re_assets(db_conn):
    re_aid = _insert_asset(db_conn, "HOUSE", "My House", "real_estate")
    _insert_property(db_conn, re_aid)
    stock_aid = _insert_asset(db_conn, "AAPL", "Apple", "stock")
    _insert_transaction(db_conn, "2025-01-01", "buy", stock_aid, -1500)
    etf_aid = _insert_asset(db_conn, "VTI", "Vanguard", "etf")
    crypto_aid = _insert_asset(db_conn, "BTC", "Bitcoin", "crypto")
    debt_aid = _insert_asset(db_conn, "LOAN", "Loan", "debt")
    _insert_debt(db_conn, debt_aid)
    custom_aid = _insert_asset(db_conn, "GOLD", "Gold Bar", "custom")

    clear_all_properties(db_conn)
    remaining = {
        row[0] for row in db_conn.execute("SELECT asset_type FROM assets").fetchall()
    }
    assert "stock" in remaining
    assert "etf" in remaining
    assert "crypto" in remaining
    assert "debt" in remaining
    assert "custom" in remaining
    assert "real_estate" not in remaining


def test_clear_all_properties_preserves_cash_transactions(db_conn):
    _insert_cash_txn(db_conn, "2025-01-01", "deposit_cash", 100000)
    _insert_cash_txn(db_conn, "2025-01-02", "withdraw_cash", -500)
    re_aid = _insert_asset(db_conn, "HOUSE", "My House", "real_estate")
    _insert_property(db_conn, re_aid)
    _insert_transaction(db_conn, "2025-02-01", "add_property", re_aid, -50000)

    clear_all_properties(db_conn)
    assert db_conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 2


def test_clear_all_properties_creates_no_cash_transaction(db_conn):
    _insert_cash_txn(db_conn, "2025-01-01", "deposit_cash", 100000)
    re_aid = _insert_asset(db_conn, "HOUSE", "My House", "real_estate")
    _insert_property(db_conn, re_aid)
    _insert_transaction(db_conn, "2025-02-01", "add_property", re_aid, -50000)

    cash_before = db_conn.execute(
        "SELECT COALESCE(SUM(total_amount), 0) FROM transactions WHERE asset_id IS NULL"
    ).fetchone()[0]

    clear_all_properties(db_conn)

    cash_after = db_conn.execute(
        "SELECT COALESCE(SUM(total_amount), 0) FROM transactions WHERE asset_id IS NULL"
    ).fetchone()[0]
    assert cash_after == cash_before


def test_clear_all_properties_empty_is_safe(db_conn):
    deleted = clear_all_properties(db_conn)
    assert deleted["assets"] == 0
    assert deleted["properties"] == 0


def test_clear_all_properties_removes_fee_breakdowns(db_conn):
    re_aid = _insert_asset(db_conn, "HOUSE", "My House", "real_estate")
    _insert_property(db_conn, re_aid)
    txn_id = _insert_transaction(db_conn, "2025-01-01", "add_property", re_aid, -50000)
    db_conn.execute(
        "INSERT INTO transaction_fee_breakdown (transaction_id, fee_type, amount) "
        "VALUES (?, 'commission', 250.0)",
        (txn_id,),
    )
    db_conn.commit()

    clear_all_properties(db_conn)

    assert db_conn.execute(
        "SELECT COUNT(*) FROM transaction_fee_breakdown"
    ).fetchone()[0] == 0


# --- FK ordering: debt_payment_records / mortgage_payment_records / mortgages ---
#
# `delete_asset_with_related_data` and `clear_all_assets` did not delete
# the v10 (`debt_payment_records`) and v11 (`mortgage_payment_records`,
# `mortgages`) child tables before parents. With PRAGMA foreign_keys=ON
# the deletes raised `sqlite3.IntegrityError`. These tests use the real
# ledger to seed realistic FK graphs.

def test_delete_asset_with_debt_payment_records(db_conn):
    from src.engines import ledger
    ledger.deposit_cash(db_conn, "2026-01-01", 10000.0)
    asset, _, _ = ledger.add_debt(
        db_conn, "2026-01-01", symbol="LOAN", name="Test Loan",
        amount=5000.0, interest_rate=0.06, payment_per_period=100.0,
        schedule_frequency="monthly", cash_received=False,
    )
    ledger.pay_debt(db_conn, "2026-02-01", asset.id, 200.0)
    assert db_conn.execute(
        "SELECT COUNT(*) FROM debt_payment_records"
    ).fetchone()[0] == 1

    delete_asset_with_related_data(db_conn, asset.id)

    assert db_conn.execute(
        "SELECT COUNT(*) FROM debt_payment_records"
    ).fetchone()[0] == 0
    assert db_conn.execute(
        "SELECT COUNT(*) FROM debts WHERE asset_id=?", (asset.id,)
    ).fetchone()[0] == 0


def test_delete_asset_with_mortgage_payment_records(db_conn):
    from src.engines import ledger
    ledger.deposit_cash(db_conn, "2026-01-01", 100000.0)
    asset, prop, _ = ledger.add_property(
        db_conn, "2026-01-01", symbol="HSE", name="House",
        purchase_price=200000.0,
        acquisition_mode="existing_property",
        down_payment=50000.0,
    )
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=150000.0,
        interest_rate=0.05, payment_per_period=900.0,
    )
    ledger.pay_mortgage(db_conn, "2026-02-01", asset.id, 1000.0)
    assert db_conn.execute(
        "SELECT COUNT(*) FROM mortgage_payment_records"
    ).fetchone()[0] == 1

    delete_asset_with_related_data(db_conn, asset.id)

    assert db_conn.execute(
        "SELECT COUNT(*) FROM mortgage_payment_records"
    ).fetchone()[0] == 0
    assert db_conn.execute(
        "SELECT COUNT(*) FROM mortgages"
    ).fetchone()[0] == 0
    assert db_conn.execute(
        "SELECT COUNT(*) FROM properties"
    ).fetchone()[0] == 0


def test_clear_all_assets_with_debt_payment_records(db_conn):
    from src.engines import ledger
    ledger.deposit_cash(db_conn, "2026-01-01", 10000.0)
    asset, _, _ = ledger.add_debt(
        db_conn, "2026-01-01", symbol="LOAN", name="Test Loan",
        amount=5000.0, interest_rate=0.06, payment_per_period=100.0,
        schedule_frequency="monthly", cash_received=False,
    )
    ledger.pay_debt(db_conn, "2026-02-01", asset.id, 200.0)

    clear_all_assets(db_conn)

    assert db_conn.execute(
        "SELECT COUNT(*) FROM debt_payment_records"
    ).fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM debts").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0


def test_clear_all_assets_with_mortgages(db_conn):
    from src.engines import ledger
    ledger.deposit_cash(db_conn, "2026-01-01", 100000.0)
    asset, prop, _ = ledger.add_property(
        db_conn, "2026-01-01", symbol="HSE", name="House",
        purchase_price=200000.0,
        acquisition_mode="existing_property",
        down_payment=50000.0,
    )
    ledger.add_mortgage(
        db_conn, property_id=prop.id, original_amount=150000.0,
        interest_rate=0.05, payment_per_period=900.0,
    )
    ledger.pay_mortgage(db_conn, "2026-02-01", asset.id, 1000.0)

    clear_all_assets(db_conn)

    assert db_conn.execute(
        "SELECT COUNT(*) FROM mortgage_payment_records"
    ).fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM mortgages").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0] == 0
    assert db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0
