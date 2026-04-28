import pytest
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.price_repo import upsert_price
from src.engines import ledger
from src.engines.risk import (
    check_concentration,
    check_crypto_exposure,
    check_low_cash,
    check_leverage,
    check_illiquidity,
    check_real_estate_ltv,
    check_missing_prices,
    check_missing_journal,
    get_all_warnings,
)


# --- Rule 1 & 2: Concentration ---

def test_concentration_medium_above_25pct(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    stock = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    # buy 30k of stock => 30% of 100k net worth
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=200, price=150.0)
    upsert_price(db_conn, stock.id, "2025-01-15", 150.0)

    warnings = check_concentration(db_conn)
    assert len(warnings) == 1
    assert warnings[0].severity == "medium"
    assert "Apple" in warnings[0].message


def test_concentration_high_above_40pct(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    stock = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    # buy 50k => 50% of 100k
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=250, price=200.0)
    upsert_price(db_conn, stock.id, "2025-01-15", 200.0)

    warnings = check_concentration(db_conn)
    assert len(warnings) == 1
    assert warnings[0].severity == "high"


def test_no_concentration_below_25pct(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    s1 = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    s2 = create_asset(db_conn, Asset(symbol="GOOG", name="Google", asset_type="stock"))
    # 10k each => 10% each
    ledger.buy(db_conn, "2025-01-15", s1.id, quantity=100, price=100.0)
    ledger.buy(db_conn, "2025-01-15", s2.id, quantity=100, price=100.0)

    warnings = check_concentration(db_conn)
    assert len(warnings) == 0


# --- Rule 3: Crypto > 20% ---

def test_crypto_exposure_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    btc = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    # 25k crypto => 25% of 100k total
    ledger.buy(db_conn, "2025-01-15", btc.id, quantity=0.5, price=50000.0)

    warnings = check_crypto_exposure(db_conn)
    assert len(warnings) == 1
    assert warnings[0].severity == "high"
    assert "Crypto" in warnings[0].message


def test_no_crypto_warning_below_20pct(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    btc = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    # 10k crypto => 10% of 100k
    ledger.buy(db_conn, "2025-01-15", btc.id, quantity=0.25, price=40000.0)

    warnings = check_crypto_exposure(db_conn)
    assert len(warnings) == 0


# --- Rule 4: Cash < 5% ---

def test_low_cash_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    stock = create_asset(db_conn, Asset(symbol="VTI", name="Vanguard", asset_type="etf"))
    # buy 97k => cash left is 3k => 3% of 100k
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=970, price=100.0)

    warnings = check_low_cash(db_conn)
    assert len(warnings) == 1
    assert warnings[0].severity == "medium"
    assert "below" in warnings[0].message


def test_no_low_cash_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    stock = create_asset(db_conn, Asset(symbol="VTI", name="Vanguard", asset_type="etf"))
    # buy 50k => cash left 50k => 50%
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=500, price=100.0)

    warnings = check_low_cash(db_conn)
    assert len(warnings) == 0


# --- Rule 7: Negative cash ---

def test_negative_cash_critical(db_conn):
    # `withdraw_cash` now refuses to overdraft (the simulator's source-of-truth
    # contract). The negative-cash risk warning still applies — for example
    # when a user uses `manual_adjustment` (the documented escape hatch) to
    # encode an inherited deficit, or after a future rule change.
    ledger.manual_adjustment(db_conn, "2025-01-01", -1000.0, notes="seed deficit")

    warnings = check_low_cash(db_conn)
    assert len(warnings) == 1
    assert warnings[0].severity == "critical"
    assert "negative" in warnings[0].message.lower()


# --- Rule 5: Debt ratio > 50% ---

def test_leverage_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, mortgage_balance=450000.0,
        down_payment=50000.0,
    )
    # total assets: 50k cash + 500k property = 550k
    # liabilities: 450k => ratio 450/550 = 81.8%
    warnings = check_leverage(db_conn)
    assert len(warnings) == 1
    assert warnings[0].severity == "high"
    assert "Debt ratio" in warnings[0].message


def test_no_leverage_warning(db_conn):
    # Sufficient cash to cover the 200k down payment.
    ledger.deposit_cash(db_conn, "2025-01-01", 300000.0)
    ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=300000.0, mortgage_balance=100000.0,
    )
    # cash: 300k - 200k = 100k; total_assets = 100k + 300k = 400k
    # liabilities: 100k; ratio = 100/400 = 25% (below 50% threshold).
    warnings = check_leverage(db_conn)
    assert len(warnings) == 0


def test_no_leverage_warning_low_debt(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
    ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=300000.0, mortgage_balance=100000.0,
    )
    # cash: 500k - 200k (down) = 300k, total assets: 300k + 300k = 600k
    # liabilities: 100k, ratio = 100/600 = 16.7%
    warnings = check_leverage(db_conn)
    assert len(warnings) == 0


# --- Rule 6: Illiquid > 60% of net worth ---

def test_illiquidity_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 200000.0)
    ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, mortgage_balance=400000.0,
    )
    # cash: 200k - 100k = 100k, net worth: 100k + 500k - 400k = 200k
    # illiquid: 500k, pct of net worth: 500k/200k = 250%
    warnings = check_illiquidity(db_conn)
    assert len(warnings) == 1
    assert "Illiquid" in warnings[0].message


def test_no_illiquidity_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
    # all cash, nothing illiquid
    warnings = check_illiquidity(db_conn)
    assert len(warnings) == 0


# --- Rule 8: Real estate LTV > 80% ---

def test_real_estate_ltv_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, mortgage_balance=450000.0,
        down_payment=50000.0,
    )
    # LTV = 450k / 500k = 90%
    warnings = check_real_estate_ltv(db_conn)
    assert len(warnings) == 1
    assert warnings[0].severity == "high"
    assert "loan-to-value" in warnings[0].message.lower()


def test_no_ltv_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 500000.0)
    ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, mortgage_balance=300000.0,
    )
    # LTV = 300k / 500k = 60%
    warnings = check_real_estate_ltv(db_conn)
    assert len(warnings) == 0


# --- Rule 10: Missing price data ---

def test_missing_price_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    stock = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=10, price=150.0)
    # no price in market_prices table

    warnings = check_missing_prices(db_conn)
    assert len(warnings) == 1
    assert warnings[0].severity == "info"
    assert "AAPL" in warnings[0].message


def test_no_missing_price_when_data_exists(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
    stock = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=10, price=150.0)
    upsert_price(db_conn, stock.id, "2025-01-15", 155.0)

    warnings = check_missing_prices(db_conn)
    assert len(warnings) == 0


def test_no_missing_price_for_sold_position(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
    stock = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=10, price=150.0)
    ledger.sell(db_conn, "2025-02-15", stock.id, quantity=10, price=170.0)

    warnings = check_missing_prices(db_conn)
    assert len(warnings) == 0


# --- Rule 11: Missing journal ---

def test_missing_journal_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
    stock = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=10, price=150.0)

    warnings = check_missing_journal(db_conn)
    assert len(warnings) == 1
    assert warnings[0].severity == "info"
    assert "journal" in warnings[0].message.lower()
    assert warnings[0].metric_value == 1.0


def test_no_journal_warning_for_non_trades(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 10000.0)
    ledger.withdraw_cash(db_conn, "2025-01-02", 500.0)

    warnings = check_missing_journal(db_conn)
    assert len(warnings) == 0


# --- get_all_warnings ---

def test_all_warnings_sorted_by_severity(db_conn):
    # Seed enough cash to make the buy first, then drive cash negative
    # afterwards via the manual_adjustment escape hatch.
    ledger.deposit_cash(db_conn, "2025-01-01", 1000.0)
    stock = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    ledger.buy(db_conn, "2025-01-15", stock.id, quantity=1, price=10.0)  # info: missing journal
    # withdraw_cash now refuses to overdraft, so use manual_adjustment to
    # drive the balance negative; check_low_cash still emits a critical warning.
    ledger.manual_adjustment(db_conn, "2025-01-20", -5000.0, notes="seed deficit")

    warnings = get_all_warnings(db_conn)
    assert len(warnings) >= 2
    assert warnings[0].severity == "critical"
    assert warnings[-1].severity in ("info", "low")


def test_empty_portfolio_no_warnings(db_conn):
    warnings = get_all_warnings(db_conn)
    assert len(warnings) == 0


def test_healthy_portfolio_minimal_warnings(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    s1 = create_asset(db_conn, Asset(symbol="VTI", name="Vanguard Total", asset_type="etf"))
    s2 = create_asset(db_conn, Asset(symbol="BND", name="Vanguard Bond", asset_type="etf"))
    ledger.buy(db_conn, "2025-01-15", s1.id, quantity=100, price=200.0)
    ledger.buy(db_conn, "2025-01-15", s2.id, quantity=100, price=100.0)
    upsert_price(db_conn, s1.id, "2025-01-15", 200.0)
    upsert_price(db_conn, s2.id, "2025-01-15", 100.0)

    warnings = get_all_warnings(db_conn)
    # only info-level warnings expected (missing journal)
    non_info = [w for w in warnings if w.severity != "info"]
    assert len(non_info) == 0


def test_warning_messages_no_recommendations(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    btc = create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    ledger.buy(db_conn, "2025-01-15", btc.id, quantity=1, price=50000.0)

    warnings = get_all_warnings(db_conn)
    for w in warnings:
        msg = w.message.lower()
        assert "should buy" not in msg
        assert "should sell" not in msg
        assert "recommend" not in msg
        assert "good investment" not in msg
        assert "bad investment" not in msg


# --- Sold properties excluded from risk checks ---

def test_sold_property_no_ltv_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset, _, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, mortgage_balance=450000.0,
        down_payment=50000.0,
    )
    assert len(check_real_estate_ltv(db_conn)) == 1
    ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0)
    assert len(check_real_estate_ltv(db_conn)) == 0


def test_sold_property_no_leverage_warning(db_conn):
    ledger.deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset, _, _ = ledger.add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, mortgage_balance=450000.0,
        down_payment=50000.0,
    )
    warns_before = check_leverage(db_conn)
    assert len(warns_before) == 1
    ledger.sell_property(db_conn, "2025-06-01", asset.id, 550000.0)
    warns_after = check_leverage(db_conn)
    assert len(warns_after) == 0
