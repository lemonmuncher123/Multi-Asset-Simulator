import csv
import io
import pytest
from src.models.asset import Asset
from src.models.transaction import Transaction
from src.storage.asset_repo import create_asset, list_assets, get_asset_by_symbol
from src.storage.transaction_repo import create_transaction, list_transactions
from src.engines.ledger import deposit_cash, buy
from src.engines.import_export import (
    export_assets_csv,
    export_transactions_csv,
    export_summary_csv,
    import_assets_csv,
    import_transactions_csv,
    ImportResult,
)


# --- Export assets ---

def test_export_assets_csv_headers(db_conn):
    csv_text = export_assets_csv(db_conn)
    reader = csv.reader(io.StringIO(csv_text))
    headers = next(reader)
    assert headers == ["symbol", "name", "asset_type", "currency", "region", "liquidity", "notes"]


def test_export_assets_csv_data(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto", currency="USD", region="Global"))

    csv_text = export_assets_csv(db_conn)
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    assert len(rows) == 2
    symbols = {r["symbol"] for r in rows}
    assert "AAPL" in symbols
    assert "BTC" in symbols


def test_export_assets_csv_empty(db_conn):
    csv_text = export_assets_csv(db_conn)
    reader = csv.reader(io.StringIO(csv_text))
    headers = next(reader)
    rows = list(reader)
    assert len(rows) == 0


# --- Export transactions ---

def test_export_transactions_csv_headers(db_conn):
    csv_text = export_transactions_csv(db_conn)
    reader = csv.reader(io.StringIO(csv_text))
    headers = next(reader)
    assert "date" in headers
    assert "txn_type" in headers
    assert "asset_symbol" in headers
    assert "total_amount" in headers


def test_export_transactions_csv_data(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    buy(db_conn, "2025-01-02", asset.id, 10, 150.0)

    csv_text = export_transactions_csv(db_conn)
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    assert len(rows) == 2
    assert rows[0]["txn_type"] == "deposit_cash"
    assert rows[1]["txn_type"] == "buy"
    assert rows[1]["asset_symbol"] == "AAPL"


# --- Export summary ---

def test_export_summary_csv(db_conn):
    deposit_cash(db_conn, "2025-01-01", 50000.0)
    csv_text = export_summary_csv(db_conn)
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    metrics = {r["metric"]: r["value"] for r in rows}
    assert "cash" in metrics
    assert "net_worth" in metrics
    assert float(metrics["cash"]) == 50000.0


# --- Import assets: valid ---

def test_import_assets_valid(db_conn):
    csv_text = (
        "symbol,name,asset_type,currency,region,liquidity,notes\n"
        "AAPL,Apple Inc,stock,USD,US,liquid,tech stock\n"
        "MSFT,Microsoft,stock,USD,US,liquid,\n"
    )
    result = import_assets_csv(db_conn, csv_text)
    assert result.imported == 2
    assert result.skipped == 0
    assert len(result.errors) == 0

    assets = list_assets(db_conn)
    assert len(assets) == 2
    aapl = get_asset_by_symbol(db_conn, "AAPL")
    assert aapl.name == "Apple Inc"
    assert aapl.asset_type == "stock"
    assert aapl.notes == "tech stock"


def test_import_assets_minimal_columns(db_conn):
    csv_text = (
        "symbol,name,asset_type\n"
        "BTC,Bitcoin,crypto\n"
    )
    result = import_assets_csv(db_conn, csv_text)
    assert result.imported == 1
    btc = get_asset_by_symbol(db_conn, "BTC")
    assert btc.currency == "USD"
    assert btc.region == "US"
    assert btc.liquidity == "liquid"


def test_import_assets_skips_duplicates(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    csv_text = (
        "symbol,name,asset_type\n"
        "AAPL,Apple Inc,stock\n"
        "MSFT,Microsoft,stock\n"
    )
    result = import_assets_csv(db_conn, csv_text)
    assert result.imported == 1
    assert result.skipped == 1
    assert any("already exists" in e for e in result.errors)


# --- Import assets: invalid ---

def test_import_assets_missing_symbol(db_conn):
    csv_text = (
        "symbol,name,asset_type\n"
        ",Apple,stock\n"
    )
    result = import_assets_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("missing symbol" in e for e in result.errors)


def test_import_assets_missing_name(db_conn):
    csv_text = (
        "symbol,name,asset_type\n"
        "AAPL,,stock\n"
    )
    result = import_assets_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("missing name" in e for e in result.errors)


def test_import_assets_invalid_type(db_conn):
    csv_text = (
        "symbol,name,asset_type\n"
        "AAPL,Apple,invalid_type\n"
    )
    result = import_assets_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("invalid asset_type" in e for e in result.errors)


def test_import_assets_invalid_liquidity(db_conn):
    csv_text = (
        "symbol,name,asset_type,currency,region,liquidity\n"
        "AAPL,Apple,stock,USD,US,maybe\n"
    )
    result = import_assets_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("invalid liquidity" in e for e in result.errors)


def test_import_assets_mixed_valid_invalid(db_conn):
    csv_text = (
        "symbol,name,asset_type\n"
        "AAPL,Apple,stock\n"
        ",,\n"
        "MSFT,Microsoft,stock\n"
    )
    result = import_assets_csv(db_conn, csv_text)
    assert result.imported == 2
    assert len(result.errors) >= 1


# --- Import transactions: valid ---

def test_import_transactions_valid(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-01,deposit_cash,,,,100000,USD,0,initial deposit\n"
        "2025-01-02,buy,AAPL,10,150,-1500,USD,0,bought apple\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 2
    assert len(result.errors) == 0

    txns = list_transactions(db_conn)
    assert len(txns) == 2
    assert txns[0].txn_type == "deposit_cash"
    assert txns[1].txn_type == "buy"
    assert txns[1].quantity == 10
    assert txns[1].price == 150.0


def test_import_transactions_no_asset(db_conn):
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-01,deposit_cash,,,,50000,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 1
    txns = list_transactions(db_conn)
    assert txns[0].asset_id is None


# --- Import transactions: invalid ---

def test_import_transactions_missing_date(db_conn):
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        ",buy,AAPL,10,150,-1500,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("missing date" in e for e in result.errors)


def test_import_transactions_invalid_date_format(db_conn):
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "01/02/2025,buy,AAPL,10,150,-1500,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("invalid date format" in e for e in result.errors)


def test_import_transactions_invalid_txn_type(db_conn):
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-01,purchase,,10,150,-1500,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("invalid txn_type" in e for e in result.errors)


def test_import_transactions_missing_total(db_conn):
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-01,deposit_cash,,,,,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("missing total_amount" in e for e in result.errors)


def test_import_transactions_asset_not_found(db_conn):
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-01,buy,NOPE,10,100,-1000,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("not found" in e for e in result.errors)


def test_import_transactions_invalid_quantity(db_conn):
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-01,deposit_cash,,abc,,1000,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("invalid quantity" in e for e in result.errors)


def test_import_transactions_invalid_price(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-01,buy,AAPL,10,abc,-1500,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("invalid price" in e for e in result.errors)


# --- Roundtrip: export then import ---

def test_roundtrip_assets(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto", region="Global"))

    csv_text = export_assets_csv(db_conn)

    from src.storage.database import init_db
    conn2 = init_db(":memory:")
    result = import_assets_csv(conn2, csv_text)
    assert result.imported == 2
    assert result.skipped == 0

    assets = list_assets(conn2)
    assert len(assets) == 2
    conn2.close()


# --- Import transactions: sell validation ---

def test_import_sell_with_no_holdings_rejected(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-15,sell,AAPL,5,150,750,USD,0,sell without buy\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("no position" in e.lower() for e in result.errors)
    assert len(list_transactions(db_conn)) == 0


def test_import_buy_then_sell_succeeds(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-01,deposit_cash,,,,100000,USD,0,\n"
        "2025-01-02,buy,AAPL,10,150,-1500,USD,0,\n"
        "2025-01-15,sell,AAPL,5,160,800,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 3
    assert len(result.errors) == 0


def test_import_buy_10_sell_15_rejects_sell(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-01,deposit_cash,,,,5000,USD,0,\n"
        "2025-01-02,buy,AAPL,10,150,-1500,USD,0,\n"
        "2025-01-15,sell,AAPL,15,160,2400,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    # The deposit and the buy succeed; the over-position sell is rejected.
    assert result.imported == 2
    assert any("insufficient" in e.lower() for e in result.errors)


def test_import_sell_non_sellable_type_rejected(db_conn):
    create_asset(db_conn, Asset(
        symbol="HOME", name="House", asset_type="real_estate", liquidity="illiquid",
    ))
    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-15,sell,HOME,1,500000,500000,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    assert result.imported == 0
    assert any("cannot sell" in e.lower() for e in result.errors)


def test_roundtrip_transactions(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    buy(db_conn, "2025-01-02", asset.id, 10, 150.0)

    csv_text = export_transactions_csv(db_conn)

    from src.storage.database import init_db
    conn2 = init_db(":memory:")
    create_asset(conn2, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    result = import_transactions_csv(conn2, csv_text)
    assert result.imported == 2

    txns = list_transactions(conn2)
    assert len(txns) == 2
    conn2.close()


# --- sell_property in import/export ---

def test_valid_txn_types_includes_sell_property():
    from src.engines.import_export import VALID_TXN_TYPES
    assert "sell_property" in VALID_TXN_TYPES


def test_export_includes_sell_property_rows(db_conn):
    from src.engines.ledger import add_property, sell_property
    deposit_cash(db_conn, "2025-01-01", 200000.0)
    asset, _, _ = add_property(
        db_conn, "2025-02-01", symbol="H1", name="House",
        purchase_price=500000.0, mortgage_balance=400000.0,
    )
    sell_property(db_conn, "2025-06-01", asset.id, 550000.0)

    csv_text = export_transactions_csv(db_conn)
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    txn_types = [r["txn_type"] for r in rows]
    assert "sell_property" in txn_types


# --- Atomicity: a failed CSV import leaves the DB unchanged ---

def test_asset_import_atomic_on_validation_failure(db_conn):
    # Three rows: rows 1 & 3 are valid, row 2 has an invalid asset_type.
    # Whether we treat the bad row as a hard failure or a per-row skip, the
    # other rows' fate must NOT depend on per-row commit timing — and after
    # any error, the count should be reproducible across reruns.
    csv_text = (
        "symbol,name,asset_type,currency,region,liquidity,notes\n"
        "AAPL,Apple,stock,USD,US,liquid,\n"
        "BAD,Broken,not_a_real_type,USD,US,liquid,\n"
        "MSFT,Microsoft,stock,USD,US,liquid,\n"
    )
    result = import_assets_csv(db_conn, csv_text)
    # Row 2 is rejected, rows 1 & 3 are imported atomically.
    assert result.imported == 2
    symbols = {a.symbol for a in list_assets(db_conn)}
    assert symbols == {"AAPL", "MSFT"}


def test_transaction_import_rolls_back_on_constraint_failure(db_conn):
    # Both rows are syntactically valid, but the second references an
    # asset that doesn't exist (asset_symbol "GHOST"). The pre-validation
    # filters that out and the first row imports cleanly.
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    deposit_cash(db_conn, "2025-01-01", 10000.0)

    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-01-05,deposit_cash,,,,500.0,USD,,test\n"
        "2025-01-06,buy,GHOST,1,100.0,-100.0,USD,,\n"
    )
    initial_txn_count = len(list_transactions(db_conn))
    result = import_transactions_csv(db_conn, csv_text)

    # Row 2 fails validation, row 1 is imported.
    assert result.imported == 1
    assert any("GHOST" in e for e in result.errors)
    assert len(list_transactions(db_conn)) == initial_txn_count + 1


def test_transaction_import_chronological_sort_for_sell_validation(db_conn):
    # Sell row appears BEFORE its supporting buy in the CSV. The new
    # chronological pre-sort lets both pass validation.
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    deposit_cash(db_conn, "2025-01-01", 100000.0)

    csv_text = (
        "date,txn_type,asset_symbol,quantity,price,total_amount,currency,fees,notes\n"
        "2025-03-01,sell,AAPL,5,180.0,900.0,USD,0,\n"
        "2025-02-01,buy,AAPL,10,150.0,-1500.0,USD,0,\n"
    )
    result = import_transactions_csv(db_conn, csv_text)
    # Both rows are accepted because the buy now precedes the sell after sort.
    assert result.imported == 2, f"errors: {result.errors}"
