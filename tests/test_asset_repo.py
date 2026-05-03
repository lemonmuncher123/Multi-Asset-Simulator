from src.models.asset import Asset
from src.storage.asset_repo import create_asset, get_asset, get_asset_by_symbol, list_assets, update_asset, delete_asset


def test_create_and_get_asset(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple Inc", asset_type="stock"))
    assert asset.id is not None

    fetched = get_asset(db_conn, asset.id)
    assert fetched.symbol == "AAPL"
    assert fetched.name == "Apple Inc"
    assert fetched.asset_type == "stock"
    assert fetched.currency == "USD"
    assert fetched.region == "US"
    assert fetched.liquidity == "liquid"


def test_get_asset_by_symbol(db_conn):
    create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    fetched = get_asset_by_symbol(db_conn, "BTC")
    assert fetched is not None
    assert fetched.name == "Bitcoin"


def test_get_asset_not_found(db_conn):
    assert get_asset(db_conn, 9999) is None


def test_list_assets_empty(db_conn):
    assert list_assets(db_conn) == []


def test_list_assets(db_conn):
    create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    create_asset(db_conn, Asset(symbol="BTC", name="Bitcoin", asset_type="crypto"))
    create_asset(db_conn, Asset(symbol="VTI", name="Vanguard Total", asset_type="etf"))

    all_assets = list_assets(db_conn)
    assert len(all_assets) == 3

    stocks = list_assets(db_conn, asset_type="stock")
    assert len(stocks) == 1
    assert stocks[0].symbol == "AAPL"


def test_update_asset(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    asset.name = "Apple Inc"
    asset.region = "Global"
    update_asset(db_conn, asset)

    fetched = get_asset(db_conn, asset.id)
    assert fetched.name == "Apple Inc"
    assert fetched.region == "Global"


def test_delete_asset(db_conn):
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    delete_asset(db_conn, asset.id)
    assert get_asset(db_conn, asset.id) is None
