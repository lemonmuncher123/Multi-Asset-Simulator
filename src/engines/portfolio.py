import sqlite3
from src.models.position import Position
from src.storage.transaction_repo import list_transactions
from src.storage.property_repo import list_active_properties
from src.storage.debt_repo import list_debts
from src.storage.asset_repo import get_asset
from src.storage.price_repo import get_latest_price


def calc_cash_balance(
    conn: sqlite3.Connection, as_of_date: str | None = None,
) -> float:
    if as_of_date is None:
        row = conn.execute(
            "SELECT COALESCE(SUM(total_amount), 0) as cash FROM transactions"
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COALESCE(SUM(total_amount), 0) as cash FROM transactions "
            "WHERE date <= ?",
            (as_of_date,),
        ).fetchone()
    return row["cash"]


def calc_positions(
    conn: sqlite3.Connection, as_of_date: str | None = None,
) -> list[Position]:
    # `manual_adjustment` rows participate when they carry both quantity and
    # price (validated by ledger.manual_adjustment); they then behave as a
    # buy-equivalent for the cost-basis average.
    sql = """
        SELECT
            t.asset_id,
            a.symbol,
            a.name,
            a.asset_type,
            a.currency,
            SUM(CASE
                    WHEN t.txn_type = 'buy' THEN t.quantity
                    WHEN t.txn_type = 'sell' THEN -t.quantity
                    WHEN t.txn_type = 'manual_adjustment' AND t.quantity IS NOT NULL
                        THEN t.quantity
                    ELSE 0
                END) as net_quantity,
            SUM(CASE
                    WHEN t.txn_type = 'buy' THEN t.quantity * t.price
                    WHEN t.txn_type = 'manual_adjustment'
                        AND t.quantity IS NOT NULL AND t.price IS NOT NULL
                        THEN t.quantity * t.price
                    ELSE 0
                END) as total_cost_buy,
            SUM(CASE
                    WHEN t.txn_type = 'buy' THEN t.quantity
                    WHEN t.txn_type = 'manual_adjustment'
                        AND t.quantity IS NOT NULL AND t.price IS NOT NULL
                        THEN t.quantity
                    ELSE 0
                END) as total_qty_buy
        FROM transactions t
        JOIN assets a ON t.asset_id = a.id
        WHERE t.asset_id IS NOT NULL
          AND a.asset_type IN ('stock', 'etf', 'crypto', 'custom')
    """
    params: tuple = ()
    if as_of_date is not None:
        sql += " AND t.date <= ?"
        params = (as_of_date,)
    sql += """
        GROUP BY t.asset_id
        HAVING net_quantity > 0
    """
    rows = conn.execute(sql, params).fetchall()

    positions = []
    for r in rows:
        qty = r["net_quantity"]
        # `total_cost_buy` is the SQL `SUM(qty*price)` over buys; it can
        # be NULL when every buy row has `price IS NULL` (e.g. a "free
        # shares" entry simulating a stock split or gift). Coerce to 0
        # so the avg-price denominator stays well-defined.
        total_cost = r["total_cost_buy"] or 0.0
        avg_price = total_cost / r["total_qty_buy"] if r["total_qty_buy"] else 0.0
        cost_basis = avg_price * qty

        current_price = get_latest_price(conn, r["asset_id"])
        market_value = current_price * qty if current_price else None
        unrealized = market_value - cost_basis if market_value is not None else None

        positions.append(Position(
            asset_id=r["asset_id"],
            symbol=r["symbol"],
            name=r["name"],
            asset_type=r["asset_type"],
            quantity=qty,
            cost_basis=cost_basis,
            average_price=avg_price,
            current_price=current_price,
            market_value=market_value,
            unrealized_pnl=unrealized,
            currency=r["currency"],
        ))
    return positions


def calc_total_property_value(conn: sqlite3.Connection) -> float:
    props = list_active_properties(conn)
    return sum(p.current_value or 0 for p in props)


def calc_total_mortgage(conn: sqlite3.Connection) -> float:
    """Sum of current_balance across all active mortgages. Mortgages
    moved out of `properties` in schema v11 — they live in their own
    table linked to properties via property_id. Active = balance > 0,
    same convention as debt."""
    from src.storage.mortgage_repo import list_active_mortgages
    return sum(m.current_balance for m in list_active_mortgages(conn))


def calc_total_debt(conn: sqlite3.Connection) -> float:
    debts = list_debts(conn)
    return sum(d.current_balance for d in debts)


def calc_total_liabilities(conn: sqlite3.Connection) -> float:
    return calc_total_mortgage(conn) + calc_total_debt(conn)


def calc_position_value(conn: sqlite3.Connection) -> float:
    return sum(p.effective_value() for p in calc_positions(conn))


def calc_total_assets(conn: sqlite3.Connection) -> float:
    cash = calc_cash_balance(conn)
    positions_val = calc_position_value(conn)
    property_val = calc_total_property_value(conn)
    return cash + positions_val + property_val


def calc_net_worth(conn: sqlite3.Connection) -> float:
    return calc_total_assets(conn) - calc_total_liabilities(conn)


def calc_real_estate_equity(conn: sqlite3.Connection) -> float:
    return calc_total_property_value(conn) - calc_total_mortgage(conn)


def get_portfolio_summary(conn: sqlite3.Connection) -> dict:
    cash = calc_cash_balance(conn)
    positions = calc_positions(conn)
    position_val = sum(p.effective_value() for p in positions)
    property_val = calc_total_property_value(conn)
    total_assets = cash + position_val + property_val

    mortgage = calc_total_mortgage(conn)
    debt = calc_total_debt(conn)
    total_liabilities = mortgage + debt
    net_worth = total_assets - total_liabilities

    return {
        "cash": cash,
        "positions_value": position_val,
        "property_value": property_val,
        "total_assets": total_assets,
        "mortgage": mortgage,
        "debt": debt,
        "total_liabilities": total_liabilities,
        "net_worth": net_worth,
        "real_estate_equity": property_val - mortgage,
        "positions": positions,
    }
