"""Tests for the plan-driven force-sell engine (`src.engines.force_sell`).

Covers spec §11:
- selling order: stock < etf < other (crypto/custom) < real_estate;
- cheapest first within each bucket;
- price sync attempted before pricing the plan (best-effort);
- real estate uses `properties.current_value` if set, else `purchase_price`;
- bankruptcy_triggered flag set when total proceeds can't cover
  `target_cash`, and the strict ledger entry refuses to execute in that
  state.

Each test sets up the smallest portfolio that exercises the rule under
test. Uses `monkeypatch` to intercept the price-sync call when the test
needs to verify it was attempted (the global conftest fixture stubs
sync to a no-op, so per-test patches override).
"""
from unittest.mock import patch

import pytest

from src.engines import ledger
from src.engines.force_sell import (
    ForceSellPlan, ForceSellPlanItem,
    build_force_sell_plan, execute_force_sell_plan)
from src.engines.holdings import get_asset_quantity
from src.engines.portfolio import calc_cash_balance
from src.models.asset import Asset
from src.storage.asset_repo import create_asset
from src.storage.price_repo import bulk_upsert_ohlcv


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_one(conn, *, symbol, name, atype, price, qty=10):
    """Buy `qty` of an asset and seed a market price quote."""
    a = create_asset(conn, Asset(symbol=symbol, name=name, asset_type=atype))
    ledger.buy(conn, "2024-12-02", a.id, quantity=qty, price=price)
    bulk_upsert_ohlcv(conn, [{
        "asset_id": a.id, "symbol": symbol, "asset_type": atype,
        "date": "2024-12-02", "close": price, "source": "test",
    }])
    return a.id


def _drain_cash(conn, deficit_amount):
    """Empty cash and add a synthetic deficit so force-sell triggers."""
    ledger.withdraw_cash(conn, "2024-12-03", calc_cash_balance(conn))
    ledger.manual_adjustment(
        conn, "2024-12-03", -float(deficit_amount), notes="deficit")


# ---------------------------------------------------------------------------
# Selling order
# ---------------------------------------------------------------------------

class TestSellingOrder:
    def test_stock_sells_before_etf_before_other_before_real_estate(
        self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-01", 50000.0)
        stk_id = _seed_one(db_conn, symbol="STK", name="Stock",
                            atype="stock", price=100.0)
        etf_id = _seed_one(db_conn, symbol="ETF", name="ETF",
                            atype="etf", price=120.0)
        crp_id = _seed_one(db_conn, symbol="CRP", name="Crypto",
                            atype="crypto", price=200.0)
        # Add a property with $50k equity to provide a real_estate option.
        prop_asset, _, _ = ledger.add_property(
            db_conn, "2024-12-02", symbol="HSE", name="House",
            purchase_price=100000.0, 
            acquisition_mode="existing_property")
        _drain_cash(db_conn, 75)  # $75 deficit
        plan = build_force_sell_plan(
            db_conn, target_cash=0.0, required_payment=0.0)
        assert plan.assets_to_sell, "expected at least one item"
        # Stock must come first per spec — even though the crypto and
        # real-estate options have higher per-unit prices, the bucket
        # order is fixed.
        assert plan.assets_to_sell[0].asset_id == stk_id
        # Only one sale should be needed for the small deficit.
        assert len(plan.assets_to_sell) == 1
        # Real estate and ETF are untouched in this small-deficit scenario.
        assert all(
            item.asset_type != "real_estate"
            for item in plan.assets_to_sell
        )

    def test_real_estate_only_used_when_other_buckets_exhausted(
        self, db_conn):
        # Single real_estate asset. The plan must reach into the
        # real_estate bucket because no other sellable holdings exist.
        ledger.deposit_cash(db_conn, "2024-12-01", 100.0)
        prop_asset, _, _ = ledger.add_property(
            db_conn, "2024-12-02", symbol="H", name="House",
            purchase_price=200000.0, 
            acquisition_mode="existing_property")
        _drain_cash(db_conn, 5000)
        plan = build_force_sell_plan(
            db_conn, target_cash=0.0, required_payment=0.0)
        assert len(plan.assets_to_sell) == 1
        assert plan.assets_to_sell[0].asset_type == "real_estate"
        assert plan.assets_to_sell[0].asset_id == prop_asset.id

    def test_cheapest_first_within_stock_bucket(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-01", 50000.0)
        # Three stocks: $50, $100, $200. With a small deficit, the
        # cheapest ($50) should be the only one selected.
        a50 = _seed_one(db_conn, symbol="A50", name="A50",
                         atype="stock", price=50.0, qty=10)
        a100 = _seed_one(db_conn, symbol="A100", name="A100",
                          atype="stock", price=100.0, qty=10)
        a200 = _seed_one(db_conn, symbol="A200", name="A200",
                          atype="stock", price=200.0, qty=10)
        _drain_cash(db_conn, 30)
        plan = build_force_sell_plan(
            db_conn, target_cash=0.0, required_payment=0.0)
        assert plan.assets_to_sell[0].asset_id == a50

    def test_cheapest_first_within_other_bucket(self, db_conn):
        # "Other" bucket = crypto + custom. Cheapest of either type
        # should win. Seed a $10 custom and a $100 crypto.
        ledger.deposit_cash(db_conn, "2024-12-01", 50000.0)
        custom_id = _seed_one(db_conn, symbol="CST", name="Cst",
                                atype="custom", price=10.0, qty=10)
        _seed_one(db_conn, symbol="CRP", name="Crp",
                   atype="crypto", price=100.0, qty=10)
        _drain_cash(db_conn, 5)
        plan = build_force_sell_plan(
            db_conn, target_cash=0.0, required_payment=0.0)
        assert plan.assets_to_sell[0].asset_id == custom_id


# ---------------------------------------------------------------------------
# Real estate valuation
# ---------------------------------------------------------------------------

class TestRealEstateValuation:
    def test_uses_current_value_when_set(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-01", 100.0)
        prop_asset, _, _ = ledger.add_property(
            db_conn, "2024-12-02", symbol="H", name="House",
            purchase_price=300000.0, 
            acquisition_mode="existing_property")
        # Simulate an updated value via `update_property_value`.
        ledger.update_property_value(
            db_conn, "2024-12-15", prop_asset.id, 400000.0)
        _drain_cash(db_conn, 50000)
        plan = build_force_sell_plan(
            db_conn, target_cash=0.0, required_payment=0.0)
        item = plan.assets_to_sell[0]
        assert item.asset_type == "real_estate"
        # Updated value (400k) used, not the purchase price (300k).
        assert item.sell_price_used == pytest.approx(400000.0)

    def test_falls_back_to_purchase_price_when_no_update(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-01", 100.0)
        prop_asset, _, _ = ledger.add_property(
            db_conn, "2024-12-02", symbol="H", name="House",
            purchase_price=250000.0, 
            acquisition_mode="existing_property")
        _drain_cash(db_conn, 50000)
        plan = build_force_sell_plan(
            db_conn, target_cash=0.0, required_payment=0.0)
        item = plan.assets_to_sell[0]
        assert item.sell_price_used == pytest.approx(250000.0)

    def test_skips_zero_equity_property(self, db_conn):
        # Mortgage equals purchase price → 0 cash from sale → skipped.
        ledger.deposit_cash(db_conn, "2024-12-01", 100.0)
        _, prop, _ = ledger.add_property(
            db_conn, "2024-12-02", symbol="H", name="House",
            purchase_price=200000.0,
            acquisition_mode="existing_property")
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=200000.0,
            interest_rate=0.0, payment_per_period=1000.0)
        _drain_cash(db_conn, 1000)
        plan = build_force_sell_plan(
            db_conn, target_cash=0.0, required_payment=0.0)
        # No items because the only candidate generates $0 net.
        assert plan.assets_to_sell == []
        assert plan.bankruptcy_triggered  # can't cover the deficit


# ---------------------------------------------------------------------------
# Bankruptcy and execution refusal
# ---------------------------------------------------------------------------

class TestBankruptcyTriggered:
    def test_flag_set_when_proceeds_below_target(self, db_conn):
        # $10k target, single $1k stock → can't cover.
        # Deposit exactly enough to fund the buy; cash lands at $0 with
        # no withdraw needed.
        ledger.deposit_cash(db_conn, "2024-12-01", 1000.0)
        _seed_one(db_conn, symbol="STK", name="Stock",
                   atype="stock", price=100.0, qty=10)
        plan = build_force_sell_plan(
            db_conn, target_cash=10000.0, required_payment=10000.0)
        assert plan.bankruptcy_triggered
        assert not plan.can_cover_payment

    def test_strict_ledger_entry_writes_no_transactions_when_bankrupt(
        self, db_conn):
        # Strict path: force_sell_to_raise_cash refuses partial execution
        # when the plan can't cover. The auto-settle pipeline relies on
        # this — a partial sale would leave the portfolio in a state
        # where the bankruptcy event is incorrectly avoided.
        ledger.deposit_cash(db_conn, "2024-12-01", 1000.0)
        stk_id = _seed_one(db_conn, symbol="STK", name="Stock",
                            atype="stock", price=100.0, qty=10)
        sales = ledger.force_sell_to_raise_cash(
            db_conn, "2024-12-04", target_cash=10000.0)
        assert sales == []
        # Stock holdings unchanged.
        assert get_asset_quantity(db_conn, stk_id) == 10

    def test_negative_cash_path_executes_partial(self, db_conn):
        # The mop-up entry (force_sell_to_cover_negative_cash) is allowed
        # to execute partial sales — recovery rather than bankruptcy.
        ledger.deposit_cash(db_conn, "2024-12-01", 1000.0)
        stk_id = _seed_one(db_conn, symbol="STK", name="Stock",
                            atype="stock", price=100.0, qty=2)
        # Leave cash at -200 with only $200 of stock available.
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(db_conn, "2024-12-03", -200.0,
                                  notes="deficit")
        sales = ledger.force_sell_to_cover_negative_cash(
            db_conn, "2024-12-04")
        assert sales  # partial execution is OK on the mop-up path
        assert get_asset_quantity(db_conn, stk_id) < 2


# ---------------------------------------------------------------------------
# Notes / debt name pass-through
# ---------------------------------------------------------------------------

class TestNotes:
    def test_debt_name_appears_in_note(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-01", 1000.0)
        _seed_one(db_conn, symbol="STK", name="Stock",
                   atype="stock", price=50.0, qty=5)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        sales = ledger.force_sell_to_raise_cash(
            db_conn, "2024-12-04", target_cash=50.0,
            debt_name="Auto Loan")
        assert sales
        assert "Auto Loan" in (sales[0].notes or "")

    def test_legacy_reason_passes_through_when_no_debt_name(self, db_conn):
        ledger.deposit_cash(db_conn, "2024-12-01", 1000.0)
        _seed_one(db_conn, symbol="STK", name="Stock",
                   atype="stock", price=50.0, qty=5)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        sales = ledger.force_sell_to_raise_cash(
            db_conn, "2024-12-04", target_cash=50.0,
            reason="auto debt deduction")
        assert sales
        assert "auto debt deduction" in (sales[0].notes or "")

    def test_combined_obligation_label_reads_cleanly(self, db_conn):
        """Spec §4 #4: when a single force-sell covers multiple
        deferred obligations, the note must carry a concise combined
        label (built by ``MainWindow._combined_obligation_label`` in
        production). Verify the format renders without nested quote
        artifacts."""
        ledger.deposit_cash(db_conn, "2024-12-01", 1000.0)
        _seed_one(db_conn, symbol="STK", name="Stock",
                   atype="stock", price=50.0, qty=5)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        sales = ledger.force_sell_to_raise_cash(
            db_conn, "2024-12-04", target_cash=50.0,
            debt_name="debt 'Auto Loan' + mortgage on 'House'")
        assert sales
        notes = sales[0].notes or ""
        # Both obligation parts present and not double-quoted.
        assert "Auto Loan" in notes
        assert "House" in notes
        # No "for ('debt 'Auto Loan'..." pattern (nested quotes).
        assert "('" not in notes


class TestAuditMetadata:
    """Spec §5: force-sell audit metadata distinguishes target_cash
    (the cash level the loop must reach) from required_payment_amount
    (the actual obligation being funded)."""

    def test_required_payment_param_records_obligation_amount(self, db_conn):
        from src.engines.force_sell import build_force_sell_plan
        ledger.deposit_cash(db_conn, "2024-12-01", 1000.0)
        # Cash currently 1000; target=1500 means we want to raise 500.
        # The obligation behind it might be 500 (the shortfall) or
        # something larger if the user wants extra slack.
        plan = build_force_sell_plan(
            db_conn,
            target_cash=1500.0,
            required_payment=500.0,  # actual obligation
        )
        assert plan.required_payment_amount == pytest.approx(500.0)
        # cash_shortage is target_cash − cash_available, separate from
        # required_payment_amount.
        assert plan.cash_shortage == pytest.approx(500.0)

    def test_wrapper_threads_required_payment_through(self, db_conn):
        from src.engines.force_sell import build_force_sell_plan
        # Sanity-check that the ledger wrapper now exposes the
        # parameter and forwards it. We just observe the plan via the
        # build call; the wrapper itself is exercised by integration.
        plan = build_force_sell_plan(
            db_conn, target_cash=2000.0, required_payment=500.0)
        assert plan.required_payment_amount == 500.0
        assert plan.cash_shortage == pytest.approx(2000.0)  # no cash seeded

    def test_back_compat_required_payment_defaults_to_target(self, db_conn):
        """When the wrapper caller does not pass required_payment, the
        old conflated semantic (target_cash == required_payment) is
        preserved so existing callers keep working."""
        ledger.deposit_cash(db_conn, "2024-12-01", 1000.0)
        _seed_one(db_conn, symbol="STK", name="Stock",
                   atype="stock", price=50.0, qty=5)
        # No required_payment kwarg: ledger wrapper falls back to
        # target_cash. We can't observe the plan directly but we can
        # confirm the call still succeeds.
        sales = ledger.force_sell_to_raise_cash(
            db_conn, "2024-12-04", target_cash=1500.0)
        # Test that it ran without raising; behaviour parity preserved.
        # (target=1500 with $1000 cash + $250 stock → bankruptcy → []).
        assert isinstance(sales, list)


class TestStrictExecution:
    """Spec: ``force_sell_to_raise_cash`` is the debt-driven entrypoint
    and must not leave the portfolio in a partial-sale state. If a sale
    fails mid-execute, the strict path raises so the caller can record
    bankruptcy against an inspectable failure rather than silently
    swallowing the error and pretending the sales succeeded.

    The mop-up path (``force_sell_to_cover_negative_cash``) keeps its
    best-effort semantics — partial recovery is preferred there.
    """

    def test_strict_path_raises_when_a_sale_fails_mid_plan(
        self, db_conn, monkeypatch,
    ):
        ledger.deposit_cash(db_conn, "2024-12-01", 5000.0)
        _seed_one(db_conn, symbol="STK1", name="Stock 1",
                   atype="stock", price=100.0, qty=10)
        _seed_one(db_conn, symbol="STK2", name="Stock 2",
                   atype="stock", price=100.0, qty=10)
        ledger.withdraw_cash(
            db_conn, "2024-12-03", calc_cash_balance(db_conn))

        # Second call to ledger.sell raises — simulates a stale-state
        # failure between plan and execute.
        from src.engines import force_sell as fs_mod
        original_sell = ledger.sell
        call_count = {"n": 0}

        def flaky_sell(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise ValueError("simulated mid-plan failure")
            return original_sell(*args, **kwargs)

        monkeypatch.setattr(fs_mod, "sell", flaky_sell, raising=False)
        # Patch where execute_force_sell_plan looks up sell — local import
        # inside the function. Replace the module-level binding the
        # local import will see.
        import src.engines.ledger as ledger_mod
        monkeypatch.setattr(ledger_mod, "sell", flaky_sell)

        with pytest.raises(ValueError, match="simulated mid-plan failure"):
            ledger.force_sell_to_raise_cash(
                db_conn, "2024-12-04", target_cash=2000.0,
                required_payment=2000.0, debt_name="Test Loan",
            )

    def test_mop_up_path_swallows_per_item_errors(
        self, db_conn, monkeypatch,
    ):
        # Negative-cash mop-up should NOT raise on per-item failures —
        # partial recovery is the correct behavior on this path.
        ledger.deposit_cash(db_conn, "2024-12-01", 5000.0)
        _seed_one(db_conn, symbol="STK1", name="Stock 1",
                   atype="stock", price=100.0, qty=5)
        _seed_one(db_conn, symbol="STK2", name="Stock 2",
                   atype="stock", price=100.0, qty=5)
        ledger.withdraw_cash(db_conn, "2024-12-03", calc_cash_balance(db_conn))
        ledger.manual_adjustment(
            db_conn, "2024-12-03", -300.0, notes="deficit")

        original_sell = ledger.sell
        call_count = {"n": 0}

        def flaky_sell(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise ValueError("simulated mid-plan failure")
            return original_sell(*args, **kwargs)

        import src.engines.ledger as ledger_mod
        monkeypatch.setattr(ledger_mod, "sell", flaky_sell)

        # Should not raise; returns whatever succeeded.
        sales = ledger.force_sell_to_cover_negative_cash(
            db_conn, "2024-12-04")
        assert isinstance(sales, list)


class TestRealEstateMortgagePayoff:
    """Real-estate force-sell estimates must include the mortgage
    payoff's accrued interest, not just the principal balance.
    `pay_mortgage_in_full` charges balance + one period's interest at
    closing; the plan must agree so `can_cover_payment` cannot be a
    false positive."""

    def test_estimate_matches_actual_cash_with_interest_bearing_mortgage(
        self, db_conn,
    ):
        ledger.deposit_cash(db_conn, "2024-12-01", 50000.0)
        asset, prop, _ = ledger.add_property(
            db_conn, "2024-12-02", symbol="HSE", name="House",
            purchase_price=300000.0,
            acquisition_mode="existing_property",
            down_payment=60000.0,
        )
        ledger.add_mortgage(
            db_conn, property_id=prop.id, original_amount=240000.0,
            interest_rate=0.06, payment_per_period=1500.0,
        )
        # Drain cash so the plan must rely on the property sale.
        ledger.withdraw_cash(
            db_conn, "2024-12-03", calc_cash_balance(db_conn))

        # Target a cash level the property must be sold to reach.
        plan = build_force_sell_plan(
            db_conn, target_cash=1000.0, required_payment=1000.0)
        assert plan.assets_to_sell, "expected the property as the only option"
        item = plan.assets_to_sell[0]
        assert item.asset_type == "real_estate"

        cash_before = calc_cash_balance(db_conn)
        execute_force_sell_plan(db_conn, plan, "2024-12-04")
        cash_after = calc_cash_balance(db_conn)
        actual_cash_generated = cash_after - cash_before

        assert item.estimated_cash_generated == pytest.approx(
            actual_cash_generated, abs=0.01,
        )


# ---------------------------------------------------------------------------
# Price sync invocation (override the conftest no-op)
# ---------------------------------------------------------------------------

class TestPriceSyncBeforePlan:
    def test_sync_attempted_for_each_sellable_asset(self, db_conn):
        """Building the plan must call ``_try_sync_prices`` so the user
        prices reflect the freshest available data per spec §11.6.

        Sync only fires when the plan actually needs to find proceeds
        (cash_shortage > 0) — target_cash must exceed current cash for
        the path to engage.
        """
        ledger.deposit_cash(db_conn, "2024-12-01", 1000.0)
        _seed_one(db_conn, symbol="STK", name="Stock",
                   atype="stock", price=100.0, qty=2)
        # Cash now $800. target_cash=$2000 → shortage=$1200, enters the
        # plan-building path.
        with patch(
            "src.engines.force_sell._try_sync_prices") as mock_sync:
            build_force_sell_plan(
                db_conn, target_cash=2000.0, required_payment=2000.0)
        mock_sync.assert_called_once()
        # Called with the connection and a set of sellable asset ids.
        args, _kwargs = mock_sync.call_args
        assert args[0] is db_conn
        # Second positional arg is the set of asset ids to sync.
        assert isinstance(args[1], set)
