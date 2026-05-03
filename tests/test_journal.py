import json
import pytest
from src.models.asset import Asset
from src.models.decision_journal import DecisionJournalEntry
from src.storage.asset_repo import create_asset
from src.storage.journal_repo import create_journal_entry, get_journal_entry, get_journal_by_transaction
from src.storage.transaction_repo import get_transaction
from src.engines.ledger import deposit_cash, buy
from src.engines.journal import (
    capture_portfolio_snapshot,
    create_journal_for_transaction,
    set_snapshot_before,
    add_post_trade_review,
    get_before_after,
    calc_structure_changes,
    calc_training_score,
    get_lessons_learned,
    StructureChange,
    TrainingScore,
)


# --- capture_portfolio_snapshot ---

def test_snapshot_empty_portfolio(db_conn):
    snap = capture_portfolio_snapshot(db_conn)
    assert snap["cash"] == 0
    assert snap["total_assets"] == 0
    assert snap["debt_ratio"] == 0
    assert snap["liquid_pct"] == 0
    assert "risk_warnings" in snap


def test_snapshot_with_cash(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    snap = capture_portfolio_snapshot(db_conn)
    assert snap["cash"] == 100000.0
    assert snap["total_assets"] == 100000.0
    assert snap["net_worth"] == 100000.0
    assert snap["asset_type_allocation"]["cash"] == pytest.approx(1.0)


def test_snapshot_with_stock(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    buy(db_conn, "2025-01-02", asset.id, 100, 150.0)

    snap = capture_portfolio_snapshot(db_conn)
    assert snap["cash"] == 100000.0 - 15000.0
    assert snap["max_concentration"]["name"] == "Cash"
    assert "asset_type_allocation" in snap


# --- create_journal_for_transaction ---

def test_create_journal_for_transaction(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    txn = buy(db_conn, "2025-01-02", asset.id, 10, 150.0)

    entry = create_journal_for_transaction(
        db_conn, txn.id,
        thesis="Growth play",
        intended_role="core holding",
        risk_reasoning="Market downturn risk",
        exit_plan="Review in 6 months",
        confidence_level=4,
        expected_holding_period="1 year",
    )

    assert entry.id is not None
    assert entry.transaction_id == txn.id
    assert entry.title == "buy AAPL"
    assert entry.thesis == "Growth play"
    assert entry.confidence_level == 4
    assert entry.snapshot_after is not None

    after = json.loads(entry.snapshot_after)
    assert after["cash"] == 100000.0 - 1500.0

    # The pairing is now expressed only on the journal side
    # (decision_journal.transaction_id); the legacy back-pointer column on
    # `transactions` was dropped in schema v2.
    linked = get_journal_by_transaction(db_conn, txn.id)
    assert linked is not None
    assert linked.id == entry.id


def test_create_journal_links_transaction(db_conn):
    deposit_cash(db_conn, "2025-01-01", 50000.0)
    asset = create_asset(db_conn, Asset(symbol="TSLA", name="Tesla", asset_type="stock"))
    txn = buy(db_conn, "2025-01-02", asset.id, 5, 200.0)

    entry = create_journal_for_transaction(db_conn, txn.id, thesis="Momentum")

    linked = get_journal_by_transaction(db_conn, txn.id)
    assert linked is not None
    assert linked.id == entry.id


# --- set_snapshot_before ---

def test_set_snapshot_before(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    snap_before = capture_portfolio_snapshot(db_conn)

    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    txn = buy(db_conn, "2025-01-02", asset.id, 10, 150.0)

    entry = create_journal_for_transaction(db_conn, txn.id, thesis="Test")
    set_snapshot_before(db_conn, entry, snap_before)

    fetched = get_journal_entry(db_conn, entry.id)
    before = json.loads(fetched.snapshot_before)
    assert before["cash"] == 100000.0


# --- add_post_trade_review ---

def test_add_post_trade_review(db_conn):
    entry = create_journal_entry(db_conn, DecisionJournalEntry(
        date="2025-01-15", title="Buy AAPL", thesis="Growth",
    ))
    updated = add_post_trade_review(
        db_conn, entry.id,
        post_trade_review="Rose 10%",
        mistake_tags="none",
        lesson_learned="Thesis was solid",
    )
    assert updated.post_trade_review == "Rose 10%"
    assert updated.lesson_learned == "Thesis was solid"

    fetched = get_journal_entry(db_conn, entry.id)
    assert fetched.post_trade_review == "Rose 10%"


# --- get_before_after ---

def test_get_before_after_with_data(db_conn):
    before = {"cash": 100000, "debt_ratio": 0}
    after = {"cash": 85000, "debt_ratio": 0}
    entry = create_journal_entry(db_conn, DecisionJournalEntry(
        date="2025-01-15", title="Test",
        snapshot_before=json.dumps(before),
        snapshot_after=json.dumps(after),
    ))
    b, a = get_before_after(entry)
    assert b["cash"] == 100000
    assert a["cash"] == 85000


def test_get_before_after_no_snapshots():
    entry = DecisionJournalEntry(date="2025-01-15", title="Test")
    b, a = get_before_after(entry)
    assert b is None
    assert a is None


# --- calc_structure_changes ---

def test_structure_changes_cash():
    before = {
        "cash": 100000, "debt_ratio": 0, "max_concentration": {"name": "Cash", "pct": 1.0},
        "illiquid_pct": 0, "asset_type_allocation": {"cash": 1.0},
    }
    after = {
        "cash": 85000, "debt_ratio": 0, "max_concentration": {"name": "Cash", "pct": 0.85},
        "illiquid_pct": 0, "asset_type_allocation": {"cash": 0.85, "stock": 0.15},
    }
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Test",
        snapshot_before=json.dumps(before),
        snapshot_after=json.dumps(after),
    )
    changes = calc_structure_changes(entry)
    cash_changes = [c for c in changes if c.metric == "Cash"]
    assert len(cash_changes) == 1
    assert cash_changes[0].direction == "decreased"


def test_structure_changes_concentration():
    before = {
        "cash": 100000, "debt_ratio": 0,
        "max_concentration": {"name": "Cash", "pct": 1.0},
        "illiquid_pct": 0, "asset_type_allocation": {"cash": 1.0},
    }
    after = {
        "cash": 50000, "debt_ratio": 0,
        "max_concentration": {"name": "AAPL", "pct": 0.50},
        "illiquid_pct": 0, "asset_type_allocation": {"cash": 0.5, "stock": 0.5},
    }
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Test",
        snapshot_before=json.dumps(before),
        snapshot_after=json.dumps(after),
    )
    changes = calc_structure_changes(entry)
    conc_changes = [c for c in changes if "Concentration" in c.metric]
    assert len(conc_changes) == 1
    assert conc_changes[0].direction == "decreased"


def test_structure_changes_empty_without_snapshots():
    entry = DecisionJournalEntry(date="2025-01-15", title="Test")
    changes = calc_structure_changes(entry)
    assert changes == []


# --- calc_training_score ---

def test_training_score_full_journal():
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Buy AAPL",
        thesis="Growth in AI segment",
        intended_role="core holding",
        risk_reasoning="Macro risk, valuation stretched",
        exit_plan="Review at earnings",
        confidence_level=4,
        expected_holding_period="1 year",
    )
    score = calc_training_score(entry)
    assert score.journal_quality_score == 100.0
    assert score.overall_score > 0
    assert len(score.details) == 0


def test_training_score_missing_thesis():
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Buy AAPL",
        thesis=None,
        risk_reasoning="Some risk",
        exit_plan="Review later",
        intended_role="growth",
        confidence_level=3,
        expected_holding_period="6 months",
    )
    score = calc_training_score(entry)
    assert score.journal_quality_score == 80.0
    assert any("thesis" in d.lower() for d in score.details)


def test_training_score_missing_risk_reasoning():
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Buy AAPL",
        thesis="Growth",
        risk_reasoning=None,
        exit_plan="Review later",
        intended_role="growth",
        confidence_level=3,
        expected_holding_period="6 months",
    )
    score = calc_training_score(entry)
    assert score.journal_quality_score == 80.0
    assert any("risk reasoning" in d.lower() for d in score.details)


def test_training_score_missing_exit_plan():
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Buy AAPL",
        thesis="Growth",
        risk_reasoning="Macro risk",
        exit_plan=None,
        intended_role="growth",
        confidence_level=3,
        expected_holding_period="6 months",
    )
    score = calc_training_score(entry)
    assert score.journal_quality_score == 85.0


def test_training_score_all_missing():
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Impulse buy",
    )
    score = calc_training_score(entry)
    assert score.journal_quality_score == 20.0
    assert len(score.details) == 6


def test_training_score_high_concentration():
    after = {
        "cash": 10000, "total_assets": 100000,
        "debt_ratio": 0, "crypto_pct": 0,
        "illiquid_pct": 0,
        "max_concentration": {"name": "AAPL", "pct": 0.50},
        "risk_warnings": [],
    }
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Buy AAPL",
        thesis="Growth", intended_role="core", risk_reasoning="Risk",
        exit_plan="Review", confidence_level=4, expected_holding_period="1 year",
        snapshot_after=json.dumps(after),
    )
    score = calc_training_score(entry)
    assert score.concentration_score < 100
    assert any("concentration" in d.lower() for d in score.details)


def test_training_score_high_debt():
    after = {
        "cash": 10000, "total_assets": 100000,
        "debt_ratio": 0.60, "crypto_pct": 0,
        "illiquid_pct": 0,
        "max_concentration": {"name": "Cash", "pct": 0.10},
        "risk_warnings": [],
    }
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Buy more",
        thesis="Thesis", intended_role="role", risk_reasoning="Risk",
        exit_plan="Plan", confidence_level=3, expected_holding_period="1 year",
        snapshot_after=json.dumps(after),
    )
    score = calc_training_score(entry)
    assert score.leverage_score < 100
    assert any("debt ratio" in d.lower() for d in score.details)


def test_training_score_low_cash():
    after = {
        "cash": 2000, "total_assets": 100000,
        "debt_ratio": 0, "crypto_pct": 0,
        "illiquid_pct": 0,
        "max_concentration": {"name": "AAPL", "pct": 0.10},
        "risk_warnings": [],
    }
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Buy",
        thesis="Thesis", intended_role="role", risk_reasoning="Risk",
        exit_plan="Plan", confidence_level=3, expected_holding_period="1 year",
        snapshot_after=json.dumps(after),
    )
    score = calc_training_score(entry)
    assert score.liquidity_score < 100
    assert any("cash below" in d.lower() for d in score.details)


def test_training_score_high_crypto():
    after = {
        "cash": 10000, "total_assets": 100000,
        "debt_ratio": 0, "crypto_pct": 0.30,
        "illiquid_pct": 0,
        "max_concentration": {"name": "BTC", "pct": 0.20},
        "risk_warnings": [],
    }
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Buy BTC",
        thesis="Thesis", intended_role="speculation", risk_reasoning="Risk",
        exit_plan="Plan", confidence_level=3, expected_holding_period="1 year",
        snapshot_after=json.dumps(after),
    )
    score = calc_training_score(entry)
    assert score.diversification_score < 100
    assert any("crypto" in d.lower() for d in score.details)


def test_training_score_high_illiquidity():
    after = {
        "cash": 5000, "total_assets": 100000,
        "debt_ratio": 0, "crypto_pct": 0,
        "illiquid_pct": 0.70,
        "max_concentration": {"name": "RE", "pct": 0.20},
        "risk_warnings": [],
    }
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Buy property",
        thesis="Thesis", intended_role="income", risk_reasoning="Risk",
        exit_plan="Plan", confidence_level=3, expected_holding_period="10 years",
        snapshot_after=json.dumps(after),
    )
    score = calc_training_score(entry)
    assert score.liquidity_score < 100
    assert any("illiquid" in d.lower() for d in score.details)


def test_training_score_concentration_increase():
    before = {
        "cash": 50000, "total_assets": 100000,
        "debt_ratio": 0, "crypto_pct": 0, "illiquid_pct": 0,
        "max_concentration": {"name": "Cash", "pct": 0.50},
    }
    after = {
        "cash": 20000, "total_assets": 100000,
        "debt_ratio": 0, "crypto_pct": 0, "illiquid_pct": 0,
        "max_concentration": {"name": "AAPL", "pct": 0.30},
    }
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Buy AAPL",
        thesis="Thesis", intended_role="core", risk_reasoning="Risk",
        exit_plan="Plan", confidence_level=4, expected_holding_period="1 year",
        snapshot_before=json.dumps(before),
        snapshot_after=json.dumps(after),
    )
    score = calc_training_score(entry)
    assert score.concentration_score < 100


def test_training_score_debt_increase():
    before = {
        "cash": 50000, "total_assets": 100000,
        "debt_ratio": 0.20, "crypto_pct": 0, "illiquid_pct": 0,
        "max_concentration": {"name": "Cash", "pct": 0.50},
    }
    after = {
        "cash": 50000, "total_assets": 100000,
        "debt_ratio": 0.40, "crypto_pct": 0, "illiquid_pct": 0,
        "max_concentration": {"name": "Cash", "pct": 0.50},
    }
    entry = DecisionJournalEntry(
        date="2025-01-15", title="Add debt",
        thesis="Thesis", intended_role="leverage", risk_reasoning="Risk",
        exit_plan="Plan", confidence_level=3, expected_holding_period="2 years",
        snapshot_before=json.dumps(before),
        snapshot_after=json.dumps(after),
    )
    score = calc_training_score(entry)
    assert score.leverage_score < 100
    assert any("debt ratio increased" in d.lower() for d in score.details)


# --- get_lessons_learned ---

def test_get_lessons_learned(db_conn):
    create_journal_entry(db_conn, DecisionJournalEntry(
        date="2025-01-15", title="Buy AAPL",
        lesson_learned="Always check earnings date before buying",
    ))
    create_journal_entry(db_conn, DecisionJournalEntry(
        date="2025-02-01", title="Sell TSLA",
        lesson_learned="Don't panic sell",
    ))
    create_journal_entry(db_conn, DecisionJournalEntry(
        date="2025-03-01", title="No lesson",
    ))

    lessons = get_lessons_learned(db_conn)
    assert len(lessons) == 2
    assert lessons[0]["lesson"] == "Don't panic sell"
    assert lessons[1]["lesson"] == "Always check earnings date before buying"


def test_get_lessons_empty(db_conn):
    assert get_lessons_learned(db_conn) == []


# --- Integration: full journal workflow ---

def test_full_journal_workflow(db_conn):
    deposit_cash(db_conn, "2025-01-01", 100000.0)
    snap_before = capture_portfolio_snapshot(db_conn)
    assert snap_before["cash"] == 100000.0

    asset = create_asset(db_conn, Asset(symbol="AAPL", name="Apple", asset_type="stock"))
    txn = buy(db_conn, "2025-01-02", asset.id, 50, 150.0)

    entry = create_journal_for_transaction(
        db_conn, txn.id,
        thesis="AI growth thesis",
        intended_role="core holding",
        risk_reasoning="Valuation stretched",
        exit_plan="Review after Q2 earnings",
        confidence_level=4,
        expected_holding_period="1 year",
        pre_trade_notes="Market sentiment positive",
    )
    set_snapshot_before(db_conn, entry, snap_before)

    fetched = get_journal_entry(db_conn, entry.id)
    before, after = get_before_after(fetched)
    assert before["cash"] == 100000.0
    assert after["cash"] == 100000.0 - 7500.0

    changes = calc_structure_changes(fetched)
    assert len(changes) > 0

    score = calc_training_score(fetched)
    assert score.journal_quality_score == 100.0
    assert score.overall_score > 0

    add_post_trade_review(
        db_conn, entry.id,
        post_trade_review="Stock up 5%",
        mistake_tags="none",
        lesson_learned="AI thesis played out as expected",
    )

    lessons = get_lessons_learned(db_conn)
    assert len(lessons) == 1
    assert "AI thesis" in lessons[0]["lesson"]
