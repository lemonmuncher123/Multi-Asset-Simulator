import json
import sqlite3
from dataclasses import dataclass
from src.models.decision_journal import DecisionJournalEntry
from src.storage.journal_repo import (
    create_journal_entry,
    get_journal_by_transaction,
    list_journal_entries,
    update_journal_entry,
)
from src.storage.transaction_repo import get_transaction
from src.storage.asset_repo import get_asset
from src.engines.portfolio import (
    calc_cash_balance,
    calc_positions,
    calc_total_assets,
    calc_total_liabilities,
    calc_net_worth,
)
from src.engines.allocation import (
    calc_allocation_by_asset_type,
    calc_allocation_by_liquidity,
    calc_debt_ratio,
    calc_crypto_pct,
)
from src.engines.risk import get_all_warnings


def capture_portfolio_snapshot(conn: sqlite3.Connection) -> dict:
    alloc_by_type = calc_allocation_by_asset_type(conn)
    type_pcts = {k: v["pct"] for k, v in alloc_by_type.items()}

    cash = calc_cash_balance(conn)
    total_assets = calc_total_assets(conn)
    total_liabilities = calc_total_liabilities(conn)
    net_worth = calc_net_worth(conn)
    debt_ratio = calc_debt_ratio(conn)
    crypto_pct = calc_crypto_pct(conn)

    liquidity = calc_allocation_by_liquidity(conn)
    liquid_pct = liquidity["liquid"]["pct"]
    illiquid_pct = liquidity["illiquid"]["pct"]

    positions = calc_positions(conn)
    max_conc_name = "N/A"
    max_conc_pct = 0.0
    if total_assets > 0:
        items = []
        if cash != 0:
            items.append(("Cash", abs(cash) / total_assets))
        for p in positions:
            val = p.market_value if p.market_value is not None else p.cost_basis
            items.append((p.symbol, val / total_assets))
        if items:
            items.sort(key=lambda x: x[1], reverse=True)
            max_conc_name, max_conc_pct = items[0]

    warnings = get_all_warnings(conn)
    warning_messages = [w.message for w in warnings]

    return {
        "asset_type_allocation": type_pcts,
        "cash": cash,
        "total_assets": total_assets,
        "total_liabilities": total_liabilities,
        "net_worth": net_worth,
        "debt_ratio": debt_ratio,
        "crypto_pct": crypto_pct,
        "max_concentration": {"name": max_conc_name, "pct": max_conc_pct},
        "liquid_pct": liquid_pct,
        "illiquid_pct": illiquid_pct,
        "risk_warnings": warning_messages,
    }


def create_journal_for_transaction(
    conn: sqlite3.Connection,
    transaction_id: int,
    thesis: str | None = None,
    intended_role: str | None = None,
    risk_reasoning: str | None = None,
    exit_plan: str | None = None,
    confidence_level: int | None = None,
    expected_holding_period: str | None = None,
    pre_trade_notes: str | None = None,
) -> DecisionJournalEntry:
    txn = get_transaction(conn, transaction_id)
    asset = get_asset(conn, txn.asset_id) if txn.asset_id else None
    asset_label = f"{asset.symbol}" if asset else ""
    title = f"{txn.txn_type} {asset_label}".strip()

    snapshot_after = capture_portfolio_snapshot(conn)

    entry = create_journal_entry(conn, DecisionJournalEntry(
        transaction_id=transaction_id,
        date=txn.date,
        title=title,
        thesis=thesis,
        intended_role=intended_role,
        risk_reasoning=risk_reasoning,
        exit_plan=exit_plan,
        confidence_level=confidence_level,
        expected_holding_period=expected_holding_period,
        pre_trade_notes=pre_trade_notes,
        snapshot_after=json.dumps(snapshot_after),
    ))

    return entry


def set_snapshot_before(
    conn: sqlite3.Connection,
    entry_id_or_entry: int | DecisionJournalEntry,
    snapshot: dict,
) -> None:
    from src.storage.journal_repo import get_journal_entry
    if isinstance(entry_id_or_entry, int):
        entry = get_journal_entry(conn, entry_id_or_entry)
    else:
        entry = entry_id_or_entry
    entry.snapshot_before = json.dumps(snapshot)
    update_journal_entry(conn, entry)


def add_post_trade_review(
    conn: sqlite3.Connection,
    entry_id: int,
    post_trade_review: str | None = None,
    mistake_tags: str | None = None,
    lesson_learned: str | None = None,
) -> DecisionJournalEntry:
    from src.storage.journal_repo import get_journal_entry
    entry = get_journal_entry(conn, entry_id)
    entry.post_trade_review = post_trade_review
    entry.mistake_tags = mistake_tags
    entry.lesson_learned = lesson_learned
    update_journal_entry(conn, entry)
    return entry


def get_before_after(entry: DecisionJournalEntry) -> tuple[dict | None, dict | None]:
    before = json.loads(entry.snapshot_before) if entry.snapshot_before else None
    after = json.loads(entry.snapshot_after) if entry.snapshot_after else None
    return before, after


@dataclass
class StructureChange:
    metric: str
    before: float | str
    after: float | str
    direction: str


def calc_structure_changes(entry: DecisionJournalEntry) -> list[StructureChange]:
    before, after = get_before_after(entry)
    if not before or not after:
        return []

    changes = []

    b_cash = before.get("cash", 0)
    a_cash = after.get("cash", 0)
    if b_cash != a_cash:
        direction = "increased" if a_cash > b_cash else "decreased"
        changes.append(StructureChange("Cash", b_cash, a_cash, direction))

    b_dr = before.get("debt_ratio", 0)
    a_dr = after.get("debt_ratio", 0)
    if abs(b_dr - a_dr) > 0.001:
        direction = "increased" if a_dr > b_dr else "decreased"
        changes.append(StructureChange("Debt Ratio", b_dr, a_dr, direction))

    b_conc = before.get("max_concentration", {}).get("pct", 0)
    a_conc = after.get("max_concentration", {}).get("pct", 0)
    if abs(b_conc - a_conc) > 0.001:
        a_name = after.get("max_concentration", {}).get("name", "N/A")
        direction = "increased" if a_conc > b_conc else "decreased"
        changes.append(StructureChange(f"Max Concentration ({a_name})", b_conc, a_conc, direction))

    b_illiq = before.get("illiquid_pct", 0)
    a_illiq = after.get("illiquid_pct", 0)
    if abs(b_illiq - a_illiq) > 0.001:
        direction = "increased" if a_illiq > b_illiq else "decreased"
        changes.append(StructureChange("Illiquid %", b_illiq, a_illiq, direction))

    b_types = before.get("asset_type_allocation", {})
    a_types = after.get("asset_type_allocation", {})
    all_types = set(b_types.keys()) | set(a_types.keys())
    for t in sorted(all_types):
        bp = b_types.get(t, 0)
        ap = a_types.get(t, 0)
        if abs(bp - ap) > 0.001:
            direction = "increased" if ap > bp else "decreased"
            changes.append(StructureChange(f"{t} allocation", bp, ap, direction))

    return changes


@dataclass
class TrainingScore:
    diversification_score: float
    liquidity_score: float
    concentration_score: float
    leverage_score: float
    journal_quality_score: float
    overall_score: float
    details: list[str]


def calc_training_score(entry: DecisionJournalEntry) -> TrainingScore:
    details = []

    # Journal quality: start at 100, subtract for missing fields
    jq = 100.0
    if not entry.thesis:
        jq -= 20
        details.append("Missing thesis (-20)")
    if not entry.risk_reasoning:
        jq -= 20
        details.append("Missing risk reasoning (-20)")
    if not entry.exit_plan:
        jq -= 15
        details.append("Missing exit/review plan (-15)")
    if not entry.intended_role:
        jq -= 10
        details.append("Missing intended role (-10)")
    if entry.confidence_level is None:
        jq -= 10
        details.append("Missing confidence level (-10)")
    if not entry.expected_holding_period:
        jq -= 5
        details.append("Missing expected holding period (-5)")
    jq = max(0.0, jq)

    before, after = get_before_after(entry)

    # Structure scores: start at 100, subtract for threshold breaches
    div_score = 100.0
    liq_score = 100.0
    conc_score = 100.0
    lev_score = 100.0

    if after:
        a_crypto = after.get("crypto_pct", 0)
        if a_crypto > 0.20:
            div_score -= 20
            details.append(f"Crypto exposure {a_crypto:.0%} above 20% threshold (-20 diversification)")

        a_illiq = after.get("illiquid_pct", 0)
        if a_illiq > 0.60:
            liq_score -= 25
            details.append(f"Illiquid assets {a_illiq:.0%} above 60% threshold (-25 liquidity)")

        a_cash_total = after.get("total_assets", 0)
        a_cash = after.get("cash", 0)
        if a_cash_total > 0 and (a_cash / a_cash_total) < 0.05:
            liq_score -= 25
            details.append(f"Cash below 5% of assets (-25 liquidity)")

        a_conc = after.get("max_concentration", {}).get("pct", 0)
        if a_conc > 0.40:
            conc_score -= 30
            details.append(f"Single asset concentration {a_conc:.0%} above 40% (-30 concentration)")
        elif a_conc > 0.25:
            conc_score -= 15
            details.append(f"Single asset concentration {a_conc:.0%} above 25% (-15 concentration)")

        a_dr = after.get("debt_ratio", 0)
        if a_dr > 0.50:
            lev_score -= 30
            details.append(f"Debt ratio {a_dr:.0%} above 50% threshold (-30 leverage)")

    if before and after:
        b_conc = before.get("max_concentration", {}).get("pct", 0)
        a_conc = after.get("max_concentration", {}).get("pct", 0)
        if a_conc > b_conc and a_conc > 0.25:
            conc_score -= 10
            details.append(f"Concentration increased from {b_conc:.0%} to {a_conc:.0%} (-10 concentration)")

        b_dr = before.get("debt_ratio", 0)
        a_dr = after.get("debt_ratio", 0)
        if a_dr > b_dr and a_dr > 0.30:
            lev_score -= 10
            details.append(f"Debt ratio increased from {b_dr:.0%} to {a_dr:.0%} (-10 leverage)")

        b_illiq = before.get("illiquid_pct", 0)
        a_illiq = after.get("illiquid_pct", 0)
        if a_illiq > b_illiq and a_illiq > 0.50:
            liq_score -= 10
            details.append(f"Illiquid share increased from {b_illiq:.0%} to {a_illiq:.0%} (-10 liquidity)")

    div_score = max(0.0, div_score)
    liq_score = max(0.0, liq_score)
    conc_score = max(0.0, conc_score)
    lev_score = max(0.0, lev_score)

    overall = (div_score + liq_score + conc_score + lev_score + jq) / 5.0

    return TrainingScore(
        diversification_score=div_score,
        liquidity_score=liq_score,
        concentration_score=conc_score,
        leverage_score=lev_score,
        journal_quality_score=jq,
        overall_score=overall,
        details=details,
    )


def get_lessons_learned(conn: sqlite3.Connection) -> list[dict]:
    entries = list_journal_entries(conn)
    lessons = []
    for e in entries:
        if e.lesson_learned:
            lessons.append({
                "date": e.date,
                "title": e.title,
                "lesson": e.lesson_learned,
            })
    return lessons
