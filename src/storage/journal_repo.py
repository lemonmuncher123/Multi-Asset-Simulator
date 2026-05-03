import sqlite3
from src.models.decision_journal import DecisionJournalEntry


def create_journal_entry(conn: sqlite3.Connection, entry: DecisionJournalEntry) -> DecisionJournalEntry:
    cursor = conn.execute(
        "INSERT INTO decision_journal (transaction_id, date, title, thesis, "
        "intended_role, risk_reasoning, exit_plan, confidence_level, "
        "expected_holding_period, pre_trade_notes, post_trade_review, "
        "mistake_tags, lesson_learned, snapshot_before, snapshot_after, "
        "reasoning, expected, actual, score, tags) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (entry.transaction_id, entry.date, entry.title, entry.thesis,
         entry.intended_role, entry.risk_reasoning, entry.exit_plan,
         entry.confidence_level, entry.expected_holding_period,
         entry.pre_trade_notes, entry.post_trade_review,
         entry.mistake_tags, entry.lesson_learned,
         entry.snapshot_before, entry.snapshot_after,
         entry.reasoning, entry.expected, entry.actual, entry.score, entry.tags),
    )
    conn.commit()
    entry.id = cursor.lastrowid
    return entry


def get_journal_entry(conn: sqlite3.Connection, entry_id: int) -> DecisionJournalEntry | None:
    row = conn.execute("SELECT * FROM decision_journal WHERE id = ?", (entry_id,)).fetchone()
    if row is None:
        return None
    return _row_to_entry(row)


def get_journal_by_transaction(conn: sqlite3.Connection, transaction_id: int) -> DecisionJournalEntry | None:
    row = conn.execute(
        "SELECT * FROM decision_journal WHERE transaction_id = ?", (transaction_id,)
    ).fetchone()
    if row is None:
        return None
    return _row_to_entry(row)


def list_journal_entries(conn: sqlite3.Connection) -> list[DecisionJournalEntry]:
    rows = conn.execute("SELECT * FROM decision_journal ORDER BY date DESC, id DESC").fetchall()
    return [_row_to_entry(r) for r in rows]


def update_journal_entry(conn: sqlite3.Connection, entry: DecisionJournalEntry) -> None:
    conn.execute(
        "UPDATE decision_journal SET transaction_id=?, date=?, title=?, thesis=?, "
        "intended_role=?, risk_reasoning=?, exit_plan=?, confidence_level=?, "
        "expected_holding_period=?, pre_trade_notes=?, post_trade_review=?, "
        "mistake_tags=?, lesson_learned=?, snapshot_before=?, snapshot_after=?, "
        "reasoning=?, expected=?, actual=?, score=?, tags=? WHERE id=?",
        (entry.transaction_id, entry.date, entry.title, entry.thesis,
         entry.intended_role, entry.risk_reasoning, entry.exit_plan,
         entry.confidence_level, entry.expected_holding_period,
         entry.pre_trade_notes, entry.post_trade_review,
         entry.mistake_tags, entry.lesson_learned,
         entry.snapshot_before, entry.snapshot_after,
         entry.reasoning, entry.expected, entry.actual, entry.score, entry.tags,
         entry.id),
    )
    conn.commit()


def delete_journal_entry(conn: sqlite3.Connection, entry_id: int) -> None:
    conn.execute("DELETE FROM decision_journal WHERE id = ?", (entry_id,))
    conn.commit()


def _row_to_entry(row: sqlite3.Row) -> DecisionJournalEntry:
    return DecisionJournalEntry(
        id=row["id"],
        transaction_id=row["transaction_id"],
        date=row["date"],
        title=row["title"],
        thesis=row["thesis"],
        intended_role=row["intended_role"],
        risk_reasoning=row["risk_reasoning"],
        exit_plan=row["exit_plan"],
        confidence_level=row["confidence_level"],
        expected_holding_period=row["expected_holding_period"],
        pre_trade_notes=row["pre_trade_notes"],
        post_trade_review=row["post_trade_review"],
        mistake_tags=row["mistake_tags"],
        lesson_learned=row["lesson_learned"],
        snapshot_before=row["snapshot_before"],
        snapshot_after=row["snapshot_after"],
        reasoning=row["reasoning"],
        expected=row["expected"],
        actual=row["actual"],
        score=row["score"],
        tags=row["tags"],
        created_at=row["created_at"],
    )
