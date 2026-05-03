"""Shared bankruptcy-warning banner.

Spec §8 / §12 require that when the portfolio enters bankruptcy:
- a clear warning appears on every page,
- the message names the cause (couldn't fund a debt; cash insufficient;
  asset selling could not cover; transactions disabled).

A single widget owns the styling and the predicate so the Dashboard's
banner, the Transactions page's banner, and the Real Estate page's
banner can never disagree about whether bankruptcy is active.

Usage from a page:
    self.bankruptcy_banner = BankruptcyBanner(self.conn)
    layout.addWidget(self.bankruptcy_banner)
    # ... in refresh():
    self.bankruptcy_banner.refresh()

The widget self-hides when the portfolio is not bankrupt, so pages can
add it unconditionally.
"""
from __future__ import annotations

import sqlite3

from PySide6.QtWidgets import QFrame, QLabel, QMessageBox, QVBoxLayout

from src.engines.risk import check_bankruptcy, is_bankrupt

_BANNER_STYLE = (
    "QFrame { background-color: #b71c1c; border: 2px solid #7f0000; "
    "border-radius: 6px; padding: 12px; }"
)
_LABEL_STYLE = (
    "color: white; font-size: 16px; font-weight: bold; border: none;"
)

# Spec §12 message: explains the cause and the consequence.
_DEFAULT_HEADER = (
    "⚠ Bankruptcy declared. The simulator could not cover a required "
    "debt or mortgage payment from cash, and force-selling all "
    "available assets did not raise enough to make up the shortfall. "
    "All new transactions are disabled."
)


class BankruptcyBanner(QFrame):
    """Red banner shown whenever ``risk.check_bankruptcy`` is firing.

    Holds its own connection reference so each call to ``refresh()``
    re-reads the current bankruptcy state. The body text combines the
    spec's standard message with the live trigger details from
    ``check_bankruptcy`` so the user sees both *why* the banner is up
    and *which* obligation specifically failed.
    """

    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        self.conn = conn
        self.setStyleSheet(_BANNER_STYLE)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(4)

        self.label = QLabel("")
        self.label.setWordWrap(True)
        self.label.setStyleSheet(_LABEL_STYLE)
        layout.addWidget(self.label)

        self.setVisible(False)

    def refresh(self, warnings=None) -> bool:
        """Re-read bankruptcy state and show/hide accordingly.

        ``warnings`` lets callers that have already run ``get_all_warnings``
        share the result and avoid a second DB scan — the dashboard refresh
        path passes its precomputed list. Otherwise we re-run the cheap
        ``check_bankruptcy`` query to stay self-sufficient.

        Returns ``True`` iff bankruptcy is active (banner visible).
        """
        if warnings is None:
            scan = check_bankruptcy(self.conn)
        else:
            scan = warnings
        bankruptcy = next(
            (w for w in scan if w.category == "bankruptcy"), None,
        )
        if bankruptcy is None:
            self.setVisible(False)
            return False
        # Combine the standard spec message with the live detail line.
        self.label.setText(f"{_DEFAULT_HEADER}\n\n{bankruptcy.message}")
        self.setVisible(True)
        return True


def guard_transaction_or_warn(
    conn: sqlite3.Connection, parent=None,
) -> bool:
    """Block a transaction-creating UI action when the portfolio is
    bankrupt, after showing a warning dialog.

    Returns ``True`` when the caller should abort (bankrupt and warning
    was shown), ``False`` otherwise. Callers wire this at the top of
    every ``_submit_*`` / ``_confirm_*`` handler:

        if guard_transaction_or_warn(self.conn, self):
            return

    Spec §6 #25 — when bankrupt, all transactions are banned. The
    per-page banner is informational; this dialog is the unmissable
    interruption that prevents the action from proceeding.

    **Day-boundary preflight.** Before checking bankruptcy, the guard
    asks the enclosing ``MainWindow`` (when reachable via
    ``parent.window()``) to ``ensure_auto_settle_current()``. That call
    runs auto-settle if the calendar date has changed since the last
    settle — closes the loophole where a user could leave the app open
    across midnight and bypass scheduled debt/rent/mortgage processing
    on their next action. If preflight declares bankruptcy, the
    bankruptcy check below catches it and aborts the user action.
    Reachability via ``window()`` is best-effort; tests that construct
    a ``Page`` standalone (no MainWindow) get the no-op fallback.
    """
    _preflight_auto_settle_if_possible(parent)
    if not is_bankrupt(conn):
        return False
    QMessageBox.warning(
        parent,
        "Account is bankrupt",
        "All transactions are disabled because the simulator could "
        "not cover a required debt or mortgage payment from cash, "
        "and force-selling all available assets did not raise enough "
        "to make up the shortfall. See the Dashboard for the specific "
        "obligation that triggered bankruptcy.",
    )
    return True


def _preflight_auto_settle_if_possible(parent) -> None:
    """Walk to the enclosing window and call its
    ``ensure_auto_settle_current`` if it has one. Silent no-op when the
    parent is None, doesn't have ``window()``, or the window is not a
    MainWindow with the preflight method. Exceptions are swallowed so a
    pipeline failure can never break a user submit path — the engine-
    level bankruptcy guard still has the user's back.
    """
    if parent is None:
        return
    window = None
    try:
        window = parent.window()
    except Exception:
        return
    ensure = getattr(window, "ensure_auto_settle_current", None)
    if ensure is None:
        return
    try:
        ensure()
    except Exception:
        # Logged inside ensure_auto_settle_current's own try/except; we
        # still want the submit handler to reach the bankruptcy check.
        pass
