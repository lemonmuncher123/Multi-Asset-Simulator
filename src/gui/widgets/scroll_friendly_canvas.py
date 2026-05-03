"""Matplotlib FigureCanvas that lets wheel events bubble to the parent.

Default `FigureCanvasQTAgg` accepts wheel events for matplotlib's built-in
zoom/pan tools, which means a chart placed inside a `QScrollArea` traps
the trackpad/scroll wheel — the user can't scroll the page when the
cursor is over the chart. This subclass calls `event.ignore()` so Qt
propagates the event up to the QScrollArea instead.

Used by every chart on the Dashboard and Asset Analysis pages.
"""
from __future__ import annotations

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg


class ScrollFriendlyCanvas(FigureCanvasQTAgg):
    """FigureCanvasQTAgg that does not consume wheel events."""

    def wheelEvent(self, event):  # noqa: N802 — Qt naming
        event.ignore()
