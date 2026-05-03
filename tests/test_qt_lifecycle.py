"""
Regression tests for the PySide6/Qt teardown SIGSEGV.

Original crash signature (see crash_investigation.md):

    QWidget::~QWidget()
    QObjectPrivate::deleteChildren()
    QScrollAreaWrapper::~QScrollAreaWrapper()
    SbkDeallocWrapperCommon
    _Py_Dealloc
    gc_collect_region
    _PyGC_Collect

These tests exercise the create / show / close / deleteLater / processEvents /
gc.collect cycle on the widget paths involved in the original crash:
QScrollArea-bearing pages, the MainWindow (owns nine pages incl. matplotlib
FigureCanvasQTAgg via _swap_canvas), and the page that originally tripped the
NameError (RealEstatePage shown without being hidden).

A failure here would show up as a SIGSEGV / EXC_BAD_ACCESS, a "wrapped C/C++
object has been deleted" RuntimeError, or leftover top-level widgets after the
deferred-delete + gc cycle.
"""
import gc

import pytest
import shiboken6
from PySide6.QtCore import QEvent
from PySide6.QtWidgets import QApplication

from src.gui.main_window import MainWindow
from src.gui.pages.asset_analysis import AssetAnalysisPage
from src.gui.pages.dashboard import DashboardPage
from src.gui.pages.real_estate import RealEstatePage
from src.storage.database import init_db


def _flush(app):
    """Drive the cleanup the way conftest does — close, deleteLater, flush
    DeferredDelete, gc. This is the codepath the original crash hit."""
    for w in list(app.topLevelWidgets()):
        if shiboken6.isValid(w) and w.parent() is None:
            w.close()
            w.deleteLater()
    app.sendPostedEvents(None, QEvent.Type.DeferredDelete)
    gc.collect()


def _owned_top_level_count(app):
    return sum(
        1 for w in app.topLevelWidgets()
        if shiboken6.isValid(w) and w.parent() is None
    )


@pytest.mark.parametrize("iterations", [10])
def test_dashboard_create_refresh_teardown_loop(qapp, iterations):
    """DashboardPage owns a QScrollArea + matplotlib FigureCanvasQTAgg. Each
    refresh() calls _swap_canvas which orphans old canvases via setParent(None)
    — exactly the pattern that surfaced the original gc-driven crash."""
    for _ in range(iterations):
        conn = init_db(":memory:")
        page = DashboardPage(conn)
        page.show()
        page.refresh()
        page.refresh()  # second refresh exercises _swap_canvas with old canvas
        qapp.processEvents()
        page._cleanup_figures()
        page.close()
        page.deleteLater()
        del page
        qapp.sendPostedEvents(None, QEvent.Type.DeferredDelete)
        gc.collect()
        conn.close()
    _flush(qapp)
    assert _owned_top_level_count(qapp) == 0


@pytest.mark.parametrize("iterations", [10])
def test_main_window_create_teardown_loop(qapp, iterations):
    """MainWindow owns nine pages, multiple QScrollAreas, and three matplotlib
    canvases via Dashboard. Stresses the full parent → child destruction chain
    that the original crash hit."""
    for _ in range(iterations):
        conn = init_db(":memory:")
        w = MainWindow(conn, enable_startup_sync=False)
        for page in w.page_widgets:
            if hasattr(page, "refresh"):
                try:
                    page.refresh()
                except Exception as e:
                    pytest.fail(f"refresh() failed for {type(page).__name__}: {e}")
        qapp.processEvents()
        w.close()
        w.deleteLater()
        del w
        qapp.sendPostedEvents(None, QEvent.Type.DeferredDelete)
        gc.collect()
        conn.close()
    _flush(qapp)
    assert _owned_top_level_count(qapp) == 0


def test_real_estate_page_shown_then_destroyed(qapp):
    """RealEstatePage was the page that originally surfaced the NameError test.
    The bug was that page.show() leaves it as a top-level widget; this
    reproduces that exact state and verifies the cleanup path works on it."""
    for _ in range(5):
        conn = init_db(":memory:")
        page = RealEstatePage(conn)
        page.resize(400, 800)
        page.show()
        qapp.processEvents()
        page.close()
        page.deleteLater()
        del page
        qapp.sendPostedEvents(None, QEvent.Type.DeferredDelete)
        gc.collect()
        conn.close()
    _flush(qapp)
    assert _owned_top_level_count(qapp) == 0


def test_asset_analysis_canvas_swap_teardown(qapp):
    """AssetAnalysisPage's _refresh_chart also uses canvas.setParent(None) to
    orphan old canvases. Exercises that path under repeated refresh + teardown."""
    for _ in range(5):
        conn = init_db(":memory:")
        page = AssetAnalysisPage(conn)
        page.refresh()
        page.refresh()
        page._cleanup_figures()
        page.close()
        page.deleteLater()
        del page
        qapp.sendPostedEvents(None, QEvent.Type.DeferredDelete)
        gc.collect()
        conn.close()
    _flush(qapp)
    assert _owned_top_level_count(qapp) == 0


def test_python_wrappers_invalidated_after_cleanup(qapp):
    """After the conftest-style cleanup, the Python wrappers for the destroyed
    widgets must report shiboken6.isValid == False. If they didn't, a future
    gc cycle could find a wrapper that still thinks it owns a live C++ object,
    which is the precise condition that produced the original SIGSEGV."""
    conn = init_db(":memory:")
    w = MainWindow(conn, enable_startup_sync=False)
    pages = list(w.page_widgets)
    qapp.processEvents()
    w.close()
    w.deleteLater()
    qapp.sendPostedEvents(None, QEvent.Type.DeferredDelete)
    gc.collect()

    assert not shiboken6.isValid(w)
    for page in pages:
        assert not shiboken6.isValid(page), f"{type(page).__name__} wrapper still valid"
    conn.close()
