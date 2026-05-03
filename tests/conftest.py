import gc
import os
import pytest
from src.storage.database import init_db

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


@pytest.fixture(scope="session", autouse=True)
def qapp():
    from PySide6.QtGui import QFont
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance() or QApplication([])
    app.setFont(QFont("Helvetica"))
    return app


def _owned_top_levels(app):
    # QApplication.topLevelWidgets() includes child popups (e.g., QComboBox
    # views) whose lifetime is owned by their parent. Skip those — destroying
    # them out from under the parent breaks state and slows teardown.
    import shiboken6
    return [
        w for w in app.topLevelWidgets()
        if shiboken6.isValid(w) and w.parent() is None
    ]


@pytest.fixture(autouse=True)
def _qt_cleanup():
    yield
    from PySide6.QtWidgets import QApplication
    from PySide6.QtCore import QEvent
    app = QApplication.instance()
    if app is None:
        return
    widgets = _owned_top_levels(app)
    if not widgets:
        return
    app.processEvents()
    for w in widgets:
        if hasattr(w, 'page_widgets'):
            for page in w.page_widgets:
                if hasattr(page, '_cleanup_figures'):
                    page._cleanup_figures()
        if hasattr(w, '_cleanup_figures'):
            w._cleanup_figures()
    # Use Qt's deferred-delete pattern instead of shiboken6.delete().
    # shiboken6.delete() destroys C++ objects synchronously and bypasses the
    # event loop; under Python gc this can leave dangling pointers in parent
    # child-lists and crash with SIGSEGV inside QObjectPrivate::deleteChildren.
    # sendPostedEvents(None, DeferredDelete) is required to actually flush the
    # delete events — plain processEvents() does not always run them.
    for w in _owned_top_levels(app):
        w.close()
        w.deleteLater()
    app.sendPostedEvents(None, QEvent.Type.DeferredDelete)
    gc.collect()


@pytest.fixture
def db_conn():
    conn = init_db(":memory:")
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def _disable_force_sell_price_sync(monkeypatch):
    """Stop the force-sell engine from hitting yfinance during tests.

    The new plan-driven force-sell calls ``pricing_engine.sync_asset_price``
    on every sellable holding before pricing. Real ticker symbols (e.g.
    "S", "A", "X") would otherwise return real network prices in tests
    that seeded their own synthetic prices, and tests that asserted "no
    market_prices row → asset is skipped" would now see a network-priced
    sale instead. The autouse patch keeps tests offline-deterministic.

    Tests that intentionally verify sync behavior (Phase 3
    ``test_force_sell_plan.py``) override the patch with their own
    ``unittest.mock.patch`` on the same target.
    """
    import src.engines.force_sell as fs
    monkeypatch.setattr(fs, "_try_sync_prices", lambda *_a, **_k: None)
    yield
