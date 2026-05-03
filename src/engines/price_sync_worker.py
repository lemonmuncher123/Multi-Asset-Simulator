import sqlite3
from PySide6.QtCore import QThread, Signal

from src.storage.database import init_db, DEFAULT_DB_PATH


class PriceSyncWorker(QThread):
    result_ready = Signal(dict)

    def __init__(self, db_path=None, providers=None, parent=None):
        super().__init__(parent)
        self._db_path = db_path or str(DEFAULT_DB_PATH)
        self._providers = providers

    def run(self):
        try:
            conn = init_db(self._db_path)
            try:
                from src.engines.pricing_engine import sync_all_market_assets
                result = sync_all_market_assets(
                    conn,
                    providers=self._providers,
                    cancelled=self.isInterruptionRequested,
                )
            finally:
                conn.close()
        except Exception as e:
            result = {
                "attempted": 0,
                "succeeded": 0,
                "failed": 0,
                "errors": [str(e)],
                "status": "failed",
            }
        self.result_ready.emit(result)


class PriceSyncController:
    def __init__(self):
        self._worker: PriceSyncWorker | None = None
        self._callbacks: list = []

    @property
    def is_running(self) -> bool:
        return self._worker is not None and self._worker.isRunning()

    def start_sync(self, db_path=None, providers=None, on_finished=None):
        if self.is_running:
            return False

        if on_finished:
            self._callbacks.append(on_finished)

        self._worker = PriceSyncWorker(db_path=db_path, providers=providers)
        self._worker.result_ready.connect(self._on_worker_finished)
        self._worker.start()
        return True

    def _on_worker_finished(self, result: dict):
        callbacks = self._callbacks[:]
        self._callbacks.clear()
        worker = self._worker
        self._worker = None
        if worker is not None:
            worker.wait()
            worker.deleteLater()
        for cb in callbacks:
            cb(result)

    def shutdown(self, timeout_ms: int = 5000):
        worker = self._worker
        if worker is None:
            return
        worker.requestInterruption()
        worker.wait(timeout_ms)

    def add_callback(self, callback):
        if self.is_running:
            self._callbacks.append(callback)
