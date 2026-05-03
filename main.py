import getpass
import logging
import sys
from pathlib import Path

from PySide6.QtCore import QLockFile
from PySide6.QtGui import QFont
from PySide6.QtNetwork import QLocalServer, QLocalSocket
from PySide6.QtWidgets import QApplication

from src.storage.database import init_db, DEFAULT_DB_PATH, _user_data_dir
from src.gui.main_window import MainWindow
from src.utils.app_logging import setup_logging


# Per-user socket name so two users on the same machine don't collide on
# the same Unix-domain socket / Windows named pipe.
_SINGLETON_SOCKET_NAME = f"asset-trainer-singleton-{getpass.getuser()}"


def _activate_running_instance() -> bool:
    """Ping the already-running instance to bring its window to the front."""
    socket = QLocalSocket()
    socket.connectToServer(_SINGLETON_SOCKET_NAME)
    if not socket.waitForConnected(1000):
        return False
    socket.write(b"activate")
    socket.flush()
    socket.waitForBytesWritten(1000)
    socket.disconnectFromServer()
    return True


def _install_activation_listener(window: MainWindow) -> QLocalServer:
    """Listen for activation pings from future launches and raise the window."""
    # A crashed previous run can leave a stale socket file that blocks listen().
    QLocalServer.removeServer(_SINGLETON_SOCKET_NAME)
    server = QLocalServer()

    def _bring_to_front():
        sock = server.nextPendingConnection()
        if sock is not None:
            sock.close()
            sock.deleteLater()
        if window.isMinimized():
            window.showNormal()
        else:
            window.show()
        window.raise_()
        window.activateWindow()

    server.newConnection.connect(_bring_to_front)
    if not server.listen(_SINGLETON_SOCKET_NAME):
        logging.getLogger(__name__).warning(
            "Failed to start singleton activation server: %s",
            server.errorString(),
        )
    return server


def main():
    log_path = setup_logging()
    logging.getLogger(__name__).info("Asset trainer starting; logging to %s", log_path)

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setFont(QFont("Helvetica"))

    # Single-instance guard. Two concurrent processes on the same DB would
    # double-fire the auto-settle pipeline (per-process `_in_auto_settle`
    # flag can't coordinate across instances) and race writes to
    # market_prices / reports. Lock auto-releases on exit; stale-lock
    # detection (default 30 s) covers SIGKILL / crashes.
    lock_dir = _user_data_dir()
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock = QLockFile(str(lock_dir / "asset-trainer.lock"))
    lock.setStaleLockTime(30_000)
    if not lock.tryLock(0):
        # The lock might be genuinely held by a live instance, OR be a
        # leftover from a previous SIGSEGV / SIGKILL / force-quit (Qt's
        # destructor doesn't run on those, so the file persists pointing
        # at a dead PID). `tryLock(0)` returns immediately on contention
        # without applying the staleness check, so we have to invoke
        # `removeStaleLockFile()` explicitly: it inspects PID + hostname +
        # mtime and only deletes the file when Qt confirms it's stale.
        # If the lock is genuinely live, the second `tryLock(0)` still
        # fails and we fall through to the activation path.
        if lock.removeStaleLockFile() and lock.tryLock(0):
            # Stale lock was cleared, we now hold a fresh one — proceed.
            pass
        else:
            # Already running — ping the existing instance to come to the
            # front, then exit cleanly without opening a duplicate window.
            _activate_running_instance()
            sys.exit(0)

    DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = init_db()
    window = MainWindow(conn)
    window.show()

    # Listen for activation pings from future launches. Bound to a local
    # variable so the server lives for the running event loop's lifetime.
    activation_server = _install_activation_listener(window)

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
