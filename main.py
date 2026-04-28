import logging
import sys
from pathlib import Path

from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication

from src.storage.database import init_db, DEFAULT_DB_PATH
from src.gui.main_window import MainWindow
from src.utils.app_logging import setup_logging


def main():
    log_path = setup_logging()
    logging.getLogger(__name__).info("Asset trainer starting; logging to %s", log_path)

    DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = init_db()

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setFont(QFont("Helvetica"))
    window = MainWindow(conn)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
