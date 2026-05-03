import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_REQUIREMENTS_TXT = _PROJECT_ROOT / "requirements.txt"


def is_yfinance_available() -> bool:
    try:
        import yfinance
        return True
    except ImportError:
        return False


def get_dependency_status() -> dict:
    return {
        "yfinance": is_yfinance_available(),
        "python": sys.executable,
        "requirements_txt": str(_REQUIREMENTS_TXT),
    }


def get_install_command() -> str:
    return f'"{sys.executable}" -m pip install -r "{_REQUIREMENTS_TXT}"'


def get_install_args() -> list[str]:
    return [sys.executable, "-m", "pip", "install", "-r", str(_REQUIREMENTS_TXT)]


def yfinance_missing_message() -> str:
    return (
        "Price sync requires yfinance, which is not installed "
        f"for the current Python interpreter ({sys.executable}).\n\n"
        f"To install all dependencies, run:\n  {get_install_command()}"
    )
