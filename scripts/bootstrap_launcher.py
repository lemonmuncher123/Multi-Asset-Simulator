"""Bootstrap launcher for source-tree users.

Locates the project root, ensures a `.venv` exists with the dependencies in
`requirements.txt`, then launches `main.py` using the venv interpreter. A
small marker file inside the venv records the hash of `requirements.txt`
that was last installed; if that hash is unchanged on the next launch we
skip the (slow) pip install step.

Run from a double-click launcher (`Launch Portfolio Trainer.command` /
`.bat`) or directly:

    python3 scripts/bootstrap_launcher.py
"""

from __future__ import annotations

import datetime
import hashlib
import logging
import os
import shutil
import subprocess
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Python versions known to install our PySide6 / yfinance / pandas pins
# cleanly. Outside this range pip wheels may be missing or the install will
# error out partway through.
SUPPORTED_MINORS = (12, 13)
PREFERRED_MINORS = (12, 13)

# Versions we explicitly refuse, with a friendly message. 3.13+ wheels for
# some packages are still patchy; 3.11 and below are below our PySide6 pin's
# happy path. We allow a fallback to "any 3.x >= 3.10" with a warning rather
# than hard-fail, since users may have unusual layouts.
MIN_FALLBACK_MINOR = 10

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_DIR = PROJECT_ROOT / ".venv"
REQUIREMENTS = PROJECT_ROOT / "requirements.txt"
MAIN_PY = PROJECT_ROOT / "main.py"
LAUNCHER_DIR = PROJECT_ROOT / ".launcher"
LAUNCHER_LOG = LAUNCHER_DIR / "launcher.log"
DEPS_MARKER = VENV_DIR / ".deps-hash"

_log = logging.getLogger("bootstrap_launcher")


def _setup_logging() -> None:
    LAUNCHER_DIR.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(LAUNCHER_LOG, maxBytes=512_000, backupCount=3)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s"
    ))
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)
    root.addHandler(stream)


def _venv_python() -> Path:
    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def _python_version_tuple(python_exe: Path) -> tuple[int, int] | None:
    try:
        out = subprocess.check_output(
            [str(python_exe), "-c",
             "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"],
            text=True,
            stderr=subprocess.STDOUT,
        ).strip()
        major, minor = out.split(".", 1)
        return int(major), int(minor)
    except (subprocess.CalledProcessError, ValueError, FileNotFoundError):
        return None


def _candidate_pythons() -> list[Path]:
    """Return candidate Python interpreters in preference order."""
    candidates: list[Path] = []
    seen: set[Path] = set()

    # Prefer pinned versions found on PATH (python3.13, python3.12).
    for minor in PREFERRED_MINORS[::-1]:  # try newest preferred first
        for name in (f"python3.{minor}", f"python{minor}"):
            found = shutil.which(name)
            if found:
                p = Path(found).resolve()
                if p not in seen:
                    candidates.append(p)
                    seen.add(p)

    # Fall back to whatever `python3` / `python` resolve to, then the
    # interpreter that's running this script (so users with only a custom
    # build can still proceed).
    for name in ("python3", "python"):
        found = shutil.which(name)
        if found:
            p = Path(found).resolve()
            if p not in seen:
                candidates.append(p)
                seen.add(p)

    self_py = Path(sys.executable).resolve()
    if self_py not in seen:
        candidates.append(self_py)

    return candidates


def _pick_python() -> Path:
    """Pick a usable interpreter or exit with a clear message."""
    best: Path | None = None
    best_minor: int | None = None
    fallbacks: list[tuple[Path, tuple[int, int]]] = []

    for cand in _candidate_pythons():
        ver = _python_version_tuple(cand)
        if ver is None:
            continue
        major, minor = ver
        if major != 3:
            continue
        if minor in SUPPORTED_MINORS:
            # Pick the highest supported minor we find.
            if best_minor is None or minor > best_minor:
                best = cand
                best_minor = minor
        elif minor >= MIN_FALLBACK_MINOR:
            fallbacks.append((cand, ver))

    if best is not None:
        _log.info("Using Python %d.%d at %s", 3, best_minor, best)
        return best

    if fallbacks:
        cand, ver = fallbacks[0]
        _log.warning(
            "No Python %s found on PATH; falling back to Python %d.%d at %s. "
            "Dependency installation may fail; if it does, install Python 3.12 or 3.13 "
            "from https://www.python.org/downloads/ and re-run the launcher.",
            " or ".join(f"3.{m}" for m in PREFERRED_MINORS),
            ver[0], ver[1], cand,
        )
        return cand

    _log.error(
        "Could not find any Python 3.%d+ interpreter on PATH. "
        "Install Python 3.12 or 3.13 from https://www.python.org/downloads/ "
        "(macOS users: also accept the 'Install Certificates.command') "
        "and try again.",
        MIN_FALLBACK_MINOR,
    )
    sys.exit(2)


def _hash_requirements() -> str:
    h = hashlib.sha256()
    h.update(REQUIREMENTS.read_bytes())
    return h.hexdigest()


def _venv_was_built_here() -> bool:
    """Return True if `.venv` was created for the current project location.

    pyvenv.cfg records the absolute path passed to `python -m venv ...` at
    creation time. If the user moves the project folder (drag-and-drop in
    Finder, re-extract from a fresh download into a different location,
    rsync, etc.), that recorded path no longer matches reality. Many
    things still work — the `.venv/bin/python` symlink points at the
    system Python which is location-independent — but pip console scripts
    have absolute shebangs to the old path, dist-info files carry stale
    install records, and some packages cache absolute paths under
    `__pycache__/`. The cleanest fix is to detect the move and rebuild.

    Returns True when the venv looks fine for the current location;
    False when we can confirm it was built somewhere else. Defaults to
    True (don't rebuild) on parse errors so a borderline case doesn't
    trigger a slow surprise re-install for the user.
    """
    cfg = VENV_DIR / "pyvenv.cfg"
    if not cfg.exists():
        # No marker file — pre-3.6 layout or partially built; treat as
        # unknown and let the caller's downstream checks handle it.
        return True
    try:
        text = cfg.read_text(encoding="utf-8")
    except OSError:
        return True
    for line in text.splitlines():
        if not line.startswith("command"):
            continue
        # Format: "command = /path/to/system/python -m venv /path/to/.venv"
        body = line.split("=", 1)[-1].strip()
        tokens = body.split()
        if not tokens:
            return True
        try:
            recorded = Path(tokens[-1]).resolve()
            current = VENV_DIR.resolve()
        except (OSError, ValueError):
            return True
        return recorded == current
    # No `command` line found — older venv layout that didn't record
    # the creation path. Assume valid; the user can delete `.venv` by
    # hand if anything misbehaves.
    return True


def _rebuild_venv_for_current_location(host_python: Path) -> None:
    """Wipe the existing `.venv` and rebuild it at PROJECT_ROOT.

    Called when `_venv_was_built_here()` returns False. Logs a clear
    explanation so the user understands why the launcher is taking a
    few minutes on this run instead of the usual fast path.
    """
    _log.warning(
        "Detected that .venv was built at a different location than "
        "where this folder lives now. The project must have been moved, "
        "copied, or extracted from a fresh download. Rebuilding the "
        "virtual environment for the current location — this is a "
        "one-time refresh that can take a few minutes."
    )
    try:
        shutil.rmtree(VENV_DIR)
    except OSError as e:
        _log.error(
            "Failed to remove the relocated .venv at %s: %s. "
            "Delete the .venv folder by hand and re-run the launcher.",
            VENV_DIR, e,
        )
        sys.exit(2)
    _create_venv(host_python)
    # Force a fresh dependency install — the deps-hash file would have
    # matched the new requirements.txt, so without `force=True` we'd skip
    # pip and end up with an empty venv.
    _ensure_dependencies(force=True)


def _create_venv(python_exe: Path) -> None:
    _log.info("Creating virtual environment at %s", VENV_DIR)
    try:
        subprocess.check_call([str(python_exe), "-m", "venv", str(VENV_DIR)])
    except subprocess.CalledProcessError as e:
        _log.error("Failed to create virtual environment: %s", e)
        _log.error(
            "Next steps:\n"
            "  1. Make sure Python 3.12 or 3.13 is installed: python3 --version\n"
            "  2. Delete the .venv folder if it exists and try again.\n"
            "  3. On macOS, run the bundled 'Install Certificates.command' once."
        )
        sys.exit(2)


def _ensure_dependencies(force: bool = False) -> None:
    venv_py = _venv_python()
    expected_hash = _hash_requirements()

    if not force and DEPS_MARKER.exists():
        recorded = DEPS_MARKER.read_text(encoding="utf-8").strip()
        if recorded == expected_hash:
            _log.info("Dependencies already up to date (hash matched).")
            return
        _log.info("requirements.txt changed since last install; refreshing.")
    else:
        _log.info("Installing dependencies into venv (this can take several minutes).")

    pip_cmd = [str(venv_py), "-m", "pip", "install", "--upgrade", "pip"]
    install_cmd = [
        str(venv_py), "-m", "pip", "install",
        "-r", str(REQUIREMENTS),
    ]
    try:
        subprocess.check_call(pip_cmd)
        subprocess.check_call(install_cmd)
    except subprocess.CalledProcessError as e:
        _log.error("Dependency install failed: %s", e)
        _log.error(
            "Next steps:\n"
            "  1. Check your internet connection.\n"
            "  2. Re-run the launcher; pip caches partial downloads.\n"
            "  3. If a specific package keeps failing, install Python 3.12 or 3.13.\n"
            "  4. As a last resort, delete the .venv folder and try again.\n"
            "Full log: %s", LAUNCHER_LOG,
        )
        sys.exit(2)

    DEPS_MARKER.write_text(expected_hash, encoding="utf-8")


def _launch_main() -> None:
    venv_py = _venv_python()
    _log.info("Launching %s", MAIN_PY)
    env = os.environ.copy()
    # Make sure the project root is importable as the working directory.
    try:
        completed = subprocess.run(
            [str(venv_py), str(MAIN_PY)],
            cwd=str(PROJECT_ROOT),
            env=env,
        )
    except KeyboardInterrupt:
        _log.info("Launcher interrupted by user.")
        sys.exit(130)

    if completed.returncode != 0:
        _log.error(
            "Application exited with status %d. See the app log for details "
            "(see README → Data & Privacy → Log file location).",
            completed.returncode,
        )
        sys.exit(completed.returncode)


def main() -> None:
    _setup_logging()
    _log.info("--- bootstrap_launcher start: %s ---", datetime.datetime.now().isoformat())
    _log.info("Project root: %s", PROJECT_ROOT)

    if not REQUIREMENTS.is_file():
        _log.error("requirements.txt not found at %s", REQUIREMENTS)
        sys.exit(2)
    if not MAIN_PY.is_file():
        _log.error("main.py not found at %s", MAIN_PY)
        sys.exit(2)

    if not VENV_DIR.exists():
        host_python = _pick_python()
        _create_venv(host_python)
        # Force-install on a fresh venv even though no marker exists.
        _ensure_dependencies(force=True)
    elif not _venv_python().exists():
        _log.error(
            "Existing .venv looks broken (missing %s). Delete the .venv "
            "folder and re-run the launcher.", _venv_python(),
        )
        sys.exit(2)
    elif not _venv_was_built_here():
        # Folder was moved or copied with the venv inside. Rebuild for
        # the current location so pip console scripts and any path-
        # sensitive package internals point at the right place.
        host_python = _pick_python()
        _rebuild_venv_for_current_location(host_python)
    else:
        # Venv exists and is for the current project root — just refresh
        # dependencies if requirements.txt changed since the last install.
        _ensure_dependencies(force=False)

    _launch_main()


if __name__ == "__main__":
    main()
