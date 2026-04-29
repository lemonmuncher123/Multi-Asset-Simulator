#!/usr/bin/env bash
# Double-click launcher for macOS source-tree users.
# Sets the working directory to this script's folder, then runs the
# bootstrap launcher. Keeps the Terminal window open if anything fails so
# the user can read the error.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN=""
for candidate in python3.13 python3.12 python3 python; do
    if command -v "$candidate" >/dev/null 2>&1; then
        PYTHON_BIN="$candidate"
        break
    fi
done

if [ -z "$PYTHON_BIN" ]; then
    echo
    echo "ERROR: No Python 3 interpreter found on PATH."
    echo "Install Python 3.12 or 3.13 from https://www.python.org/downloads/ and try again."
    echo
    echo "Press Return to close this window..."
    read -r _
    exit 1
fi

"$PYTHON_BIN" "scripts/bootstrap_launcher.py"
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo
    echo "Launcher exited with status $EXIT_CODE."
    echo "See .launcher/launcher.log for details."
    echo
    echo "Press Return to close this window..."
    read -r _
fi

exit $EXIT_CODE
