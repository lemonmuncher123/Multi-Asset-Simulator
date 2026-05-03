#!/usr/bin/env bash
# Double-click launcher for macOS source-tree users.
# Sets the working directory to this script's folder, then runs the
# bootstrap launcher. Keeps the Terminal window open if anything fails so
# the user can read the error.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Self-healing: strip the macOS download-quarantine attribute from every
# file in this folder. Files extracted from a GitHub-downloaded zip carry
# `com.apple.quarantine`, and Gatekeeper blocks Finder double-click on any
# quarantined `.command`. The very first run still needs the user to
# right-click → Open (or grant permission in Privacy & Security) because
# we couldn't run if Gatekeeper had blocked us — but after this script
# starts once, every other file is de-quarantined, so future double-
# clicks (and folder moves) work without further prompts.
xattr -dr com.apple.quarantine "$SCRIPT_DIR" 2>/dev/null || true

# Pre-flight: test whether macOS TCC (Privacy & Security) is blocking
# file reads in this folder. macOS protects ~/Desktop, ~/Documents,
# ~/Downloads, and iCloud Drive — Terminal can `cd` into them (navigation
# is unrestricted) but file-read syscalls return EPERM until the user
# grants the responsible app permission. Python launched as a child of
# Terminal counts as a different responsible app, so its first file open
# fails with the cryptic `[Errno 1] Operation not permitted`. Detect that
# here, before Python is involved, and tell the user what to do.
if [ ! -r "scripts/bootstrap_launcher.py" ]; then
    echo
    echo "================================================================"
    echo "ERROR: macOS is blocking file access in this folder."
    echo "================================================================"
    echo
    echo "Folder: $SCRIPT_DIR"
    echo
    echo "macOS protects these locations by default:"
    echo "  • ~/Desktop"
    echo "  • ~/Documents"
    echo "  • ~/Downloads"
    echo "  • iCloud Drive (~/Library/Mobile Documents/...)"
    echo
    echo "Terminal can navigate into them, but it can't read files in them"
    echo "without your explicit permission. That's what's blocking us now."
    echo
    echo "TWO WAYS TO FIX:"
    echo
    echo "  1. EASIEST — move this folder somewhere unprotected."
    echo "     Examples that work without any system prompts:"
    echo "        ~/Projects/multi-asset-simulator"
    echo "        ~/Applications/multi-asset-simulator"
    echo "        ~/multi-asset-simulator"
    echo "     Then double-click Mac Launch Portfolio Trainer.command again."
    echo
    echo "  2. Grant Terminal access to this folder:"
    echo "     System Settings → Privacy & Security → Files and Folders"
    echo "     Find Terminal in the list and enable the matching folder"
    echo "     (Desktop / Documents / Downloads). Then re-run the launcher."
    echo
    echo "================================================================"
    echo
    echo "Press Return to close this window..."
    read -r _
    exit 1
fi

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
