# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for Portfolio Trainer.

Build:
    pyinstaller --noconfirm PortfolioTrainer.spec

Output:
    dist/Portfolio Trainer/                  (one-folder layout)
    dist/Portfolio Trainer.app/  (macOS bundle, BUNDLE step below)
"""

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# Data files: bundle the schema next to its package so
# importlib.resources.files("src.storage") can find it at runtime.
datas = [
    ("src/storage/schema.sql", "src/storage"),
]

# Collect runtime data and plugins for the GUI / chart / data stack.
datas += collect_data_files("PySide6")
datas += collect_data_files("matplotlib")
datas += collect_data_files("pandas")
datas += collect_data_files("openpyxl")
datas += collect_data_files("yfinance")

# Submodules whose imports are resolved dynamically (so PyInstaller's
# static analysis can miss them).
hiddenimports = []
hiddenimports += collect_submodules("PySide6")
hiddenimports += collect_submodules("matplotlib.backends")
hiddenimports += collect_submodules("yfinance")
hiddenimports += [
    # Import roots discovered through string-based loading.
    "pandas",
    "openpyxl",
    "plotly",
    "dateutil",
]

excludes = [
    # Don't drag the test toolchain into the user-facing build.
    "pytest",
    "tests",
    # Other GUI toolkits that pandas/matplotlib may try to import.
    "tkinter",
    "PyQt5",
    "PyQt6",
    "PySide2",
    "IPython",
    "notebook",
    "jupyter",
]


a = Analysis(
    ["main.py"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="Portfolio Trainer",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,           # GUI/windowed: no terminal window for end users
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="Portfolio Trainer",
)

# macOS .app bundle wrapping the one-folder collection.
app = BUNDLE(
    coll,
    name="Portfolio Trainer.app",
    icon=None,
    bundle_identifier="com.assettrainer.local",
    info_plist={
        "CFBundleName": "Portfolio Trainer",
        "CFBundleDisplayName": "Portfolio Trainer",
        "CFBundleShortVersionString": "0.1.0",
        "CFBundleVersion": "0.1.0",
        "NSHighResolutionCapable": True,
        # The app reads/writes only its own per-user dir; no network entitlements.
        "LSApplicationCategoryType": "public.app-category.finance",
    },
)
