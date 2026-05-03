from src.models.security_master import SecurityMasterRecord


def _etf(symbol: str, name: str, category: str) -> SecurityMasterRecord:
    return SecurityMasterRecord(
        symbol=symbol, name=name, asset_type="etf",
        exchange="NYSE Arca", etf_category=category, is_common_etf=True,
    )


COMMON_ETFS: list[SecurityMasterRecord] = [
    # --- Broad Market ---
    _etf("SPY", "SPDR S&P 500 ETF Trust", "Broad Market"),
    _etf("VOO", "Vanguard S&P 500 ETF", "Broad Market"),
    _etf("IVV", "iShares Core S&P 500 ETF", "Broad Market"),
    _etf("VTI", "Vanguard Total Stock Market ETF", "Broad Market"),
    _etf("QQQ", "Invesco QQQ Trust", "Broad Market"),
    _etf("DIA", "SPDR Dow Jones Industrial Average ETF", "Broad Market"),
    _etf("RSP", "Invesco S&P 500 Equal Weight ETF", "Broad Market"),

    # --- S&P Sector ---
    _etf("XLK", "Technology Select Sector SPDR", "Sector"),
    _etf("XLF", "Financial Select Sector SPDR", "Sector"),
    _etf("XLE", "Energy Select Sector SPDR", "Sector"),
    _etf("XLV", "Health Care Select Sector SPDR", "Sector"),
    _etf("XLI", "Industrial Select Sector SPDR", "Sector"),
    _etf("XLP", "Consumer Staples Select Sector SPDR", "Sector"),
    _etf("XLY", "Consumer Discretionary Select Sector SPDR", "Sector"),
    _etf("XLU", "Utilities Select Sector SPDR", "Sector"),
    _etf("XLB", "Materials Select Sector SPDR", "Sector"),
    _etf("XLRE", "Real Estate Select Sector SPDR", "Sector"),
    _etf("XLC", "Communication Services Select Sector SPDR", "Sector"),

    # --- Bond / Fixed Income ---
    _etf("AGG", "iShares Core U.S. Aggregate Bond ETF", "Bond"),
    _etf("BND", "Vanguard Total Bond Market ETF", "Bond"),
    _etf("TLT", "iShares 20+ Year Treasury Bond ETF", "Bond"),
    _etf("IEF", "iShares 7-10 Year Treasury Bond ETF", "Bond"),
    _etf("SHY", "iShares 1-3 Year Treasury Bond ETF", "Bond"),
    _etf("TIP", "iShares TIPS Bond ETF", "Bond"),
    _etf("HYG", "iShares iBoxx $ High Yield Corporate Bond ETF", "Bond"),
    _etf("LQD", "iShares iBoxx $ Investment Grade Corporate Bond ETF", "Bond"),
    _etf("VCSH", "Vanguard Short-Term Corporate Bond ETF", "Bond"),

    # --- International ---
    _etf("EFA", "iShares MSCI EAFE ETF", "International"),
    _etf("VEA", "Vanguard FTSE Developed Markets ETF", "International"),
    _etf("VWO", "Vanguard FTSE Emerging Markets ETF", "International"),
    _etf("EEM", "iShares MSCI Emerging Markets ETF", "International"),
    _etf("VXUS", "Vanguard Total International Stock ETF", "International"),
    _etf("IEMG", "iShares Core MSCI Emerging Markets ETF", "International"),

    # --- Commodity ---
    _etf("GLD", "SPDR Gold Shares", "Commodity"),
    _etf("SLV", "iShares Silver Trust", "Commodity"),
    _etf("USO", "United States Oil Fund", "Commodity"),
    _etf("DBC", "Invesco DB Commodity Index Tracking Fund", "Commodity"),
    _etf("IAU", "iShares Gold Trust", "Commodity"),

    # --- Real Estate ---
    _etf("VNQ", "Vanguard Real Estate ETF", "Real Estate"),
    _etf("IYR", "iShares U.S. Real Estate ETF", "Real Estate"),
    _etf("SCHH", "Schwab U.S. REIT ETF", "Real Estate"),

    # --- Dividend ---
    _etf("VYM", "Vanguard High Dividend Yield ETF", "Dividend"),
    _etf("SCHD", "Schwab U.S. Dividend Equity ETF", "Dividend"),
    _etf("DVY", "iShares Select Dividend ETF", "Dividend"),
    _etf("HDV", "iShares Core High Dividend ETF", "Dividend"),

    # --- Growth / Value ---
    _etf("VUG", "Vanguard Growth ETF", "Growth"),
    _etf("VTV", "Vanguard Value ETF", "Value"),
    _etf("IWF", "iShares Russell 1000 Growth ETF", "Growth"),
    _etf("IWD", "iShares Russell 1000 Value ETF", "Value"),

    # --- Small / Mid Cap ---
    _etf("IWM", "iShares Russell 2000 ETF", "Small Cap"),
    _etf("VB", "Vanguard Small-Cap ETF", "Small Cap"),
    _etf("IJR", "iShares Core S&P Small-Cap ETF", "Small Cap"),
    _etf("VO", "Vanguard Mid-Cap ETF", "Mid Cap"),
    _etf("IJH", "iShares Core S&P Mid-Cap ETF", "Mid Cap"),

    # --- Thematic ---
    _etf("ARKK", "ARK Innovation ETF", "Thematic"),
    _etf("ICLN", "iShares Global Clean Energy ETF", "Thematic"),
    _etf("SOXX", "iShares Semiconductor ETF", "Thematic"),
    _etf("XBI", "SPDR S&P Biotech ETF", "Thematic"),

    # --- Crypto-related ---
    _etf("BITO", "ProShares Bitcoin Strategy ETF", "Crypto"),
]
