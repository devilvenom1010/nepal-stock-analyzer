# backend/app/scrapers/nepsealpha.py
#
# NepseAlpha uses a TradingView-compatible hidden JSON API discovered via
# browser DevTools XHR inspection:
#
#   GET /trading/1/history
#       ?symbol=NABIL
#       &resolution=1D
#       &from=<unix_timestamp>
#       &to=<unix_timestamp>
#       &currencyCode=NRS
#
# The site uses Cloudflare + Laravel sessions. We maintain a persistent
# httpx session so cookies (laravel_session, cf_clearance) are carried
# across all requests automatically.
#
# Response JSON shape:
#   {
#     "s": "ok",          # status ("ok" or "no_data")
#     "t": [1620000000],  # timestamps (Unix)
#     "o": [450.0],       # open prices
#     "h": [460.0],       # high prices
#     "l": [445.0],       # low prices
#     "c": [455.0],       # close prices
#     "v": [12000.0]      # volume
#   }
#
# Symbol list is fetched from the public stocks listing page.

import json
import logging
import time
from datetime import date, datetime, timezone
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from .base import BaseScraper, HEADERS

logger = logging.getLogger(__name__)


class NepseAlphaScraper(BaseScraper):
    SOURCE_NAME = "nepsealpha"
    BASE_URL    = "https://nepsealpha.com"

    # Endpoints
    HISTORY_URL = f"{BASE_URL}/trading/1/history"
    SYMBOLS_URL = f"{BASE_URL}/trading/1/symbols"    # used for session init
    LISTING_URL = f"{BASE_URL}/stocks"               # HTML listing page

    def __init__(self):
        # Use a real browser-like client that persists cookies across requests.
        # Cloudflare protection requires us to first hit the main site so that
        # laravel_session and __cf_bm cookies are set before hitting the API.
        self.client = httpx.AsyncClient(
            headers=HEADERS,
            timeout=30.0,
            follow_redirects=True,
        )
        self._session_ready = False

    async def _init_session(self):
        """
        Hit the main chart page first so Cloudflare and Laravel issue the
        necessary session cookies. Without this the history API returns 403.
        We only need to do this once per scraper instance.
        """
        if self._session_ready:
            return
        try:
            # Step 1: land on the chart page to get cf + laravel cookies
            await self.client.get(
                f"{self.BASE_URL}/nepse-chart",
                headers={**HEADERS, "Accept": "text/html"},
            )
            # Step 2: hit the symbols endpoint to warm the Laravel session
            await self.client.get(
                f"{self.SYMBOLS_URL}?symbol=NABIL",
                headers={**HEADERS, "Accept": "application/json"},
            )
            self._session_ready = True
            logger.info("[nepsealpha] Session initialized with cookies")
        except Exception as e:
            logger.warning(f"[nepsealpha] Session init warning: {e}")

    # ------------------------------------------------------------------ #
    #  Public interface                                                    #
    # ------------------------------------------------------------------ #

    async def scrape_all_symbols(self) -> list[str]:
        """
        Scrape the full list of NEPSE-listed symbols from the NepseAlpha
        stocks listing page (/stocks). Falls back to a hardcoded seed list
        if the page structure changes.
        """
        soup = await self.fetch(self.LISTING_URL)
        if not soup:
            logger.warning("[nepsealpha] Could not load listing page, using seed list")
            return _SEED_SYMBOLS

        symbols = []
        # NepseAlpha renders company cards / table rows with data-symbol attr,
        # or plain <a> links like /stocks/NABIL/info
        for tag in soup.select("a[href*='/stocks/']"):
            href = tag.get("href", "")
            parts = [p for p in href.split("/") if p]
            # href pattern: /stocks/{SYMBOL}/info  → parts = ['stocks','SYMBOL','info']
            if len(parts) >= 2 and parts[0] == "stocks":
                sym = parts[1].upper()
                if sym and sym not in symbols and sym != "INFO":
                    symbols.append(sym)

        if not symbols:
            logger.warning("[nepsealpha] Symbol parse yielded nothing, using seed list")
            return _SEED_SYMBOLS

        logger.info(f"[nepsealpha] Found {len(symbols)} symbols from listing page")
        return symbols

    async def scrape_historical(self, symbol: str, days: int = 30) -> list[dict]:
        """
        Fetch daily OHLCV bars for `symbol` going back `days` calendar days.
        Returns a list of dicts ready for DB insertion.
        """
        await self._init_session()
        start, end = self.date_range(days)
        from_ts = _to_unix(start)
        to_ts   = _to_unix(end)

        url = (
            f"{self.HISTORY_URL}"
            f"?symbol={symbol}"
            f"&resolution=1D"
            f"&from={from_ts}"
            f"&to={to_ts}"
            f"&currencyCode=NRS"
        )

        try:
            resp = await self.client.get(
                url,
                headers={
                    "Referer": "https://nepsealpha.com/nepse-chart",
                    "Accept": "application/json, text/plain, */*",
                    "X-Requested-With": "XMLHttpRequest",
                }
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"[nepsealpha] Historical fetch failed for {symbol}: {e}")
            return []

        return _parse_history_response(data, symbol, self.SOURCE_NAME)

    async def scrape_daily(self, symbol: str) -> Optional[dict]:
        """
        Fetch today's OHLCV bar.  We ask for the last 3 days to be safe
        (in case market was closed yesterday) and take the most recent row.
        """
        records = await self.scrape_historical(symbol, days=3)
        if not records:
            return None
        # Most recent record is last in the list (ascending timestamps)
        return records[-1]


# ------------------------------------------------------------------ #
#  Helpers                                                            #
# ------------------------------------------------------------------ #

def _to_unix(d: date) -> int:
    """Convert a date to a UTC Unix timestamp (midnight Nepal time ≈ UTC+5:45)."""
    dt = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    return int(dt.timestamp())


def _parse_history_response(data: dict, symbol: str, source: str) -> list[dict]:
    """
    Turn the NepseAlpha TradingView-format JSON into a list of price dicts.

    Expected shape:
      {"s":"ok","t":[...],"o":[...],"h":[...],"l":[...],"c":[...],"v":[...]}
    """
    if data.get("s") != "ok":
        logger.debug(f"[nepsealpha] No data for {symbol}: status={data.get('s')}")
        return []

    timestamps = data.get("t", [])
    opens      = data.get("o", [])
    highs      = data.get("h", [])
    lows       = data.get("l", [])
    closes     = data.get("c", [])
    volumes    = data.get("v", [])

    records = []
    for i, ts in enumerate(timestamps):
        try:
            day = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
            records.append({
                "symbol": symbol,
                "date":   day,
                "open":   _safe_float(opens,   i),
                "high":   _safe_float(highs,   i),
                "low":    _safe_float(lows,    i),
                "close":  _safe_float(closes,  i),
                "volume": _safe_float(volumes, i),
                "source": source,
            })
        except Exception as e:
            logger.debug(f"[nepsealpha] Row parse error index {i} for {symbol}: {e}")
    return records


def _safe_float(lst: list, idx: int) -> Optional[float]:
    try:
        v = lst[idx]
        return float(v) if v is not None else None
    except (IndexError, TypeError, ValueError):
        return None


# ------------------------------------------------------------------ #
#  Seed symbol list — used as fallback if listing page fails          #
#  (a representative subset of major NEPSE listings)                 #
# ------------------------------------------------------------------ #
_SEED_SYMBOLS = [
    # Commercial Banks
    "NABIL", "NICA", "SANIMA", "EBL", "MBL", "PRVU", "SBI", "ADBL",
    "GBIME", "KBL", "PCBL", "SRBL", "NBL", "RBB", "BOK", "NMB",
    "NIMB", "HIDCL", "CBL", "JBNL", "LBBL", "MNBBL", "CCBL", "SADBL",
    # Development Banks
    "SHINE", "CORBL", "KSBBL", "SAPDBL", "EDBL",
    # Finance Companies
    "GUFL", "NFS", "ICFC", "SIFC",
    # Hydropower
    "UPPER", "NHPC", "AHPC", "CHCL", "API", "AKPL", "GHL",
    "RIDI", "HPPL", "BARUN", "SHPC", "KPCL", "BPCL", "NGPL",
    # Insurance
    "NLIC", "LICN", "ALICL", "PICL", "SIC", "NICL",
    # Microfinance
    "CBBL", "SKBBL", "SWBBL", "DDBL", "MLBBL",
    # Manufacturing & Others
    "UNL", "BNT", "SARBTM", "HDL",
]