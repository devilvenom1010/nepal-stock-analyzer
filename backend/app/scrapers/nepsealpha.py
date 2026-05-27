# backend/app/scrapers/nepsealpha.py
#
# NepseAlpha is behind Cloudflare. We use cloudscraper to bypass it.
# If cloudscraper also fails, we fall back to merolagani.com which has
# an identical TradingView JSON API and NO Cloudflare protection.
#
# Merolagani fallback endpoint (discovered via DevTools):
#   GET https://merolagani.com/handlers/TechnicalChartHandler.ashx
#       ?type=full&symbol=NABIL&from=MM/DD/YYYY&to=MM/DD/YYYY
#
# This returns the same {d,o,h,l,c,v} JSON as NepseAlpha.

import json
import logging
from datetime import date, datetime, timezone, timedelta
from typing import Optional

from .base import BaseScraper, HEADERS

logger = logging.getLogger(__name__)


class NepseAlphaScraper(BaseScraper):
    SOURCE_NAME = "nepsealpha"

    # ── Primary: cloudscraper → NepseAlpha hidden TradingView API ───────
    NEPSEALPHA_HISTORY = "https://nepsealpha.com/trading/1/history"
    NEPSEALPHA_CHART   = "https://nepsealpha.com/nepse-chart"

    # ── Fallback: Merolagani chart handler (open, no auth needed) ───────
    MEROLAGANI_HISTORY = (
        "https://merolagani.com/handlers/TechnicalChartHandler.ashx"
    )
    MEROLAGANI_LISTING = "https://merolagani.com/listingcompany.aspx"

    # ── Today's price fallback: Sharesansar today table ─────────────────
    SHARESANSAR_TODAY  = "https://www.sharesansar.com/today-share-price"

    async def scrape_all_symbols(self) -> list[str]:
        """
        Get every listed symbol.  Tries Merolagani listing page first
        (reliable HTML table), falls back to seed list.
        """
        soup = await self.fetch(self.MEROLAGANI_LISTING)
        if soup:
            symbols = []
            table = soup.find("table", {"class": "table"})
            if table:
                for row in table.find_all("tr")[1:]:
                    cols = row.find_all("td")
                    if cols:
                        s = cols[0].get_text(strip=True).upper()
                        if s:
                            symbols.append(s)
            if symbols:
                logger.info(f"[nepsealpha] {len(symbols)} symbols from merolagani listing")
                return symbols

        logger.warning("[nepsealpha] Using seed symbol list")
        return _SEED_SYMBOLS

    async def scrape_historical(self, symbol: str, days: int = 30) -> list[dict]:
        """
        Try NepseAlpha via cloudscraper first, fall back to Merolagani.
        """
        # ── Attempt 1: NepseAlpha via cloudscraper ───────────────────────
        records = await self._try_nepsealpha(symbol, days)
        if records:
            return records

        # ── Attempt 2: Merolagani chart handler (no auth, no Cloudflare) ─
        records = await self._try_merolagani_history(symbol, days)
        if records:
            return records

        return []

    async def scrape_daily(self, symbol: str) -> Optional[dict]:
        """Get today's data — asks for last 3 days and takes the most recent."""
        records = await self.scrape_historical(symbol, days=3)
        return records[-1] if records else None

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    async def _try_nepsealpha(self, symbol: str, days: int) -> list[dict]:
        try:
            import cloudscraper
            scraper = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
            # Warm session to get Cloudflare cookies
            scraper.get(self.NEPSEALPHA_CHART, timeout=20)

            start, end = self.date_range(days)
            from_ts = _to_unix(start)
            to_ts   = _to_unix(end)
            url = (
                f"{self.NEPSEALPHA_HISTORY}"
                f"?symbol={symbol}&resolution=1D"
                f"&from={from_ts}&to={to_ts}&currencyCode=NRS"
            )
            resp = scraper.get(url, timeout=30)
            if resp.status_code == 200:
                data    = resp.json()
                records = _parse_tv_response(data, symbol, self.SOURCE_NAME)
                if records:
                    logger.info(f"[nepsealpha/cloudscr] {symbol}: {len(records)} records")
                    return records
        except ImportError:
            logger.warning("[nepsealpha] cloudscraper not installed — skipping")
        except Exception as e:
            logger.debug(f"[nepsealpha/cloudscr] {symbol}: {e}")
        return []

    async def _try_merolagani_history(self, symbol: str, days: int) -> list[dict]:
        """
        Merolagani chart handler returns JSON like:
        [{"d":"2024-05-01","o":450,"h":460,"l":445,"c":455,"v":12000}, ...]
        """
        start, end = self.date_range(days)
        url = (
            f"{self.MEROLAGANI_HISTORY}"
            f"?type=full&symbol={symbol}"
            f"&from={start.strftime('%m/%d/%Y')}"
            f"&to={end.strftime('%m/%d/%Y')}"
        )
        soup = await self.fetch(url)
        if not soup:
            return []
        try:
            data = json.loads(soup.get_text())
            records = []
            for item in data:
                close = _f(item.get("c"))
                if not close:
                    continue
                records.append({
                    "symbol": symbol,
                    "date":   _iso(item.get("d")),
                    "open":   _f(item.get("o")),
                    "high":   _f(item.get("h")),
                    "low":    _f(item.get("l")),
                    "close":  close,
                    "volume": _f(item.get("v")),
                    "source": "merolagani",
                })
            if records:
                logger.info(f"[nepsealpha/merolagani] {symbol}: {len(records)} records")
            return records
        except Exception as e:
            logger.debug(f"[nepsealpha/merolagani] {symbol}: {e}")
            return []


# ─────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────

def _to_unix(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def _parse_tv_response(data: dict, symbol: str, source: str) -> list[dict]:
    if data.get("s") != "ok":
        return []
    records = []
    for i, ts in enumerate(data.get("t", [])):
        try:
            records.append({
                "symbol": symbol,
                "date":   datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat(),
                "open":   _safe(data.get("o", []), i),
                "high":   _safe(data.get("h", []), i),
                "low":    _safe(data.get("l", []), i),
                "close":  _safe(data.get("c", []), i),
                "volume": _safe(data.get("v", []), i),
                "source": source,
            })
        except Exception:
            pass
    return records


def _safe(lst, i):
    try:
        v = lst[i]; return float(v) if v is not None else None
    except Exception: return None

def _f(v):
    try: return float(v) if v is not None else None
    except Exception: return None

def _iso(v) -> str:
    if not v: return date.today().isoformat()
    s = str(v).strip()
    # Already ISO
    if len(s) == 10 and s[4] == "-": return s
    # Try parsing other formats
    for fmt in ("%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    return s


_SEED_SYMBOLS = [
    "NABIL","NICA","SANIMA","EBL","MBL","PRVU","SBI","ADBL",
    "GBIME","KBL","PCBL","SRBL","NBL","RBB","BOK","NMB",
    "NIMB","HIDCL","CBL","JBNL","LBBL","MNBBL",
    "UPPER","NHPC","AHPC","CHCL","AKPL","RIDI","SHPC","KPCL","BPCL",
    "NLIC","LICN","ALICL","PICL",
    "CBBL","SKBBL","SWBBL","DDBL",
    "NTC","UNL","HDL",
]