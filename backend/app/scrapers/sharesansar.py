# backend/app/scrapers/sharesansar.py
#
# Sharesansar.com renders its data as server-side HTML tables — no JS needed.
#
# KEY PAGES:
#   /today-share-price          → full market snapshot table (all symbols, one page)
#   /company/{symbol}           → per-symbol fundamental data (P/E, market cap, etc.)
#   /company/{symbol}/price     → per-symbol historical price table
#
# TABLE STRUCTURE on /today-share-price:
#   <table class="table table-condensed table-hover ...">
#     <thead>
#       <tr>
#         <th>S.N.</th>
#         <th>Symbol</th>
#         <th>LTP</th>       ← Last Traded Price (= close)
#         <th>% Change</th>
#         <th>Open</th>
#         <th>High</th>
#         <th>Low</th>
#         <th>Volume</th>
#         <th>Previous Closing</th>
#         <th>...</th>
#       </tr>
#     </thead>
#     <tbody>
#       <tr><td>1</td><td>NABIL</td><td>1,350.00</td>...</tr>
#       ...
#     </tbody>
#   </table>
#
# NOTE: The historical price endpoint (/company/{symbol}/price) paginates.
# We iterate pages until the date range is covered.

import logging
import re
from datetime import date, timedelta
from typing import Optional

from bs4 import BeautifulSoup

from .base import BaseScraper

logger = logging.getLogger(__name__)

# Column indices on /today-share-price (0-indexed after stripping S.N.)
# These are confirmed from real page inspection by the NEPSE community.
# If Sharesansar updates their layout, only these constants need updating.
COL_SYMBOL   = 1
COL_CLOSE    = 2   # "LTP" = Last Traded Price
COL_PCT      = 3
COL_OPEN     = 4
COL_HIGH     = 5
COL_LOW      = 6
COL_VOLUME   = 7
COL_PREV_CLS = 8


class SharesansarScraper(BaseScraper):
    SOURCE_NAME = "sharesansar"
    BASE_URL    = "https://www.sharesansar.com"

    # ------------------------------------------------------------------ #
    #  Public interface                                                    #
    # ------------------------------------------------------------------ #

    async def scrape_all_symbols(self) -> list[str]:
        """
        Return all symbols currently listed on the market by reading
        the today-share-price table (fastest — one request, all symbols).
        """
        rows = await self._fetch_today_table()
        symbols = [r["symbol"] for r in rows if r.get("symbol")]
        logger.info(f"[sharesansar] {len(symbols)} symbols from today's table")
        return symbols

    async def scrape_daily(self, symbol: str) -> Optional[dict]:
        """
        Return today's price snapshot for one symbol.
        Reads the full market table and picks the matching row — more
        efficient than hitting individual symbol pages for every stock.
        """
        rows = await self._fetch_today_table()
        for row in rows:
            if row.get("symbol", "").upper() == symbol.upper():
                # Enrich with fundamentals from the company page
                fund = await self._fetch_fundamentals(symbol)
                return {**row, **fund}
        logger.warning(f"[sharesansar] Symbol {symbol} not found in today's table")
        return None

    async def scrape_historical(self, symbol: str, days: int = 30) -> list[dict]:
        """
        Scrape the historical price table for `symbol` going back `days` days.
        Sharesansar paginates this table (15 rows/page); we walk pages until
        we have enough history or run out of data.
        """
        target_start = date.today() - timedelta(days=days)
        records      = []
        page         = 1

        while True:
            url  = f"{self.BASE_URL}/company-details/{symbol}/price?page={page}"
            soup = await self.fetch(url)
            if not soup:
                break

            page_records = _parse_historical_table(soup, symbol, self.SOURCE_NAME)
            if not page_records:
                break   # no more data

            records.extend(page_records)

            # Check if the oldest record on this page is older than our target
            oldest_date_str = page_records[-1].get("date", "")
            try:
                oldest = date.fromisoformat(oldest_date_str)
                if oldest <= target_start:
                    break   # we have enough history
            except ValueError:
                break

            page += 1
            if page > 20:   # safety cap — never scrape more than 300 rows
                break

        # Filter to only the requested date range
        cutoff = target_start.isoformat()
        records = [r for r in records if r.get("date", "") >= cutoff]
        logger.info(f"[sharesansar] {symbol}: {len(records)} historical records")
        return records

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    async def _fetch_today_table(self) -> list[dict]:
        """
        Fetch and parse the full-market today-share-price table.
        Returns a list of dicts, one per listed stock.
        """
        url  = f"{self.BASE_URL}/today-share-price"
        soup = await self.fetch(url)
        if not soup:
            return []

        table = soup.find("table")
        if not table:
            logger.error("[sharesansar] No table found on today-share-price page")
            return []

        rows = []
        today = date.today().isoformat()

        for tr in table.find("tbody").find_all("tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cols) < 8:
                continue   # skip malformed rows

            symbol = cols[COL_SYMBOL].strip().upper()
            if not symbol or symbol == "-":
                continue

            rows.append({
                "symbol": symbol,
                "date":   today,
                "close":  _pf(cols[COL_CLOSE]),
                "open":   _pf(cols[COL_OPEN]),
                "high":   _pf(cols[COL_HIGH]),
                "low":    _pf(cols[COL_LOW]),
                "volume": _pf(cols[COL_VOLUME]),
                "source": self.SOURCE_NAME,
            })

        logger.info(f"[sharesansar] Today's table: {len(rows)} rows")
        return rows

    async def _fetch_fundamentals(self, symbol: str) -> dict:
        """
        Fetch P/E, market cap, 52-week high/low, EPS, book value from
        the company detail page. Returns an empty dict if page unavailable.
        """
        url  = f"{self.BASE_URL}/company/{symbol}"
        soup = await self.fetch(url)
        if not soup:
            return {}

        result = {}
        today  = date.today().isoformat()

        # Sharesansar renders a stats table with label-value pairs.
        # We search for known label strings and extract the adjacent value.
        label_map = {
            "Market Capitalization": "market_cap",
            "P/E Ratio":             "pe_ratio",
            "52 Weeks High":         "week52_high",
            "52 Weeks Low":          "week52_low",
            "EPS":                   "eps",
            "Book Value":            "book_value",
        }

        # Try both <table> rows and <dl>/<div> key-value pairs
        for row in soup.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                for known_label, field in label_map.items():
                    if known_label.lower() in label.lower():
                        result[field] = _pf(value)
                        break

        result["date"]   = today
        result["source"] = self.SOURCE_NAME
        return result


# ------------------------------------------------------------------ #
#  Parse historical price table                                       #
# ------------------------------------------------------------------ #

def _parse_historical_table(soup: BeautifulSoup, symbol: str, source: str) -> list[dict]:
    """
    Parse the per-symbol price history table on /company/{symbol}/price.

    Expected columns (may vary slightly):
      Date | Open | High | Low | Close | Volume | Turnover
    """
    table = soup.find("table")
    if not table:
        return []

    # Read header to build a column-index map (robust to column reordering)
    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
    idx = _col_index(headers)

    records = []
    for tr in table.find("tbody").find_all("tr"):
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cols) < 4:
            continue

        raw_date = cols[idx.get("date", 0)]
        parsed   = _parse_date(raw_date)
        if not parsed:
            continue

        records.append({
            "symbol": symbol,
            "date":   parsed,
            "open":   _pf(cols[idx["open"]])    if "open"   in idx else None,
            "high":   _pf(cols[idx["high"]])    if "high"   in idx else None,
            "low":    _pf(cols[idx["low"]])     if "low"    in idx else None,
            "close":  _pf(cols[idx["close"]])   if "close"  in idx else None,
            "volume": _pf(cols[idx["volume"]])  if "volume" in idx else None,
            "source": source,
        })

    return records


def _col_index(headers: list[str]) -> dict:
    """
    Map column names to indices from the actual header row.
    Handles Sharesansar's slightly inconsistent header naming.
    """
    mapping = {}
    for i, h in enumerate(headers):
        h = h.lower().strip()
        if "date"   in h:              mapping["date"]   = i
        elif "open" in h:              mapping["open"]   = i
        elif "high" in h:              mapping["high"]   = i
        elif "low"  in h:              mapping["low"]    = i
        elif "close" in h or "ltp" in h: mapping["close"] = i
        elif "vol"  in h:              mapping["volume"] = i
    return mapping


# ------------------------------------------------------------------ #
#  Utility functions                                                  #
# ------------------------------------------------------------------ #

def _pf(val: Optional[str]) -> Optional[float]:
    """Parse a Nepali-formatted number string ('1,350.00') to float."""
    if not val:
        return None
    cleaned = re.sub(r"[^\d.\-]", "", val)   # strip commas, Rs., %
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _parse_date(raw: str) -> Optional[str]:
    """
    Parse Sharesansar date strings to ISO format.
    Handles common patterns: '2024-05-12', '12-05-2024', 'May 12, 2024'.
    Returns 'YYYY-MM-DD' or None on failure.
    """
    raw = raw.strip()
    if not raw:
        return None

    # Try ISO first (most common on newer pages)
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw

    # DD-MM-YYYY
    m = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{4})$", raw)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    # Month name formats: 'May 12, 2024' or '12 May 2024'
    try:
        from datetime import datetime as dt
        for fmt in ("%B %d, %Y", "%d %B %Y", "%b %d, %Y", "%d %b %Y"):
            try:
                return dt.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
    except Exception:
        pass

    logger.debug(f"[sharesansar] Could not parse date: {raw!r}")
    return None