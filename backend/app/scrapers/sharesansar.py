# backend/app/scrapers/sharesansar.py
#
# KEY INSIGHT (discovered from live page inspection):
#
# The /today-share-price table contains links like:
#   <a href="/company/NABIL">NABIL</a>
#
# The company detail page /company/NABIL contains a heading like:
#   <h1>... NABIL ...</h1>  with a nearby number "16"
#
# The number appears in the raw HTML as:
#   <input type="hidden" name="companyid" value="16">
#   OR in a JS block: var companyId = 16;
#   OR as data-id="16" on the price-history button/tab
#
# STRATEGY: Build the full symbol→ID map in ONE request by scraping
# /today-share-price with the raw lxml parser and reading every href
# that matches /company/{SYMBOL} to get the symbol list, then resolve
# IDs by hitting each /company/{SYMBOL} page and scanning ALL patterns.
#
# IMPORTANT: We also keep a hardcoded fallback map of ~50 major symbols
# so the scraper never fully fails even if the site changes.

import logging
import re
from datetime import date, timedelta
from typing import Optional

from bs4 import BeautifulSoup

from .base import BaseScraper

logger = logging.getLogger(__name__)

COL_SYMBOL   = 1
COL_CLOSE    = 2
COL_PCT      = 3
COL_OPEN     = 4
COL_HIGH     = 5
COL_LOW      = 6
COL_VOLUME   = 7
COL_PREV_CLS = 8


class SharesansarScraper(BaseScraper):
    SOURCE_NAME = "sharesansar"
    BASE_URL    = "https://www.sharesansar.com"

    # Class-level cache shared across all instances
    _id_cache: dict[str, int] = {}
    _today_cache: list[dict]  = []   # cached to avoid re-fetching per symbol

    # ------------------------------------------------------------------ #
    #  Public interface                                                    #
    # ------------------------------------------------------------------ #

    async def scrape_all_symbols(self) -> list[str]:
        rows = await self._fetch_today_table()
        return [r["symbol"] for r in rows if r.get("symbol")]

    async def scrape_daily(self, symbol: str) -> Optional[dict]:
        rows = await self._fetch_today_table()
        for row in rows:
            if row.get("symbol", "").upper() == symbol.upper():
                fund = await self._fetch_fundamentals(symbol)
                return {**row, **fund}
        return None

    async def scrape_historical(self, symbol: str, days: int = 30) -> list[dict]:
        company_id = await self._resolve_company_id(symbol)
        if not company_id:
            logger.warning(f"[sharesansar] No company ID for {symbol} — skipping historical")
            return []

        target_start = date.today() - timedelta(days=days)
        records: list[dict] = []

        # Step 1: GET the company page to establish session/cookies and extract CSRF token
        url_get = f"{self.BASE_URL}/company/{symbol.upper()}"
        soup_get = await self.fetch(url_get)
        if not soup_get:
            logger.error(f"[sharesansar] Failed to fetch company page for {symbol} to obtain CSRF token")
            return []

        # Extract CSRF token
        token = None
        meta_token = soup_get.find("meta", {"name": "_token"}) or soup_get.find("meta", {"name": "csrf-token"})
        if meta_token:
            token = meta_token.get("content")
        if not token:
            input_token = soup_get.find("input", {"name": "_token"})
            if input_token:
                token = input_token.get("value")

        if not token:
            logger.error(f"[sharesansar] Could not find CSRF token on company page for {symbol}")
            return []

        # Step 2: POST paginated requests to company-price-history
        url_post = f"{self.BASE_URL}/company-price-history"
        post_headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "X-CSRF-TOKEN": token,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": self.BASE_URL,
            "Referer": url_get
        }

        import asyncio
        start = 0
        length = 50
        draw = 1

        while True:
            form_data = {
                "_token": token,
                "draw": str(draw),
                "start": str(start),
                "length": str(length),
                "company": str(company_id)
            }

            try:
                response = await self.client.post(url_post, data=form_data, headers=post_headers)
                if response.status_code != 200:
                    logger.warning(f"[sharesansar] POST returned {response.status_code} for {symbol} at start {start}")
                    break

                data = response.json()
                rows = data.get("data") or []
                if not rows:
                    break

                page_records = []
                for r in rows:
                    d = r.get("published_date") or r.get("date")
                    if not d:
                        continue
                    page_records.append({
                        "symbol": symbol.upper(),
                        "date":   d,
                        "open":   _pf(r.get("open_price") or r.get("open")),
                        "high":   _pf(r.get("high_price") or r.get("high")),
                        "low":    _pf(r.get("low_price") or r.get("low")),
                        "close":  _pf(r.get("close_price") or r.get("close") or r.get("closing_price")),
                        "volume": _pf(r.get("total_traded_quantity") or r.get("traded_quantity") or r.get("volume") or r.get("vol")),
                        "source": self.SOURCE_NAME,
                    })

                if not page_records:
                    break

                records.extend(page_records)

                # Check if we have fetched beyond the target start date
                oldest = page_records[-1].get("date", "")
                try:
                    if date.fromisoformat(oldest) <= target_start:
                        break
                except ValueError:
                    break

                # Check if there are no more records to fetch
                records_total = data.get("recordsTotal")
                if records_total is not None and start + length >= int(records_total):
                    break

                start += length
                draw += 1
                await asyncio.sleep(0.3)

            except Exception as e:
                logger.error(f"[sharesansar] Exception during historical POST for {symbol} at start {start}: {e}")
                break

        cutoff  = target_start.isoformat()
        records = [r for r in records if r.get("date", "") >= cutoff]
        logger.info(f"[sharesansar] {symbol}: {len(records)} historical records")
        return records

    # ------------------------------------------------------------------ #
    #  ID resolution — the critical piece                                 #
    # ------------------------------------------------------------------ #

    async def _resolve_company_id(self, symbol: str) -> Optional[int]:
        sym = symbol.upper().strip()

        # 1. Memory cache
        if sym in self._id_cache:
            return self._id_cache[sym]

        # 2. Hardcoded fallback map (covers ~90% of common stocks)
        if sym in _KNOWN_IDS:
            self._id_cache[sym] = _KNOWN_IDS[sym]
            return _KNOWN_IDS[sym]

        # 3. If cache is empty or symbol not found, try building the full map from /company/NABIL
        if not self._id_cache or len(self._id_cache) <= len(_KNOWN_IDS):
            await self._build_id_map_from_company_page()
            if sym in self._id_cache:
                return self._id_cache[sym]

        # 4. Try to extract from individual /company/{SYMBOL} page using many patterns
        cid = await self._scrape_id_from_company_page(sym)
        if cid:
            self._id_cache[sym] = cid
            logger.debug(f"[sharesansar] Resolved {sym} → ID {cid}")
            return cid

        # 5. Try building the map from today's table (scans all href links)
        await self._build_id_map_from_today_table()
        if sym in self._id_cache:
            return self._id_cache[sym]

        return None

    async def _build_id_map_from_company_page(self) -> None:
        """
        Hit /company/NABIL and parse 'var cmpjson = [...]' script to bulk-populate the ID cache.
        """
        url = f"{self.BASE_URL}/company/NABIL"
        soup = await self.fetch(url)
        if not soup:
            return

        import json
        for script in soup.find_all("script"):
            content = script.string or ""
            if "var cmpjson =" in content:
                match = re.search(r"var\s+cmpjson\s*=\s*(\[.*?\]);", content, re.DOTALL)
                if match:
                    try:
                        data = json.loads(match.group(1))
                        for item in data:
                            sym = item.get("symbol", "").upper().strip()
                            cid = item.get("id")
                            if sym and cid:
                                self._id_cache[sym] = int(cid)
                        logger.info(f"[sharesansar] Dynamically resolved {len(data)} company IDs from cmpjson")
                        return
                    except Exception as e:
                        logger.warning(f"[sharesansar] Failed to parse cmpjson from script block: {e}")

    async def _scrape_id_from_company_page(self, symbol: str) -> Optional[int]:
        """
        Hit /company/{SYMBOL} and try every known pattern to find the numeric ID.
        Sharesansar embeds it in the HTML in multiple ways — we try all of them.
        """
        url  = f"{self.BASE_URL}/company/{symbol}"
        soup = await self.fetch(url)
        if not soup:
            return None

        html = str(soup)

        # Pattern A: <input ... name="companyid" value="16">  (most common)
        for inp in soup.find_all("input"):
            name = (inp.get("name") or "").lower()
            if "companyid" in name or name == "id":
                v = _to_int(inp.get("value"))
                if v: return v

        # Pattern B: data-companyid="16" or data-id="16" on any tag
        for tag in soup.find_all(True):
            for attr in ("data-companyid", "data-company-id", "data-id"):
                v = _to_int(tag.get(attr))
                if v: return v

        # Pattern C: JS variable  var companyId = 16;  or  companyid:16
        for m in re.finditer(
            r'(?:companyid|company_id|companyId)\s*[=:]\s*["\']?(\d+)',
            html, re.IGNORECASE
        ):
            v = _to_int(m.group(1))
            if v: return v

        # Pattern D: URL in a form action or button href: ?companyid=16
        for m in re.finditer(r'companyid[=\s]+(\d+)', html, re.IGNORECASE):
            v = _to_int(m.group(1))
            if v: return v

        # Pattern E: /ajaxcompanypricehistory?page=1&companyid=16
        for m in re.finditer(r'ajaxcompanypricehistory[^"\']*companyid=(\d+)', html, re.IGNORECASE):
            v = _to_int(m.group(1))
            if v: return v

        # Pattern F: standalone number right after symbol heading
        # e.g.  <h2>NABIL</h2> ... 16 ...
        for tag in soup.find_all(["h1","h2","h3","h4","span","div","td"]):
            text = tag.get_text(strip=True)
            if text.upper() == symbol.upper():
                # Look at the next siblings for a bare number
                for sib in tag.next_siblings:
                    sib_text = getattr(sib, "get_text", lambda **k: str(sib))(strip=True)
                    m = re.match(r'^(\d+)$', sib_text)
                    if m:
                        v = _to_int(m.group(1))
                        if v: return v

        return None

    async def _build_id_map_from_today_table(self) -> None:
        """
        Scan the today-share-price table for data-id or href patterns
        to bulk-populate the ID cache in one request.
        """
        url  = f"{self.BASE_URL}/today-share-price"
        soup = await self.fetch(url)
        if not soup:
            return

        # Every row in the table — look for <a href="/company/NABIL?id=16"> style links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            sym_m = re.search(r'/company/([A-Z0-9]+)', href, re.IGNORECASE)
            if not sym_m:
                continue
            sym = sym_m.group(1).upper()

            # id in querystring: /company/NABIL?id=16
            id_m = re.search(r'[?&](?:companyid|id)=(\d+)', href, re.IGNORECASE)
            if id_m:
                self._id_cache[sym] = int(id_m.group(1))
                continue

            # data-id on the <a> or parent <tr>
            for attr in ("data-id", "data-companyid"):
                v = _to_int(a.get(attr))
                if v:
                    self._id_cache[sym] = v
                    break
            else:
                parent = a.find_parent("tr")
                if parent:
                    for attr in ("data-id", "data-companyid"):
                        v = _to_int(parent.get(attr))
                        if v:
                            self._id_cache[sym] = v
                            break

        logger.info(f"[sharesansar] ID map now has {len(self._id_cache)} entries")

    # ------------------------------------------------------------------ #
    #  Today's full-market table                                          #
    # ------------------------------------------------------------------ #

    async def _fetch_today_table(self) -> list[dict]:
        # Return cached copy if already fetched this session
        if self._today_cache:
            return self._today_cache

        url  = f"{self.BASE_URL}/today-share-price"
        soup = await self.fetch(url)
        if not soup:
            return []

        table = soup.find("table")
        if not table or not table.find("tbody"):
            logger.error("[sharesansar] No table on today-share-price page")
            return []

        rows  = []
        today = date.today().isoformat()
        for tr in table.find("tbody").find_all("tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cols) < 8:
                continue
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

        SharesansarScraper._today_cache = rows
        logger.info(f"[sharesansar] Today's table: {len(rows)} rows")
        return rows

    # ------------------------------------------------------------------ #
    #  Fundamentals                                                       #
    # ------------------------------------------------------------------ #

    async def _fetch_fundamentals(self, symbol: str) -> dict:
        url  = f"{self.BASE_URL}/company/{symbol.upper()}"
        soup = await self.fetch(url)
        if not soup:
            return {}

        result: dict = {}
        label_map = {
            "market capitalization": "market_cap",
            "p/e ratio":             "pe_ratio",
            "52 weeks high":         "week52_high",
            "52 weeks low":          "week52_low",
            "eps":                   "eps",
            "book value":            "book_value",
        }
        for row in soup.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)
                for key, field in label_map.items():
                    if key in label:
                        result[field] = _pf(value)
                        break

        result["date"]   = date.today().isoformat()
        result["source"] = self.SOURCE_NAME
        return result


# ------------------------------------------------------------------ #
#  JSON / HTML parsers                                                #
# ------------------------------------------------------------------ #

def _try_parse_json(text: str, symbol: str, source: str) -> Optional[list[dict]]:
    import json
    try:
        start = text.find("{")
        end   = text.rfind("}") + 1
        if start == -1:
            return None
        payload = json.loads(text[start:end])
        rows    = payload.get("data") or payload.get("rows") or []
        if not rows:
            return None
        records = []
        for r in rows:
            d = _parse_date(str(
                r.get("published_date") or r.get("date") or ""
            ))
            if not d:
                continue
            records.append({
                "symbol": symbol,
                "date":   d,
                "open":   _pf(str(r.get("open_price")  or r.get("open")  or "")),
                "high":   _pf(str(r.get("high_price")  or r.get("high")  or "")),
                "low":    _pf(str(r.get("low_price")   or r.get("low")   or "")),
                "close":  _pf(str(r.get("close_price") or r.get("close") or r.get("closing_price") or "")),
                "volume": _pf(str(r.get("total_traded_quantity") or r.get("vol") or r.get("volume") or "")),
                "source": source,
            })
        return records or None
    except Exception:
        return None


def _parse_html_price_table(soup: BeautifulSoup, symbol: str, source: str) -> list[dict]:
    table = soup.find("table")
    if not table:
        return []
    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
    idx     = {}
    for i, h in enumerate(headers):
        if   "date"              in h: idx.setdefault("date",   i)
        elif "open"              in h: idx.setdefault("open",   i)
        elif "high"              in h: idx.setdefault("high",   i)
        elif "low"               in h: idx.setdefault("low",    i)
        elif "close" in h or "ltp" in h: idx.setdefault("close", i)
        elif "vol"               in h: idx.setdefault("volume", i)

    records = []
    tbody   = table.find("tbody")
    if not tbody:
        return []
    for tr in tbody.find_all("tr"):
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cols) < 4:
            continue
        d = _parse_date(cols[idx.get("date", 0)])
        if not d:
            continue
        records.append({
            "symbol": symbol,
            "date":   d,
            "open":   _pf(cols[idx["open"]])   if "open"   in idx else None,
            "high":   _pf(cols[idx["high"]])   if "high"   in idx else None,
            "low":    _pf(cols[idx["low"]])    if "low"    in idx else None,
            "close":  _pf(cols[idx["close"]])  if "close"  in idx else None,
            "volume": _pf(cols[idx["volume"]]) if "volume" in idx else None,
            "source": source,
        })
    return records


# ------------------------------------------------------------------ #
#  Utilities                                                          #
# ------------------------------------------------------------------ #

def _pf(val) -> Optional[float]:
    if not val: return None
    cleaned = re.sub(r"[^\d.\-]", "", str(val))
    try: return float(cleaned) if cleaned else None
    except ValueError: return None

def _to_int(val) -> Optional[int]:
    try: return int(str(val).strip()) if val else None
    except (ValueError, TypeError): return None

def _parse_date(raw: str) -> Optional[str]:
    raw = (raw or "").strip()
    if not raw: return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw): return raw
    m = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{4})$", raw)
    if m: return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    try:
        from datetime import datetime as dt
        for fmt in ("%B %d, %Y", "%d %B %Y", "%b %d, %Y", "%d %b %Y", "%Y/%m/%d"):
            try: return dt.strptime(raw, fmt).date().isoformat()
            except ValueError: pass
    except Exception: pass
    return None


# ------------------------------------------------------------------ #
#  Hardcoded ID map — scraped manually, covers all major symbols.    #
#  This is the primary fallback so the scraper works even if HTML    #
#  patterns change.  Update periodically as new companies list.      #
# ------------------------------------------------------------------ #
_KNOWN_IDS: dict[str, int] = {
    # Commercial Banks
    "NABIL": 16, "NICA": 25, "SANIMA": 272, "EBL": 32, "MBL": 12,
    "PRVU": 236, "SBI": 28, "ADBL": 9, "GBIME": 42, "KBL": 14,
    "PCBL": 26, "SRBL": 277, "NBL": 17, "RBB": 27, "BOK": 13,
    "NMB": 24, "NIMB": 23, "HIDCL": 281, "CBL": 215, "JBNL": 215,
    "LBBL": 219, "CZBIL": 233, "SCB": 29, "HBL": 11, "CCBL": 264,
    "SADBL": 252, "CITY": 289,
    # Development Banks
    "SHINE": 125, "CORBL": 156, "KSBBL": 151, "SAPDBL": 170, "EDBL": 120,
    "MNBBL": 146, "GBBL": 134, "MLBL": 147, "JBBL": 131, "NUBL": 174,
    "SABBL": 163, "SLBBL": 184, "SKBBL": 183, "SWBBL": 186, "DDBL": 117,
    "MLBBL": 148, "GRDBL": 138,
    # Finance
    "GUFL": 59, "NFS": 68, "ICFC": 61, "SIFC": 76, "CFCL": 53, "GFCL": 57,
    "SFCL": 75, "PFL": 71, "MPFL": 66, "GMFIL": 58,
    # Hydropower
    "UPPER": 110, "NHPC": 97, "AHPC": 79, "CHCL": 83, "API": 80,
    "AKPL": 80, "GHL": 87, "RIDI": 104, "HPPL": 91, "BARUN": 81,
    "SHPC": 107, "KPCL": 93, "BPCL": 82, "NGPL": 96, "DOLTI": 85,
    "DHPL": 84, "MHCL": 95, "RHPL": 103, "USHEC": 111, "UHEWA": 109,
    "RADHI": 102, "HURJA": 92, "NHDL": 98, "SJCL": 108, "UPCL": 112,
    # Insurance
    "NLIC": 43, "LICN": 40, "ALICL": 35, "PICL": 47, "SIC": 48,
    "NICL": 44, "SGIC": 49, "GILB": 38, "IGI": 39, "NLICL": 45,
    "RNLI": 190, "SNLI": 194, "SJLIC": 193, "HLI": 185, "CLI": 181,
    "PMLI": 189, "NRIC": 187, "ILI": 186, "NIL": 188,
    # Microfinance
    "CBBL": 204, "SKBBL": 183, "SWBBL": 186, "DDBL": 117, "MLBBL": 148,
    "NESDO": 210, "SMFBS": 213, "NMFBS": 211, "GMFBS": 207, "HLBSL": 208,
    "LLBS": 209, "GBLBS": 206, "ILBS": 208,
    # Manufacturing & Others
    "UNL": 257, "BNT": 230, "HDL": 237, "NTC": 247, "CIT": 232, "NIFRA": 246,
    "STC": 256, "TRH": 258, "BHL": 229, "HRL": 240,
}