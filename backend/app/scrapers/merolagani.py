# backend/app/scrapers/merolagani.py
import asyncio
import logging
import re
from datetime import date
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from .base import BaseScraper

logger = logging.getLogger(__name__)


class MerolaganiScraper(BaseScraper):
    SOURCE_NAME = "merolagani"
    BASE_URL    = "https://merolagani.com"

    # ── ASP.NET pagination helpers ───────────────────────────────────

    @staticmethod
    def _extract_aspnet_fields(soup: BeautifulSoup) -> dict:
        """Extract hidden ASP.NET form fields needed for postback."""
        fields = {}
        for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
            tag = soup.find("input", {"name": name})
            if tag:
                fields[name] = tag.get("value", "")
        return fields

    @staticmethod
    def _get_total_pages(soup: BeautifulSoup) -> int:
        """Parse 'Total pages: N' from the pager control."""
        pager = soup.find("span", id=re.compile(r"PagerControl.*litRecords"))
        if pager:
            m = re.search(r"Total pages:\s*(\d+)", pager.get_text())
            if m:
                return int(m.group(1))
        return 1

    async def _fetch_page_post(self, page_num: int, asp_fields: dict) -> Optional[BeautifulSoup]:
        """POST the ASP.NET form to fetch a specific page of results."""
        url = f"{self.BASE_URL}/StockQuote.aspx"
        form_data = {
            **asp_fields,
            "ctl00$ContentPlaceHolder1$PagerControl1$hdnPCID": "PC1",
            "ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage": str(page_num),
            "ctl00$ContentPlaceHolder1$PagerControl1$btnPaging": "",
        }
        try:
            response = await self.client.post(
                url, data=form_data,
                headers={
                    **self.client.headers,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            response.raise_for_status()
            return BeautifulSoup(response.text, "lxml")
        except Exception as e:
            logger.error(f"[merolagani] Failed to POST page {page_num}: {e}")
            return None

    async def _fetch_company_sector(self, symbol: str, sem: asyncio.Semaphore) -> tuple[str, Optional[str]]:
        """Fetch the actual sector name for a symbol from its details page.
        
        Retries up to 3 times with exponential backoff to handle transient
        server-side rate limits or connection resets.
        """
        url = f"{self.BASE_URL}/CompanyDetail.aspx?symbol={symbol}"
        headers = {
            **dict(self.client.headers),
            "Referer": f"{self.BASE_URL}/StockQuote.aspx",
        }
        max_retries = 3

        async with sem:
            for attempt in range(1, max_retries + 1):
                try:
                    response = await self.client.get(url, headers=headers)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "lxml")

                    sector = None

                    # Strategy 1: <th> sibling containing "Sector"
                    th = soup.find(
                        lambda tag: tag.name == "th"
                        and "Sector" in tag.get_text(strip=True)
                    )
                    if th:
                        td = th.find_next_sibling("td")
                        if td:
                            sector = td.get_text(strip=True)

                    # Strategy 2: table row where first cell is "Sector"
                    if not sector:
                        for row in soup.find_all("tr"):
                            cells = row.find_all(["th", "td"])
                            if len(cells) >= 2 and "sector" in cells[0].get_text(strip=True).lower():
                                sector = cells[1].get_text(strip=True)
                                break

                    if sector:
                        logger.debug(f"[merolagani] Sector for {symbol}: {sector}")
                    else:
                        logger.warning(
                            f"[merolagani] Sector field not found in page for {symbol} "
                            f"(page title: {soup.title.string if soup.title else 'N/A'})"
                        )

                    return symbol, sector

                except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError) as e:
                    logger.warning(
                        f"[merolagani] Attempt {attempt}/{max_retries} failed for {symbol} "
                        f"({type(e).__name__}: {e}). "
                        + ("Retrying..." if attempt < max_retries else "Giving up.")
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(2 ** attempt)  # 2s, 4s backoff
                    else:
                        return symbol, None

                except httpx.HTTPStatusError as e:
                    logger.warning(
                        f"[merolagani] HTTP {e.response.status_code} fetching sector for {symbol}: {e}"
                    )
                    return symbol, None

                except Exception as e:
                    logger.warning(
                        f"[merolagani] Unexpected error fetching sector for {symbol} "
                        f"({type(e).__name__}: {e})"
                    )
                    return symbol, None

        return symbol, None

    # Class-level cache shared across all instances
    _today_cache: list[dict] = []

    # ── Main scraper ─────────────────────────────────────────────────

    async def scrape_all_stocks(self, resolve_sectors: bool = True) -> list[dict]:
        """
        Fetch the full list of NEPSE-listed companies across all pages.
        Returns dicts with keys: symbol, company, sector, and daily price data.
        Used to populate/sync the stocks master table and cache daily quotes.
        """
        # ── Page 1 (GET) ─────────────────────────────────────────────
        soup = await self.fetch(f"{self.BASE_URL}/StockQuote.aspx")
        if not soup:
            logger.error("[merolagani] Failed to fetch listing page")
            return []

        # Detect column layout once from headers
        table = soup.find("table", {"class": "table"})
        if not table:
            logger.error("[merolagani] No table found on listing page")
            return []

        header_row = table.find("tr")
        raw_headers = []
        if header_row:
            raw_headers = [
                th.get_text(strip=True).lower()
                for th in header_row.find_all(["th", "td"])
            ]

        idx_map = {
            "symbol":     _find_col(raw_headers, ["symbol", "ticker", "scrip"]),
            "close":      _find_col(raw_headers, ["ltp", "close", "last trade"]),
            "pct_change": _find_col(raw_headers, ["% change", "pct change", "percentage"]),
            "high":       _find_col(raw_headers, ["high"]),
            "low":        _find_col(raw_headers, ["low"]),
            "open":       _find_col(raw_headers, ["open"]),
            "volume":     _find_col(raw_headers, ["qty", "quantity", "volume"]),
        }

        logger.debug(
            f"[merolagani] Detected column map: {idx_map} "
            f"(headers: {raw_headers})"
        )

        # Collect rows from page 1
        stocks = _parse_stock_rows(table, idx_map)

        # ── Pages 2..N (POST) ────────────────────────────────────────
        total_pages = self._get_total_pages(soup)
        logger.info(f"[merolagani] Total pages detected: {total_pages}")

        if total_pages > 1:
            asp_fields = self._extract_aspnet_fields(soup)

            for page in range(2, total_pages + 1):
                page_soup = await self._fetch_page_post(page, asp_fields)
                if not page_soup:
                    logger.warning(f"[merolagani] Skipping page {page} (fetch failed)")
                    continue

                page_table = page_soup.find("table", {"class": "table"})
                if page_table:
                    page_stocks = _parse_stock_rows(page_table, idx_map)
                    stocks.extend(page_stocks)
                    logger.debug(
                        f"[merolagani] Page {page}: {len(page_stocks)} stocks"
                    )

                # Update ASP.NET fields for next postback (ViewState changes per page)
                asp_fields = self._extract_aspnet_fields(page_soup)

        # Cache the daily price results so daily scrape can load them instantly
        MerolaganiScraper._today_cache = stocks

        # ── Resolve Sectors Concurrently (Only if requested) ──────────
        if resolve_sectors and stocks:
            logger.info(f"[merolagani] Resolving sectors for {len(stocks)} stocks...")
            sem = asyncio.Semaphore(5)  # Conservative limit to avoid server-side rate limits
            tasks = [self._fetch_company_sector(s["symbol"], sem) for s in stocks]
            sector_results = await asyncio.gather(*tasks)
            
            # Map sectors back to stocks
            sector_map = dict(sector_results)
            for s in stocks:
                s["sector"] = sector_map.get(s["symbol"])

        logger.info(f"[merolagani] scrape_all_stocks: {len(stocks)} stocks found")
        return stocks

    async def scrape_all_symbols(self) -> list[str]:
        """Fetch the full list of NEPSE symbols from Merolagani."""
        stocks = await self.scrape_all_stocks(resolve_sectors=False)
        if stocks:
            return [s["symbol"] for s in stocks]

        # Fallback: parse symbols only (handles layout changes)
        soup = await self.fetch(f"{self.BASE_URL}/listingcompany.aspx")
        if not soup:
            return []
        symbols = []
        table = soup.find("table", {"class": "table"})
        if table:
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if cols:
                    symbol = cols[0].get_text(strip=True)
                    if symbol:
                        symbols.append(symbol)
        logger.info(f"[merolagani] Found {len(symbols)} symbols")
        return symbols

    async def scrape_historical(self, symbol: str, days: int = 30) -> list[dict]:
        """Scrape historical OHLCV for a symbol."""
        from datetime import datetime
        import time
        
        start, end = self.date_range(days)
        start_dt = datetime.combine(start, datetime.min.time())
        end_dt = datetime.combine(end, datetime.max.time())
        start_ts = int(time.mktime(start_dt.timetuple()))
        end_ts = int(time.mktime(end_dt.timetuple()))
        
        url = (
            f"{self.BASE_URL}/handlers/TechnicalChartHandler.ashx"
            f"?type=get_advanced_chart&symbol={symbol.upper()}&resolution=1D"
            f"&rangeStartDate={start_ts}&rangeEndDate={end_ts}"
            f"&isAdjust=1&currencyCode=NPR"
        )
        soup = await self.fetch(url)
        if not soup:
            return []

        records = []
        try:
            import json
            text = soup.get_text().strip()
            # Safety check: if response is not valid JSON, log warning and return gracefully
            if not (text.startswith("[") or text.startswith("{")):
                logger.debug(f"[merolagani] No valid chart data for {symbol} (response is not JSON)")
                return []

            data = json.loads(text)
            if isinstance(data, dict) and "t" in data and "c" in data:
                timestamps = data.get("t") or []
                opens = data.get("o") or []
                highs = data.get("h") or []
                lows = data.get("l") or []
                closes = data.get("c") or []
                volumes = data.get("v") or []
                
                for i in range(len(timestamps)):
                    dt_str = datetime.fromtimestamp(timestamps[i]).date().isoformat()
                    records.append({
                        "symbol": symbol.upper(),
                        "date":   dt_str,
                        "open":   opens[i] if i < len(opens) else None,
                        "high":   highs[i] if i < len(highs) else None,
                        "low":    lows[i] if i < len(lows) else None,
                        "close":  closes[i] if i < len(closes) else None,
                        "volume": volumes[i] if i < len(volumes) else None,
                        "source": self.SOURCE_NAME,
                    })
            elif isinstance(data, list):
                # Legacy array format
                for item in data:
                    records.append({
                        "symbol": symbol,
                        "date":   item.get("d"),
                        "open":   item.get("o"),
                        "high":   item.get("h"),
                        "low":    item.get("l"),
                        "close":  item.get("c"),
                        "volume": item.get("v"),
                        "source": self.SOURCE_NAME,
                    })
            else:
                logger.debug(f"[merolagani] No valid chart data for {symbol} (response status: {data.get('s')})")
        except Exception as e:
            logger.error(f"[merolagani] Parse error for {symbol}: {e}")
        return records

    async def scrape_daily(self, symbol: str) -> Optional[dict]:
        """Scrape today's snapshot for a single symbol."""
        # 1. Try to read from the cached StockQuote table first
        cached_stock = None
        if not self._today_cache:
            try:
                await self.scrape_all_stocks(resolve_sectors=False)
            except Exception as e:
                logger.warning(f"[merolagani] Failed to populate daily cache: {e}")

        for s in self._today_cache:
            if s["symbol"].upper() == symbol.upper():
                cached_stock = s.copy()
                break

        # 2. Scrape individual company detail page to extract fundamentals
        url = f"{self.BASE_URL}/CompanyDetail.aspx?symbol={symbol}"
        soup = await self.fetch(url)
        if not soup:
            return cached_stock

        try:
            # Close price
            price_box = soup.find("div", {"class": "price-box"})
            close = _parse_float(
                price_box.find("span", {"class": "stat-value"}).text
                if price_box else None
            )
            
            # Pct change
            pct_change = None
            th_pct = soup.find(lambda tag: tag.name == "th" and "% Change" in tag.get_text())
            if th_pct:
                td_pct = th_pct.find_next_sibling("td")
                if td_pct:
                    pct_change = _parse_float(td_pct.get_text(strip=True))

            result = {
                "symbol":     symbol,
                "date":       date.today().isoformat(),
                "close":      close,
                "pct_change": pct_change,
                "source":     self.SOURCE_NAME,
            }

            table = soup.find("table", {"class": "table-zeromargin"})
            if table:
                rows = {}
                for tbody in table.find_all("tbody"):
                    cls = tbody.get("class") or []
                    if "panel" in cls:
                        tr = tbody.find("tr")
                        if tr:
                            th = tr.find("th")
                            td = tr.find("td")
                            if th and td:
                                label = th.get_text(strip=True)
                                val = td.get_text(strip=True)
                                rows[label] = val
                
                # Dynamic parsing helpers
                def clean_num(val_str):
                    if not val_str: return None
                    m = re.match(r"^\s*([Rs.\d,\-\s]+)", str(val_str))
                    if m:
                        cleaned = m.group(1).replace(",", "").replace("Rs.", "").strip()
                        try:
                            return float(cleaned) if cleaned else None
                        except ValueError:
                            return None
                    return None

                val_52 = rows.get("52 Weeks High - Low")
                week52_high = None
                week52_low = None
                if val_52 and "-" in val_52:
                    parts = val_52.split("-")
                    if len(parts) >= 2:
                        week52_high = clean_num(parts[0])
                        week52_low = clean_num(parts[1])

                result.update({
                    "open":        clean_num(rows.get("Open")),
                    "high":        clean_num(rows.get("High")),
                    "low":         clean_num(rows.get("Low")),
                    "volume":      clean_num(rows.get("Volume")),
                    "market_cap":  clean_num(rows.get("Market Capitalization")),
                    "pe_ratio":    clean_num(rows.get("P/E Ratio")),
                    "week52_high": week52_high,
                    "week52_low":  week52_low,
                    "eps":         clean_num(rows.get("EPS")),
                    "book_value":  clean_num(rows.get("Book Value")),
                })

            if cached_stock:
                # Merge cached daily price info with scraped fundamentals
                for k in ["market_cap", "pe_ratio", "eps", "book_value", "week52_high", "week52_low"]:
                    if k in result:
                        cached_stock[k] = result[k]
                return cached_stock
            
            return result
        except Exception as e:
            logger.error(f"[merolagani] Daily scrape fallback error for {symbol}: {e}")
            return cached_stock


# ─────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────

def _parse_stock_rows(table, idx_map: dict) -> list[dict]:
    """Extract stock dicts from all <tr> rows in a table (skips header row)."""
    idx_symbol = idx_map.get("symbol", 1)
    idx_close  = idx_map.get("close", 2)
    idx_pct    = idx_map.get("pct_change", 3)
    idx_high   = idx_map.get("high", 4)
    idx_low    = idx_map.get("low", 5)
    idx_open   = idx_map.get("open", 6)
    idx_vol    = idx_map.get("volume", 7)

    stocks: list[dict] = []
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if not cols:
            continue

        symbol = _col_text(cols, idx_symbol).upper()
        if not symbol or symbol == "-":
            continue

        company = None
        try:
            link = cols[idx_symbol].find("a")
            if link and link.get("title"):
                company = link["title"].strip()
        except (IndexError, AttributeError):
            pass

        stocks.append({
            "symbol":     symbol,
            "company":    company,
            "sector":     None,
            "close":      _parse_float(_col_text(cols, idx_close)),
            "pct_change": _parse_float(_col_text(cols, idx_pct)),
            "open":       _parse_float(_col_text(cols, idx_open)),
            "high":       _parse_float(_col_text(cols, idx_high)),
            "low":        _parse_float(_col_text(cols, idx_low)),
            "volume":     _parse_float(_col_text(cols, idx_vol)),
            "date":       date.today().isoformat(),
            "source":     "merolagani",
        })
    return stocks


def _find_col(headers: list[str], keywords: list[str]) -> int:
    """Return the index of the first header that contains any of the keywords."""
    for i, h in enumerate(headers):
        for kw in keywords:
            if kw in h:
                return i
    return 0   # safe default — symbol is almost always first


def _col_text(cols: list, idx: int) -> str:
    """Safely get stripped text from a column list by index."""
    try:
        return cols[idx].get_text(strip=True)
    except (IndexError, AttributeError):
        return ""


def _parse_float(val: Optional[str]) -> Optional[float]:
    """Safely convert a string like '1,234.56' to float."""
    if not val:
        return None
    try:
        return float(val.replace(",", "").replace("Rs.", "").strip())
    except ValueError:
        return None