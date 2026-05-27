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
        """Fetch the actual sector name for a symbol from its details page."""
        async with sem:
            url = f"{self.BASE_URL}/CompanyDetail.aspx?symbol={symbol}"
            try:
                response = await self.client.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "lxml")
                
                sector = None
                th = soup.find(lambda tag: tag.name == "th" and "Sector" in tag.get_text())
                if th:
                    td = th.find_next_sibling("td")
                    if td:
                        sector = td.get_text(strip=True)
                return symbol, sector
            except Exception as e:
                logger.warning(f"[merolagani] Failed to fetch sector for {symbol}: {e}")
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
            sem = asyncio.Semaphore(15)  # Safe limit to avoid server-side rate limits
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
        start, end = self.date_range(days)
        url = (
            f"{self.BASE_URL}/handlers/TechnicalChartHandler.ashx"
            f"?type=full&symbol={symbol}"
            f"&from={start.strftime('%m/%d/%Y')}"
            f"&to={end.strftime('%m/%d/%Y')}"
        )
        soup = await self.fetch(url)
        if not soup:
            return []

        records = []
        try:
            import json
            data = json.loads(soup.get_text())
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
        except Exception as e:
            logger.error(f"[merolagani] Parse error for {symbol}: {e}")
        return records

    async def scrape_daily(self, symbol: str) -> Optional[dict]:
        """Scrape today's snapshot for a single symbol."""
        # 1. Try to read from the cached StockQuote table first (fast, complete, handles pct_change)
        if not self._today_cache:
            try:
                await self.scrape_all_stocks(resolve_sectors=False)
            except Exception as e:
                logger.warning(f"[merolagani] Failed to populate daily cache: {e}")

        for s in self._today_cache:
            if s["symbol"].upper() == symbol.upper():
                return s

        # 2. Fallback: individual company detail page
        url = f"{self.BASE_URL}/CompanyDetail.aspx?symbol={symbol}"
        soup = await self.fetch(url)
        if not soup:
            return None

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

            stats = soup.find("div", {"class": "company-stats"})
            if stats:
                rows = {
                    r.find_all("td")[0].text.strip(): r.find_all("td")[1].text.strip()
                    for r in stats.find_all("tr")
                    if len(r.find_all("td")) >= 2
                }
                result.update({
                    "open":        _parse_float(rows.get("Open")),
                    "high":        _parse_float(rows.get("High")),
                    "low":         _parse_float(rows.get("Low")),
                    "volume":      _parse_float(rows.get("Volume")),
                    "market_cap":  _parse_float(rows.get("Market Capitalization")),
                    "pe_ratio":    _parse_float(rows.get("P/E Ratio")),
                    "week52_high": _parse_float(rows.get("52 Weeks High")),
                    "week52_low":  _parse_float(rows.get("52 Weeks Low")),
                    "eps":         _parse_float(rows.get("EPS")),
                    "book_value":  _parse_float(rows.get("Book Value")),
                })
            return result
        except Exception as e:
            logger.error(f"[merolagani] Daily scrape fallback error for {symbol}: {e}")
            return None


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