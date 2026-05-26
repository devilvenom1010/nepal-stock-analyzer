# backend/app/scrapers/merolagani.py
import logging
from datetime import date
from typing import Optional
from .base import BaseScraper

logger = logging.getLogger(__name__)

class MerolaganiScraper(BaseScraper):
    SOURCE_NAME = "merolagani"
    BASE_URL    = "https://merolagani.com"

    async def scrape_all_symbols(self) -> list[str]:
        """Fetch the full list of NEPSE symbols from Merolagani."""
        soup = await self.fetch(f"{self.BASE_URL}/listingcompany.aspx")
        if not soup:
            return []

        symbols = []
        # Merolagani lists all companies in a table
        table = soup.find("table", {"class": "table"})
        if table:
            for row in table.find_all("tr")[1:]:   # skip header
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
        # Parse the JSON-like response Merolagani returns
        try:
            import json
            data = json.loads(soup.get_text())
            for item in data:
                records.append({
                    "symbol":  symbol,
                    "date":    item.get("d"),      # date string
                    "open":    item.get("o"),
                    "high":    item.get("h"),
                    "low":     item.get("l"),
                    "close":   item.get("c"),
                    "volume":  item.get("v"),
                    "source":  self.SOURCE_NAME,
                })
        except Exception as e:
            logger.error(f"[merolagani] Parse error for {symbol}: {e}")
        return records

    async def scrape_daily(self, symbol: str) -> Optional[dict]:
        """Scrape today's snapshot for a single symbol."""
        url = f"{self.BASE_URL}/company-detail/{symbol}"
        soup = await self.fetch(url)
        if not soup:
            return None

        try:
            price_box = soup.find("div", {"class": "price-box"})
            stats = soup.find("div", {"class": "company-stats"})

            close = _parse_float(price_box.find("span", {"class": "stat-value"}).text if price_box else None)
            result = {
                "symbol": symbol,
                "date":   date.today().isoformat(),
                "close":  close,
                "source": self.SOURCE_NAME,
            }
            if stats:
                rows = {r.find_all("td")[0].text.strip(): r.find_all("td")[1].text.strip()
                        for r in stats.find_all("tr") if len(r.find_all("td")) >= 2}
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
            logger.error(f"[merolagani] Daily scrape error for {symbol}: {e}")
            return None


def _parse_float(val: Optional[str]) -> Optional[float]:
    """Safely convert a string like '1,234.56' to float."""
    if not val:
        return None
    try:
        return float(val.replace(",", "").replace("Rs.", "").strip())
    except ValueError:
        return None