# backend/app/scrapers/base.py
import logging
from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

class BaseScraper(ABC):
    SOURCE_NAME: str = "base"

    def __init__(self):
        self.client = httpx.AsyncClient(
            headers=HEADERS,
            timeout=30.0,
            follow_redirects=True,
        )

    async def fetch(self, url: str) -> Optional[BeautifulSoup]:
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.text, "lxml")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"[{self.SOURCE_NAME}] Page not found (404) for {url}")
            else:
                logger.error(f"[{self.SOURCE_NAME}] HTTP error {e.response.status_code} fetching {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"[{self.SOURCE_NAME}] Failed to fetch {url}: {e}")
            return None

    async def close(self):
        await self.client.aclose()

    @abstractmethod
    async def scrape_historical(self, symbol: str, days: int = 30) -> list[dict]: ...

    @abstractmethod
    async def scrape_all_symbols(self) -> list[str]: ...

    @abstractmethod
    async def scrape_daily(self, symbol: str) -> Optional[dict]: ...

    def date_range(self, days: int) -> tuple[date, date]:
        end   = date.today()
        start = end - timedelta(days=days)
        return start, end