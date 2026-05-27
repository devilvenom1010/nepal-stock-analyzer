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
    MEROLAGANI_LISTING = "https://merolagani.com/StockQuote.aspx"

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
    "ACLBSL","ADBL","ADBLD83","AHL","AHPC","AKJCL","AKPL","ALBSL","ALICL","ANLB","API","AVYAN","BANDIPUR","BARUN","BBC","BEDC","BFC","BGWT","BHCL"
    ,"BHDC","BHL","BHPL","BJHL","BNHC","BNL","BOKD86","BPCL","BUNGAL","C30MF","CBBL","CBLD88","CCBD88","CFCL","CGH","CHCL","CHDC","CHL","CIT","CITY"
    ,"CIZBD90","CKHL","CLI","CMF2","CORBL","CREST","CSY","CYCL","CZBIL","DDBL","DHEL","DHPL","DLBS","DOLTI","DORDI","EBL","EBLD85","EBLD86","EBLD91"
    ,"EDBL","EHPL","ENL","FMDBL","FOWAD","GBBD85","GBBL","GBILD84/85","GBIME","GBIMESY2","GBLBS","GCIL","GFCL","GHL","GIBF1","GILB","GLBSL","GLH"
    ,"GMFBS","GMFIL","GMLI","GRDBL","GSY","GUFL","GVL","H8020","HATHY","HBL","HBLD86","HDHPC","HDL","HEI","HEIP","HFIN","HHL","HIDCL","HIDCLP"
    ,"HIMSTAR","HLBSL","HLI","HLICF","HPPL","HRL","HURJA","ICFC","ICFCD88","ICFCD89","IGI","IHL","ILBS","ILI","JBBL","JBBLPO","JBLB","JFL","JHAPA"
    ,"JOSHI","JSLBB","KBL","KBLD86","KBSH","KDBY","KDL","KEF","KHPL","KKHC","KMCDB","KPCL","KSBBL","KSY","LBBL","LBBLD89","LBLD86","LBLD88","LEC"
    ,"LICN","LLBS","LSL","LUK","LVF2","MABEL","MAKAR","MANDU","MATRI","MBJC","MBL","MBLD2085","MBLEF","MCHL","MDB","MEHL","MEL","MEN","MERO","MFIL"
    ,"MFLD85","MHCL","MHL","MHNL","MKCL","MKHC","MKHL","MKJC","MLBBL","MLBL","MLBS","MLBSL","MMF1","MMKJL","MNBBL","MND84/85","MNMF1","MPFL","MSHL"
    ,"MSLB","NABBC","NABIL","NABILD2089","NABILD87","NADEP","NBBD2085","NBF2","NBF3","NBL","NBLD85","NBLD87","NESDO","NFS","NGPL","NHDL","NHPC","NIBLGF"
    ,"NIBLSTF","NIBSF2","NICA","NICAD2091","NICBF","NICFC","NICGF2","NICL","NICLBSL","NICSF","NIFRA","NIFRAGED","NIL","NIMB","NIMBD90","NLG","NLIC","NLICL"
    ,"NMB","NMB50","NMBD2085","NMBHF2","NMBMF","NMFBS","NMIC","NMLBBL","NRIC","NRM","NRN","NSIF2","NSY","NTC","NUBL","NWCL","NYADI","OHL","OMPL","PBD84","PBD85"
    ,"PBD88","PBLD87","PCBL","PCIL","PFL","PHCL","PMHPL","PMLI","PPCL","PPL","PRIN","PROFL","PRSF","PRVU","PSF","PURE","RADHI","RAWA","RBBF40","RBCL","RBCLPO"
    ,"RFPL","RHGCL","RHPL","RIDI","RLEL","RLFL","RMF1","RMF2","RNLI","RSDC","RSML","RSY","RURU","SABBL","SADBL","SAGAR","SAGF","SAHAS","SAIL","SALICO","SANIMA"
    ,"SANVI","SAPDBL","SARBTM","SBCF","SBD89","SBI","SBIBD86","SBID89","SBL","SBLD2091","SBLD89","SCB","SEF","SFCL","SFEF","SGHC","SGIC","SHEL","SHINE","SHIVM"
    ,"SHL","SHLB","SHPC","SICL","SIFC","SIGS2","SIGS3","SIKLES","SINDU","SIPD","SJCL","SJLIC","SKBBL","SKHEL","SKHL","SLBBL","SLBSL","SLCF","SMATA","SMB","SMFBS"
    ,"SMH","SMHL","SMJC","SMPDA","SNLI","SOHL","SONA","SPC","SPDL","SPHL","SPIL","SPL","SRLI","SSHL","STC","SWASTIK","SWBBL","SWMF","SYPNL","TAMOR","TPC"
    ,"TRH","TSHL","TTL","TVCL","UAIL","UHEWA","ULBSL","ULHC","UMHL","UMRH","UNHPL","UNL","UNLB","UPCL","UPPER","USHEC","USHL","USLB","VLBS","VLUCL"
    ,"WNLB",
]