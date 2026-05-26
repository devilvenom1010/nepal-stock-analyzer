# backend/app/scheduler.py
#
# APScheduler job definitions:
#   - run_daily_scrape()      → runs every evening at 4:30 PM Nepal time
#   - run_historical_scrape() → called once on first setup for 30-day backfill
#
# Both can also be triggered manually via POST /api/v1/scrape/trigger
# and POST /api/v1/scrape/historical from the React frontend.

import logging
from datetime import datetime, date
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

from .database import SessionLocal
from .models import StockPrice, StockFundamentals, ScrapeLog
from .scrapers.nepsealpha  import NepseAlphaScraper
from .scrapers.sharesansar import SharesansarScraper
from .scrapers.merolagani  import MerolaganiScraper

logger    = logging.getLogger(__name__)
scheduler = AsyncIOScheduler(timezone="Asia/Kathmandu")


# ─────────────────────────────────────────────────────────────────
#  Main daily job
# ─────────────────────────────────────────────────────────────────

async def run_daily_scrape() -> dict:
    """
    Scrapes today's price and fundamentals from all three sources.
    Priority for conflict resolution: nepsealpha > sharesansar > merolagani.
    Runs automatically every day at 4:30 PM Nepal time (market closes at 3 PM).
    Can also be triggered manually from the frontend.
    """
    logger.info("=== Daily scrape started ===")
    db       = SessionLocal()
    started  = datetime.now()
    total    = 0
    errors   = []

    # Source priority order: first source to write a (symbol, date) wins.
    # Subsequent sources skip the row due to the UNIQUE constraint.
    scrapers = [
        NepseAlphaScraper(),    # Best real-time data — goes first
        SharesansarScraper(),   # Good fundamentals data — goes second
        MerolaganiScraper(),    # Additional fundamentals — fills gaps
    ]

    for scraper in scrapers:
        src_name = scraper.SOURCE_NAME
        try:
            symbols = await scraper.scrape_all_symbols()
            count   = 0

            for symbol in symbols:
                try:
                    data = await scraper.scrape_daily(symbol)
                    if not data:
                        continue

                    _upsert_price(db, data)
                    _upsert_fundamentals(db, data)
                    count += 1

                    # Commit in batches of 50 to avoid giant transactions
                    if count % 50 == 0:
                        db.commit()
                        logger.debug(f"[{src_name}] {count} rows committed so far")

                except Exception as sym_err:
                    logger.warning(f"[{src_name}] Error on {symbol}: {sym_err}")

            db.commit()
            total += count
            _log_run(db, src_name, "success", count, started)
            logger.info(f"[{src_name}] Done — {count} records")

        except Exception as e:
            db.rollback()
            errors.append(f"{src_name}: {e}")
            _log_run(db, src_name, "failed", 0, started, str(e))
            logger.error(f"[{src_name}] Scraper failed: {e}")

        finally:
            await scraper.close()

    db.close()
    status = "success" if not errors else "partial" if total > 0 else "failed"
    logger.info(f"=== Daily scrape done — {total} records, status={status} ===")
    return {"status": status, "records_saved": total, "errors": errors}


# ─────────────────────────────────────────────────────────────────
#  Historical backfill job (run once on first setup)
# ─────────────────────────────────────────────────────────────────

async def run_historical_scrape(days: int = 30) -> dict:
    """
    Backfill the last `days` calendar days of OHLCV data.
    Uses NepseAlpha as the primary source (best history API).
    Sharesansar is used as a fallback for symbols NepseAlpha misses.
    """
    logger.info(f"=== Historical scrape started ({days} days) ===")
    db      = SessionLocal()
    started = datetime.now()
    total   = 0

    # Phase 1 — NepseAlpha (has a clean JSON history API)
    scraper = NepseAlphaScraper()
    try:
        symbols = await scraper.scrape_all_symbols()
        for symbol in symbols:
            try:
                records = await scraper.scrape_historical(symbol, days=days)
                for r in records:
                    _upsert_price(db, r)
                    total += 1
                if total % 200 == 0 and total > 0:
                    db.commit()
            except Exception as e:
                logger.warning(f"[nepsealpha historical] {symbol}: {e}")
        db.commit()
        _log_run(db, "nepsealpha_historical", "success", total, started)
    except Exception as e:
        db.rollback()
        _log_run(db, "nepsealpha_historical", "failed", 0, started, str(e))
        logger.error(f"NepseAlpha historical scrape failed: {e}")
    finally:
        await scraper.close()

    # Phase 2 — Merolagani historical (fills any gaps)
    m_scraper = MerolaganiScraper()
    m_total   = 0
    try:
        m_symbols = await m_scraper.scrape_all_symbols()
        for symbol in m_symbols:
            try:
                records = await m_scraper.scrape_historical(symbol, days=days)
                for r in records:
                    # Only insert if NepseAlpha/Merolagani didn't already cover this day
                    if not _price_exists(db, r["symbol"], r["date"]):
                        _upsert_price(db, r)
                        m_total += 1
            except Exception as e:
                logger.warning(f"[merolagani historical] {symbol}: {e}")
        db.commit()
        _log_run(db, "merolagani_historical", "success", m_total, started)
    except Exception as e:
        db.rollback()
        _log_run(db, "merolagani_historical", "failed", 0, started, str(e))
    finally:
        await m_scraper.close()

    # Phase 3 — Sharesansar historical table (fills any gaps)
    ss_scraper = SharesansarScraper()
    ss_total   = 0
    try:
        ss_symbols = await ss_scraper.scrape_all_symbols()
        for symbol in ss_symbols:
            try:
                records = await ss_scraper.scrape_historical(symbol, days=days)
                for r in records:
                    # Only insert if not already covered
                    if not _price_exists(db, r["symbol"], r["date"]):
                        _upsert_price(db, r)
                        ss_total += 1
            except Exception as e:
                logger.warning(f"[sharesansar historical] {symbol}: {e}")
        db.commit()
        _log_run(db, "sharesansar_historical", "success", ss_total, started)
    except Exception as e:
        db.rollback()
        _log_run(db, "sharesansar_historical", "failed", 0, started, str(e))
    finally:
        await ss_scraper.close()

    db.close()
    grand_total = total + m_total + ss_total
    logger.info(f"=== Historical scrape complete — {grand_total} total records ===")
    return {"status": "done", "records_saved": grand_total}


# ─────────────────────────────────────────────────────────────────
#  DB helpers
# ─────────────────────────────────────────────────────────────────

def _upsert_price(db: Session, data: dict) -> None:
    """
    Insert a price row. Skip silently if (symbol, trade_date) already exists.
    We use try/except rather than ON CONFLICT because MSSQL's merge syntax
    is verbose — and for a daily scraper, duplicates are rare enough that
    catching the IntegrityError is cleaner.
    """
    close = data.get("close") or data.get("close_price")
    if not close:
        return

    # Normalise key names (scrapers may use 'close' or 'close_price')
    try:
        row = StockPrice(
            symbol      = data["symbol"].upper(),
            trade_date  = _to_date(data["date"]),
            open_price  = data.get("open")  or data.get("open_price"),
            high_price  = data.get("high")  or data.get("high_price"),
            low_price   = data.get("low")   or data.get("low_price"),
            close_price = close,
            volume      = _to_int(data.get("volume")),
            prev_close  = data.get("prev_close"),
            pct_change  = data.get("pct_change"),
            source      = data.get("source"),
        )
        db.add(row)
        db.flush()  # let the DB check the unique constraint immediately
    except Exception:
        db.rollback()   # duplicate or bad data — silently skip


def _upsert_fundamentals(db: Session, data: dict) -> None:
    """Insert fundamentals row if any fundamental fields are present."""
    fund_keys = ["market_cap", "pe_ratio", "eps", "book_value", "week52_high", "week52_low"]
    if not any(data.get(k) for k in fund_keys):
        return
    try:
        row = StockFundamentals(
            symbol      = data["symbol"].upper(),
            trade_date  = _to_date(data["date"]),
            market_cap  = data.get("market_cap"),
            pe_ratio    = data.get("pe_ratio"),
            eps         = data.get("eps"),
            book_value  = data.get("book_value"),
            week52_high = data.get("week52_high"),
            week52_low  = data.get("week52_low"),
            source      = data.get("source"),
        )
        db.add(row)
        db.flush()
    except Exception:
        db.rollback()


def _price_exists(db: Session, symbol: str, date_str: str) -> bool:
    """Return True if we already have a price row for this symbol + date."""
    d = _to_date(date_str)
    return db.query(StockPrice).filter(
        StockPrice.symbol     == symbol.upper(),
        StockPrice.trade_date == d,
    ).first() is not None


def _log_run(db: Session, source: str, status: str, records: int,
             started: datetime, error: Optional[str] = None) -> None:
    log = ScrapeLog(
        source        = source,
        status        = status,
        records_saved = records,
        error_message = error,
        started_at    = started,
    )
    db.add(log)
    db.commit()


def _to_date(val):
    """Accept a date object, ISO string, or datetime — return a date."""
    if isinstance(val, date):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, str):
        return date.fromisoformat(val[:10])
    return val


def _to_int(val) -> Optional[int]:
    try:
        return int(float(val)) if val is not None else None
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────
#  Scheduler registration
# ─────────────────────────────────────────────────────────────────

def start_scheduler():
    """
    Register the daily scrape job.
    Nepal Stock Exchange closes at 3:00 PM NPT.
    We run at 4:30 PM to give sites time to publish end-of-day data.
    """
    scheduler.add_job(
        run_daily_scrape,
        trigger=CronTrigger(hour=16, minute=30, timezone="Asia/Kathmandu"),
        id="daily_scrape",
        replace_existing=True,
        misfire_grace_time=3600,   # run even if server was down for up to 1 hour
    )
    scheduler.start()
    logger.info("Scheduler started — daily scrape at 16:30 Nepal time")