# backend/app/scheduler.py
import asyncio
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
#  Daily scrape (runs at 4:30 PM Nepal time every trading day)
# ─────────────────────────────────────────────────────────────────

async def run_daily_scrape() -> dict:
    logger.info("=== Daily scrape started ===")
    db      = SessionLocal()
    started = datetime.now()
    total   = 0
    errors  = []

    # Priority: nepsealpha first (best real-time data), others fill gaps
    scrapers = [
        NepseAlphaScraper(),
        SharesansarScraper(),
        MerolaganiScraper(),
    ]

    for scraper in scrapers:
        src = scraper.SOURCE_NAME
        try:
            symbols = await scraper.scrape_all_symbols()
            count   = 0
            for symbol in symbols:
                try:
                    data = await scraper.scrape_daily(symbol)
                    if data:
                        _upsert_price(db, data)
                        _upsert_fundamentals(db, data)
                        count += 1
                    if count % 50 == 0 and count > 0:
                        db.commit()
                    # Small polite delay between requests
                    await asyncio.sleep(0.2)
                except Exception as sym_err:
                    logger.warning(f"[{src}] {symbol}: {sym_err}")

            db.commit()
            total += count
            _log_run(db, src, "success", count, started)
            logger.info(f"[{src}] Done — {count} records")
        except Exception as e:
            db.rollback()
            errors.append(f"{src}: {e}")
            _log_run(db, src, "failed", 0, started, str(e))
            logger.error(f"[{src}] Scraper failed: {e}")
        finally:
            await scraper.close()

    db.close()
    status = "success" if not errors else "partial" if total > 0 else "failed"
    logger.info(f"=== Daily scrape done — {total} records, status={status} ===")
    return {"status": status, "records_saved": total, "errors": errors}


# ─────────────────────────────────────────────────────────────────
#  Historical backfill (run once on first setup)
# ─────────────────────────────────────────────────────────────────

async def run_historical_scrape(days: int = 30) -> dict:
    logger.info(f"=== Historical scrape started ({days} days) ===")
    db      = SessionLocal()
    started = datetime.now()
    grand   = 0

    # Phase order: NepseAlpha (best API) → Sharesansar → Merolagani (fills gaps)
    phases = [
        (NepseAlphaScraper(),  "nepsealpha_historical",  True),
        (SharesansarScraper(), "sharesansar_historical", False),
        (MerolaganiScraper(),  "merolagani_historical",  False),
    ]

    for scraper, log_name, is_primary in phases:
        src_total = 0
        try:
            symbols = await scraper.scrape_all_symbols()
            logger.info(f"[{log_name}] {len(symbols)} symbols to process")

            for symbol in symbols:
                try:
                    records = await scraper.scrape_historical(symbol, days=days)
                    for r in records:
                        # Non-primary sources only fill missing days
                        if is_primary or not _price_exists(db, r["symbol"], r["date"]):
                            _upsert_price(db, r)
                            src_total += 1
                    if src_total % 200 == 0 and src_total > 0:
                        db.commit()
                    await asyncio.sleep(0.3)  # polite delay
                except Exception as e:
                    logger.warning(f"[{log_name}] {symbol}: {e}")

            db.commit()
            grand += src_total
            _log_run(db, log_name, "success", src_total, started)
            logger.info(f"[{log_name}] Done — {src_total} records")
        except Exception as e:
            db.rollback()
            _log_run(db, log_name, "failed", 0, started, str(e))
            logger.error(f"[{log_name}] Failed: {e}")
        finally:
            await scraper.close()

    db.close()
    logger.info(f"=== Historical scrape complete — {grand} total records ===")
    return {"status": "done", "records_saved": grand}


# ─────────────────────────────────────────────────────────────────
#  DB helpers
# ─────────────────────────────────────────────────────────────────

def _upsert_price(db: Session, data: dict) -> None:
    close = data.get("close") or data.get("close_price")
    if not close:
        return
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
        db.flush()
    except Exception:
        db.rollback()


def _upsert_fundamentals(db: Session, data: dict) -> None:
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
    d = _to_date(date_str)
    return db.query(StockPrice).filter(
        StockPrice.symbol     == symbol.upper(),
        StockPrice.trade_date == d,
    ).first() is not None


def _log_run(db, source, status, records, started, error=None):
    log = ScrapeLog(
        source=source, status=status, records_saved=records,
        error_message=error, started_at=started,
    )
    db.add(log)
    db.commit()


def _to_date(val):
    if isinstance(val, date): return val
    if isinstance(val, datetime): return val.date()
    if isinstance(val, str): return date.fromisoformat(val[:10])
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
    scheduler.add_job(
        run_daily_scrape,
        trigger=CronTrigger(hour=16, minute=30, timezone="Asia/Kathmandu"),
        id="daily_scrape",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    scheduler.start()
    logger.info("Scheduler started — daily scrape at 16:30 Nepal time")