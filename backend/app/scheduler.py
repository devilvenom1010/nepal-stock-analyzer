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
    started = datetime.utcnow()   # UTC — matches SQL Server func.now()
    total   = 0
    errors  = []

    # Priority: Sharesansar first, others fill gaps
    scrapers = [
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
#
#  Phase 1 — NepseAlpha (primary, with retry-once per symbol).
#  Phase 2 — Sharesansar + Merolagani ONLY if NepseAlpha saved 0
#             records (e.g. Cloudflare fully blocked, all fallbacks
#             inside NepseAlphaScraper also failed).
# ─────────────────────────────────────────────────────────────────

async def run_historical_scrape(days: int = 30) -> dict:
    logger.info(f"=== Historical scrape started ({days} days) ===")
    db      = SessionLocal()
    started = datetime.utcnow()   # UTC — matches SQL Server func.now()
    grand   = 0

    # ── Phase 1: NepseAlpha ──────────────────────────────────────
    nepse_scraper = NepseAlphaScraper()
    nepse_total   = 0
    try:
        symbols = await nepse_scraper.scrape_all_symbols()
        logger.info(f"[nepsealpha_historical] {len(symbols)} symbols to process")

        for symbol in symbols:
            records = await _fetch_with_retry(nepse_scraper, symbol, days)
            for r in records:
                _upsert_price(db, r)
                nepse_total += 1
            if nepse_total % 200 == 0 and nepse_total > 0:
                db.commit()
            await asyncio.sleep(0.3)

        db.commit()
        grand += nepse_total
        _log_run(db, "nepsealpha_historical", "success", nepse_total, started)
        logger.info(f"[nepsealpha_historical] Done — {nepse_total} records")
    except Exception as e:
        db.rollback()
        _log_run(db, "nepsealpha_historical", "failed", 0, started, str(e))
        logger.error(f"[nepsealpha_historical] Failed: {e}")
    finally:
        await nepse_scraper.close()

    # ── Phase 2: Fallback scrapers — only if NepseAlpha saved nothing ──
    if nepse_total == 0:
        logger.warning(
            "[historical] NepseAlpha saved 0 records — "
            "activating Sharesansar + Merolagani fallback"
        )
        fallback_phases = [
            (SharesansarScraper(), "sharesansar_historical"),
            (MerolaganiScraper(),  "merolagani_historical"),
        ]
        for scraper, log_name in fallback_phases:
            src_total = 0
            try:
                symbols = await scraper.scrape_all_symbols()
                logger.info(f"[{log_name}] {len(symbols)} symbols to process")

                for symbol in symbols:
                    try:
                        records = await scraper.scrape_historical(symbol, days=days)
                        for r in records:
                            # Only fill gaps not already covered
                            if not _price_exists(db, r["symbol"], r["date"]):
                                _upsert_price(db, r)
                                src_total += 1
                        if src_total % 200 == 0 and src_total > 0:
                            db.commit()
                        await asyncio.sleep(0.3)
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
    else:
        logger.info(
            f"[historical] NepseAlpha saved {nepse_total} records — "
            "fallback scrapers skipped"
        )

    db.close()
    logger.info(f"=== Historical scrape complete — {grand} total records ===")
    return {"status": "done", "records_saved": grand}


# ─────────────────────────────────────────────────────────────────
#  Retry helper — try once, on failure wait 2 s and retry once more
# ─────────────────────────────────────────────────────────────────

async def _fetch_with_retry(
    scraper: NepseAlphaScraper,
    symbol: str,
    days: int,
    max_attempts: int = 2,
) -> list[dict]:
    """
    Attempt to scrape historical data for *symbol*.
    On the first exception, waits 2 seconds and tries once more.
    Returns an empty list if both attempts fail.
    """
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            records = await scraper.scrape_historical(symbol, days=days)
            if records:
                return records
            # Empty result is not an exception; just return it
            return []
        except Exception as e:
            last_error = e
            if attempt < max_attempts:
                logger.debug(
                    f"[nepsealpha_historical] {symbol} attempt {attempt} failed "
                    f"({e}) — retrying in 2 s"
                )
                await asyncio.sleep(2)
            else:
                logger.warning(
                    f"[nepsealpha_historical] {symbol} failed after "
                    f"{max_attempts} attempts: {last_error}"
                )
    return []


# ─────────────────────────────────────────────────────────────────
#  DB helpers
# ─────────────────────────────────────────────────────────────────

def _upsert_price(db: Session, data: dict) -> None:
    close = data.get("close") or data.get("close_price")
    if not close:
        return
    sym = data["symbol"].upper()
    t_date = _to_date(data["date"])
    try:
        existing = db.query(StockPrice).filter(
            StockPrice.symbol == sym,
            StockPrice.trade_date == t_date
        ).first()

        if existing:
            # Update existing row with non-None values
            if data.get("open") or data.get("open_price"):
                existing.open_price = data.get("open") or data.get("open_price")
            if data.get("high") or data.get("high_price"):
                existing.high_price = data.get("high") or data.get("high_price")
            if data.get("low") or data.get("low_price"):
                existing.low_price = data.get("low") or data.get("low_price")
            if close:
                existing.close_price = close
            if data.get("volume"):
                existing.volume = _to_int(data.get("volume"))
            if data.get("prev_close"):
                existing.prev_close = data.get("prev_close")
            if data.get("pct_change") is not None and data.get("source") == "merolagani":
                existing.pct_change = data.get("pct_change")
            if data.get("source"):
                existing.source = data.get("source")
        else:
            # Insert new row
            row = StockPrice(
                symbol      = sym,
                trade_date  = t_date,
                open_price  = data.get("open")  or data.get("open_price"),
                high_price  = data.get("high")  or data.get("high_price"),
                low_price   = data.get("low")   or data.get("low_price"),
                close_price = close,
                volume      = _to_int(data.get("volume")),
                prev_close  = data.get("prev_close"),
                pct_change  = data.get("pct_change") if data.get("source") == "merolagani" else None,
                source      = data.get("source"),
            )
            db.add(row)
        db.flush()
    except Exception as e:
        db.rollback()
        logger.warning(f"[_upsert_price] Failed to upsert {sym} on {t_date}: {e}")



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
        source        = source,
        status        = status,
        records_saved = records,
        error_message = error,
        started_at    = started,   # UTC datetime from datetime.utcnow()
        # finished_at uses server_default=func.now() in the model (also UTC
        # on a correctly configured SQL Server instance)
    )
    db.add(log)
    db.commit()


def _to_date(val):
    if isinstance(val, date):     return val
    if isinstance(val, datetime): return val.date()
    if isinstance(val, str):      return date.fromisoformat(val[:10])
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