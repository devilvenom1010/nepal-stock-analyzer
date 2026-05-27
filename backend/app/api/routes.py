# backend/app/api/routes.py
#
# All REST API endpoints for the React frontend.
# Mounted at /api/v1 in main.py.

import logging
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, text
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import (
    AISignal, ScrapeLog, Stock,
    StockFundamentals, StockPrice, TechnicalIndicator,
)
from ..scrapers.merolagani import MerolaganiScraper

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────────────────────────
#  Stocks — master registry
# ─────────────────────────────────────────────────────────────────

@router.get("/stocks")
def list_stocks(
    sector: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Return all stocks, optionally filtered by sector."""
    q = db.query(Stock).filter(Stock.is_active == True)
    if sector:
        q = q.filter(Stock.sector.ilike(f"%{sector}%"))
    return q.order_by(Stock.symbol).all()


@router.get("/stocks/{symbol}")
def get_stock(symbol: str, db: Session = Depends(get_db)):
    """Return a single stock's master record."""
    stock = db.query(Stock).filter(Stock.symbol == symbol.upper()).first()
    if not stock:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
    return stock


@router.post("/stocks/populate")
async def populate_stocks(db: Session = Depends(get_db)):
    """
    Scrape the full NEPSE listing from Merolagani and upsert into the
    stocks master table.

    - New symbols   → inserted with company name, sector, is_active=True
    - Existing rows → company / sector updated only when the scraped
                      value differs and is non-empty
    - Rows not seen in the scrape → left untouched (no deactivation)

    Returns a summary: { inserted, updated, unchanged, total, errors }
    """
    scraper = MerolaganiScraper()
    scrape_errors: list[str] = []

    try:
        stocks_data = await scraper.scrape_all_stocks()
    except Exception as e:
        logger.error(f"[populate_stocks] Scraper failed: {e}")
        raise HTTPException(status_code=502, detail=f"Scraper error: {e}")
    finally:
        await scraper.close()

    if not stocks_data:
        raise HTTPException(
            status_code=502,
            detail="Scraper returned no stocks — Merolagani listing page may be unreachable.",
        )

    inserted  = 0
    updated   = 0
    unchanged = 0

    for item in stocks_data:
        symbol  = item["symbol"].upper()
        company = (item.get("company") or "").strip() or None
        sector  = (item.get("sector")  or "").strip() or None

        try:
            existing = db.query(Stock).filter(Stock.symbol == symbol).first()

            if existing is None:
                db.add(Stock(
                    symbol    = symbol,
                    company   = company,
                    sector    = sector,
                    is_active = True,
                ))
                inserted += 1

            else:
                changed = False

                if company and existing.company != company:
                    existing.company = company
                    changed = True

                if sector and existing.sector != sector:
                    existing.sector = sector
                    changed = True

                if changed:
                    existing.updated_at = datetime.utcnow()
                    updated += 1
                else:
                    unchanged += 1

        except Exception as e:
            db.rollback()
            scrape_errors.append(f"{symbol}: {e}")
            logger.warning(f"[populate_stocks] Upsert failed for {symbol}: {e}")
            continue

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"[populate_stocks] Final commit failed: {e}")
        raise HTTPException(status_code=500, detail=f"DB commit failed: {e}")

    logger.info(
        f"[populate_stocks] Done — "
        f"inserted={inserted} updated={updated} unchanged={unchanged} "
        f"errors={len(scrape_errors)}"
    )

    return {
        "inserted":  inserted,
        "updated":   updated,
        "unchanged": unchanged,
        "total":     len(stocks_data),
        "errors":    scrape_errors,
    }


@router.get("/sectors")
def list_sectors(db: Session = Depends(get_db)):
    """Return all distinct sectors for the sidebar filter."""
    rows = db.query(Stock.sector).distinct().filter(Stock.sector.isnot(None)).all()
    return sorted([r[0] for r in rows if r[0]])


# ─────────────────────────────────────────────────────────────────
#  Prices
# ─────────────────────────────────────────────────────────────────

@router.get("/prices/latest")
def latest_prices(
    sector: Optional[str] = None,
    limit: int = Query(default=100, le=500),
    db: Session = Depends(get_db),
):
    """
    Return the most recent price row for every stock.
    Used by the dashboard market overview table.
    Optionally filter by sector.
    """
    try:
        sql = "SELECT * FROM vw_latest_prices"
        params = {}
        if sector:
            sql += " WHERE sector LIKE :sector"
            params["sector"] = f"%{sector}%"
        sql += f" ORDER BY symbol OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
        result = db.execute(text(sql), params)
        cols   = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]
    except Exception:
        q = (
            db.query(StockPrice)
            .order_by(StockPrice.symbol, desc(StockPrice.trade_date))
            .distinct(StockPrice.symbol)
            .limit(limit)
        )
        return q.all()


@router.get("/prices/{symbol}")
def price_history(
    symbol: str,
    days:   int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
):
    """
    Return daily OHLCV history for one symbol.
    Used by the stock detail candlestick chart.
    """
    rows = (
        db.query(StockPrice)
        .filter(StockPrice.symbol == symbol.upper())
        .order_by(StockPrice.trade_date)
        .limit(days)
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No price data for {symbol}")
    return rows


# ─────────────────────────────────────────────────────────────────
#  Fundamentals
# ─────────────────────────────────────────────────────────────────

@router.get("/fundamentals/{symbol}")
def get_fundamentals(symbol: str, db: Session = Depends(get_db)):
    """Return the latest fundamental snapshot for one symbol."""
    row = (
        db.query(StockFundamentals)
        .filter(StockFundamentals.symbol == symbol.upper())
        .order_by(desc(StockFundamentals.trade_date))
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"No fundamentals for {symbol}")
    return row


# ─────────────────────────────────────────────────────────────────
#  Technical indicators
# ─────────────────────────────────────────────────────────────────

@router.get("/indicators/{symbol}")
def get_indicators(
    symbol: str,
    days:   int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
):
    """Return technical indicator history for one symbol."""
    rows = (
        db.query(TechnicalIndicator)
        .filter(TechnicalIndicator.symbol == symbol.upper())
        .order_by(TechnicalIndicator.trade_date)
        .limit(days)
        .all()
    )
    return rows


# ─────────────────────────────────────────────────────────────────
#  AI Signals
# ─────────────────────────────────────────────────────────────────

@router.get("/signals/latest")
def latest_signals(
    signal_type: Optional[str] = Query(default=None, description="BUY | SELL | HOLD"),
    limit: int = Query(default=50, le=200),
    db: Session = Depends(get_db),
):
    """
    Return the most recent AI signal for every stock.
    Optionally filter by signal type (BUY / SELL / HOLD).
    """
    try:
        sql = "SELECT * FROM vw_latest_signals"
        params = {}
        if signal_type:
            sql += " WHERE signal = :signal"
            params["signal"] = signal_type.upper()
        sql += f" ORDER BY confidence DESC OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
        result = db.execute(text(sql), params)
        cols   = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]
    except Exception:
        q = db.query(AISignal).order_by(desc(AISignal.signal_date))
        if signal_type:
            q = q.filter(AISignal.signal == signal_type.upper())
        return q.limit(limit).all()


@router.get("/signals/{symbol}")
def get_signal(symbol: str, db: Session = Depends(get_db)):
    """Return the latest AI signal for one symbol."""
    row = (
        db.query(AISignal)
        .filter(AISignal.symbol == symbol.upper())
        .order_by(desc(AISignal.signal_date))
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"No signal yet for {symbol}")
    return row


@router.get("/signals/{symbol}/history")
def signal_history(
    symbol: str,
    limit: int = Query(default=30, le=90),
    db: Session = Depends(get_db),
):
    """Return past AI signals for one symbol."""
    return (
        db.query(AISignal)
        .filter(AISignal.symbol == symbol.upper())
        .order_by(desc(AISignal.signal_date))
        .limit(limit)
        .all()
    )


# ─────────────────────────────────────────────────────────────────
#  Scrape logs — for the admin/monitoring panel
# ─────────────────────────────────────────────────────────────────

@router.get("/scrape/logs")
def scrape_logs(
    limit: int = Query(default=20, le=100),
    db: Session = Depends(get_db),
):
    """Return the most recent scrape run logs."""
    return (
        db.query(ScrapeLog)
        .order_by(desc(ScrapeLog.started_at))
        .limit(limit)
        .all()
    )


@router.get("/scrape/status")
def scrape_status(db: Session = Depends(get_db)):
    """
    Return a quick summary: last successful run time, total stocks tracked,
    and whether data is fresh (scraped today).
    """
    latest_log = (
        db.query(ScrapeLog)
        .filter(ScrapeLog.status == "success")
        .order_by(desc(ScrapeLog.finished_at))
        .first()
    )
    total_stocks  = db.query(Stock).filter(Stock.is_active == True).count()
    total_prices  = db.query(StockPrice).count()
    today_records = (
        db.query(StockPrice)
        .filter(StockPrice.trade_date == date.today())
        .count()
    )

    return {
        "last_scrape":   latest_log.finished_at.isoformat() if latest_log else None,
        "is_fresh":      today_records > 0,
        "today_records": today_records,
        "total_stocks":  total_stocks,
        "total_prices":  total_prices,
    }