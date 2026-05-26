# backend/app/models.py
#
# SQLAlchemy ORM models — mirror the tables created by db_setup.sql exactly.
# Column names here match the SQL column names so no translation is needed.

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, Column, Date, DateTime,
    Integer, Numeric, String, Text, UniqueConstraint
)
from sqlalchemy.sql import func
from .database import Base


class Stock(Base):
    """Master registry of every NEPSE-listed company."""
    __tablename__ = "stocks"

    id         = Column(Integer,  primary_key=True, index=True)
    symbol     = Column(String(20),  nullable=False, unique=True, index=True)
    company    = Column(String(200), nullable=True)
    sector     = Column(String(100), nullable=True, index=True)
    is_active  = Column(Boolean,     nullable=False, default=True, index=True)
    created_at = Column(DateTime,    server_default=func.now())
    updated_at = Column(DateTime,    server_default=func.now(), onupdate=func.now())


class StockPrice(Base):
    """Daily OHLCV data — one canonical row per (symbol, trade_date)."""
    __tablename__ = "stock_prices"

    id          = Column(Integer,        primary_key=True, index=True)
    symbol      = Column(String(20),     nullable=False, index=True)
    trade_date  = Column(Date,           nullable=False, index=True)
    open_price  = Column(Numeric(12, 2), nullable=True)
    high_price  = Column(Numeric(12, 2), nullable=True)
    low_price   = Column(Numeric(12, 2), nullable=True)
    close_price = Column(Numeric(12, 2), nullable=True)
    volume      = Column(BigInteger,     nullable=True)
    prev_close  = Column(Numeric(12, 2), nullable=True)
    pct_change  = Column(Numeric(8, 4),  nullable=True)
    source      = Column(String(50),     nullable=True)
    scraped_at  = Column(DateTime,       server_default=func.now())

    __table_args__ = (
        UniqueConstraint("symbol", "trade_date", name="uq_price_symbol_date"),
    )


class StockFundamentals(Base):
    """Daily snapshot of P/E, market cap, 52-week range, EPS, book value."""
    __tablename__ = "stock_fundamentals"

    id          = Column(Integer,        primary_key=True, index=True)
    symbol      = Column(String(20),     nullable=False, index=True)
    trade_date  = Column(Date,           nullable=False, index=True)
    market_cap  = Column(Numeric(20, 2), nullable=True)
    pe_ratio    = Column(Numeric(10, 4), nullable=True)
    eps         = Column(Numeric(10, 4), nullable=True)
    book_value  = Column(Numeric(10, 4), nullable=True)
    week52_high = Column(Numeric(12, 2), nullable=True)
    week52_low  = Column(Numeric(12, 2), nullable=True)
    source      = Column(String(50),     nullable=True)
    scraped_at  = Column(DateTime,       server_default=func.now())

    __table_args__ = (
        UniqueConstraint("symbol", "trade_date", name="uq_fund_symbol_date"),
    )


class TechnicalIndicator(Base):
    """
    Pre-computed technical analysis values — populated after each scrape.
    One row per (symbol, trade_date).
    """
    __tablename__ = "technical_indicators"

    id           = Column(Integer,        primary_key=True, index=True)
    symbol       = Column(String(20),     nullable=False, index=True)
    trade_date   = Column(Date,           nullable=False, index=True)

    # Trend
    sma_20       = Column(Numeric(12, 4), nullable=True)
    sma_50       = Column(Numeric(12, 4), nullable=True)
    ema_12       = Column(Numeric(12, 4), nullable=True)
    ema_26       = Column(Numeric(12, 4), nullable=True)

    # Momentum
    rsi_14       = Column(Numeric(8, 4),  nullable=True)
    macd         = Column(Numeric(12, 4), nullable=True)
    macd_signal  = Column(Numeric(12, 4), nullable=True)
    macd_hist    = Column(Numeric(12, 4), nullable=True)

    # Volatility
    bb_upper     = Column(Numeric(12, 4), nullable=True)
    bb_middle    = Column(Numeric(12, 4), nullable=True)
    bb_lower     = Column(Numeric(12, 4), nullable=True)
    atr_14       = Column(Numeric(12, 4), nullable=True)

    # Volume
    obv          = Column(Numeric(20, 4), nullable=True)

    computed_at  = Column(DateTime,       server_default=func.now())

    __table_args__ = (
        UniqueConstraint("symbol", "trade_date", name="uq_ti_symbol_date"),
    )


class AISignal(Base):
    """LLM-generated Buy / Sell / Hold recommendation per symbol per day."""
    __tablename__ = "ai_signals"

    id             = Column(Integer,       primary_key=True, index=True)
    symbol         = Column(String(20),    nullable=False, index=True)
    signal_date    = Column(Date,          nullable=False, index=True)
    signal         = Column(String(10),    nullable=False)  # BUY | SELL | HOLD
    confidence     = Column(Numeric(5, 2), nullable=True)   # 0.00 – 100.00
    reasoning      = Column(Text,          nullable=True)   # full LLM text
    model_name     = Column(String(100),   nullable=True)
    prompt_version = Column(String(20),    nullable=True)
    generated_at   = Column(DateTime,      server_default=func.now())

    __table_args__ = (
        UniqueConstraint("symbol", "signal_date", name="uq_signal_symbol_date"),
        CheckConstraint("signal IN ('BUY', 'SELL', 'HOLD')", name="ck_signal_value"),
    )


class ScrapeLog(Base):
    """Audit trail for every scrape run — useful for monitoring from the UI."""
    __tablename__ = "scrape_logs"

    id            = Column(Integer,     primary_key=True, index=True)
    source        = Column(String(50),  nullable=False, index=True)
    status        = Column(String(20),  nullable=False, index=True)  # success|failed|partial
    records_saved = Column(Integer,     nullable=False, default=0)
    error_message = Column(Text,        nullable=True)
    started_at    = Column(DateTime,    nullable=False)
    finished_at   = Column(DateTime,    server_default=func.now())