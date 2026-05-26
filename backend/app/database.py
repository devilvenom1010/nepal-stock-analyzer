# backend/app/database.py
#
# Builds the MSSQL connection using individual env vars rather than a
# single URL string. This avoids URL-encoding issues with passwords that
# contain special characters like @, $, #, %, etc.

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
import os
import urllib

load_dotenv()

# ── Read individual env vars ────────────────────────────────────────────
DB_SERVER   = os.getenv("DB_SERVER",   "localhost")
DB_PORT     = os.getenv("DB_PORT",     "1433")
DB_NAME     = os.getenv("DB_NAME",     "NepseDB")
DB_USER     = os.getenv("DB_USER",     "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_DRIVER   = os.getenv("DB_DRIVER",   "ODBC Driver 18 for SQL Server")

# ── Build the pyodbc connection string safely ───────────────────────────
# Using urllib.parse.quote is NOT needed here — we pass the string directly
# to pyodbc via SQLAlchemy's creator, bypassing URL parsing entirely.
# This is the only reliable way to handle passwords with special characters.

def _make_connection_string() -> str:
    return (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER},{DB_PORT};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
    )

# SQLAlchemy creator function — called every time a new raw connection is needed
import pyodbc

def _creator():
    return pyodbc.connect(_make_connection_string())

engine = create_engine(
    "mssql+pyodbc://",          # dialect only — credentials come from _creator
    creator=_creator,
    pool_pre_ping=True,         # auto-reconnect if connection dropped
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    """FastAPI dependency — yields one DB session per request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()