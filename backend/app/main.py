# backend/app/main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

from .database import engine, Base
from .scheduler import start_scheduler, run_daily_scrape, run_historical_scrape
from .api.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create all tables on startup (safe to run repeatedly — skips existing tables)
    Base.metadata.create_all(bind=engine)
    start_scheduler()
    yield


app = FastAPI(
    title="Nepal Stock Analyzer API",
    version="1.0.0",
    lifespan=lifespan,
)

# Allow the React dev server (and production build) to call the API
cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins.split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api/v1")


# Manual trigger endpoints — callable from the React frontend's admin panel
@app.post("/api/v1/scrape/trigger")
async def trigger_scrape():
    """Manually kick off today's scrape from the frontend."""
    return await run_daily_scrape()


@app.post("/api/v1/scrape/historical")
async def trigger_historical(days: int = 30):
    """Backfill historical data. Run once on first setup."""
    return await run_historical_scrape(days=days)


@app.get("/health")
def health():
    return {"status": "ok"}