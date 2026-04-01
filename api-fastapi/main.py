from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from loguru import logger
import sys

from config import get_settings
from routers import scraper, analysis, reports, products

settings = get_settings()

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{name}</cyan> | {message}",
    level="DEBUG" if settings.debug else "INFO",
    colorize=True,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")
    yield
    logger.info("Shutting down...")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Multi-source scraping, AI analysis, ranking, and automated report generation.",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5678", "http://localhost:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(scraper.router,  prefix="/api/v1/scrape",   tags=["Scraping"])
app.include_router(analysis.router, prefix="/api/v1/analysis", tags=["AI Analysis"])
app.include_router(products.router, prefix="/api/v1/products", tags=["Products"])
app.include_router(reports.router,  prefix="/api/v1/reports",  tags=["Reports"])


@app.get("/health", tags=["System"])
async def health_check():
    return {"status": "healthy", "version": settings.app_version}


@app.get("/", tags=["System"])
async def root():
    return {"message": f"Welcome to {settings.app_name}", "docs": "/docs"}


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(status_code=500, content={"error": str(exc)})

