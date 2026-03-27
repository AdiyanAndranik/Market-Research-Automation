"""
Scraping endpoints.

Endpoints:
  POST /api/v1/scrape/search     — trigger a new scrape job
  GET  /api/v1/scrape/session/{id} — check session status
"""
import time
import uuid

import asyncpg
from fastapi import APIRouter, HTTPException
from loguru import logger

from config import get_settings
from models.product import ScrapeRequest, ScrapeResponse, ProductBase
from services.scraper_service import scrape_all_sources

router = APIRouter()
settings = get_settings()


async def _get_db_conn():
    """Open a raw asyncpg connection. Used for simple inserts."""
    return await asyncpg.connect(settings.database_url)


@router.post("/search", response_model=ScrapeResponse, summary="Scrape products from all sources")
async def scrape_products(request: ScrapeRequest):
    """
    Trigger a multi-source product scrape.

    - Scrapes Amazon, eBay, Walmart in parallel
    - Deduplicates results
    - Saves all products to PostgreSQL
    - Returns product list immediately

    Called by n8n workflow after the Webhook trigger.
    """
    start_time = time.time()
    session_id = request.session_id or str(uuid.uuid4())
    sources = [s.value for s in request.sources]

    logger.info(f"[{session_id}] Scrape started — keyword='{request.keyword}' sources={sources}")

    # ── Create session record in DB ───
    conn = await _get_db_conn()
    try:
        await conn.execute(
            """
            INSERT INTO search_sessions (id, keyword, sources, status)
            VALUES ($1, $2, $3::jsonb, 'running')
            """,
            session_id,
            request.keyword,
            str(sources).replace("'", '"'),
        )
    except Exception as e:
        logger.warning(f"Could not create session record: {e}")
    finally:
        await conn.close()

    # ── Run scrapers ──
    try:
        raw_products = await scrape_all_sources(
            keyword=request.keyword,
            sources=sources,
            max_per_source=request.max_results,
        )
    except Exception as e:
        logger.error(f"[{session_id}] Scraping failed: {e}")
        raise HTTPException(status_code=500, detail=f"Scraping failed: {str(e)}")

    if not raw_products:
        logger.warning(f"[{session_id}] No products found for '{request.keyword}'")

    # Save to PostgreSQL
    saved_products = []
    conn = await _get_db_conn()
    try:
        for p in raw_products:
            import json
            row = await conn.fetchrow(
                """
                INSERT INTO products
                    (external_id, source, keyword, title, price, currency,
                     rating, review_count, image_url, product_url, brand,
                     availability, raw_data)
                VALUES
                    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)
                ON CONFLICT DO NOTHING
                RETURNING id, source, keyword, title, price, currency,
                          rating, review_count, image_url, product_url,
                          brand, scraped_at
                """,
                p.get("external_id"),
                p["source"],
                p["keyword"],
                p["title"],
                p.get("price"),
                p.get("currency", "USD"),
                p.get("rating"),
                p.get("review_count", 0),
                p.get("image_url"),
                p.get("product_url"),
                p.get("brand"),
                p.get("availability"),
                json.dumps(p),
            )
            if row:
                saved_products.append(dict(row))

        await conn.execute(
            """
            UPDATE search_sessions
            SET status = 'complete', products_found = $1, completed_at = NOW()
            WHERE id = $2
            """,
            len(saved_products),
            session_id,
        )
    except Exception as e:
        logger.error(f"[{session_id}] DB save failed: {e}")
    finally:
        await conn.close()

    duration = round(time.time() - start_time, 2)
    logger.success(
        f"[{session_id}] Scrape complete — "
        f"{len(saved_products)} products saved in {duration}s"
    )

    return ScrapeResponse(
        session_id=session_id,
        keyword=request.keyword,
        products_found=len(saved_products),
        sources_scraped=sources,
        products=[ProductBase(**p) for p in saved_products],
        duration_seconds=duration,
    )


@router.get(
    "/session/{session_id}",
    summary="Get scrape session status",
)
async def get_session_status(session_id: str):
    """
    Check the status of a scrape session.
    n8n polls this after triggering a scrape to know when to proceed.
    """
    conn = await _get_db_conn()
    try:
        row = await conn.fetchrow(
            "SELECT * FROM search_sessions WHERE id = $1", session_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Session not found")
        return dict(row)
    finally:
        await conn.close()

