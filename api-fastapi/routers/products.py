"""
Product listing, ranking, and retrieval endpoints.

Endpoints:
  POST /api/v1/products/rank          — score and rank products for a keyword
  GET  /api/v1/products               — list products with filters
  GET  /api/v1/products/{id}          — single product with full analysis
  GET  /api/v1/products/top/{keyword} — top ranked products for a keyword
"""
import json
import asyncpg
from fastapi import APIRouter, HTTPException, Query
from loguru import logger
from typing import Optional
from config import get_settings
from services.ranking_service import rank_products, compute_market_stats

router = APIRouter()
settings = get_settings()


async def _get_db_conn():
    return await asyncpg.connect(settings.database_url)


# ─────────────────────────────────────────────────────────
# Rank products for a keyword
# ─────────────────────────────────────────────────────────

@router.post("/rank", summary="Score and rank all products for a keyword")
async def rank_keyword_products(keyword: str, session_id: Optional[str] = None):
    """
    Run the ranking algorithm on all scraped + analyzed products
    for a given keyword.

    - Fetches products + their AI analysis from DB
    - Computes scores using the ranking algorithm
    - Saves rankings back to product_rankings table
    - Returns ranked list with categories

    Called by n8n after AI analysis is complete.
    """
    conn = await _get_db_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT
                p.id::text,
                p.title,
                p.price,
                p.rating,
                p.review_count,
                p.source,
                p.keyword,
                p.image_url,
                p.product_url,
                p.brand,
                p.availability,
                pa.sentiment_score,
                pa.sentiment_label,
                pa.pros,
                pa.cons,
                pa.summary,
                pa.fake_review_risk
            FROM products p
            LEFT JOIN product_analysis pa ON pa.product_id = p.id
            WHERE LOWER(p.keyword) = LOWER($1)
            ORDER BY p.scraped_at DESC
            """,
            keyword,
        )

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No products found for keyword '{keyword}'. Run scrape first."
            )

        logger.info(f"Ranking {len(rows)} products for '{keyword}'")

        products = []
        for row in rows:
            p = dict(row)
            p["pros"] = json.loads(p["pros"]) if p.get("pros") else []
            p["cons"] = json.loads(p["cons"]) if p.get("cons") else []
            products.append(p)

        ranked = rank_products(products)

        for product in ranked:
            await conn.execute(
                """
                INSERT INTO product_rankings
                    (product_id, keyword, score, rank_position, category, price_tier)
                VALUES ($1::uuid, $2, $3, $4, $5, $6)
                ON CONFLICT (product_id) DO UPDATE SET
                    score          = EXCLUDED.score,
                    rank_position  = EXCLUDED.rank_position,
                    category       = EXCLUDED.category,
                    price_tier     = EXCLUDED.price_tier,
                    ranked_at      = NOW()
                """,
                product["id"],
                keyword,
                product["score"],
                product["rank_position"],
                product["category"],
                product["price_tier"],
            )

        stats = compute_market_stats(ranked)

        logger.success(f"Ranking saved for {len(ranked)} products — keyword='{keyword}'")

        return {
            "keyword": keyword,
            "total_ranked": len(ranked),
            "market_stats": stats,
            "ranked_products": [
                {
                    "rank":          p["rank_position"],
                    "id":            p["id"],
                    "title":         p["title"],
                    "source":        p["source"],
                    "price":         p.get("price"),
                    "price_tier":    p["price_tier"],
                    "rating":        p.get("rating"),
                    "review_count":  p.get("review_count"),
                    "score":         p["score"],
                    "category":      p["category"],
                    "all_categories": p.get("all_categories", []),
                    "sentiment":     p.get("sentiment_label"),
                    "fake_risk":     p.get("fake_review_risk"),
                    "product_url":   p.get("product_url"),
                }
                for p in ranked
            ],
        }

    finally:
        await conn.close()


# ─────────────────────────────────────────────────────────
# List products with filters
# ─────────────────────────────────────────────────────────

@router.get("", summary="List products with optional filters")
async def list_products(
    keyword: Optional[str] = Query(None, description="Filter by keyword"),
    source: Optional[str] = Query(None, description="amazon | ebay | walmart"),
    min_rating: Optional[float] = Query(None, description="Minimum rating"),
    max_price: Optional[float] = Query(None, description="Maximum price"),
    sentiment: Optional[str] = Query(None, description="positive | neutral | negative"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """
    List products with filtering and pagination.
    Supports filtering by keyword, source, rating, price, and sentiment.
    """
    conn = await _get_db_conn()
    try:
        conditions = []
        params = []
        idx = 1

        if keyword:
            conditions.append(f"LOWER(p.keyword) = LOWER(${idx})")
            params.append(keyword)
            idx += 1

        if source:
            conditions.append(f"p.source = ${idx}")
            params.append(source)
            idx += 1

        if min_rating is not None:
            conditions.append(f"p.rating >= ${idx}")
            params.append(min_rating)
            idx += 1

        if max_price is not None:
            conditions.append(f"p.price <= ${idx}")
            params.append(max_price)
            idx += 1

        if sentiment:
            conditions.append(f"pa.sentiment_label = ${idx}")
            params.append(sentiment)
            idx += 1

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        params.extend([limit, offset])

        rows = await conn.fetch(
            f"""
            SELECT
                p.id::text, p.source, p.keyword, p.title,
                p.price, p.rating, p.review_count,
                p.image_url, p.product_url, p.brand, p.scraped_at,
                pa.sentiment_label, pa.sentiment_score,
                r.score, r.rank_position, r.category
            FROM products p
            LEFT JOIN product_analysis pa ON pa.product_id = p.id
            LEFT JOIN product_rankings  r  ON r.product_id  = p.id
            {where_clause}
            ORDER BY r.rank_position ASC NULLS LAST, p.scraped_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}
            """,
            *params,
        )

        return {
            "total": len(rows),
            "limit": limit,
            "offset": offset,
            "products": [dict(r) for r in rows],
        }

    finally:
        await conn.close()


# ─────────────────────────────────────────────────────────
# Single product with full details
# ─────────────────────────────────────────────────────────

@router.get("/{product_id}", summary="Get a single product with full analysis")
async def get_product(product_id: str):
    """
    Retrieve a single product with its AI analysis and ranking data.
    """
    conn = await _get_db_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT
                p.*,
                pa.sentiment_score, pa.sentiment_label,
                pa.pros, pa.cons, pa.fake_review_risk,
                pa.summary, pa.keywords_extracted, pa.analyzed_at,
                r.score, r.rank_position, r.category, r.price_tier
            FROM products p
            LEFT JOIN product_analysis pa ON pa.product_id = p.id
            LEFT JOIN product_rankings  r  ON r.product_id  = p.id
            WHERE p.id::text = $1
            """,
            product_id,
        )

        if not row:
            raise HTTPException(status_code=404, detail="Product not found")

        result = dict(row)
        for field in ("pros", "cons", "keywords_extracted", "raw_data"):
            if result.get(field) and isinstance(result[field], str):
                result[field] = json.loads(result[field])

        return result

    finally:
        await conn.close()


# ─────────────────────────────────────────────────────────
# Top ranked products for a keyword
# ─────────────────────────────────────────────────────────

@router.get("/top/{keyword}", summary="Get top ranked products for a keyword")
async def get_top_products(
    keyword: str,
    limit: int = Query(5, ge=1, le=20),
):
    """
    Get the top N ranked products for a keyword.
    Returns products sorted by rank with category labels.
    Used by n8n to feed data into the report generator.
    """
    conn = await _get_db_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT
                p.id::text, p.title, p.source, p.price,
                p.rating, p.review_count, p.image_url, p.product_url,
                pa.sentiment_label, pa.pros, pa.cons, pa.summary,
                r.score, r.rank_position, r.category, r.price_tier
            FROM products p
            LEFT JOIN product_analysis pa ON pa.product_id = p.id
            LEFT JOIN product_rankings  r  ON r.product_id  = p.id
            WHERE LOWER(p.keyword) = LOWER($1)
              AND r.rank_position IS NOT NULL
            ORDER BY r.rank_position ASC
            LIMIT $2
            """,
            keyword,
            limit,
        )

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No ranked products for '{keyword}'. Run /rank first."
            )

        products = []
        for row in rows:
            p = dict(row)
            p["pros"] = json.loads(p["pros"]) if p.get("pros") else []
            p["cons"] = json.loads(p["cons"]) if p.get("cons") else []
            products.append(p)

        return {
            "keyword": keyword,
            "top_products": products,
        }

    finally:
        await conn.close()


