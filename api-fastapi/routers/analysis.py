"""
routers/analysis.py — AI analysis endpoints.

Endpoints:
  POST /api/v1/analysis/analyze        — analyze a list of product IDs
  POST /api/v1/analysis/analyze-single — analyze one product by ID
  GET  /api/v1/analysis/{product_id}   — get existing analysis
"""
import asyncio
import json
import asyncpg
from fastapi import APIRouter, HTTPException
from loguru import logger
from config import get_settings
from models.product import AnalyzeRequest
from services.ai_service import analyze_product_full

router = APIRouter()
settings = get_settings()

async def _get_db_conn():
    return await asyncpg.connect(settings.database_url)

@router.post("/analyze", summary="Run AI analysis on a batch of products")
async def analyze_products(request: AnalyzeRequest):
    """
    Run full AI analysis pipeline on a list of product IDs.

    For each product:
    - Sentiment analysis
    - Pros & cons extraction
    - Fake review detection
    - Product summary generation

    Called by n8n after scraping is complete.
    """

    if not request.product_ids:
        raise HTTPException(status_code=400, detail="No product IDS provided")
    
    conn = await _get_db_conn()
    try:
        id_list = [str(pid) for pid in request.product_ids]
        placeholders = ", ".join(f"${i+1}" for i in range(len(id_list)))

        rows = await conn.fetch(
            f"""
            SELECT id, title, price, rating, review_count, source, keyword
            FROM products
            WHERE id::text = ANY($1::text[])
            """,
            id_list,
        )

        if not rows:
            raise HTTPException(status_code=404, detail="No products found for given IDs")
        
        logger.info(f"Starting AI analysis for {len(rows)} products")
        
        results = []
        for row in rows:
            product = dict(row)
            product["id"] = str(product["id"])

            try:
                analysis = await analyze_product_full(product)

                await conn.execute(
                    """
                    INSERT INTO product_analysis
                        (product_id, sentiment_score, sentiment_label,
                         pros, cons, fake_review_risk, summary,
                         keywords_extracted, model_used, tokens_used)
                    VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8::jsonb, $9, $10)
                    ON CONFLICT (product_id) DO UPDATE SET
                        sentiment_score  = EXCLUDED.sentiment_score,
                        sentiment_label  = EXCLUDED.sentiment_label,
                        pros             = EXCLUDED.pros,
                        cons             = EXCLUDED.cons,
                        fake_review_risk = EXCLUDED.fake_review_risk,
                        summary          = EXCLUDED.summary,
                        keywords_extracted = EXCLUDED.keywords_extracted,
                        model_used       = EXCLUDED.model_used,
                        tokens_used      = EXCLUDED.tokens_used,
                        analyzed_at      = NOW()
                    """,
                    product["id"],
                    analysis["sentiment_score"],
                    analysis["sentiment_label"],
                    json.dumps(analysis["pros"]),
                    json.dumps(analysis["cons"]),
                    analysis["fake_review_risk"],
                    analysis["summary"],
                    json.dumpts(analysis["keywords_extracted"]),
                    analysis["model_used"],
                    analysis["tokens_used"],
                )

                results.append({
                    "product_id": product["id"],
                    "title": product["title"][:60],
                    "status": "analyzed",
                    "sentiment": analysis["sentiment_label"],
                    "fake_risk": analysis["fake_review_risk"],
                    "tokens_used": analysis["tokens_used"],
                })

                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Analysis failed for product {product['id']}: {e}")
                results.append({
                    "product_id": product["id"],
                    "status": "failed",
                    "error": str(e),
                })

        total_tokens = sum(r.get("tokens_used", 0) for r in results)
        logger.success(
            f"Analysis complete: {len(results)} products, "
            f"{total_tokens} total tokens used"
        )

        return {
            "analyzed": len([r for r in results if r.get("status") == "analyzed"]),
            "failed": len([r for r in results if r.get("status") == "failed"]),
            "total_tokens_used": total_tokens,
            "results": results,
        }
    
    finally:
        await conn.close()


@router.post("/analyze-single/{product_id}", summary="Analyze a single product by ID")
async def analyze_single_product(product_id: str):
    """
    Run AI analysis on one product.
    Useful for testing or re-analyzing a specific product.
    """
    conn = await _get_db_conn()
    try:
        row = await conn.fetchrow(
            "SELECT * FROM products WHERE id::text = $1", product_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Product not found")
        
        product = dict(row)
        product["id"] = str(product["id"])
        analysis = await analyze_product_full(product)

        await conn.execute(
            """
            INSERT INTO product_analysis
                (product_id, sentiment_score, sentiment_label,
                pros, cons, fake_review_risk, summary,
                keywords_extracted, model_used, tokens_used)
            VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8::jsonb, $9, $10)
            ON CONFLICT (product_id) DO UPDATE SET
                sentiment_score = EXCLUDED.sentiment_score,
                sentiment_label = EXCLUDED.sentiment_label,
                pros = EXCLUDED.pros,
                cons = EXCLUDED.cons,
                fake_review_risk = EXCLUDED.fake_review_risk,
                summary = EXCLUDED.summary,
                keywords_extracted = EXCLUDED.keywords_extracted,
                model_used = EXCLUDED.model_used,
                tokens_used = EXCLUDED.tokens_used,
                analyzed_at = NOW()
            """,
            product["id"],
            analysis["sentiment_score"],
            analysis["sentiment_label"],
            json.dumps(analysis["pros"]),
            json.dumps(analysis["cons"]),
            analysis["fake_review_risk"],
            analysis["summary"],
            json.dumps(analysis["keywords_extracted"]),
            analysis["model_used"],
            analysis["tokens_used"],
        )

        return {
            "product_id": product_id,
            "title": product["title"],
            "analysis": analysis,
        }

    finally:
        await conn.close()

@router.get("/{product_id}", summary="Get analysis results for a product")
async def get_analysis(product_id: str):
    """
    Retrieve existing AI analysis for a product.
    Returns 404 if product hasn't been analyzed yet.
    """
    conn = await _get_db_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT pa.*, p.title, p.source, p.keyword
            FROM product_analysis pa
            JOIN products p ON p.id = pa.product_id
            WHERE pa.product_id::text = $1
            """,
            product_id,
        )
        if not row:
            raise HTTPException(
                status_code = 404,
                detail = "No analysis found. Run /analyze first."
            )
        return dict(row)
    finally:
        await conn.close()

        
