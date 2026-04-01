"""
Report generation and retrieval endpoints.

Endpoints:
  POST /api/v1/reports/generate — generate a full market research report
  GET  /api/v1/reports — list all reports
  GET  /api/v1/reports/{id} — get a single report
  GET  /api/v1/reports/{id}/download — download PDF
  POST /api/v1/reports/full-pipeline — run scrape+analyze+rank+report in one call
"""
import json
from pathlib import Path
from typing import Optional
import asyncpg
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from loguru import logger
from config import get_settings
from models.report import ReportRequest
from services.report_service import generate_full_report

router = APIRouter()
settings = get_settings()


async def _get_db_conn():
    return await asyncpg.connect(settings.database_url)


# ─────────────────────────────────────────────────────────
# Generate report
# ─────────────────────────────────────────────────────────

@router.post("/generate", summary="Generate a full market research report")
async def generate_report(request: ReportRequest):
    """
    Generate a complete market research report for a keyword.

    Prerequisites — must run in order first:
      1. POST /api/v1/scrape/search
      2. POST /api/v1/analysis/analyze (batch)
      3. POST /api/v1/products/rank

    This endpoint then:
      - Fetches all ranked products from DB
      - Generates AI executive summary and market overview
      - Builds a professional PDF report
      - Optionally emails the report
      - Saves everything to the reports table
    """
    try:
        result = await generate_full_report(
            keyword = request.keyword,
            send_email = request.send_email,
            email_to = request.email_to,
            generate_pdf = request.generate_pdf,
            triggered_by = "manual",
        )
        return result

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Report generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────
# List reports
# ─────────────────────────────────────────────────────────

@router.get("", summary="List all generated reports")
async def list_reports(
    limit:  int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None, description="pending | complete | failed"),
):
    """List all reports with optional status filter."""
    conn = await _get_db_conn()
    try:
        conditions = []
        params = []
        idx = 1

        if status:
            conditions.append(f"status = ${idx}")
            params.append(status)
            idx += 1

        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.extend([limit, offset])

        rows = await conn.fetch(
            f"""
            SELECT id::text, keyword, title, status, summary,
                   products_count, sources_used, triggered_by,
                   created_at, completed_at
            FROM reports
            {where}
            ORDER BY created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}
            """,
            *params,
        )

        return {
            "total": len(rows),
            "reports": [dict(r) for r in rows],
        }

    finally:
        await conn.close()


# ─────────────────────────────────────────────────────────
# Get single report
# ─────────────────────────────────────────────────────────

@router.get("/{report_id}", summary="Get a report by ID")
async def get_report(report_id: str):
    """Retrieve a full report including AI content and product list."""
    conn = await _get_db_conn()
    try:
        row = await conn.fetchrow(
            "SELECT * FROM reports WHERE id::text = $1",
            report_id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Report not found")

        result = dict(row)
        if result.get("content") and isinstance(result["content"], str):
            result["content"] = json.loads(result["content"])
        if result.get("sources_used") and isinstance(result["sources_used"], str):
            result["sources_used"] = json.loads(result["sources_used"])

        return result

    finally:
        await conn.close()


# ─────────────────────────────────────────────────────────
# Download PDF
# ─────────────────────────────────────────────────────────

@router.get("/{report_id}/download", summary="Download the PDF report")
async def download_report(report_id: str):
    """
    Download the generated PDF for a report.
    Returns 404 if PDF was not generated or file is missing.
    """
    conn = await _get_db_conn()
    try:
        row = await conn.fetchrow(
            "SELECT pdf_path, keyword FROM reports WHERE id::text = $1",
            report_id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Report not found")

        pdf_path = row["pdf_path"]
        if not pdf_path or not Path(pdf_path).exists():
            raise HTTPException(
                status_code=404,
                detail="PDF not found. Re-generate with generate_pdf=true."
            )

        return FileResponse(
            path = pdf_path,
            media_type = "application/pdf",
            filename = f"market_report_{row['keyword'].replace(' ', '_')}.pdf",
        )

    finally:
        await conn.close()


# ─────────────────────────────────────────────────────────
# Full pipeline — one endpoint to run everything
# ─────────────────────────────────────────────────────────

@router.post("/full-pipeline", summary="Run complete pipeline: scrape → analyze → rank → report")
async def run_full_pipeline(
    keyword: str = Query(..., description="Search keyword"),
    sources: str = Query("amazon,ebay,walmart", description="Comma-separated sources"),
    max_results: int = Query(10, ge=1, le=30),
    send_email: bool = Query(False),
    email_to: Optional[str] = Query(None),
):
    """
    Convenience endpoint that runs the complete pipeline in one call.

    Steps executed in sequence:
      1. Scrape all sources
      2. Analyze all products with AI
      3. Rank all products
      4. Generate report + PDF

    This is what n8n calls via a single HTTP Request node
    for the simplified workflow path.
    """
    import asyncpg
    from services.scraper_service import scrape_all_sources
    from services.ai_service import analyze_product_full
    from services.ranking_service import rank_products
    import json as _json
    import uuid

    source_list = [s.strip() for s in sources.split(",")]
    session_id  = str(uuid.uuid4())

    logger.info(f"Full pipeline started — keyword='{keyword}' session={session_id}")

    conn = await _get_db_conn()
    try:
        logger.info("Pipeline step 1/4: Scraping...")
        raw_products = await scrape_all_sources(
            keyword = keyword,
            sources = source_list,
            max_per_source = max_results,
        )

        if not raw_products:
            raise HTTPException(status_code=404, detail="No products found during scraping")

        product_ids = []
        for p in raw_products:
            row = await conn.fetchrow(
                """
                INSERT INTO products
                    (external_id, source, keyword, title, price, currency,
                     rating, review_count, image_url, product_url, brand,
                     availability, raw_data)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)
                ON CONFLICT DO NOTHING
                RETURNING id::text
                """,
                p.get("external_id"), p["source"], p["keyword"], p["title"],
                p.get("price"), p.get("currency", "USD"), p.get("rating"),
                p.get("review_count", 0), p.get("image_url"), p.get("product_url"),
                p.get("brand"), p.get("availability"), _json.dumps(p),
            )
            if row:
                product_ids.append(row["id"])

        logger.info(f"Pipeline step 2/4: Analyzing {len(product_ids)} products...")
        product_rows = await conn.fetch(
            "SELECT * FROM products WHERE id::text = ANY($1::text[])",
            product_ids,
        )

        import asyncio
        for row in product_rows:
            product = dict(row)
            product["id"] = str(product["id"])
            analysis = await analyze_product_full(product)
            await conn.execute(
                """
                INSERT INTO product_analysis
                    (product_id, sentiment_score, sentiment_label,
                     pros, cons, fake_review_risk, summary,
                     keywords_extracted, model_used, tokens_used)
                VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7,$8::jsonb,$9,$10)
                ON CONFLICT (product_id) DO UPDATE SET
                    sentiment_score=EXCLUDED.sentiment_score,
                    sentiment_label=EXCLUDED.sentiment_label,
                    pros=EXCLUDED.pros, cons=EXCLUDED.cons,
                    fake_review_risk=EXCLUDED.fake_review_risk,
                    summary=EXCLUDED.summary,
                    keywords_extracted=EXCLUDED.keywords_extracted,
                    model_used=EXCLUDED.model_used,
                    tokens_used=EXCLUDED.tokens_used,
                    analyzed_at=NOW()
                """,
                product["id"],
                analysis["sentiment_score"], analysis["sentiment_label"],
                _json.dumps(analysis["pros"]), _json.dumps(analysis["cons"]),
                analysis["fake_review_risk"], analysis["summary"],
                _json.dumps(analysis["keywords_extracted"]),
                analysis["model_used"], analysis["tokens_used"],
            )
            await asyncio.sleep(0.3)

        logger.info("Pipeline step 3/4: Ranking...")
        rows = await conn.fetch(
            """
            SELECT p.id::text, p.title, p.price, p.rating, p.review_count,
                   p.source, p.keyword, p.image_url, p.product_url, p.brand,
                   pa.sentiment_score, pa.sentiment_label,
                   pa.pros, pa.cons, pa.summary, pa.fake_review_risk
            FROM products p
            LEFT JOIN product_analysis pa ON pa.product_id = p.id
            WHERE LOWER(p.keyword) = LOWER($1)
            """,
            keyword,
        )
        products_for_ranking = []
        for row in rows:
            p = dict(row)
            p["pros"] = _json.loads(p["pros"]) if p.get("pros") else []
            p["cons"] = _json.loads(p["cons"]) if p.get("cons") else []
            products_for_ranking.append(p)

        ranked = rank_products(products_for_ranking)

        for product in ranked:
            await conn.execute(
                """
                INSERT INTO product_rankings
                    (product_id, keyword, score, rank_position, category, price_tier)
                VALUES ($1::uuid, $2, $3, $4, $5, $6)
                ON CONFLICT (product_id) DO UPDATE SET
                    score=EXCLUDED.score, rank_position=EXCLUDED.rank_position,
                    category=EXCLUDED.category, price_tier=EXCLUDED.price_tier,
                    ranked_at=NOW()
                """,
                product["id"], keyword,
                product["score"], product["rank_position"],
                product["category"], product["price_tier"],
            )

        logger.info("Pipeline step 4/4: Generating report...")
        result = await generate_full_report(
            keyword = keyword,
            send_email = send_email,
            email_to = email_to,
            generate_pdf = True,
            triggered_by = "pipeline",
        )

        logger.success(f"Full pipeline complete for '{keyword}'")
        return {
            "session_id": session_id,
            "keyword": keyword,
            "products_found": len(raw_products),
            "products_saved": len(product_ids),
            "report_id": result["report_id"],
            "pdf_path": result.get("pdf_path"),
            "summary": result.get("summary"),
            "status": "complete",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Full pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()

