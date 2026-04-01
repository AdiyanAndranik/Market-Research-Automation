"""
Market research report assembly, PDF generation, and email delivery.

Flow:
    1. Fetch ranked products + market stats from DB
    2. Call AI to generate executive summary and market overview
    3. Assemble full structured report
    4. Generate PDF using ReportLab
    5. Save report record to DB
    6. Optionally send via email
"""
import json
import os
import smtplib
import uuid
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional
import asyncpg
from loguru import logger
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from config import get_settings
from services.ai_service import generate_market_overview
from services.ranking_service import compute_market_stats

settings = get_settings()

PDF_OUTPUT_DIR = Path("/app/reports/output")
PDF_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────
# Safe type helpers
# ─────────────────────────────────────────────────────────

def _f(value) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _i(value) -> int:
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


# ─────────────────────────────────────────────────────────
# Fetch data from DB
# ─────────────────────────────────────────────────────────

async def _fetch_ranked_products(conn, keyword: str) -> list:
    """Fetch all ranked products with analysis for a keyword."""
    rows = await conn.fetch(
        """
        SELECT
            p.id::text,
            p.title,
            p.source,
            p.price,
            p.rating,
            p.review_count,
            p.brand,
            p.product_url,
            p.image_url,
            pa.sentiment_label,
            pa.sentiment_score,
            pa.pros,
            pa.cons,
            pa.summary,
            pa.fake_review_risk,
            r.score,
            r.rank_position,
            r.category,
            r.price_tier
        FROM products p
        LEFT JOIN product_analysis pa ON pa.product_id = p.id
        LEFT JOIN product_rankings  r  ON r.product_id  = p.id
        WHERE LOWER(p.keyword) = LOWER($1)
          AND r.rank_position IS NOT NULL
        ORDER BY r.rank_position ASC
        """,
        keyword,
    )

    products = []
    for row in rows:
        p = dict(row)
        p["pros"] = json.loads(p["pros"]) if p.get("pros") else []
        p["cons"] = json.loads(p["cons"]) if p.get("cons") else []
        products.append(p)

    return products


# ─────────────────────────────────────────────────────────
# PDF Styles
# ─────────────────────────────────────────────────────────

def _build_styles() -> dict:
    """Build all paragraph styles for the PDF."""
    base = getSampleStyleSheet()

    DARK_BLUE = colors.HexColor("#1a237e")
    MID_BLUE = colors.HexColor("#283593")
    ACCENT = colors.HexColor("#1565c0")
    LIGHT_GRAY = colors.HexColor("#f5f5f5")
    TEXT_DARK = colors.HexColor("#212121")
    TEXT_GRAY = colors.HexColor("#616161")
    WHITE = colors.white

    return {
        "cover_title": ParagraphStyle(
            "cover_title",
            fontName="Helvetica-Bold",
            fontSize=28,
            textColor=WHITE,
            alignment=TA_CENTER,
            spaceAfter=12,
        ),
        "cover_subtitle": ParagraphStyle(
            "cover_subtitle",
            fontName="Helvetica",
            fontSize=14,
            textColor=colors.HexColor("#90caf9"),
            alignment=TA_CENTER,
            spaceAfter=8,
        ),
        "cover_meta": ParagraphStyle(
            "cover_meta",
            fontName="Helvetica",
            fontSize=10,
            textColor=colors.HexColor("#bbdefb"),
            alignment=TA_CENTER,
        ),
        "section_heading": ParagraphStyle(
            "section_heading",
            fontName="Helvetica-Bold",
            fontSize=16,
            textColor=DARK_BLUE,
            spaceBefore=20,
            spaceAfter=8,
            borderPadding=(0, 0, 4, 0),
        ),
        "subsection_heading": ParagraphStyle(
            "subsection_heading",
            fontName="Helvetica-Bold",
            fontSize=12,
            textColor=MID_BLUE,
            spaceBefore=12,
            spaceAfter=6,
        ),
        "body": ParagraphStyle(
            "body",
            fontName="Helvetica",
            fontSize=10,
            textColor=TEXT_DARK,
            leading=16,
            spaceAfter=8,
        ),
        "body_gray": ParagraphStyle(
            "body_gray",
            fontName="Helvetica",
            fontSize=9,
            textColor=TEXT_GRAY,
            leading=14,
        ),
        "product_title": ParagraphStyle(
            "product_title",
            fontName="Helvetica-Bold",
            fontSize=11,
            textColor=DARK_BLUE,
            spaceAfter=4,
        ),
        "badge": ParagraphStyle(
            "badge",
            fontName="Helvetica-Bold",
            fontSize=8,
            textColor=WHITE,
            alignment=TA_CENTER,
        ),
        "stat_number": ParagraphStyle(
            "stat_number",
            fontName="Helvetica-Bold",
            fontSize=22,
            textColor=DARK_BLUE,
            alignment=TA_CENTER,
        ),
        "stat_label": ParagraphStyle(
            "stat_label",
            fontName="Helvetica",
            fontSize=9,
            textColor=TEXT_GRAY,
            alignment=TA_CENTER,
        ),
        "footer": ParagraphStyle(
            "footer",
            fontName="Helvetica",
            fontSize=8,
            textColor=TEXT_GRAY,
            alignment=TA_CENTER,
        ),
        "pros": ParagraphStyle(
            "pros",
            fontName="Helvetica",
            fontSize=9,
            textColor=colors.HexColor("#2e7d32"),
            leading=14,
        ),
        "cons": ParagraphStyle(
            "cons",
            fontName="Helvetica",
            fontSize=9,
            textColor=colors.HexColor("#c62828"),
            leading=14,
        ),
    }


CATEGORY_COLORS = {
    "best_quality": colors.HexColor("#1565c0"),
    "best_value": colors.HexColor("#2e7d32"),
    "cheapest": colors.HexColor("#6a1b9a"),
    "most_popular": colors.HexColor("#e65100"),
    "hidden_gem": colors.HexColor("#00695c"),
    "standard": colors.HexColor("#546e7a"),
}

CATEGORY_LABELS = {
    "best_quality": "BEST QUALITY",
    "best_value": "BEST VALUE",
    "cheapest": "CHEAPEST",
    "most_popular": "MOST POPULAR",
    "hidden_gem": "HIDDEN GEM",
    "standard": "RANKED",
}


# ─────────────────────────────────────────────────────────
# PDF Builder
# ─────────────────────────────────────────────────────────

def _add_header_footer(canvas, doc):
    """Add header and footer to every page after the first."""
    canvas.saveState()
    page_num = canvas.getPageNumber()

    if page_num > 1:
        canvas.setStrokeColor(colors.HexColor("#1a237e"))
        canvas.setLineWidth(0.5)
        canvas.line(2 * cm, A4[1] - 1.5 * cm, A4[0] - 2 * cm, A4[1] - 1.5 * cm)

        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#616161"))
        canvas.drawString(2 * cm, A4[1] - 1.2 * cm, "AI Market Research Agent")
        canvas.drawRightString(
            A4[0] - 2 * cm, A4[1] - 1.2 * cm,
            f"Confidential — {datetime.now().strftime('%B %Y')}"
        )

        canvas.line(2 * cm, 1.5 * cm, A4[0] - 2 * cm, 1.5 * cm)
        canvas.drawString(2 * cm, 1.0 * cm, "Generated by AI Market Research Agent")
        canvas.drawRightString(
            A4[0] - 2 * cm, 1.0 * cm, f"Page {page_num}"
        )

    canvas.restoreState()


def generate_pdf(
    keyword: str,
    products: list,
    stats: dict,
    ai_overview: dict,
    report_id: str,
) -> str:
    """
    Generate a professional PDF market research report.
    Returns the file path of the generated PDF.
    """
    filename = f"report_{report_id}.pdf"
    filepath = str(PDF_OUTPUT_DIR / filename)
    styles = _build_styles()
    story = []

    DARK_BLUE = colors.HexColor("#1a237e")
    LIGHT_BLUE = colors.HexColor("#e3f2fd")
    LIGHT_GRAY = colors.HexColor("#f5f5f5")
    GREEN = colors.HexColor("#e8f5e9")
    WHITE = colors.white

    doc = SimpleDocTemplate(
        filepath,
        pagesize=A4,
        rightMargin=2 * cm,
        leftMargin=2 * cm,
        topMargin=2.5 * cm,
        bottomMargin=2.5 * cm,
        title=f"Market Research Report — {keyword.title()}",
        author="AI Market Research Agent",
    )

    cover_table = Table(
        [[Paragraph(f"Market Research Report", styles["cover_title"])],
         [Paragraph(f'"{keyword.title()}"', styles["cover_subtitle"])],
         [Spacer(1, 0.5 * cm)],
         [Paragraph(
             f"Generated: {datetime.now().strftime('%B %d, %Y')}  |  "
             f"{stats.get('total_products', 0)} Products Analyzed  |  "
             f"{len(stats.get('sources', []))} Sources",
             styles["cover_meta"]
         )],
         [Spacer(1, 0.3 * cm)],
         [Paragraph("Powered by AI Market Research Agent", styles["cover_meta"])],
        ],
        colWidths=[17 * cm],
    )
    cover_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), DARK_BLUE),
        ("ROUNDEDCORNERS", [8]),
        ("TOPPADDING", (0, 0), (-1, -1), 30),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 30),
        ("LEFTPADDING", (0, 0), (-1, -1), 20),
        ("RIGHTPADDING", (0, 0), (-1, -1), 20),
    ]))
    story.append(Spacer(1, 2 * cm))
    story.append(cover_table)
    story.append(Spacer(1, 1 * cm))

    price_dist = stats.get("price_distribution", {})
    avg_price = _f(price_dist.get("avg", 0))
    avg_rating = _f(stats.get("avg_rating", 0))
    sentiments = stats.get("sentiment_breakdown", {})
    pos_pct = 0
    total_sent = sum(sentiments.values())
    if total_sent > 0:
        pos_pct = round(sentiments.get("positive", 0) / total_sent * 100)

    stat_data = [
        [
            Paragraph(str(stats.get("total_products", 0)), styles["stat_number"]),
            Paragraph(f"${avg_price:.2f}", styles["stat_number"]),
            Paragraph(f"{avg_rating:.1f}/5", styles["stat_number"]),
            Paragraph(f"{pos_pct}%", styles["stat_number"]),
        ],
        [
            Paragraph("Products", styles["stat_label"]),
            Paragraph("Avg Price", styles["stat_label"]),
            Paragraph("Avg Rating", styles["stat_label"]),
            Paragraph("Positive", styles["stat_label"]),
        ],
    ]
    stat_table = Table(stat_data, colWidths=[4.25 * cm] * 4)
    stat_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BLUE),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
        ("LINEAFTER", (0, 0), (2, -1), 0.5, colors.HexColor("#bbdefb")),
        ("ROUNDEDCORNERS", [6]),
    ]))
    story.append(stat_table)
    story.append(PageBreak())

    story.append(Paragraph("Executive Summary", styles["section_heading"]))
    story.append(HRFlowable(width="100%", thickness=1, color=DARK_BLUE, spaceAfter=10))

    if ai_overview.get("overview"):
        story.append(Paragraph(ai_overview["overview"], styles["body"]))
    story.append(Spacer(1, 0.3 * cm))

    if ai_overview.get("opportunity"):
        story.append(Paragraph("Market Opportunity", styles["subsection_heading"]))
        story.append(Paragraph(ai_overview["opportunity"], styles["body"]))

    if ai_overview.get("recommendation"):
        story.append(Paragraph("Our Recommendation", styles["subsection_heading"]))
        rec_table = Table(
            [[Paragraph(ai_overview["recommendation"], styles["body"])]],
            colWidths=[17 * cm],
        )
        rec_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), GREEN),
            ("LEFTPADDING", (0, 0), (-1, -1), 12),
            ("RIGHTPADDING", (0, 0), (-1, -1), 12),
            ("TOPPADDING", (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ("LINEBEFORETHE", (0, 0), (0, -1), 3, colors.HexColor("#2e7d32")),
        ]))
        story.append(rec_table)

    story.append(Spacer(1, 0.5 * cm))

    story.append(Paragraph("Market Statistics", styles["section_heading"]))
    story.append(HRFlowable(width="100%", thickness=1, color=DARK_BLUE, spaceAfter=10))

    story.append(Paragraph("Price Distribution", styles["subsection_heading"]))
    price_table_data = [
        ["Metric", "Value"],
        ["Lowest Price", f"${_f(price_dist.get('min', 0)):.2f}"],
        ["Highest Price", f"${_f(price_dist.get('max', 0)):.2f}"],
        ["Average Price", f"${_f(price_dist.get('avg', 0)):.2f}"],
        ["Median Price", f"${_f(price_dist.get('median', 0)):.2f}"],
        ["Budget (< $30)", str(price_dist.get("budget_count", 0)) + " products"],
        ["Mid ($30–$100)", str(price_dist.get("mid_count", 0)) + " products"],
        ["Premium (> $100)", str(price_dist.get("premium_count", 0)) + " products"],
    ]
    price_table = Table(price_table_data, colWidths=[8.5 * cm, 8.5 * cm])
    price_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), DARK_BLUE),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        ("BACKGROUND", (0, 1), (-1, -1), LIGHT_GRAY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GRAY]),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#e0e0e0")),
    ]))
    story.append(price_table)
    story.append(Spacer(1, 0.5 * cm))

    story.append(Paragraph("Customer Sentiment", styles["subsection_heading"]))
    sent_data = [["Sentiment", "Count", "Percentage"]]
    for label, count in sentiments.items():
        pct = round(count / total_sent * 100) if total_sent > 0 else 0
        sent_data.append([label.title(), str(count), f"{pct}%"])

    sent_table = Table(sent_data, colWidths=[5.67 * cm] * 3)
    sent_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), DARK_BLUE),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GRAY]),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("ALIGN", (1, 0), (-1, -1), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#e0e0e0")),
    ]))
    story.append(sent_table)
    story.append(Spacer(1, 0.5 * cm))

    top_pros = stats.get("top_pros", [])
    top_cons = stats.get("top_cons", [])

    if top_pros or top_cons:
        story.append(Paragraph("What Customers Say", styles["subsection_heading"]))
        pros_text = "<br/>".join([f"✓  {p}" for p in top_pros]) if top_pros else "N/A"
        cons_text = "<br/>".join([f"✗  {c}" for c in top_cons]) if top_cons else "N/A"

        voice_data = [[
            Paragraph("<b>Top Positives</b><br/><br/>" + pros_text, styles["pros"]),
            Paragraph("<b>Common Complaints</b><br/><br/>" + cons_text, styles["cons"]),
        ]]
        voice_table = Table(voice_data, colWidths=[8.5 * cm, 8.5 * cm])
        voice_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (0, 0), colors.HexColor("#f1f8e9")),
            ("BACKGROUND", (1, 0), (1, 0), colors.HexColor("#ffebee")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 12),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
            ("LEFTPADDING", (0, 0), (-1, -1), 12),
            ("RIGHTPADDING", (0, 0), (-1, -1), 12),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#e0e0e0")),
        ]))
        story.append(voice_table)

    story.append(PageBreak())

    story.append(Paragraph("Top Ranked Products", styles["section_heading"]))
    story.append(HRFlowable(width="100%", thickness=1, color=DARK_BLUE, spaceAfter=10))
    story.append(Paragraph(
        "Products ranked by our AI scoring algorithm: "
        "40% rating quality + 30% review volume + 30% value for money.",
        styles["body_gray"]
    ))
    story.append(Spacer(1, 0.3 * cm))

    top_products = products[:10]
    for product in top_products:
        category = product.get("category", "standard")
        badge_color = CATEGORY_COLORS.get(category, colors.HexColor("#546e7a"))
        badge_label = CATEGORY_LABELS.get(category, "RANKED")

        price = _f(product.get("price"))
        rating = _f(product.get("rating"))
        reviews = _i(product.get("review_count"))
        score = _f(product.get("score"))
        rank = _i(product.get("rank_position"))
        pros = product.get("pros") or []
        cons = product.get("cons") or []
        summary = product.get("summary") or ""
        fake_risk = product.get("fake_review_risk") or "unknown"
        sentiment = product.get("sentiment_label") or "neutral"
        source = product.get("source", "").upper()
        price_tier = product.get("price_tier", "")

        badge_table = Table(
            [[Paragraph(badge_label, styles["badge"])]],
            colWidths=[3.5 * cm],
        )
        badge_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), badge_color),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("ROUNDEDCORNERS", [4]),
        ]))

        title_text  = product.get("title", "")[:80]
        header_data = [[
            Paragraph(f"#{rank}", styles["subsection_heading"]),
            Paragraph(title_text, styles["product_title"]),
            badge_table,
        ]]
        header_table = Table(header_data, colWidths=[1.2 * cm, 12.3 * cm, 3.5 * cm])
        header_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (2, 0), (2, 0), "RIGHT"),
        ]))

        stars = "★" * int(rating) + "☆" * (5 - int(rating))
        sentiment_map = {"positive": "😊 Positive", "neutral": "😐 Neutral", "negative": "😞 Negative"}
        risk_map = {"low": "✓ Low", "medium": "⚠ Medium", "high": "✗ High", "unknown": "? Unknown"}

        metrics_data = [[
            Paragraph(f"<b>${price:.2f}</b>  {price_tier.title()}", styles["body"]),
            Paragraph(f"{stars} {rating:.1f}  ({reviews:,} reviews)", styles["body"]),
            Paragraph(f"Score: <b>{score:.3f}</b>", styles["body"]),
            Paragraph(f"Source: <b>{source}</b>", styles["body"]),
        ]]
        metrics_table = Table(metrics_data, colWidths=[4.25 * cm] * 4)
        metrics_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), LIGHT_GRAY),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
        ]))

        summary_para = Paragraph(summary, styles["body_gray"]) if summary else Spacer(1, 0)

        pros_text = "  ".join([f"✓ {p}" for p in pros[:3]]) if pros else ""
        cons_text = "  ".join([f"✗ {c}" for c in cons[:2]]) if cons else ""

        pros_cons_data = [[
            Paragraph(pros_text, styles["pros"]) if pros_text else Paragraph("", styles["body"]),
            Paragraph(cons_text, styles["cons"]) if cons_text else Paragraph("", styles["body"]),
        ]]
        pros_cons_table = Table(pros_cons_data, colWidths=[8.5 * cm, 8.5 * cm])
        pros_cons_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ]))

        ai_row_data = [[
            Paragraph(
                f"Sentiment: {sentiment_map.get(sentiment, sentiment)}  |  "
                f"Fake Review Risk: {risk_map.get(fake_risk, fake_risk)}",
                styles["body_gray"]
            ),
        ]]
        ai_row_table = Table(ai_row_data, colWidths=[17 * cm])
        ai_row_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#ede7f6")),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ]))

        card_data = [[header_table],
                     [metrics_table],
                     [summary_para],
                     [pros_cons_table],
                     [ai_row_table]]
        card = Table(card_data, colWidths=[17 * cm])
        card.setStyle(TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#90caf9")),
            ("TOPPADDING", (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("ROUNDEDCORNERS", [6]),
        ]))

        story.append(card)
        story.append(Spacer(1, 0.4 * cm))

    story.append(PageBreak())

    story.append(Paragraph("Full Product Comparison", styles["section_heading"]))
    story.append(HRFlowable(width="100%", thickness=1, color=DARK_BLUE, spaceAfter=10))

    table_data = [["#", "Product", "Source", "Price", "Rating", "Reviews", "Score", "Category"]]
    for p in products:
        title_short = (p.get("title") or "")[:40] + ("..." if len(p.get("title") or "") > 40 else "")
        table_data.append([
            str(_i(p.get("rank_position"))),
            title_short,
            (p.get("source") or "").upper(),
            f"${_f(p.get('price')):.2f}" if p.get("price") else "N/A",
            f"{_f(p.get('rating')):.1f}" if p.get("rating") else "N/A",
            f"{_i(p.get('review_count')):,}",
            f"{_f(p.get('score')):.3f}",
            CATEGORY_LABELS.get(p.get("category", "standard"), "RANKED"),
        ])

    full_table = Table(
        table_data,
        colWidths=[0.8*cm, 5.5*cm, 1.8*cm, 1.8*cm, 1.5*cm, 1.8*cm, 1.5*cm, 2.3*cm],
    )
    full_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), DARK_BLUE),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GRAY]),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 7.5),
        ("ALIGN", (0, 0), (0, -1), "CENTER"),
        ("ALIGN", (3, 0), (-1, -1), "RIGHT"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#e0e0e0")),
    ]))
    story.append(full_table)

    doc.build(story, onFirstPage=_add_header_footer, onLaterPages=_add_header_footer)
    logger.success(f"PDF generated: {filepath}")
    return filepath


# ─────────────────────────────────────────────────────────
# Email delivery
# ─────────────────────────────────────────────────────────

def send_report_email(
    to_email: str,
    keyword: str,
    pdf_path: str,
    summary: str,
) -> bool:
    """
    Send the generated PDF report via email.
    Uses SMTP settings from config.
    Returns True on success, False on failure.
    """
    if not settings.smtp_user or not settings.smtp_password:
        logger.warning("Email not configured — skipping send")
        return False

    try:
        msg = MIMEMultipart()
        msg["From"] = settings.smtp_user
        msg["To"] = to_email
        msg["Subject"] = f"Market Research Report: {keyword.title()}"

        body = f"""
Hello,

Your AI Market Research Report for "{keyword.title()}" is ready.

Summary:
{summary}

Please find the full PDF report attached.

---
AI Market Research Agent
        """.strip()

        msg.attach(MIMEText(body, "plain"))

        with open(pdf_path, "rb") as f:
            pdf_attachment = MIMEApplication(f.read(), _subtype="pdf")
            pdf_attachment.add_header(
                "Content-Disposition",
                "attachment",
                filename=os.path.basename(pdf_path),
            )
            msg.attach(pdf_attachment)

        with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
            server.starttls()
            server.login(settings.smtp_user, settings.smtp_password)
            server.send_message(msg)

        logger.success(f"Report emailed to {to_email}")
        return True

    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return False


# ─────────────────────────────────────────────────────────
# Main report generation pipeline
# ─────────────────────────────────────────────────────────

async def generate_full_report(
    keyword: str,
    send_email: bool = False,
    email_to: Optional[str] = None,
    generate_pdf: bool = True,
    triggered_by: str = "manual",
) -> dict:
    """
    Full report generation pipeline.

    1. Fetch ranked products from DB
    2. Compute market stats
    3. Generate AI market overview
    4. Build PDF
    5. Save report to DB
    6. Optionally email report
    """
    report_id = str(uuid.uuid4())
    logger.info(f"[{report_id}] Generating report for '{keyword}'")

    conn = await asyncpg.connect(settings.database_url)
    try:
        await conn.execute(
            """
            INSERT INTO reports (id, keyword, status, triggered_by)
            VALUES ($1, $2, 'running', $3)
            """,
            report_id, keyword, triggered_by,
        )

        products = await _fetch_ranked_products(conn, keyword)
        if not products:
            await conn.execute(
                "UPDATE reports SET status='failed', completed_at=NOW() WHERE id=$1",
                report_id,
            )
            raise ValueError(
                f"No ranked products found for '{keyword}'. "
                "Run scrape → analyze → rank first."
            )

        stats = compute_market_stats(products)

        price_dist = stats.get("price_distribution", {})
        ai_overview = await generate_market_overview(
            keyword       = keyword,
            total_products= stats.get("total_products", 0),
            avg_price     = _f(price_dist.get("avg", 0)),
            avg_rating    = _f(stats.get("avg_rating", 0)),
            top_pros      = stats.get("top_pros", []),
            top_cons      = stats.get("top_cons", []),
            price_range   = {
                "min": _f(price_dist.get("min", 0)),
                "max": _f(price_dist.get("max", 0)),
            },
        )

        summary = ai_overview.get("overview", f"Market research report for {keyword}")

        pdf_path = None
        if generate_pdf:
            pdf_path = generate_pdf_report(
                keyword = keyword,
                products = products,
                stats = stats,
                ai_overview = ai_overview,
                report_id = report_id,
            )

        report_content = {
            "keyword": keyword,
            "stats": stats,
            "ai_overview": ai_overview,
            "top_products": [
                {
                    "rank": _i(p.get("rank_position")),
                    "title": p.get("title"),
                    "source": p.get("source"),
                    "price": _f(p.get("price")),
                    "rating": _f(p.get("rating")),
                    "score": _f(p.get("score")),
                    "category": p.get("category"),
                    "summary": p.get("summary"),
                    "pros": p.get("pros", []),
                    "cons": p.get("cons", []),
                }
                for p in products[:10]
            ],
        }

        await conn.execute(
            """
            UPDATE reports SET
                title = $1,
                summary = $2,
                content = $3::jsonb,
                pdf_path = $4,
                products_count = $5,
                sources_used = $6::jsonb,
                status = 'complete',
                completed_at = NOW()
            WHERE id = $7
            """,
            f"Market Research: {keyword.title()}",
            summary,
            json.dumps(report_content),
            pdf_path,
            len(products),
            json.dumps(stats.get("sources", [])),
            report_id,
        )

        logger.success(f"[{report_id}] Report complete — {len(products)} products")

        email_sent = False
        if send_email and email_to and pdf_path:
            email_sent = send_report_email(email_to, keyword, pdf_path, summary)

        return {
            "report_id":   report_id,
            "keyword":     keyword,
            "status":      "complete",
            "summary":     summary,
            "pdf_path":    pdf_path,
            "email_sent":  email_sent,
            "products":    len(products),
            "content":     report_content,
        }

    except Exception as e:
        logger.error(f"[{report_id}] Report generation failed: {e}")
        await conn.execute(
            "UPDATE reports SET status='failed', completed_at=NOW() WHERE id=$1",
            report_id,
        )
        raise

    finally:
        await conn.close()


def generate_pdf_report(
    keyword: str,
    products: list,
    stats: dict,
    ai_overview: dict,
    report_id: str,
) -> str:
    """Thin wrapper so generate_full_report can call generate_pdf without name collision."""
    return generate_pdf(keyword, products, stats, ai_overview, report_id)


