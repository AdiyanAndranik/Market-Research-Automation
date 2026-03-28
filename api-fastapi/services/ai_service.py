"""
OpenAI-powered product analysis.

Handles:
  - Sentiment analysis of reviews
  - Pros & cons extraction
  - Fake review detection
  - Product summary generation
  - Market category summary
"""
import json
from typing import Optional
from loguru import logger
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from config import get_settings

settings = get_settings()
client = AsyncOpenAI(api_key=settings.openai_api_key)


# ─────────────────────────────────────────────────────────
# Core LLM caller — single place for all OpenAI calls
# ─────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def _call_llm(
    system_prompt: str,
    user_prompt: str,
    expect_json: bool = True,
) -> tuple[str, int]:
    """
    Call OpenAI and return (response_text, tokens_used).
    All prompts go through here so we have one place to
    swap models, add logging, or handle rate limits.
    """
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_prompt},
    ]

    response = await client.chat.completions.create(
        model=settings.openai_model,
        messages=messages,
        max_tokens=settings.openai_max_tokens,
        temperature=0.2,        # low temp = consistent, structured output
        response_format={"type": "json_object"} if expect_json else {"type": "text"},
    )

    content = response.choices[0].message.content or ""
    tokens  = response.usage.total_tokens if response.usage else 0

    logger.debug(f"LLM call: {tokens} tokens used")
    return content, tokens


# ─────────────────────────────────────────────────────────
# Sentiment Analysis
# ─────────────────────────────────────────────────────────

async def analyze_sentiment(
    title: str,
    rating: Optional[float],
    review_count: int,
) -> dict:
    """
    Analyze product sentiment based on title, rating, and review volume.

    Returns:
        {
            "sentiment_score": float,   # -1.0 (very negative) to 1.0 (very positive)
            "sentiment_label": str,     # positive | neutral | negative
            "confidence": float         # 0.0 to 1.0
        }
    """
    system_prompt = """You are a product sentiment analysis expert.
    Analyze the product information and return ONLY a JSON object with these exact keys:
    - sentiment_score: float between -1.0 and 1.0
    - sentiment_label: exactly one of "positive", "neutral", or "negative"
    - confidence: float between 0.0 and 1.0

    Base your analysis on: product title tone, rating value, and review volume.
    A rating >= 4.0 with many reviews = positive.
    A rating <= 2.5 = negative.
    No rating = neutral with low confidence.
    """

    user_prompt = f"""Product: {title}
    Rating: {rating or 'Not available'} out of 5
    Number of reviews: {review_count}

    Analyze the sentiment of this product listing."""

    try:
        raw, tokens = await _call_llm(system_prompt, user_prompt)
        result = json.loads(raw)

        # Validate and clamp values
        score = max(-1.0, min(1.0, float(result.get("sentiment_score", 0.0))))
        label = result.get("sentiment_label", "neutral")
        if label not in ("positive", "neutral", "negative"):
            label = "neutral"

        return {
            "sentiment_score": round(score, 3),
            "sentiment_label": label,
            "confidence": round(float(result.get("confidence", 0.5)), 2),
            "tokens_used": tokens,
        }

    except Exception as e:
        logger.error(f"Sentiment analysis failed: {e}")
        return {
            "sentiment_score": 0.0,
            "sentiment_label": "neutral",
            "confidence": 0.0,
            "tokens_used": 0,
        }


# ─────────────────────────────────────────────────────────
# Pros & Cons Extraction
# ─────────────────────────────────────────────────────────

async def extract_pros_cons(
    title: str,
    rating: Optional[float],
    review_count: int,
    price: Optional[float],
    source: str,
) -> dict:
    """
    Extract likely pros and cons for a product based on available data.

    Returns:
        {
            "pros": ["Fast delivery", "Great battery life", ...],
            "cons": ["Expensive", "Poor build quality", ...],
            "keywords": ["wireless", "noise-cancelling", ...]
        }
    """
    system_prompt = """You are an expert e-commerce product analyst.
    Based on the product information provided, infer the most likely pros and cons.
    Use your knowledge of this product category to make intelligent inferences.

    Return ONLY a JSON object with these exact keys:
    - pros: array of 3-5 short strings (each max 8 words)
    - cons: array of 2-4 short strings (each max 8 words)
    - keywords: array of 3-6 important product feature keywords

    Be specific to the product type. Do not use generic phrases like "good product"."""

    user_prompt = f"""Product Title: {title}
    Source: {source}
    Price: ${price if price else 'Unknown'}
    Rating: {rating or 'No rating'} / 5.0
    Review Count: {review_count}

    Extract the pros, cons, and keywords for this product."""

    try:
        raw, tokens = await _call_llm(system_prompt, user_prompt)
        result = json.loads(raw)

        pros = result.get("pros", [])
        cons = result.get("cons", [])
        keywords = result.get("keywords", [])

        # Ensure they are lists of strings
        pros = [str(p) for p in pros if p][:5]
        cons = [str(c) for c in cons if c][:4]
        keywords = [str(k) for k in keywords if k][:6]

        return {
            "pros": pros,
            "cons": cons,
            "keywords": keywords,
            "tokens_used": tokens,
        }

    except Exception as e:
        logger.error(f"Pros/cons extraction failed: {e}")
        return {"pros": [], "cons": [], "keywords": [], "tokens_used": 0}


# ─────────────────────────────────────────────────────────
# Fake Review Detection
# ─────────────────────────────────────────────────────────

async def detect_fake_reviews(
    title: str,
    rating: Optional[float],
    review_count: int,
    price: Optional[float],
) -> dict:
    """
    Detect likelihood of fake or manipulated reviews.

    Red flags: suspiciously high rating with very few reviews,
    extremely low price with perfect rating, etc.

    Returns:
        {
            "risk_level": "low" | "medium" | "high",
            "risk_score": float,   # 0.0 to 1.0
            "reasons": [...]
        }
    """
    system_prompt = """You are a fraud detection specialist for e-commerce reviews.
    Analyze product data for signs of fake or manipulated reviews.

    Red flags to look for:
    - Perfect 5.0 rating with very few reviews (< 10)
    - Very high review count but low rating variance
    - Price is suspiciously low for the product category
    - Title contains excessive superlatives or keyword stuffing

    Return ONLY a JSON object with:
    - risk_level: exactly one of "low", "medium", or "high"
    - risk_score: float between 0.0 and 1.0
    - reasons: array of 1-3 short explanation strings (empty array if low risk)"""

    user_prompt = f"""Product: {title}
    Rating: {rating or 'None'}
    Review Count: {review_count}
    Price: ${price or 'Unknown'}

    Assess the fake review risk for this product."""

    try:
        raw, tokens = await _call_llm(system_prompt, user_prompt)
        result = json.loads(raw)

        risk_level = result.get("risk_level", "low")
        if risk_level not in ("low", "medium", "high"):
            risk_level = "low"

        return {
            "risk_level": risk_level,
            "risk_score": round(float(result.get("risk_score", 0.0)), 2),
            "reasons": result.get("reasons", []),
            "tokens_used": tokens,
        }

    except Exception as e:
        logger.error(f"Fake review detection failed: {e}")
        return {"risk_level": "unknown", "risk_score": 0.0, "reasons": [], "tokens_used": 0}


# ─────────────────────────────────────────────────────────
# Product Summary
# ─────────────────────────────────────────────────────────

async def generate_product_summary(
    title: str,
    price: Optional[float],
    rating: Optional[float],
    review_count: int,
    pros: list,
    cons: list,
    source: str,
) -> dict:
    """
    Generate a short, human-readable product summary.

    Returns:
        {
            "summary": str,   # 2-3 sentence summary
            "tokens_used": int
        }
    """
    system_prompt = """You are a professional product reviewer.
    Write a concise, objective 2-3 sentence summary of the product.
    Mention the price point, rating, and key strengths/weaknesses.
    Write in third person. Be factual and helpful to a buyer.

    Return ONLY a JSON object with key: "summary" """

    user_prompt = f"""Product: {title}
    Source: {source}
    Price: ${price or 'Unknown'}
    Rating: {rating or 'N/A'} / 5.0  ({review_count} reviews)
    Pros: {', '.join(pros) if pros else 'N/A'}
    Cons: {', '.join(cons) if cons else 'N/A'}

    Write a product summary."""

    try:
        raw, tokens = await _call_llm(system_prompt, user_prompt)
        result = json.loads(raw)
        return {
            "summary": result.get("summary", ""),
            "tokens_used": tokens,
        }

    except Exception as e:
        logger.error(f"Summary generation failed: {e}")
        return {"summary": "", "tokens_used": 0}


# ─────────────────────────────────────────────────────────
# Market Overview (used by report generator)
# ─────────────────────────────────────────────────────────

async def generate_market_overview(
    keyword: str,
    total_products: int,
    avg_price: float,
    avg_rating: float,
    top_pros: list,
    top_cons: list,
    price_range: dict,
) -> dict:
    """
    Generate an executive-level market overview for the full report.

    Returns:
        {
            "overview": str,
            "opportunity": str,
            "recommendation": str,
            "tokens_used": int
        }
    """
    system_prompt = """You are a senior market research analyst.
    Write a professional market overview for an e-commerce product category.
    Be data-driven, specific, and actionable.

    Return ONLY a JSON object with these keys:
    - overview: 3-4 sentence market summary paragraph
    - opportunity: 2-3 sentence market opportunity analysis
    - recommendation: 1-2 sentence top recommendation for buyers"""

    user_prompt = f"""Market Research Report for: "{keyword}"

    Data Summary:
    - Total products analyzed: {total_products}
    - Average price: ${avg_price:.2f}
    - Price range: ${price_range.get('min', 0):.2f} — ${price_range.get('max', 0):.2f}
    - Average rating: {avg_rating:.1f} / 5.0

    What customers like most: {', '.join(top_pros[:5]) if top_pros else 'N/A'}
    Common complaints: {', '.join(top_cons[:5]) if top_cons else 'N/A'}

    Write the market overview."""

    try:
        raw, tokens = await _call_llm(system_prompt, user_prompt)
        result = json.loads(raw)
        result["tokens_used"] = tokens
        return result

    except Exception as e:
        logger.error(f"Market overview generation failed: {e}")
        return {
            "overview": "",
            "opportunity": "",
            "recommendation": "",
            "tokens_used": 0,
        }


# ─────────────────────────────────────────────────────────
# Full product analysis pipeline (combines all steps)
# ─────────────────────────────────────────────────────────

async def analyze_product_full(product: dict) -> dict:
    """
    Run all AI analysis steps for a single product.
    Calls sentiment, pros/cons, fake detection, and summary in sequence.

    Returns a complete analysis dict ready to insert into product_analysis table.
    """
    title        = product.get("title", "")
    price        = product.get("price")
    rating       = product.get("rating")
    review_count = product.get("review_count", 0)
    source       = product.get("source", "unknown")

    logger.info(f"Analyzing: {title[:60]}...")

    sentiment_task  = analyze_sentiment(title, rating, review_count)
    pros_cons_task  = extract_pros_cons(title, rating, review_count, price, source)

    import asyncio
    sentiment, pros_cons = await asyncio.gather(sentiment_task, pros_cons_task)

    fake_task    = detect_fake_reviews(title, rating, review_count, price)
    summary_task = generate_product_summary(
        title, price, rating, review_count,
        pros_cons["pros"], pros_cons["cons"], source,
    )

    fake, summary = await asyncio.gather(fake_task, summary_task)

    total_tokens = (
        sentiment.get("tokens_used", 0)
        + pros_cons.get("tokens_used", 0)
        + fake.get("tokens_used", 0)
        + summary.get("tokens_used", 0)
    )

    return {
        "sentiment_score": sentiment["sentiment_score"],
        "sentiment_label": sentiment["sentiment_label"],
        "pros": pros_cons["pros"],
        "cons": pros_cons["cons"],
        "keywords_extracted": pros_cons["keywords"],
        "fake_review_risk": fake["risk_level"],
        "summary": summary["summary"],
        "model_used": settings.openai_model,
        "tokens_used": total_tokens,
    }


