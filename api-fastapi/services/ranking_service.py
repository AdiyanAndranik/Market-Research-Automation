"""
Product scoring and ranking algorithm.

Scoring formula:
    score = (rating * 0.40) + (log(reviews) * 0.30) + (value_score * 0.30)

Categories assigned after ranking:
    best_quality  — highest overall score
    best_value    — best score-to-price ratio
    cheapest      — lowest price with acceptable rating
    most_popular  — highest review count
    hidden_gem    — high score but low review count (underrated)
"""
import math
from typing import Optional
from loguru import logger


# ─────────────────────────────────────────────────────────
# Price tier classification
# ─────────────────────────────────────────────────────────

def classify_price_tier(price: Optional[float]) -> str:
    """Classify a product into budget / mid / premium tier."""
    if price is None:
        return "unknown"
    if price < 30:
        return "budget"
    if price < 100:
        return "mid"
    return "premium"


# ─────────────────────────────────────────────────────────
# Value score — how much quality per dollar
# ─────────────────────────────────────────────────────────

def compute_value_score(
    price: Optional[float],
    rating: Optional[float],
    all_prices: list[float],
) -> float:
    """
    Compute a value score between 0.0 and 1.0.
    Higher = better value for money.

    Formula: normalize (rating / price) relative to all products.
    Products with no price get a neutral 0.5.
    """
    if not price or price <= 0 or not rating:
        return 0.5

    this_qpd = rating / price

    valid_prices  = [p for p in all_prices if p and p > 0]
    valid_ratings = [rating] * len(valid_prices)

    if not valid_prices:
        return 0.5

    max_qpd = max((r / p for r, p in zip(valid_ratings, valid_prices)), default=1.0)

    if max_qpd == 0:
        return 0.5

    return round(min(this_qpd / max_qpd, 1.0), 4)


# ─────────────────────────────────────────────────────────
# Core scoring formula
# ─────────────────────────────────────────────────────────

def compute_score(
    rating: Optional[float],
    review_count: int,
    price: Optional[float],
    all_prices: list[float],
    sentiment_score: Optional[float] = None,
) -> float:
    """
    Compute a final ranking score for a product.

    Weights:
        40% — rating (normalized to 0-1 from 0-5 scale)
        30% — review volume (log scale so 10k reviews isn't 1000x better than 10)
        30% — value score (quality per dollar, normalized)

    Sentiment score from AI adds a small bonus/penalty (+/- 5%).

    Returns float between 0.0 and 1.0.
    """
    rating_norm = (rating / 5.0) if rating else 0.0
    rating_component = rating_norm * 0.40

    if review_count > 0:
        review_log  = math.log(review_count + 1)
        review_max  = math.log(100_001)
        review_norm = min(review_log / review_max, 1.0)
    else:
        review_norm = 0.0
    review_component = review_norm * 0.30

    value  = compute_value_score(price, rating, all_prices)
    value_component = value * 0.30

    score = rating_component + review_component + value_component

    if sentiment_score is not None:
        sentiment_adjustment = sentiment_score * 0.05
        score = score + sentiment_adjustment

    return round(max(0.0, min(1.0, score)), 4)


# ─────────────────────────────────────────────────────────
# Category assignment
# ─────────────────────────────────────────────────────────

def assign_categories(ranked_products: list[dict]) -> list[dict]:
    """
    Assign special category labels to standout products.

    Rules:
        best_quality  — #1 overall score
        best_value    — best (score / price) ratio, min rating 3.5
        cheapest      — lowest price with rating >= 3.5
        most_popular  — highest review count
        hidden_gem    — top 25% score but bottom 25% review count (underrated)

    One product can hold multiple categories.
    """
    if not ranked_products:
        return []

    products = [p.copy() for p in ranked_products]
    for p in products:
        p["category"] = []

    rated = [p for p in products if p.get("rating") and p["rating"] >= 3.5]
    priced = [p for p in products if p.get("price") and p["price"] > 0]

    if products:
        products[0]["category"].append("best_quality")

    if priced and rated:
        value_candidates = [
            p for p in products
            if p.get("price") and p.get("score") and p.get("rating", 0) >= 3.5
        ]
        if value_candidates:
            best_val = max(
                value_candidates,
                key=lambda p: p["score"] / p["price"]
            )
            best_val["category"].append("best_value")

    cheap_candidates = [p for p in products if p.get("price") and p.get("rating", 0) >= 3.5]
    if cheap_candidates:
        cheapest = min(cheap_candidates, key=lambda p: p["price"])
        cheapest["category"].append("cheapest")

    if products:
        most_popular = max(products, key=lambda p: p.get("review_count", 0))
        most_popular["category"].append("most_popular")


    if len(products) >= 4:
        scores       = sorted([p.get("score", 0) for p in products], reverse=True)
        review_counts = sorted([p.get("review_count", 0) for p in products])

        score_threshold  = scores[len(scores) // 4]
        review_threshold = review_counts[len(review_counts) // 4] 

        for p in products:
            if (
                p.get("score", 0) >= score_threshold
                and p.get("review_count", 0) <= review_threshold
                and p.get("review_count", 0) > 0
                and "hidden_gem" not in p["category"]
            ):
                p["category"].append("hidden_gem")
                break

    for p in products:
        cats = p["category"]
        p["category"] = cats[0] if cats else "standard"
        p["all_categories"] = cats

    return products


# ─────────────────────────────────────────────────────────
# Main ranking pipeline
# ─────────────────────────────────────────────────────────

def rank_products(products: list[dict]) -> list[dict]:
    """
    Full ranking pipeline for a list of products.

    1. Compute scores for all products
    2. Sort by score descending
    3. Assign rank positions
    4. Assign categories
    5. Classify price tiers

    Input: list of product dicts (with optional analysis data merged in)
    Output: same list, sorted and enriched with score/rank/category fields
    """
    if not products:
        return []

    logger.info(f"Ranking {len(products)} products")

    all_prices = [
        p["price"] for p in products
        if p.get("price") and p["price"] > 0
    ]

    scored = []
    for p in products:
        score = compute_score(
            rating=p.get("rating"),
            review_count=p.get("review_count", 0),
            price=p.get("price"),
            all_prices=all_prices,
            sentiment_score=p.get("sentiment_score"),
        )
        scored.append({**p, "score": score})

    scored.sort(key=lambda p: p["score"], reverse=True)

    for i, p in enumerate(scored):
        p["rank_position"] = i + 1
        p["price_tier"] = classify_price_tier(p.get("price"))

    ranked = assign_categories(scored)

    logger.success(
        f"Ranking complete. Top product: '{ranked[0]['title'][:50]}' "
        f"score={ranked[0]['score']}"
    )

    return ranked


# ─────────────────────────────────────────────────────────
# Market statistics — used by report generator
# ─────────────────────────────────────────────────────────

def compute_market_stats(products: list[dict]) -> dict:
    """
    Compute aggregate statistics across all ranked products.
    Used as input to the AI report generator.
    """
    if not products:
        return {}

    prices  = [p["price"] for p in products if p.get("price")]
    ratings = [p["rating"] for p in products if p.get("rating")]
    reviews = [p["review_count"] for p in products if p.get("review_count")]

    sentiments = {"positive": 0, "neutral": 0, "negative": 0}
    for p in products:
        label = p.get("sentiment_label", "neutral")
        if label in sentiments:
            sentiments[label] += 1

    price_dist = {
        "min": round(min(prices), 2) if prices else 0,
        "max": round(max(prices), 2) if prices else 0,
        "avg": round(sum(prices) / len(prices), 2) if prices else 0,
        "median": round(sorted(prices)[len(prices) // 2], 2) if prices else 0,
        "budget_count": len([p for p in prices if p < 30]),
        "mid_count": len([p for p in prices if 30 <= p < 100]),
        "premium_count": len([p for p in prices if p >= 100]),
    }

    all_pros = []
    all_cons = []
    for p in products:
        all_pros.extend(p.get("pros", []) or [])
        all_cons.extend(p.get("cons", []) or [])

    from collections import Counter
    top_pros = [item for item, _ in Counter(all_pros).most_common(5)]
    top_cons = [item for item, _ in Counter(all_cons).most_common(5)]

    sources = list(set(p.get("source", "") for p in products))

    brands = [p.get("brand") for p in products if p.get("brand")]
    top_brands = [
        {"brand": brand, "count": count}
        for brand, count in Counter(brands).most_common(5)
    ]

    return {
        "total_products": len(products),
        "sources": sources,
        "avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else 0,
        "avg_reviews": int(sum(reviews) / len(reviews)) if reviews else 0,
        "price_distribution": price_dist,
        "sentiment_breakdown": sentiments,
        "top_pros": top_pros,
        "top_cons": top_cons,
        "top_brands": top_brands,
    }


