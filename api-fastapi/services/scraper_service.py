"""
scraper_service.py — Multi-source product scraper.

Uses ScraperAPI as a proxy to handle anti-bot protection on
Amazon, eBay, and Walmart. Falls back to direct requests if
no API key is provided (works for testing, may get blocked).
"""
import asyncio
import hashlib
import re
from datetime import datetime
from typing import Optional
from uuid import uuid4

import httpx
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from config import get_settings

settings = get_settings()


def _proxy_url(url: str) -> str:
    """Wrap a URL with ScraperAPI proxy if key is available."""
    if settings.scraper_api_key:
        return (
            f"{settings.scraper_api_url}"
            f"?api_key={settings.scraper_api_key}"
            f"&url={url}"
            f"&render=false"
        )
    return url


def _clean_price(raw: str) -> Optional[float]:
    """Extract a float from messy price strings like '$29.99', '29,99', etc."""
    if not raw:
        return None
    cleaned = re.sub(r"[^\d.]", "", raw.replace(",", "."))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _clean_rating(raw: str) -> Optional[float]:
    """Extract float rating from strings like '4.5 out of 5 stars'."""
    if not raw:
        return None
    match = re.search(r"(\d+\.?\d*)", raw)
    return float(match.group(1)) if match else None


def _clean_reviews(raw: str) -> int:
    """Extract int from strings like '1,234 ratings'."""
    if not raw:
        return 0
    cleaned = re.sub(r"[^\d]", "", raw)
    return int(cleaned) if cleaned else 0


def _make_external_id(source: str, url: str) -> str:
    """Stable unique ID for deduplication across runs."""
    return hashlib.md5(f"{source}:{url}".encode()).hexdigest()


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


async def _fetch(client: httpx.AsyncClient, url: str) -> Optional[str]:
    """Fetch a URL and return HTML string, or None on failure."""
    try:
        proxied = _proxy_url(url)
        resp = await client.get(proxied, headers=HEADERS, timeout=settings.request_timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.warning(f"Fetch failed for {url}: {e}")
        return None


# ─────────────────────────────────────────────────────────
# Amazon Scraper
# ─────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
async def scrape_amazon(keyword: str, max_results: int = 10) -> list[dict]:
    """
    Scrape Amazon search results for a keyword.
    Returns list of product dicts.
    """
    products = []
    url = f"https://www.amazon.com/s?k={keyword.replace(' ', '+')}&ref=nb_sb_noss"

    async with httpx.AsyncClient(follow_redirects=True) as client:
        html = await _fetch(client, url)
        if not html:
            logger.error(f"Amazon: no HTML returned for '{keyword}'")
            return []

        soup = BeautifulSoup(html, "lxml")

        items = soup.select("div[data-component-type='s-search-result']")
        logger.info(f"Amazon: found {len(items)} raw items for '{keyword}'")

        for item in items[:max_results]:
            try:
                title_el = item.select_one("h2 a span")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)

                link_el = item.select_one("h2 a")
                product_url = (
                    "https://www.amazon.com" + link_el["href"]
                    if link_el and link_el.get("href")
                    else None
                )

                price_whole = item.select_one("span.a-price-whole")
                price_frac  = item.select_one("span.a-price-fraction")
                if price_whole:
                    raw_price = price_whole.get_text(strip=True)
                    if price_frac:
                        raw_price += price_frac.get_text(strip=True)
                    price = _clean_price(raw_price)
                else:
                    price = None

                rating_el = item.select_one("span.a-icon-alt")
                rating = _clean_rating(rating_el.get_text(strip=True)) if rating_el else None

                reviews_el = item.select_one("span.a-size-base.s-underline-text")
                review_count = _clean_reviews(reviews_el.get_text(strip=True)) if reviews_el else 0

                img_el = item.select_one("img.s-image")
                image_url = img_el["src"] if img_el else None

                brand_el = item.select_one("span.a-size-base-plus.a-color-base")
                brand = brand_el.get_text(strip=True) if brand_el else None

                products.append({
                    "external_id": _make_external_id("amazon", product_url or title),
                    "source": "amazon",
                    "keyword": keyword,
                    "title": title,
                    "price": price,
                    "currency": "USD",
                    "rating": rating,
                    "review_count": review_count,
                    "image_url": image_url,
                    "product_url": product_url,
                    "brand": brand,
                    "availability": "In Stock",
                    "scraped_at": datetime.utcnow().isoformat(),
                })

            except Exception as e:
                logger.warning(f"Amazon: failed to parse item — {e}")
                continue

    logger.success(f"Amazon: scraped {len(products)} products for '{keyword}'")
    return products


# ─────────────────────────────────────────────────────────
# eBay Scraper
# ─────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
async def scrape_ebay(keyword: str, max_results: int = 10) -> list[dict]:
    """Scrape eBay search results for a keyword."""
    products = []
    url = f"https://www.ebay.com/sch/i.html?_nkw={keyword.replace(' ', '+')}&_sacat=0"

    async with httpx.AsyncClient(follow_redirects=True) as client:
        html = await _fetch(client, url)
        if not html:
            logger.error(f"eBay: no HTML returned for '{keyword}'")
            return []

        soup = BeautifulSoup(html, "lxml")
        items = soup.select("li.s-item")
        logger.info(f"eBay: found {len(items)} raw items for '{keyword}'")

        for item in items[:max_results + 5]:
            try:
                title_el = item.select_one("div.s-item__title span")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)

                if "Shop on eBay" in title:
                    continue

                link_el = item.select_one("a.s-item__link")
                product_url = link_el["href"] if link_el else None

                price_el = item.select_one("span.s-item__price")
                price_text = price_el.get_text(strip=True) if price_el else ""
                price = _clean_price(price_text.split(" to ")[0])

                rating_el = item.select_one("div.x-star-rating span.clipped")
                rating = _clean_rating(rating_el.get_text(strip=True)) if rating_el else None

                reviews_el = item.select_one("span.s-item__reviews-count span")
                review_count = _clean_reviews(reviews_el.get_text(strip=True)) if reviews_el else 0

                img_el = item.select_one("img.s-item__image-img")
                image_url = img_el.get("src") or img_el.get("data-src") if img_el else None

                availability_el = item.select_one("span.s-item__availability")
                availability = availability_el.get_text(strip=True) if availability_el else "Available"

                products.append({
                    "external_id": _make_external_id("ebay", product_url or title),
                    "source": "ebay",
                    "keyword": keyword,
                    "title": title,
                    "price": price,
                    "currency": "USD",
                    "rating": rating,
                    "review_count": review_count,
                    "image_url": image_url,
                    "product_url": product_url,
                    "brand": None,
                    "availability": availability,
                    "scraped_at": datetime.utcnow().isoformat(),
                })

                if len(products) >= max_results:
                    break

            except Exception as e:
                logger.warning(f"eBay: failed to parse item — {e}")
                continue

    logger.success(f"eBay: scraped {len(products)} products for '{keyword}'")
    return products


# ─────────────────────────────────────────────────────────
# Walmart Scraper
# ─────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
async def scrape_walmart(keyword: str, max_results: int = 10) -> list[dict]:
    """Scrape Walmart search results for a keyword."""
    products = []
    url = f"https://www.walmart.com/search?q={keyword.replace(' ', '+')}"

    async with httpx.AsyncClient(follow_redirects=True) as client:
        html = await _fetch(client, url)
        if not html:
            logger.error(f"Walmart: no HTML returned for '{keyword}'")
            return []

        soup = BeautifulSoup(html, "lxml")

        script = soup.find("script", {"id": "__NEXT_DATA__"})

        if script:
            import json
            try:
                data = json.loads(script.string)
                search_results = (
                    data.get("props", {})
                    .get("pageProps", {})
                    .get("initialData", {})
                    .get("searchResult", {})
                    .get("itemStacks", [{}])[0]
                    .get("items", [])
                )

                for item in search_results[:max_results]:
                    try:
                        title = item.get("name", "")
                        if not title:
                            continue

                        price_info = item.get("priceInfo", {})
                        price = price_info.get("currentPrice", {}).get("price")

                        rating = item.get("averageRating")
                        review_count = item.get("numberOfReviews", 0)

                        image_url = item.get("imageInfo", {}).get("thumbnailUrl")
                        product_id = item.get("usItemId", "")
                        product_url = f"https://www.walmart.com/ip/{product_id}" if product_id else None
                        brand = item.get("brand")
                        availability = "In Stock" if item.get("availabilityStatus") == "IN_STOCK" else "Limited"

                        products.append({
                            "external_id": _make_external_id("walmart", product_url or title),
                            "source": "walmart",
                            "keyword": keyword,
                            "title": title,
                            "price": float(price) if price else None,
                            "currency": "USD",
                            "rating": float(rating) if rating else None,
                            "review_count": int(review_count),
                            "image_url": image_url,
                            "product_url": product_url,
                            "brand": brand,
                            "availability": availability,
                            "scraped_at": datetime.utcnow().isoformat(),
                        })
                    except Exception as e:
                        logger.warning(f"Walmart JSON item parse error: {e}")
                        continue

            except Exception as e:
                logger.warning(f"Walmart: JSON parse failed, falling back to HTML — {e}")

        if not products:
            items = soup.select("div[data-item-id]")
            for item in items[:max_results]:
                try:
                    title_el = item.select_one("span.lh-title")
                    if not title_el:
                        continue
                    title = title_el.get_text(strip=True)

                    price_el = item.select_one("div[itemprop='price']")
                    price = _clean_price(price_el.get_text(strip=True)) if price_el else None

                    img_el = item.select_one("img")
                    image_url = img_el.get("src") if img_el else None

                    products.append({
                        "external_id": _make_external_id("walmart", title),
                        "source": "walmart",
                        "keyword": keyword,
                        "title": title,
                        "price": price,
                        "currency": "USD",
                        "rating": None,
                        "review_count": 0,
                        "image_url": image_url,
                        "product_url": None,
                        "brand": None,
                        "availability": "Unknown",
                        "scraped_at": datetime.utcnow().isoformat(),
                    })
                except Exception as e:
                    logger.warning(f"Walmart HTML fallback parse error: {e}")
                    continue

    logger.success(f"Walmart: scraped {len(products)} products for '{keyword}'")
    return products



def deduplicate_products(products: list[dict]) -> list[dict]:
    """
    Remove duplicate products by external_id.
    If same product appears across sources, keep all (different source = different listing).
    """
    seen = set()
    unique = []
    for p in products:
        key = p.get("external_id", "")
        if key not in seen:
            seen.add(key)
            unique.append(p)
    removed = len(products) - len(unique)
    if removed:
        logger.info(f"Deduplication: removed {removed} duplicates, {len(unique)} remaining")
    return unique



async def scrape_all_sources(
    keyword: str,
    sources: list[str],
    max_per_source: int = 10,
) -> list[dict]:
    """
    Scrape all requested sources concurrently using asyncio.gather.
    Returns merged, deduplicated product list.
    """
    tasks = []

    if "amazon" in sources:
        tasks.append(scrape_amazon(keyword, max_per_source))
    if "ebay" in sources:
        tasks.append(scrape_ebay(keyword, max_per_source))
    if "walmart" in sources:
        tasks.append(scrape_walmart(keyword, max_per_source))

    logger.info(f"Scraping {len(tasks)} sources in parallel for '{keyword}'")

    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_products = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Source scrape failed: {result}")
        else:
            all_products.extend(result)

    await asyncio.sleep(settings.request_delay)

    return deduplicate_products(all_products)



