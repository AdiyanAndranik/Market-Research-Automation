from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from uuid import UUID
from enum import Enum


class SourceEnum(str, Enum):
    amazon  = "amazon"
    ebay    = "ebay"
    walmart = "walmart"


class SentimentEnum(str, Enum):
    positive = "positive"
    neutral  = "neutral"
    negative = "negative"


class CategoryEnum(str, Enum):
    best_quality  = "best_quality"
    best_value    = "best_value"
    cheapest      = "cheapest"
    most_popular  = "most_popular"
    hidden_gem    = "hidden_gem"


class ScrapeRequest(BaseModel):
    keyword: str = Field(..., min_length=2, max_length=200, example="wireless earbuds")
    sources: List[SourceEnum] = Field(default=[SourceEnum.amazon, SourceEnum.ebay, SourceEnum.walmart])
    max_results: int = Field(default=10, ge=1, le=50)
    session_id: Optional[str] = None


class AnalyzeRequest(BaseModel):
    product_ids: List[UUID]
    include_fake_detection: bool = True


class ProductBase(BaseModel):
    id: UUID
    source: SourceEnum
    keyword: str
    title: str
    price: Optional[float]
    currency: str = "USD"
    rating: Optional[float]
    review_count: int = 0
    image_url: Optional[str]
    product_url: Optional[str]
    brand: Optional[str]
    scraped_at: datetime

    class Config:
        from_attributes = True


class ProductAnalysis(BaseModel):
    sentiment_score: Optional[float]
    sentiment_label: Optional[SentimentEnum]
    pros: Optional[List[str]]
    cons: Optional[List[str]]
    fake_review_risk: str = "unknown"
    summary: Optional[str]
    model_used: Optional[str]


class ProductRanking(BaseModel):
    score: Optional[float]
    rank_position: Optional[int]
    category: Optional[CategoryEnum]
    price_tier: Optional[str]


class ProductFull(ProductBase):
    analysis: Optional[ProductAnalysis] = None
    ranking: Optional[ProductRanking] = None


class ScrapeResponse(BaseModel):
    session_id: str
    keyword: str
    products_found: int
    sources_scraped: List[str]
    products: List[ProductBase]
    duration_seconds: float


    