from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID

class ReportRequest(BaseModel):
    keyword: str = Field(..., example="wireless earbuds")
    session_id: Optional[str] = None
    send_email: bool = False
    email_to: Optional[str] = None
    generate_pdf: bool = True

class PriceDistribution(BaseModel):
    min_price: float
    max_price: float
    avg_price: float
    median_price: float
    budget_count: int
    mid_count: int
    premium_count: int

class MarketInsight(BaseModel):
    total_products: int
    sources: List[str]
    avg_rating: float
    avg_reviews: int
    price_distribution: PriceDistribution
    sentiment_breakdown: Dict[str, int]
    top_brands: List[Dict[str, Any]]

class ReportResponse(BaseModel):
    id: UUID
    keyword: str
    title: str
    status: str
    summary: str
    insights: Optional[MarketInsight] = None
    top_products: Optional[List[Dict]] = None
    pdf_url: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None

    class Config:
        from_attributes = True

