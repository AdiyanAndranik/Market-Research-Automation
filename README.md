# AI Market Research Agent

An AI-powered market research automation system that scrapes products from multiple e-commerce sources, analyzes them using OpenAI, ranks them with a custom scoring algorithm, and delivers professional PDF reports — fully automated via n8n workflows.

---

## Architecture

```
Webhook / Cron Trigger
        ↓
    n8n Workflow
        ↓
FastAPI Backend
   ├── Scraper       → Amazon, eBay, Walmart
   ├── AI Analysis   → OpenAI GPT-4o-mini
   ├── Ranking       → Custom scoring algorithm
   └── Report Gen    → PDF + Email delivery
        ↓
   PostgreSQL DB
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Workflow Automation | n8n |
| Backend API | FastAPI (Python) |
| AI Analysis | OpenAI GPT-4o-mini |
| Database | PostgreSQL |
| Scraping | BeautifulSoup + httpx |
| PDF Generation | ReportLab |
| Containerization | Docker + Docker Compose |
| Notifications | Slack + Email |
| Export | Google Sheets |

---

## Features

- Multi-source scraping — Amazon, eBay, Walmart in parallel
- AI sentiment analysis on every product
- Automated pros & cons extraction
- Fake review risk detection
- Custom ranking algorithm (rating + volume + value score)
- Category labels — Best Quality, Best Value, Hidden Gem, Cheapest, Most Popular
- Professional PDF report generation
- Price drop alert detection
- Scheduled weekly automation via Cron
- External trigger via Webhook
- PostgreSQL storage for all data
- Google Sheets export
- Email + Slack notifications
- Full error handling pipeline

---

## Quick Start

**1. Clone and configure**
```bash
git clone https://github.com/yourusername/ai-market-research-agent.git
cd ai-market-research-agent
# Create .env
# Fill in your API keys in .env
```

**2. Start all services**
```bash
docker compose up --build
```

**3. Access**
- FastAPI docs: `http://localhost:8000/docs`
- n8n workflow: `http://localhost:5678`
- Database: `localhost:5432`

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/scrape/search` | Scrape products from all sources |
| POST | `/api/v1/analysis/analyze` | Run AI analysis on products |
| POST | `/api/v1/products/rank` | Score and rank products |
| GET | `/api/v1/products/top/{keyword}` | Get top ranked products |
| POST | `/api/v1/reports/generate` | Generate PDF report |
| GET | `/api/v1/reports/{id}/download` | Download PDF |
| POST | `/api/v1/reports/full-pipeline` | Run entire pipeline in one call |

---

## Ranking Algorithm

```
score = (rating × 0.40) + (log(reviews) × 0.30) + (value_score × 0.30)
```

- **40%** — Product rating quality (normalized 0–5)
- **30%** — Review volume (log scale to prevent outlier dominance)
- **30%** — Value score (quality per dollar, normalized across batch)
- **±5%** — AI sentiment adjustment bonus

---

## n8n Workflow

The workflow has 22 nodes and runs automatically every Monday at 8AM or on demand via webhook.
