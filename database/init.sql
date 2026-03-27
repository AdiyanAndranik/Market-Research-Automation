CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

CREATE TABLE IF NOT EXISTS products (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    external_id     VARCHAR(255),
    source          VARCHAR(50) NOT NULL,
    keyword         VARCHAR(255) NOT NULL,
    title           TEXT NOT NULL,
    price           DECIMAL(10, 2),
    currency        VARCHAR(10) DEFAULT 'USD',
    rating          DECIMAL(3, 2),
    review_count    INTEGER DEFAULT 0,
    image_url       TEXT,
    product_url     TEXT,
    availability    VARCHAR(100),
    brand           VARCHAR(255),
    category        VARCHAR(255),
    raw_data        JSONB,
    scraped_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_products_keyword ON products (keyword);
CREATE INDEX IF NOT EXISTS idx_products_source ON products (source);
CREATE INDEX IF NOT EXISTS idx_products_scraped_at ON products (scraped_at DESC);

CREATE TABLE IF NOT EXISTS product_analysis (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id          UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    sentiment_score     DECIMAL(4, 3),
    sentiment_label     VARCHAR(20),
    pros                JSONB,
    cons                JSONB,
    fake_review_risk    VARCHAR(20) DEFAULT 'unknown',
    summary             TEXT,
    keywords_extracted  JSONB,
    model_used          VARCHAR(100),
    tokens_used         INTEGER,
    analyzed_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analysis_product_id ON product_analysis (product_id);

CREATE TABLE IF NOT EXISTS product_rankings (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id      UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    keyword         VARCHAR(255) NOT NULL,
    score           DECIMAL(6, 4),
    rank_position   INTEGER,
    category        VARCHAR(50),
    price_tier      VARCHAR(20),
    ranked_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rankings_keyword ON product_rankings (keyword);
CREATE INDEX IF NOT EXISTS idx_rankings_score ON product_rankings (score DESC);

CREATE TABLE IF NOT EXISTS reports (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    keyword         VARCHAR(255) NOT NULL,
    title           TEXT,
    summary         TEXT,
    content         JSONB,
    pdf_path        TEXT,
    products_count  INTEGER DEFAULT 0,
    sources_used    JSONB,
    status          VARCHAR(20) DEFAULT 'pending',
    triggered_by    VARCHAR(50) DEFAULT 'manual',
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at    TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_reports_keyword ON reports (keyword);
CREATE INDEX IF NOT EXISTS idx_reports_created_at ON reports (created_at DESC);

CREATE TABLE IF NOT EXISTS price_alerts (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id      UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    keyword         VARCHAR(255) NOT NULL,
    old_price       DECIMAL(10, 2),
    new_price       DECIMAL(10, 2),
    change_pct      DECIMAL(6, 2),
    alert_sent      BOOLEAN DEFAULT FALSE,
    detected_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS search_sessions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    keyword         VARCHAR(255) NOT NULL,
    sources         JSONB,
    status          VARCHAR(20) DEFAULT 'running',
    products_found  INTEGER DEFAULT 0,
    error_message   TEXT,
    n8n_execution_id VARCHAR(255),
    started_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at    TIMESTAMP WITH TIME ZONE
);

CREATE OR REPLACE VIEW v_products_analyzed AS
SELECT
    p.id, p.source, p.keyword, p.title, p.price, p.rating,
    p.review_count, p.product_url, p.scraped_at,
    a.sentiment_label, a.sentiment_score, a.pros, a.cons, a.summary,
    r.score, r.rank_position, r.category
FROM products p
LEFT JOIN product_analysis a ON a.product_id = p.id
LEFT JOIN product_rankings r ON r.product_id = p.id;

SELECT 'Database initialized successfully' AS status;