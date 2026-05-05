-- ============================================================
--  Stock Market Data Pipeline  |  Database Schema
-- ============================================================

-- ─────────────────────────────────────────
--  DIMENSION: Companies
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_company (
    ticker          VARCHAR(10)  PRIMARY KEY,
    company_name    VARCHAR(100) NOT NULL,
    sector          VARCHAR(100),
    created_at      TIMESTAMP    DEFAULT NOW(),
    updated_at      TIMESTAMP    DEFAULT NOW()
);

-- ─────────────────────────────────────────
--  FACT: Daily Stock Prices
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_stock_prices (
    date            DATE          NOT NULL,
    ticker          VARCHAR(10)   NOT NULL REFERENCES dim_company(ticker),
    open            NUMERIC(12,4) NOT NULL,
    high            NUMERIC(12,4) NOT NULL,
    low             NUMERIC(12,4) NOT NULL,
    close           NUMERIC(12,4) NOT NULL,
    volume          BIGINT        NOT NULL,
    daily_return    NUMERIC(10,6),
    price_range     NUMERIC(12,4) GENERATED ALWAYS AS (high - low) STORED,
    moving_avg_7    NUMERIC(12,4),
    volatility_7    NUMERIC(10,6),
    loaded_at       TIMESTAMP     DEFAULT NOW(),
    PRIMARY KEY (date, ticker)
);

-- ─────────────────────────────────────────
--  AUDIT: Pipeline Run Log
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit_pipeline_log (
    run_id          SERIAL        PRIMARY KEY,
    dag_run_id      VARCHAR(200),
    task_name       VARCHAR(100)  NOT NULL,
    ticker          VARCHAR(10),
    run_date        DATE          NOT NULL DEFAULT CURRENT_DATE,
    started_at      TIMESTAMP     NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMP,
    status          VARCHAR(20)   NOT NULL DEFAULT 'RUNNING'
                    CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
    records_found   INTEGER       DEFAULT 0,
    records_inserted INTEGER      DEFAULT 0,
    records_skipped  INTEGER      DEFAULT 0,
    records_invalid  INTEGER      DEFAULT 0,
    error_message   TEXT,
    details         JSONB
);

-- ─────────────────────────────────────────
--  AUDIT: Data Quality Issues
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit_quality_issues (
    issue_id        SERIAL        PRIMARY KEY,
    run_id          INTEGER       REFERENCES audit_pipeline_log(run_id),
    detected_at     TIMESTAMP     DEFAULT NOW(),
    ticker          VARCHAR(10),
    issue_date      DATE,
    check_name      VARCHAR(100)  NOT NULL,
    severity        VARCHAR(10)   NOT NULL CHECK (severity IN ('ERROR','WARNING','INFO')),
    description     TEXT,
    raw_value       TEXT,
    resolved        BOOLEAN       DEFAULT FALSE
);

-- ─────────────────────────────────────────
--  INDEXES
-- ─────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_ticker      ON fact_stock_prices (ticker);
CREATE INDEX IF NOT EXISTS idx_fact_date        ON fact_stock_prices (date DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_date   ON audit_pipeline_log (run_date DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_status ON audit_pipeline_log (status);
CREATE INDEX IF NOT EXISTS idx_quality_ticker   ON audit_quality_issues (ticker, issue_date);

-- ─────────────────────────────────────────
--  SEED: Dimension Data
-- ─────────────────────────────────────────
INSERT INTO dim_company (ticker, company_name, sector) VALUES
    ('AAPL',  'Apple Inc.',              'Technology'),
    ('MSFT',  'Microsoft Corporation',   'Technology'),
    ('AMZN',  'Amazon.com Inc.',         'Consumer Cyclical'),
    ('GOOGL', 'Alphabet Inc.',           'Technology'),
    ('META',  'Meta Platforms Inc.',     'Technology'),
    ('TSLA',  'Tesla Inc.',              'Consumer Cyclical'),
    ('NVDA',  'NVIDIA Corporation',      'Technology'),
    ('JPM',   'JPMorgan Chase & Co.',    'Financial Services'),
    ('JNJ',   'Johnson & Johnson',       'Healthcare'),
    ('V',     'Visa Inc.',               'Financial Services')
ON CONFLICT (ticker) DO UPDATE
    SET company_name = EXCLUDED.company_name,
        sector       = EXCLUDED.sector,
        updated_at   = NOW();
