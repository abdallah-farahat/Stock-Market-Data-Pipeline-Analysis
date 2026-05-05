-- ============================================================
--  Stock Market Data Pipeline  |  Analytical Queries
-- ============================================================

-- ─────────────────────────────────────────
--  Q1: Best performing stock over a period
-- ─────────────────────────────────────────
SELECT
    f.ticker,
    c.company_name,
    ROUND(
        ((MAX(f.close) FILTER (WHERE f.date = sub.last_date)
        - MAX(f.close) FILTER (WHERE f.date = sub.first_date))
        / NULLIF(MAX(f.close) FILTER (WHERE f.date = sub.first_date), 0)) * 100,
    2) AS total_return_pct
FROM fact_stock_prices f
JOIN dim_company c USING (ticker)
CROSS JOIN (
    SELECT MIN(date) AS first_date, MAX(date) AS last_date
    FROM fact_stock_prices
    WHERE date >= CURRENT_DATE - INTERVAL '90 days'
) sub
WHERE f.date BETWEEN sub.first_date AND sub.last_date
GROUP BY f.ticker, c.company_name
ORDER BY total_return_pct DESC;


-- ─────────────────────────────────────────
--  Q2: Most volatile stock (avg 7-day volatility)
-- ─────────────────────────────────────────
SELECT
    f.ticker,
    c.company_name,
    ROUND(AVG(f.volatility_7)::NUMERIC, 6) AS avg_volatility,
    ROUND(STDDEV(f.daily_return)::NUMERIC, 6) AS stddev_return
FROM fact_stock_prices f
JOIN dim_company c USING (ticker)
WHERE f.date >= CURRENT_DATE - INTERVAL '90 days'
  AND f.volatility_7 IS NOT NULL
GROUP BY f.ticker, c.company_name
ORDER BY avg_volatility DESC;


-- ─────────────────────────────────────────
--  Q3: Average closing price per company
-- ─────────────────────────────────────────
SELECT
    f.ticker,
    c.company_name,
    ROUND(AVG(f.close)::NUMERIC, 2)  AS avg_close,
    ROUND(MIN(f.close)::NUMERIC, 2)  AS min_close,
    ROUND(MAX(f.close)::NUMERIC, 2)  AS max_close,
    COUNT(*)                         AS trading_days
FROM fact_stock_prices f
JOIN dim_company c USING (ticker)
GROUP BY f.ticker, c.company_name
ORDER BY avg_close DESC;


-- ─────────────────────────────────────────
--  Q4: Price trend over the last 30 days
-- ─────────────────────────────────────────
SELECT
    f.date,
    f.ticker,
    c.company_name,
    f.close,
    f.moving_avg_7,
    f.daily_return,
    CASE
        WHEN f.daily_return > 0 THEN 'UP'
        WHEN f.daily_return < 0 THEN 'DOWN'
        ELSE 'FLAT'
    END AS day_direction
FROM fact_stock_prices f
JOIN dim_company c USING (ticker)
WHERE f.date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY f.ticker, f.date;


-- ─────────────────────────────────────────
--  Q5: Audit summary – last 7 pipeline runs
-- ─────────────────────────────────────────
SELECT
    run_id,
    dag_run_id,
    task_name,
    ticker,
    run_date,
    started_at,
    finished_at,
    EXTRACT(EPOCH FROM (finished_at - started_at))::INT AS duration_sec,
    status,
    records_found,
    records_inserted,
    records_skipped,
    records_invalid,
    error_message
FROM audit_pipeline_log
ORDER BY started_at DESC
LIMIT 50;


-- ─────────────────────────────────────────
--  Q6: Data quality issues (unresolved)
-- ─────────────────────────────────────────
SELECT
    qi.issue_id,
    qi.detected_at,
    qi.ticker,
    qi.issue_date,
    qi.check_name,
    qi.severity,
    qi.description,
    qi.raw_value,
    pl.dag_run_id
FROM audit_quality_issues qi
LEFT JOIN audit_pipeline_log pl ON qi.run_id = pl.run_id
WHERE qi.resolved = FALSE
ORDER BY qi.severity DESC, qi.detected_at DESC;
