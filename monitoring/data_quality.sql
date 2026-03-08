-- Data Quality Checks for Retail Sales Pipeline
-- All queries use fully-qualified names (DATABASE.SCHEMA.TABLE).
-- Anomaly detection uses a 3-sigma (Z-score) model trained on a rolling 7-day
-- hourly history. A flag is raised only when |Z| > 3, making the threshold
-- adaptive to actual traffic patterns rather than a hard-coded ±50% rule.

-- 1. Null check on critical columns
SELECT
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS null_check_status,
    COUNT(*) AS null_count
FROM RETAIL_DB.STREAMING.raw_retail_sales
WHERE transaction_id IS NULL
   OR store_id IS NULL
   OR product_id IS NULL
   OR quantity IS NULL
   OR price IS NULL
   OR transaction_ts IS NULL;

-- 2. Duplicate transaction detection
SELECT
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS duplicate_check_status,
    COUNT(*) AS duplicate_count
FROM (
    SELECT transaction_id
    FROM RETAIL_DB.STREAMING.raw_retail_sales
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
);

-- 3. Volume anomaly — 3-sigma Z-score over rolling 7-day hourly history.
--    PASS when: no history yet (cold start), sigma = 0 (flat traffic), or |Z| <= 3.
WITH hourly_history AS (
    SELECT
        DATE_TRUNC('hour', transaction_ts) AS hr,
        COUNT(*) AS volume
    FROM RETAIL_DB.STREAMING.raw_retail_sales
    WHERE transaction_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
      AND transaction_ts <  DATEADD('hour', -1, CURRENT_TIMESTAMP())
    GROUP BY 1
),
stats AS (
    SELECT AVG(volume) AS mu, STDDEV(volume) AS sigma FROM hourly_history
),
current_hour AS (
    SELECT COUNT(*) AS volume
    FROM RETAIL_DB.STREAMING.raw_retail_sales
    WHERE transaction_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
)
SELECT
    CASE
        WHEN s.sigma IS NULL OR s.sigma = 0 THEN 'PASS'
        WHEN ABS(c.volume - s.mu) <= 3 * s.sigma THEN 'PASS'
        ELSE 'FAIL'
    END                                                    AS volume_anomaly_status,
    c.volume                                               AS current_hour_volume,
    ROUND(s.mu, 1)                                         AS mean_volume,
    ROUND(s.sigma, 1)                                      AS stddev_volume,
    CASE WHEN s.sigma > 0
         THEN ROUND((c.volume - s.mu) / s.sigma, 2)
         ELSE 0 END                                        AS z_score
FROM current_hour c, stats s;

-- 4. Revenue anomaly — same 3-sigma model applied to hourly revenue.
WITH hourly_history AS (
    SELECT
        DATE_TRUNC('hour', transaction_ts) AS hr,
        SUM(quantity * price) AS revenue
    FROM RETAIL_DB.STREAMING.raw_retail_sales
    WHERE transaction_ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
      AND transaction_ts <  DATEADD('hour', -1, CURRENT_TIMESTAMP())
    GROUP BY 1
),
stats AS (
    SELECT AVG(revenue) AS mu, STDDEV(revenue) AS sigma FROM hourly_history
),
current_hour AS (
    SELECT SUM(quantity * price) AS revenue
    FROM RETAIL_DB.STREAMING.raw_retail_sales
    WHERE transaction_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
)
SELECT
    CASE
        WHEN s.sigma IS NULL OR s.sigma = 0 THEN 'PASS'
        WHEN ABS(COALESCE(c.revenue, 0) - s.mu) <= 3 * s.sigma THEN 'PASS'
        ELSE 'FAIL'
    END                                                    AS revenue_anomaly_status,
    ROUND(COALESCE(c.revenue, 0), 2)                       AS current_hour_revenue,
    ROUND(s.mu, 2)                                         AS mean_revenue,
    ROUND(s.sigma, 2)                                      AS stddev_revenue,
    CASE WHEN s.sigma > 0
         THEN ROUND((COALESCE(c.revenue, 0) - s.mu) / s.sigma, 2)
         ELSE 0 END                                        AS z_score
FROM current_hour c, stats s;
