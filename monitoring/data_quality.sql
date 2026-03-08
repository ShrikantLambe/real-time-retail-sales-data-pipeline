-- Data Quality Checks for Retail Sales Pipeline
-- All queries use fully-qualified names (DATABASE.SCHEMA.TABLE) so they work
-- regardless of the session's default database/schema context.

-- 1. Null check on critical columns
-- Returns PASS if no nulls exist in any required field.
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
-- Returns PASS if every transaction_id is unique.
SELECT
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS duplicate_check_status,
    COUNT(*) AS duplicate_count
FROM (
    SELECT transaction_id
    FROM RETAIL_DB.STREAMING.raw_retail_sales
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
);

-- 3. Volume anomaly detection
-- Compares last 1-hour record count to the hourly average over the prior 24 hours.
-- Returns PASS if volume is within ±50% of baseline.
-- Division-by-zero guard: PASS when no prior-day baseline exists (cold start).
WITH last_hour AS (
    SELECT COUNT(*) AS volume
    FROM RETAIL_DB.STREAMING.raw_retail_sales
    WHERE transaction_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
),
prev_24hr AS (
    SELECT COUNT(*) AS volume
    FROM RETAIL_DB.STREAMING.raw_retail_sales
    WHERE transaction_ts <  DATEADD('hour', -1, CURRENT_TIMESTAMP())
      AND transaction_ts >= DATEADD('hour', -25, CURRENT_TIMESTAMP())
)
SELECT
    CASE
        WHEN p24.volume = 0 THEN 'PASS'
        WHEN lh.volume BETWEEN (p24.volume / 24) * 0.5
                           AND (p24.volume / 24) * 1.5 THEN 'PASS'
        ELSE 'FAIL'
    END AS volume_anomaly_status,
    lh.volume         AS last_hour_volume,
    (p24.volume / 24) AS avg_hourly_volume
FROM last_hour lh, prev_24hr p24;

-- 4. Revenue anomaly detection
-- Compares last 1-hour revenue to the hourly average over the prior 24 hours.
-- Returns PASS if revenue is within ±50% of baseline.
-- NULL/zero guard: PASS when no prior-day revenue exists (cold start or no sales).
WITH last_hour AS (
    SELECT SUM(quantity * price) AS revenue
    FROM RETAIL_DB.STREAMING.raw_retail_sales
    WHERE transaction_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
),
prev_24hr AS (
    SELECT SUM(quantity * price) AS revenue
    FROM RETAIL_DB.STREAMING.raw_retail_sales
    WHERE transaction_ts <  DATEADD('hour', -1, CURRENT_TIMESTAMP())
      AND transaction_ts >= DATEADD('hour', -25, CURRENT_TIMESTAMP())
)
SELECT
    CASE
        WHEN p24.revenue IS NULL OR p24.revenue = 0 THEN 'PASS'
        WHEN lh.revenue BETWEEN (p24.revenue / 24) * 0.5
                            AND (p24.revenue / 24) * 1.5 THEN 'PASS'
        ELSE 'FAIL'
    END AS revenue_anomaly_status,
    lh.revenue         AS last_hour_revenue,
    (p24.revenue / 24) AS avg_hourly_revenue
FROM last_hour lh, prev_24hr p24;
