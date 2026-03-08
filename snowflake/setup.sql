-- Snowflake Setup for Real-Time Retail Sales Data Pipeline
-- Run this script once in a Snowflake worksheet before starting the pipeline.
-- Requires a role with CREATE WAREHOUSE, CREATE DATABASE, and GRANT privileges
-- (e.g., SYSADMIN + SECURITYADMIN, or ACCOUNTADMIN for initial setup).

-- ── Warehouse ─────────────────────────────────────────────────────────────────
-- XSMALL is sufficient for streaming micro-batches and dbt incremental runs.
-- AUTO_SUSPEND = 60s keeps idle costs near zero.
CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- ── Database & schema ─────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS RETAIL_DB;
CREATE SCHEMA IF NOT EXISTS RETAIL_DB.STREAMING;

-- ── Raw ingestion table ───────────────────────────────────────────────────────
-- Written by the PySpark Structured Streaming job via foreachBatch.
-- Clustered on transaction_ts to keep time-range queries selective.
CREATE TABLE IF NOT EXISTS RETAIL_DB.STREAMING.raw_retail_sales (
    transaction_id STRING     NOT NULL,
    store_id       STRING     NOT NULL,
    product_id     STRING     NOT NULL,
    quantity       NUMBER     NOT NULL,
    price          FLOAT      NOT NULL,
    payment_type   STRING,
    transaction_ts TIMESTAMP  NOT NULL
)
CLUSTER BY (transaction_ts);

-- ── Aggregation table ─────────────────────────────────────────────────────────
-- Written by the PySpark 5-minute windowed aggregation query.
-- Clustered on (window_start, store_id) to accelerate store-level range queries.
CREATE TABLE IF NOT EXISTS RETAIL_DB.STREAMING.agg_store_sales_5min (
    window_start   TIMESTAMP  NOT NULL,
    window_end     TIMESTAMP  NOT NULL,
    store_id       STRING     NOT NULL,
    total_revenue  FLOAT,
    total_quantity NUMBER
)
CLUSTER BY (window_start, store_id);

-- ── Role-based access ─────────────────────────────────────────────────────────
-- DATA_ENGINEER role is used by the Spark connector, Airflow, and dbt.
-- FUTURE GRANTS ensure any tables dbt creates later inherit the same permissions.
GRANT USAGE  ON WAREHOUSE RETAIL_WH                      TO ROLE DATA_ENGINEER;
GRANT USAGE  ON DATABASE RETAIL_DB                       TO ROLE DATA_ENGINEER;
GRANT USAGE  ON SCHEMA RETAIL_DB.STREAMING               TO ROLE DATA_ENGINEER;
GRANT CREATE TABLE ON SCHEMA RETAIL_DB.STREAMING         TO ROLE DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE
      ON ALL TABLES IN SCHEMA RETAIL_DB.STREAMING        TO ROLE DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE
      ON FUTURE TABLES IN SCHEMA RETAIL_DB.STREAMING     TO ROLE DATA_ENGINEER;
