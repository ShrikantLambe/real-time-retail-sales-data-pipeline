# Data Dictionary — Real-Time Retail Sales Pipeline

## Lineage Overview

```
[Kafka topic: retail_sales]
        │  Avro (Confluent wire format)
        ▼
[PySpark Structured Streaming]
        │
        ├──► RETAIL_DB.STREAMING.raw_retail_sales   (append, micro-batch)
        │
        └──► RETAIL_DB.STREAMING.agg_store_sales_5min  (5-min window, append)
                                     │
                                    dbt
                                     │  MERGE on TRANSACTION_ID
                                     ▼
                          RETAIL_DB.STREAMING.fact_sales
```

---

## Source: Kafka Topic `retail_sales`

Produced by `kafka-producer/producer.py` using Confluent Avro serialization.
Schema registered in Schema Registry as `retail_sales-value`.

| Field | Avro Type | Nullable | Description |
|---|---|---|---|
| transaction_id | string | No | UUID v4 — globally unique per transaction |
| store_id | string | No | Store identifier, format `store_N` (N = 1–20) |
| product_id | string | No | Product identifier, format `product_N` (N = 1–100) |
| quantity | int | No | Units sold per transaction (1–5) |
| price | float | No | Unit price in USD (5.00–500.00) |
| payment_type | union[null, string] | Yes | `credit_card` \| `debit_card` \| `cash` \| `online` |
| transaction_ts | string | No | ISO-8601 UTC timestamp e.g. `2026-03-08T22:00:00Z` |

---

## Table: `RETAIL_DB.STREAMING.raw_retail_sales`

**Layer:** Raw ingestion
**Written by:** PySpark `stream_processor.py` via `foreachBatch`
**Strategy:** Append (no deduplication at write time; dedup happens in Spark via `dropDuplicates`)
**Clustering:** `(transaction_ts)` — optimises time-range scans
**Freshness:** Near real-time; micro-batches every ~30s

| Column | Snowflake Type | Nullable | Description |
|---|---|---|---|
| TRANSACTION_ID | STRING | NOT NULL | UUID primary key. Deduplicated by Spark before write. |
| STORE_ID | STRING | NOT NULL | Store identifier (`store_1` … `store_20`) |
| PRODUCT_ID | STRING | NOT NULL | Product identifier (`product_1` … `product_100`) |
| QUANTITY | NUMBER | NOT NULL | Units sold (integer 1–5) |
| PRICE | FLOAT | NOT NULL | Unit price in USD |
| PAYMENT_TYPE | STRING | NULL | Payment method. NULL if not captured. |
| TRANSACTION_TS | TIMESTAMP | NOT NULL | UTC event timestamp (cast from ISO-8601 string) |

**Data quality controls:**
- Spark 10-minute watermark prevents unbounded state for late data
- `dropDuplicates(["transaction_id"])` applied before write
- Null-check run by Airflow DAG every 10 minutes

---

## Table: `RETAIL_DB.STREAMING.agg_store_sales_5min`

**Layer:** Real-time aggregation
**Written by:** PySpark `stream_processor.py` via `foreachBatch`
**Strategy:** Append (each window produces one row per store once watermark advances)
**Clustering:** `(window_start, store_id)` — optimises store-level time-range queries
**Freshness:** Emitted after watermark advances (~10 min lag from event time)

| Column | Snowflake Type | Nullable | Description |
|---|---|---|---|
| WINDOW_START | TIMESTAMP | NOT NULL | Opening boundary of the 5-minute aggregation window (UTC) |
| WINDOW_END | TIMESTAMP | NOT NULL | Closing boundary of the 5-minute aggregation window (UTC) |
| STORE_ID | STRING | NOT NULL | Store identifier |
| TOTAL_REVENUE | FLOAT | NULL | `SUM(quantity * price)` across all transactions in the window |
| TOTAL_QUANTITY | NUMBER | NULL | `SUM(quantity)` across all transactions in the window |

**Notes:**
- `TOTAL_REVENUE` and `TOTAL_QUANTITY` are nullable because Snowflake `SUM()` returns NULL for empty windows
- Each `(window_start, window_end, store_id)` triple is unique by construction (Spark watermark)
- Historical windows are never updated; new data in the same window extends state until watermark fires

---

## Model: `RETAIL_DB.STREAMING.fact_sales`

**Layer:** Analytics / mart
**Written by:** dbt incremental model (`dbt/models/fact_sales.sql`)
**Strategy:** `MERGE` on `TRANSACTION_ID` (idempotent re-runs)
**Materialization:** `incremental` (only new rows scanned per run)
**Source:** `RETAIL_DB.STREAMING.raw_retail_sales`
**Triggered by:** Airflow `run_dbt_models` task (every 10 minutes)

| Column | Snowflake Type | Nullable | dbt Test | Description |
|---|---|---|---|---|
| TRANSACTION_ID | STRING | NOT NULL | `unique`, `not_null` | UUID primary key inherited from raw source |
| STORE_ID | STRING | NOT NULL | `not_null` | Store identifier |
| PRODUCT_ID | STRING | NOT NULL | `not_null` | Product identifier |
| QUANTITY | NUMBER | NOT NULL | `not_null` | Units sold |
| PRICE | FLOAT | NOT NULL | `not_null` | Unit price in USD |
| PAYMENT_TYPE | STRING | NULL | `accepted_values: [credit_card, debit_card, cash, online]` | Payment method |
| TRANSACTION_TS | TIMESTAMP | NOT NULL | `not_null` | UTC event timestamp |
| REVENUE | FLOAT | NOT NULL | `not_null` | Computed: `QUANTITY * PRICE` |

**Incremental filter:**
```sql
WHERE TRANSACTION_TS > (SELECT MAX(TRANSACTION_TS) FROM {{ this }})
```
This pushes only new rows from the source into the MERGE, keeping scans small.
⚠️ **Known gap:** If late-arriving events have `TRANSACTION_TS` older than the current max, they are silently skipped. For full correctness, consider a lookback window (e.g. `-1 hour`).

**Lineage:**
```
raw_retail_sales  ──[MERGE on TRANSACTION_ID]──►  fact_sales
                     + REVENUE = QUANTITY * PRICE
```

---

## Environment Variable Reference

| Variable | Used By | Description |
|---|---|---|
| `KAFKA_BROKERS` | Producer, Spark | Bootstrap server(s), e.g. `kafka:9092` |
| `KAFKA_TOPIC` | Producer, Spark, Airflow | Topic name (default: `retail_sales`) |
| `SCHEMA_REGISTRY_URL` | Producer | Registry URL for Avro schema registration |
| `SNOWFLAKE_ACCOUNT` | Spark, Airflow, dbt | Snowflake account identifier |
| `SNOWFLAKE_URL` | Spark | Full hostname for sfURL connector option |
| `SNOWFLAKE_USER` | Spark, Airflow, dbt | Service account username |
| `SNOWFLAKE_PASSWORD` | Spark, Airflow, dbt | Service account password |
| `SNOWFLAKE_DATABASE` | Spark, Airflow, dbt | Target database (default: `RETAIL_DB`) |
| `SNOWFLAKE_SCHEMA` | Spark, Airflow, dbt | Target schema (default: `STREAMING`) |
| `SNOWFLAKE_WAREHOUSE` | Spark, Airflow, dbt | Compute warehouse (default: `RETAIL_WH`) |
| `SNOWFLAKE_ROLE` | Spark, Airflow, dbt | Active role (default: `DATA_ENGINEER`) |
| `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` | Airflow | Full Snowflake connection URI auto-registered on startup |
| `AIRFLOW_FERNET_KEY` | Airflow | Encryption key for storing secrets in Airflow metadata DB |
| `DAG_SCHEDULE` | Airflow | Cron schedule for the pipeline DAG (default: `*/10 * * * *`) |
| `CHECKPOINT_DIR` | Spark | Spark streaming checkpoint location (default: `/tmp/spark_checkpoint`) |
| `SLACK_WEBHOOK_URL` | Airflow | Incoming Webhook URL for quality-failure alerts (optional) |
| `SUPERSET_SECRET_KEY` | Superset | Flask session signing key (required) |
| `POSTGRES_USER` | Postgres, Airflow | Airflow metadata DB user (default: `airflow`) |
| `POSTGRES_PASSWORD` | Postgres, Airflow | Airflow metadata DB password |
| `POSTGRES_DB` | Postgres, Airflow | Airflow metadata DB name (default: `airflow`) |
