# Real-Time Retail Sales Data Pipeline

A production-grade streaming data pipeline that ingests retail transaction events from Kafka, processes them with PySpark Structured Streaming, stores results in Snowflake, models them with dbt, and orchestrates the entire workflow with Airflow — all running locally via Docker Compose.

## Architecture

```
Kafka Producer
     │  retail_sales topic
     ▼
PySpark Structured Streaming
     │  dedup → watermark → foreachBatch
     ├─── raw_retail_sales  (append, Snowflake)
     └─── agg_store_sales_5min  (5-min windows, Snowflake)
                    │
                   dbt
                    │  incremental MERGE
                    └─── fact_sales  (Snowflake)
                                │
                           Airflow DAG
                          (orchestration + data quality + alerting)
```

## Tech Stack

| Layer | Technology |
|---|---|
| Event streaming | Apache Kafka 7.4 (Confluent) |
| Stream processing | PySpark Structured Streaming 3.5 |
| Data warehouse | Snowflake |
| Analytics modeling | dbt 1.7 (dbt-snowflake) |
| Orchestration | Apache Airflow 2.7 |
| Containerization | Docker Compose |
| Metadata store | Postgres 15 |

## Repository Structure

```
├── airflow-dags/          # Airflow DAG definition
├── dbt/                   # dbt project (models, sources, profiles)
│   └── models/
│       ├── fact_sales.sql      # Incremental fact table
│       ├── fact_sales.yml      # Model docs + dbt tests
│       └── streaming.yml       # Source definitions
├── kafka-producer/        # Synthetic event producer
├── monitoring/            # Standalone data quality SQL queries
├── snowflake/             # One-time Snowflake setup script
├── spark-streaming/       # PySpark streaming job
├── Dockerfile.airflow     # Airflow image with dbt + Snowflake deps
├── Dockerfile.spark       # Spark image with pre-warmed Ivy cache
├── docker-compose.yml     # Full stack orchestration
├── .env.example           # Environment variable template
└── requirements.txt       # Local dev dependencies
```

## Data Flow

1. **Kafka Producer** (`kafka-producer/producer.py`) generates synthetic retail transaction events and publishes them to the `retail_sales` Kafka topic.
2. **PySpark Streaming** (`spark-streaming/stream_processor.py`) consumes events, parses the JSON schema, deduplicates by `transaction_id` with a 10-minute watermark, then writes micro-batches to two Snowflake tables via `foreachBatch`.
3. **dbt** (`dbt/models/fact_sales.sql`) runs an incremental MERGE on `raw_retail_sales` to produce the `fact_sales` analytics table, keyed on `TRANSACTION_ID`.
4. **Airflow DAG** (`airflow-dags/retail_pipeline_dag.py`) orchestrates every 10 minutes:
   - Validates the Snowflake connection (short-circuits on failure)
   - Runs the Kafka producer (25 events)
   - Checks that Spark streaming is actively writing to Snowflake
   - Runs `dbt run`
   - Executes four data quality checks (nulls, duplicates, volume anomaly, revenue anomaly)
   - Logs an alert if any check fails

## Snowflake Schema

**`RETAIL_DB.STREAMING.raw_retail_sales`** — transaction-level events

| Column | Type | Description |
|---|---|---|
| transaction_id | STRING | UUID primary key |
| store_id | STRING | Store identifier |
| product_id | STRING | Product identifier |
| quantity | NUMBER | Units sold |
| price | FLOAT | Unit price |
| payment_type | STRING | credit_card / debit_card / cash / online |
| transaction_ts | TIMESTAMP | UTC event time |

**`RETAIL_DB.STREAMING.agg_store_sales_5min`** — windowed aggregates

| Column | Type | Description |
|---|---|---|
| window_start | TIMESTAMP | Window open (UTC) |
| window_end | TIMESTAMP | Window close (UTC) |
| store_id | STRING | Store identifier |
| total_revenue | FLOAT | Sum of quantity × price |
| total_quantity | NUMBER | Sum of units sold |

**`RETAIL_DB.STREAMING.fact_sales`** — dbt incremental model (MERGE on transaction_id), adds `REVENUE` column.

## Quick Start

### Prerequisites

- Docker Desktop (with Compose v2)
- A Snowflake account with `ACCOUNTADMIN` or equivalent for initial setup
- `python3` (for generating the Fernet key)

### 1. Clone and configure

```bash
git clone <your-repo-url>
cd real-time-retail-sales-data-pipeline
cp .env.example .env
```

Edit `.env` and fill in all `<placeholder>` values. Generate a Fernet key:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 2. Set up Snowflake

Run `snowflake/setup.sql` once in a Snowflake worksheet (requires SYSADMIN + SECURITYADMIN or ACCOUNTADMIN). This creates the warehouse, database, schema, tables, and role grants.

### 3. Build and start all services

```bash
docker compose up --build -d
```

This starts: Zookeeper → Kafka → Spark master + worker → Spark streaming job → Postgres → Airflow init → Airflow scheduler + webserver.

Wait ~60 seconds for all health checks to pass:

```bash
docker compose ps
```

### 4. Access the UIs

| Service | URL |
|---|---|
| Airflow | http://localhost:8080 (admin / airflow) |
| Spark master | http://localhost:8081 |

### 5. Trigger the pipeline

In the Airflow UI, unpause and trigger the `retail_streaming_pipeline` DAG, or via CLI:

```bash
docker compose exec airflow-scheduler airflow dags unpause retail_streaming_pipeline
docker compose exec airflow-scheduler airflow dags trigger retail_streaming_pipeline
```

### 6. Verify results in Snowflake

```sql
SELECT COUNT(*), MAX(transaction_ts) FROM RETAIL_DB.STREAMING.raw_retail_sales;
SELECT COUNT(*), MAX(window_start)   FROM RETAIL_DB.STREAMING.agg_store_sales_5min;
SELECT COUNT(*), MAX(transaction_ts) FROM RETAIL_DB.STREAMING.fact_sales;
```

## Design Decisions

- **Micro-batch over event-at-a-time**: PySpark `foreachBatch` gives batch semantics for Snowflake writes, simplifying error handling and avoiding row-level connector overhead.
- **Watermarking (10 min)**: Bounds state size and allows Spark to emit windowed aggregates even with late-arriving events.
- **MERGE strategy in dbt**: Idempotent re-runs — duplicate `transaction_id` values from retries are handled without duplicating rows.
- **Ivy cache pre-warmed at build time** (`Dockerfile.spark`): Eliminates Maven resolution on every container restart and prevents transient network failures from breaking cold starts.
- **`AIRFLOW_CONN_*` env var**: Snowflake connection is injected automatically without any Airflow UI setup — reproducible across environments.

## Data Quality Checks

All four checks run as Python tasks in the Airflow DAG via `SnowflakeHook`, and the raw SQL is also available in `monitoring/data_quality.sql` for ad-hoc use.

| Check | Logic |
|---|---|
| Null check | Fails if any row has a NULL in a required column |
| Duplicate check | Fails if any `transaction_id` appears more than once |
| Volume anomaly | Fails if last-hour count is outside ±50% of prior-24h hourly average |
| Revenue anomaly | Fails if last-hour revenue is outside ±50% of prior-24h hourly average |

## Future Improvements

- Real-time dashboards (Apache Superset or Grafana)
- ML-based anomaly detection to replace static ±50% thresholds
- Slack / PagerDuty alerting in `alert_if_quality_fails`
- Multi-region Kafka replication for disaster recovery
- Schema evolution handling (Avro + Schema Registry)
- Kubernetes deployment via Helm charts
