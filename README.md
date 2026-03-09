# Real-Time Retail Sales Data Pipeline

[![CI](https://github.com/ShrikantLambe/real-time-retail-sales-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/ShrikantLambe/real-time-retail-sales-data-pipeline/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![dbt 1.7](https://img.shields.io/badge/dbt-1.7-orange)](https://getdbt.com)
[![Spark 3.5](https://img.shields.io/badge/spark-3.5-red)](https://spark.apache.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade streaming data pipeline that ingests retail transaction events from Kafka (Avro + Schema Registry), processes them with PySpark Structured Streaming, stores results in Snowflake, models them with dbt, orchestrates the workflow with Airflow, and visualises in Apache Superset — all running locally via Docker Compose.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Event Generation Layer                               │
│  kafka-producer/producer.py                                                  │
│  confluent-kafka · AvroSerializer · Schema Registry                          │
└────────────────────────────┬────────────────────────────────────────────────┘
                              │  Confluent Avro wire format
                              ▼
                    ┌─────────────────┐
                    │   Kafka Broker   │  retail_sales topic
                    │ + Schema Registry│
                    └────────┬────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│                      Stream Processing Layer                                  │
│  spark-streaming/stream_processor.py                                         │
│  PySpark 3.5 · from_avro() · 10-min watermark · dropDuplicates              │
│                                                                               │
│   foreachBatch ──────────────────────┐                                       │
│        │                             │                                        │
│   raw_retail_sales             agg_store_sales_5min                          │
│   (append, micro-batch)        (5-min window, append)                        │
└──────────────────┬──────────────────┬──────────────────────────────────────┘
                   │                  │
                   ▼                  ▼
         ┌─────────────────────────────────┐
         │         Snowflake               │
         │  RETAIL_DB.STREAMING            │
         │  · raw_retail_sales             │
         │  · agg_store_sales_5min         │
         │  · fact_sales  ◄── dbt MERGE    │
         └──────────────┬──────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────────────────┐
│                    Orchestration Layer (Airflow 2.7)                          │
│  retail_streaming_pipeline DAG — runs every 10 min                           │
│                                                                               │
│  validate_snowflake_conn                                                     │
│    → run_kafka_producer (25 events)                                          │
│    → check_spark_streaming_health                                            │
│    → run_dbt_models (MERGE into fact_sales)                                  │
│    → run_data_quality_checks (null · dup · Z-score volume · Z-score revenue) │
│    → send_alert_if_quality_fails (Slack webhook)                             │
└─────────────────────────────────────────────────────────────────────────────┘
                         │
                         ▼
              ┌──────────────────┐
              │ Apache Superset   │  Live dashboards from Snowflake
              │  localhost:8088   │
              └──────────────────┘
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Event streaming | Apache Kafka (Confluent) | 7.4 |
| Schema management | Confluent Schema Registry | 7.4 |
| Avro serialization | confluent-kafka (Python) | 2.3 |
| Stream processing | PySpark Structured Streaming | 3.5 |
| Data warehouse | Snowflake | — |
| Analytics modeling | dbt-snowflake | 1.7 |
| Orchestration | Apache Airflow | 2.7 |
| Dashboards | Apache Superset | 3.1 |
| Containerisation | Docker Compose | v2 |
| Metadata store | PostgreSQL | 15 |
| CI | GitHub Actions | — |

---

## Repository Structure

```
.
├── .github/
│   └── workflows/
│       └── ci.yml              # CI: lint, dbt compile, secret scan, dep audit
├── airflow-dags/
│   └── retail_pipeline_dag.py  # Airflow DAG (orchestration + quality + alerting)
├── dbt/
│   ├── models/
│   │   ├── fact_sales.sql      # Incremental MERGE fact table
│   │   ├── fact_sales.yml      # Model docs + dbt tests
│   │   └── streaming.yml       # Source definitions
│   ├── dbt_project.yml
│   └── profiles.yml            # Snowflake connection (reads from env vars)
├── helm/                       # Kubernetes Helm chart for all services
│   ├── Chart.yaml
│   ├── values.yaml
│   └── templates/              # Kafka, Spark, Airflow, Postgres, Superset
├── kafka-producer/
│   └── producer.py             # Confluent Avro producer
├── monitoring/
│   └── data_quality.sql        # Ad-hoc quality check SQL
├── snowflake/
│   └── setup.sql               # One-time DDL: warehouse, tables, grants
├── spark-streaming/
│   └── stream_processor.py     # PySpark Structured Streaming job
├── Dockerfile.airflow           # Airflow + confluent-kafka + dbt + Snowflake
├── Dockerfile.spark             # Spark + pre-warmed Ivy JAR cache
├── Dockerfile.superset          # Superset + snowflake-sqlalchemy
├── docker-compose.yml
├── superset_config.py           # Superset Postgres metadata config
├── .env.example                 # Environment variable template
├── requirements.txt             # Local dev dependencies
├── DATA_DICTIONARY.md           # Table schemas, column descriptions, lineage
├── SYSTEM_DESIGN.md             # Architecture decisions, SLAs, failure modes
├── RUNBOOK.md                   # On-call guide for top 5 failure scenarios
└── REPO_HEALTH.md               # Full audit: architecture, security, cost, CI/CD
```

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | 4.x+ | Must include Compose v2 (`docker compose`) |
| Python | 3.9+ | For generating keys locally |
| Snowflake account | — | Free trial at snowflake.com; needs ACCOUNTADMIN for initial setup |

---

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/ShrikantLambe/real-time-retail-sales-data-pipeline.git
cd real-time-retail-sales-data-pipeline
cp .env.example .env
```

Fill in all `<placeholder>` values in `.env`. Generate the required keys:

```bash
# Airflow Fernet key
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Superset secret key
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### 2. Set up Snowflake (once)

Run `snowflake/setup.sql` in a Snowflake worksheet as ACCOUNTADMIN. This creates:
- Warehouse `RETAIL_WH` (XSMALL, auto-suspend 60s)
- Database `RETAIL_DB`, schema `STREAMING`
- Tables `raw_retail_sales`, `agg_store_sales_5min`
- Role `DATA_ENGINEER` with appropriate grants

### 3. Create the Superset database in Postgres

This must be done once before starting Superset:

```bash
docker compose up -d postgres
docker compose exec postgres psql -U airflow -c "CREATE DATABASE superset;"
```

### 4. Build and start all services

```bash
docker compose up --build -d
```

First build: 5–10 min (Spark pre-downloads JARs). Subsequent starts: ~30s.

Check all services are healthy:

```bash
docker compose ps --format "table {{.Name}}\t{{.Status}}"
```

### 5. Access service UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| Superset | http://localhost:8088 | admin / admin123 |
| Spark Master | http://localhost:8081 | — |
| Schema Registry | http://localhost:8082 | — |

### 6. Trigger the pipeline

**Option A — Airflow UI:** Navigate to http://localhost:8080, find `retail_streaming_pipeline`, click **Trigger DAG**.

**Option B — CLI:**
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger retail_streaming_pipeline
```

**Option C — Run producer manually (smoke test):**
```bash
docker compose exec airflow-scheduler \
  python3 /opt/airflow/kafka-producer/producer.py
```

### 7. Verify in Snowflake

```sql
SELECT COUNT(*), MAX(transaction_ts) FROM RETAIL_DB.STREAMING.raw_retail_sales;
SELECT COUNT(*), MAX(window_start)   FROM RETAIL_DB.STREAMING.agg_store_sales_5min;
SELECT COUNT(*), MAX(transaction_ts) FROM RETAIL_DB.STREAMING.fact_sales;
```

### 8. Connect Snowflake to Superset

1. Open http://localhost:8088 → **Settings → Database Connections → + Database → Snowflake**
2. SQLAlchemy URI:
   ```
   snowflake://<USER>:<PASSWORD>@<ACCOUNT>/RETAIL_DB/STREAMING?warehouse=RETAIL_WH&role=DATA_ENGINEER
   ```
3. **Test Connection → Connect**
4. Open **SQL Lab** to build charts from live data

---

## Environment Variable Reference

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md#environment-variable-reference) for the full table.

| Variable | Required | Description |
|---|---|---|
| `KAFKA_BROKERS` | Yes | Bootstrap servers (`localhost:9092` locally; `kafka:9092` inside Docker) |
| `KAFKA_TOPIC` | Yes | Topic name (default: `retail_sales`) |
| `SCHEMA_REGISTRY_URL` | Yes | Registry URL (default: `http://localhost:8082`) |
| `SNOWFLAKE_ACCOUNT` | Yes | Account identifier from Snowflake UI |
| `SNOWFLAKE_USER` | Yes | Service account username |
| `SNOWFLAKE_PASSWORD` | Yes | Service account password |
| `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` | Yes | Full Snowflake URI for auto-registration |
| `AIRFLOW_FERNET_KEY` | Yes | Encryption key; generate with `Fernet.generate_key()` |
| `SUPERSET_SECRET_KEY` | Yes | Flask session key; generate with `secrets.token_hex(32)` |
| `SLACK_WEBHOOK_URL` | No | Leave blank to disable Slack alerts |
| `CHECKPOINT_DIR` | No | Spark checkpoint path (default: `/tmp/spark_checkpoint`) |
| `DAG_SCHEDULE` | No | Cron schedule (default: `*/10 * * * *`) |

---

## How to Run & Test

### dbt

```bash
# Run models
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt run --profiles-dir /home/airflow/.dbt"

# Run tests
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt test --profiles-dir /home/airflow/.dbt"

# Compile only (no DB connection needed)
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt compile --profiles-dir /home/airflow/.dbt"
```

### Schema Registry

```bash
curl -s http://localhost:8082/subjects
curl -s http://localhost:8082/subjects/retail_sales-value/versions/latest | python3 -m json.tool
```

### Kafka

```bash
# View messages (hex — Avro bytes start with 0x00)
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic retail_sales \
  --from-beginning --max-messages 1 --timeout-ms 5000 | xxd | head -3

# Check consumer lag
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --all-groups
```

### Full reset

```bash
# Stop services but keep data
docker compose down

# Destroy all data (volumes)
docker compose down -v
```

---

## Kubernetes (Helm)

```bash
helm lint helm/

helm install retail-pipeline ./helm \
  --set snowflake.account=MY_ACCOUNT \
  --set snowflake.user=MY_USER \
  --set-string snowflake.password=MY_PASS \
  --set airflow.fernetKey=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())") \
  --set superset.secretKey=$(openssl rand -base64 42)
```

See `helm/values.yaml` for all configurable parameters.

---

## Design Decisions

| Decision | Rationale |
|---|---|
| **Confluent Avro** | Schema versioned in Registry; forward/backward compatible evolution |
| **Spark decoupled from Registry at read** | `from_avro()` uses embedded schema string; Spark survives Registry downtime |
| **`foreachBatch` + Snowflake MERGE** | Batch semantics simplify writes; MERGE on `transaction_id` is idempotent |
| **10-minute watermark** | Bounds state; allows late events within 10 min to join windowed aggregates |
| **3-sigma Z-score anomaly detection** | Self-adapts to actual traffic; replaces brittle ±50% static thresholds |
| **Ivy cache pre-warmed in Dockerfile.spark** | Eliminates Maven resolution on restart; prevents cold-start failures |
| **`AIRFLOW_CONN_*` env var** | Snowflake connection auto-registered; no UI setup needed |
| **Superset + Postgres** | Dashboards and users persist across container restarts |

---

## Contributing

### Local setup

```bash
python3 -m venv .venv && source .venv/bin/activate
# macOS: brew install librdkafka  (required by confluent-kafka C extension)
pip install -r requirements.txt
```

### Branching

| Branch | Purpose |
|---|---|
| `main` | Protected; PRs required |
| `feature/<name>` | New features |
| `fix/<name>` | Bug fixes |
| `docs/<name>` | Documentation only |

### PR checklist

- [ ] CI passes (lint, dbt compile, secret scan)
- [ ] `README.md` updated if user-facing behaviour changed
- [ ] `DATA_DICTIONARY.md` updated if schema changed
- [ ] dbt tests added/updated for schema changes
- [ ] No secrets in diff

### Issues

Open a GitHub Issue with: steps to reproduce, `docker compose ps` output, and relevant `docker compose logs <service>` output.

---

## Additional Documentation

| Document | Contents |
|---|---|
| [DATA_DICTIONARY.md](DATA_DICTIONARY.md) | Table schemas, column descriptions, data lineage |
| [SYSTEM_DESIGN.md](SYSTEM_DESIGN.md) | Architecture decisions, SLAs, failure modes, scaling |
| [RUNBOOK.md](RUNBOOK.md) | On-call guide for top 5 failure scenarios |
| [REPO_HEALTH.md](REPO_HEALTH.md) | Full audit: architecture, security, cost, CI/CD backlog |
