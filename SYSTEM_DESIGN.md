# System Design: Real-Time Retail Sales Data Pipeline

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Flow — Ingestion → Transformation → Serving](#2-data-flow)
3. [Component Responsibilities](#3-component-responsibilities)
4. [SLAs and SLOs](#4-slas-and-slos)
5. [Failure Modes and Fallback Strategies](#5-failure-modes-and-fallback-strategies)
6. [Scalability Considerations](#6-scalability-considerations)
7. [Exactly-Once Semantics](#7-exactly-once-semantics)
8. [Cost Model](#8-cost-model)

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│  Event Generation                                                 │
│  kafka-producer/producer.py                                       │
│  confluent-kafka · AvroSerializer · Schema Registry              │
└────────────────────────┬─────────────────────────────────────────┘
                          │  Confluent Avro wire format
                          │  [0x00][4-byte schema_id][avro bytes]
                          ▼
              ┌───────────────────────┐
              │  Confluent Schema     │  Subject: retail_sales-value
              │  Registry :8081       │  Compatibility: BACKWARD
              └───────────┬───────────┘
                          │
              ┌───────────▼───────────┐
              │  Apache Kafka :9092   │  Topic: retail_sales
              │  1 broker · 1 replica │  Partitions: 1 (dev)
              └───────────┬───────────┘
                          │
┌─────────────────────────▼────────────────────────────────────────┐
│  Stream Processing (PySpark 3.5 Structured Streaming)             │
│                                                                    │
│  readStream(kafka) → from_avro(PERMISSIVE) → dropDuplicates      │
│  → 10-min watermark                                               │
│                           │                                        │
│            ┌──────────────┴──────────────┐                        │
│            ▼                             ▼                         │
│   foreachBatch write              5-min window agg                │
│   raw_retail_sales                agg_store_sales_5min            │
│   (append · micro-batch)          (group by store · append)       │
└────────────┬─────────────────────────────┬────────────────────────┘
             │                             │
             ▼                             ▼
   ┌──────────────────────────────────────────────────┐
   │  Snowflake  RETAIL_DB.STREAMING                  │
   │  · raw_retail_sales       (append)               │
   │  · agg_store_sales_5min   (append)               │
   │  · fact_sales             (dbt MERGE on tx_id)   │
   └──────────────────────────┬───────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────┐
│  Orchestration (Airflow 2.7 — every 10 min)                       │
│                                                                    │
│  validate_snowflake_conn                                          │
│    → run_kafka_producer (25 synthetic events)                     │
│    → check_spark_streaming_health                                 │
│    → run_dbt_models  (dbt run → MERGE into fact_sales)           │
│    → run_data_quality_checks (null · dup · Z-score × 2)          │
│    → send_alert_if_quality_fails (Slack webhook)                  │
└──────────────────────────────┬───────────────────────────────────┘
                                │
                   ┌────────────▼───────────┐
                   │  Apache Superset :8088  │
                   │  Live dashboards via   │
                   │  Snowflake connector   │
                   └────────────────────────┘
```

---

## 2. Data Flow

### 2.1 Ingestion

```
producer.py
    │
    ├── Connects to Schema Registry (http://schema-registry:8081)
    │   └── Registers / fetches schema id for "retail_sales-value"
    │
    ├── Generates synthetic RetailSale records:
    │   transaction_id (UUID v4)
    │   store_id       (STORE_001 … STORE_003)
    │   product_id     (PROD_A … PROD_E)
    │   quantity       (1 – 10)
    │   unit_price     (5.00 – 500.00)
    │   total_amount   = quantity × unit_price
    │   transaction_ts (epoch millis, now)
    │   payment_method (CREDIT_CARD | DEBIT_CARD | CASH | MOBILE_PAY)
    │   customer_id    (nullable UUID)
    │
    └── Serializes with AvroSerializer → publishes to retail_sales topic
        Wire format: [0x00][4-byte big-endian schema_id][avro payload]
```

### 2.2 Stream Processing

```
stream_processor.py (always-on PySpark job)
    │
    ├── readStream.format("kafka")
    │   options: failOnDataLoss=false, startingOffsets=earliest
    │
    ├── Strip Confluent magic: slice col("value")[5:]
    │
    ├── from_avro(avro_payload, RETAIL_AVRO_SCHEMA_STR, {"mode":"PERMISSIVE"})
    │   └── Null "data" rows are filtered out (bad/legacy messages dropped)
    │
    ├── dropDuplicates(["transaction_id", "transaction_ts"])
    │   └── Bounded by 10-minute watermark on transaction_ts
    │
    └── foreachBatch(write_batch):
        ├── raw_retail_sales     → sfOptions MERGE (idempotent by tx_id)
        └── agg_store_sales_5min → 5-min tumbling window, group by store_id
                                   sfOptions MERGE on (store_id, window_start)
```

### 2.3 Transformation (dbt)

```
dbt run (triggered by Airflow every 10 min)
    │
    ├── Source: RETAIL_DB.STREAMING.raw_retail_sales
    │
    └── Model: fact_sales  (incremental, unique_key=transaction_id)
        SQL: INSERT … ON CONFLICT(transaction_id) DO UPDATE …
             (Snowflake: MERGE INTO fact_sales USING …)
        Columns added vs raw:
          · ingested_at  (CURRENT_TIMESTAMP at dbt run time)
          · revenue_band (CASE WHEN total_amount … )
```

### 2.4 Serving

```
Superset  ←  Snowflake (live SQL queries)
    │
    ├── Charts: hourly revenue by store, top products, payment method split
    ├── Dashboards: auto-refresh every 60 s
    └── SQL Lab: ad-hoc exploration against fact_sales / agg_store_sales_5min
```

---

## 3. Component Responsibilities

| Component | Primary Responsibility | Failure Owner |
|---|---|---|
| **kafka-producer/producer.py** | Generate and publish Avro-serialized retail events | Airflow DAG (run_kafka_producer task) |
| **Schema Registry** | Version Avro schemas; enforce BACKWARD compatibility | Kafka ecosystem |
| **Apache Kafka** | Durable, ordered event log; decouple producer from consumer | Zookeeper-managed cluster |
| **spark-streaming/stream_processor.py** | Always-on Avro decode, dedup, windowed agg, Snowflake writes | Docker `restart: unless-stopped`; Airflow health check |
| **Snowflake** | Immutable analytical storage; MERGE idempotency | Managed SaaS; auto-resume warehouse |
| **dbt (fact_sales model)** | Incremental MERGE from raw → curated fact layer | Airflow `run_dbt_models` task |
| **Airflow DAG** | Orchestrate, schedule, retry, quality-gate, alert | Scheduler + webserver HA |
| **Airflow data quality tasks** | Null/dup/Z-score checks; block downstream on failure | Alert → Slack webhook |
| **Apache Superset** | Live BI dashboards from Snowflake | Stateless; Postgres persists metadata |
| **Postgres** | Airflow metadata DB + Superset metadata DB | Volume-backed; shared instance |

---

## 4. SLAs and SLOs

### Pipeline SLOs (target)

| Metric | Target | Measurement Point |
|---|---|---|
| **End-to-end latency** (event → Snowflake raw) | < 60 s (p99) | Kafka `timestamp` vs Snowflake `loaded_at` |
| **End-to-end latency** (event → fact_sales) | < 15 min (p99) | Kafka `timestamp` vs fact_sales `ingested_at` |
| **Throughput** | ≥ 1,000 events/min sustained | Kafka consumer lag < 5,000 messages |
| **Data completeness** | ≥ 99.9 % events reach raw_retail_sales | Row count reconciliation in DQ task |
| **Deduplication correctness** | 0 duplicate `transaction_id` in fact_sales | dbt `unique` test |
| **DAG success rate** | ≥ 99 % of scheduled runs succeed | Airflow `dagrun` table |
| **Dashboard freshness** | Data ≤ 10 min old at Superset query time | Superset auto-refresh 60 s |

### Operational SLAs (on-call)

| Scenario | Response Target | Resolution Target |
|---|---|---|
| Spark job down | Page within 5 min | Restart within 15 min |
| Kafka consumer lag > 50,000 | Alert within 10 min | Lag clear within 60 min |
| Data quality check failure | Slack alert within 1 min of DAG failure | Investigation within 30 min |
| Snowflake connection lost | DAG retry (3 × 5 min) auto-heals | Manual escalation at 30 min |

### Current Gaps vs Targets

- Kafka consumer lag monitoring: **not implemented** (OB1 in REPO_HEALTH.md)
- Loaded_at timestamp on raw_retail_sales: **not present** — latency SLO unmeasurable
- Row count reconciliation source vs sink: **partial** (DQ task counts Snowflake only)

---

## 5. Failure Modes and Fallback Strategies

### 5.1 Failure Mode Matrix

| Component | Failure Mode | Detection | Fallback | Recovery |
|---|---|---|---|---|
| **Kafka broker** | Broker crash / OOM | Producer delivery error; consumer lag spike | Events buffered in Zookeeper; producer retries with backoff | Restart broker; replay from committed offset |
| **Schema Registry** | Unavailable | Producer `SchemaRegistryError` | Spark uses **embedded** schema string (decoupled by design) | Restart schema-registry container |
| **Spark job** | OOM / crash | `docker ps` shows Exited; `check_spark_streaming_health` DAG task fails | `restart: unless-stopped` auto-restarts | Spark replays from checkpoint; `failOnDataLoss=false` skips deleted offsets |
| **Spark checkpoint lost** (`/tmp`) | Container rm clears `/tmp` | Spark starts fresh — no prior state | Job resumes from `startingOffsets`; short duplication window | Clear checkpoint, redeploy with persistent volume |
| **Snowflake** | Warehouse suspended / auth expired | `snowflake.connector.errors.OperationalError` | Spark retries per `sfOptions`; DAG retries × 3 | Resume warehouse; rotate credentials |
| **Avro decode failure** | Schema mismatch / corrupt message | `from_avro()` returns null `data` column | `PERMISSIVE` mode: null rows filtered, job continues | Purge bad messages; update schema with compatible change |
| **Airflow scheduler** | OOM / crash | DAG runs stop appearing | `restart: unless-stopped` | Restart container; DB state preserved |
| **dbt run** | Compilation error / SQL error | `run_dbt_models` task red | Upstream raw tables unaffected; fact_sales stale | Fix model SQL; re-trigger DAG |
| **Data quality failure** | Null rate high / Z-score anomaly | `run_data_quality_checks` raises | `send_alert_if_quality_fails` fires Slack | Investigate root cause; re-run after fix |
| **Superset** | Container crash | HTTP 502 at :8088 | Stateless; Postgres preserves user/dashboard state | `docker compose up -d superset` |

### 5.2 Cascading Failure Scenarios

**Kafka topic deleted then recreated**

```
Symptom : Spark logs "offset X no longer available"
Cause   : topic delete resets partition offsets to 0
           Spark checkpoint still references old high-water mark
Mitigation: failOnDataLoss=false — Spark skips the gap and continues
Recovery  : rm -rf /tmp/spark_checkpoint; restart spark-streaming
```

**Stale non-Avro messages in topic**

```
Symptom : Spark job crashes on batch 0 with GenericDatumReader exception
Cause   : Legacy producer wrote JSON; Avro magic byte (0x00) absent
Mitigation: from_avro(..., {"mode":"PERMISSIVE"}) — null rows filtered
Recovery  : Delete and recreate topic; restart spark-streaming
```

**Superset metadata DB mismatch**

```
Symptom : "no such table: user_attribute" / 500 Internal Server Error
Cause   : superset-init wrote to SQLite (ephemeral container FS);
           superset webserver started with fresh SQLite
Mitigation: superset_config.py pins SQLALCHEMY_DATABASE_URI to Postgres
Recovery  : CREATE DATABASE superset; docker compose rm -sf superset superset-init;
            docker compose up -d superset
```

### 5.3 Single Points of Failure (SPOFs)

| SPOF | Impact | Mitigation (production) |
|---|---|---|
| Single Kafka broker | Total data loss if disk fails | 3-broker cluster; `replication.factor=3` |
| Single Spark master | No new job submissions | Standby master or Kubernetes operator |
| Single Postgres instance | Airflow + Superset both down | RDS Multi-AZ or Patroni |
| Snowflake credentials in `.env` | Credential leak = full data access | Secrets manager (Vault / AWS Secrets Manager) |

---

## 6. Scalability Considerations

### 6.1 Kafka

| Lever | Current | Scale-out action |
|---|---|---|
| Partitions | 1 | Increase to N (= target Spark parallelism) |
| Brokers | 1 | Add brokers; increase `replication.factor` |
| Retention | Default (7 days) | Tune per storage budget |
| Throughput ceiling | ~100 MB/s per partition | Partition fan-out |

### 6.2 Spark Streaming

| Lever | Current | Scale-out action |
|---|---|---|
| Workers | 1 (1 core, 1 GB) | Add spark-worker replicas in docker-compose |
| Executor memory | 1 GB | Increase `SPARK_WORKER_MEMORY` |
| Micro-batch trigger | Default (as-fast-as-possible) | `trigger(processingTime="30 seconds")` for cost control |
| Parallelism | Tied to Kafka partitions | Match `spark.default.parallelism` to partition count |

### 6.3 Snowflake

| Lever | Current | Scale-out action |
|---|---|---|
| Warehouse size | XSMALL | Scale up for heavy dbt transforms |
| Auto-suspend | 60 s | Tune based on access patterns |
| Multi-cluster | Off | Enable for concurrent dashboard + streaming load |
| Clustering keys | None on raw_retail_sales | Add `(transaction_ts::DATE)` for time-range pruning |

### 6.4 Airflow

| Lever | Current | Scale-out action |
|---|---|---|
| Executor | LocalExecutor | CeleryExecutor with Redis for parallel tasks |
| Workers | 1 scheduler | Add workers; enable HA scheduler (Airflow 2.4+) |
| DAG schedule | `*/10 * * * *` | Tune to match business freshness SLA |

### 6.5 Kubernetes Migration Path

```
1. Replace docker-compose with Helm chart (helm/ already provided)
2. Kafka       → Confluent Operator or Strimzi
3. Spark       → Spark Operator (spark-on-k8s-operator)
4. Airflow     → Official Helm chart (CeleryExecutor + KubernetesExecutor)
5. Snowflake   → No change (SaaS)
6. Superset    → Official Helm chart
7. Secrets     → Kubernetes Secrets + External Secrets Operator
```

---

## 7. Exactly-Once Semantics

This pipeline targets **effectively-once** (idempotent at-least-once) semantics:

| Stage | Mechanism | Guarantee |
|---|---|---|
| Producer → Kafka | Confluent producer with `acks=all` | At-least-once delivery |
| Kafka → Spark | Checkpoint tracks committed offsets | At-least-once consumption (no data loss) |
| Spark dedup | `dropDuplicates(["transaction_id","transaction_ts"])` with watermark | Deduplication within watermark window |
| Spark → Snowflake | `MERGE ON transaction_id` (sfOptions) | Idempotent writes — replays safe |
| dbt → fact_sales | `incremental` model, `unique_key=transaction_id` | Idempotent MERGE |

**Gap**: Late arrivals beyond the 10-minute watermark are silently dropped by Spark. If a message arrives 11+ minutes late (e.g., due to consumer lag), it will not appear in `agg_store_sales_5min`. It *will* appear in `raw_retail_sales` (foreachBatch has no watermark filter), so `fact_sales` will eventually include it on the next dbt run.

---

## 8. Cost Model

### Local Development (Docker Compose)

| Resource | Cost |
|---|---|
| Compute | Developer laptop only |
| Snowflake | XSMALL warehouse; auto-suspend 60 s; ~$0.002/credit |
| Storage | Snowflake on-demand; ~$23/TB/month |
| Schema Registry / Kafka | Local; $0 |

### Production (AWS estimate, moderate traffic)

| Component | Sizing | Est. Monthly Cost |
|---|---|---|
| MSK (Kafka) | 3 × kafka.m5.large | ~$350 |
| EMR Serverless (Spark) | 4 vCPU streaming, 16 GB | ~$200 |
| Snowflake | XSMALL, 8 h/day active | ~$180 |
| MWAA (Airflow) | mw1.small | ~$170 |
| RDS Postgres | db.t3.micro | ~$15 |
| Superset (ECS Fargate) | 0.5 vCPU, 1 GB | ~$20 |
| **Total** | | **~$935/month** |

### Cost Optimisation Levers

- Snowflake: set `AUTO_SUSPEND = 60` (already done); use `XSMALL` until query SLOs breach
- Spark: use `trigger(processingTime="1 minute")` instead of continuous to reduce credit consumption by ~40 %
- Kafka: use topic compaction on `transaction_id` to reduce storage for replayability
- Airflow: reduce DAG schedule to `*/15 * * * *` if 15-min freshness SLO is acceptable
- Superset: use query result caching (`CACHE_DEFAULT_TIMEOUT = 300`) to reduce Snowflake credits

---

*Last updated: 2026-03-08*
