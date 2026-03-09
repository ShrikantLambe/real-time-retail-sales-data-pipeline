# Repository Health Report

Full audit covering architecture, quality, security, monitoring, and cost.
Generated from codebase analysis.

**Legend:** Severity — 🔴 Critical · 🟠 High · 🟡 Medium · 🟢 Low
**Effort:** S = < 1 day · M = 1–3 days · L = 1+ week

---

## Architecture Review (Tasks 4–6)

### Bottlenecks & Single Points of Failure

| # | Finding | Severity | Effort | Recommendation |
|---|---|---|---|---|
| A1 | **Kafka single partition** — `retail_sales` topic has 1 partition; max consumer throughput is bounded by one Spark task | 🟠 High | S | Set `partitions: 3` (or match Spark worker cores) in `kafka.yaml` and `docker-compose.yml` |
| A2 | **Spark checkpoint in `/tmp`** — not volume-mounted; lost on every container stop/remove. Causes full re-read from `earliest` offset on restart | 🟠 High | S | Mount a named volume: `spark_checkpoint:/tmp/spark_checkpoint` in `docker-compose.yml` |
| A3 | **LocalExecutor in Airflow** — tasks run sequentially in one process; no parallelism and a single Airflow container is a SPOF | 🟡 Medium | M | Switch to `CeleryExecutor` for production; for local dev, `LocalExecutor` is acceptable |
| A4 | **Single Kafka broker** — `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`; broker failure means no Kafka | 🟡 Medium | M | For production, add 2+ brokers and set `replication.factor: 3` |
| A5 | **No dead-letter queue (DLQ)** — messages that fail Avro decode are silently dropped (PERMISSIVE mode) | 🟠 High | M | Add a second Kafka topic `retail_sales_dlq`; write failed messages there via `foreachBatch` error handling |
| A6 | **Producer sends fixed 25 events** — the producer exits after 25 messages; not suitable for continuous simulation | 🟡 Medium | S | Add a `--continuous` flag or environment variable `PRODUCER_EVENTS=N` (default `25`, `-1` for infinite) |
| A7 | **Superset uses Flask dev server** — `--debugger --reload` is not production-safe; single-threaded, exposes debugger PIN | 🟠 High | S | Replace with Gunicorn: `gunicorn -w 4 -b 0.0.0.0:8088 "superset.app:create_app()"` |
| A8 | **dbt has only one model layer** — everything is `fact_sales` directly from raw; no staging models | 🟡 Medium | M | Add `staging/stg_retail_sales.sql` for cleaning/casting, then reference it from `fact_sales` |
| A9 | **Incremental filter may skip late data** — `WHERE TRANSACTION_TS > MAX(TRANSACTION_TS)` silently drops events with old timestamps | 🟡 Medium | S | Add a lookback window: `WHERE TRANSACTION_TS > MAX(TRANSACTION_TS) - INTERVAL '1 hour'` |
| A10 | **`agg_store_sales_5min` not tested in dbt** — only `fact_sales` has dbt tests; the aggregation table has no quality controls | 🟡 Medium | S | Add source freshness tests and `not_null` column tests in `streaming.yml` |

### DAG & Orchestration Audit (Task 5)

| # | Finding | Severity | Effort | Recommendation |
|---|---|---|---|---|
| D1 | **`run_kafka_producer` runs inside Airflow container** — couples the data-generation step to Airflow; if producer hangs, it blocks the scheduler worker | 🟡 Medium | S | Use a `DockerOperator` or `KubernetesPodOperator` to isolate the producer process |
| D2 | **`check_spark_streaming_health` is non-blocking** — logs a warning but never fails the task; a stopped Spark job won't halt the DAG | 🟡 Medium | S | Raise `AirflowException` if `count == 0` to fail the task and block downstream `dbt run` |
| D3 | **`run_dbt_models` has no `--select` scope** — runs all models; if more models are added later, CI times and blast radius increase | 🟢 Low | S | Add `--select fact_sales` to the `dbt run` command |
| D4 | **`SLA` set to 30 minutes but no SLA miss callback** — the `sla` default_arg is set but `sla_miss_callback` is not defined; SLA breaches go unnoticed | 🟡 Medium | S | Add `sla_miss_callback=task_failure_callback` to the DAG definition |
| D5 | **No `max_active_runs` guard on retries** — with `retries=2` and `retry_delay=5min`, a sustained failure floods the DAG run history | 🟢 Low | S | Already set `max_active_runs=1`; also set `max_active_tasks=3` to bound concurrent task slots |
| D6 | **`DBT_DIR=/dbt` in `.env` but `/opt/airflow/dbt` in Airflow container** — env var `DBT_DIR` is defined but not used in the DAG (BashOperator uses `cwd=` directly) | 🟢 Low | S | Remove unused `DBT_DIR` env var or wire it into the BashOperator `cwd` parameter |

### dbt Model Review (Task 6)

| # | Finding | Severity | Effort | Recommendation |
|---|---|---|---|---|
| M1 | **No staging layer** — raw Snowflake columns fed directly into `fact_sales`; any column rename in the source breaks the model | 🟡 Medium | M | Create `models/staging/stg_raw_retail_sales.sql` that casts types and renames columns to snake_case |
| M2 | **`agg_store_sales_5min` not modelled in dbt** — the aggregation table is only written by Spark; no dbt lineage or tests exist for it | 🟡 Medium | S | Add `agg_store_sales_5min` as a source in `streaming.yml` with freshness and column tests |
| M3 | **Missing source freshness assertions** — `streaming.yml` has no `freshness:` block; stale data goes undetected | 🟠 High | S | Add `freshness: {warn_after: {count: 15, period: minute}, error_after: {count: 30, period: minute}}` to each source table |
| M4 | **All models default to `incremental`** — `dbt_project.yml` sets `+materialized: incremental` globally; a new model that should be a view gets incremental by default | 🟡 Medium | S | Set global default to `view`; override to `incremental` only in models that need it |
| M5 | **`PAYMENT_TYPE` accepted_values test is case-sensitive** — Snowflake stores values as sent by producer; if the producer sends `Credit_Card`, the dbt test fails | 🟢 Low | S | Add `where: "payment_type is not null"` and normalise case in staging model |

---

## Test Coverage Audit (Task 7)

### Current State: No automated tests exist anywhere in the repo.

| Component | Test Gap | Severity | Effort | Recommended Tests |
|---|---|---|---|---|
| `stream_processor.py` | Zero unit tests | 🔴 Critical | M | Test `parse_and_cast()` with mock DataFrame (valid Avro bytes, null bytes, malformed); test `deduplicate()` with duplicate transaction IDs; test `aggregate_window()` output schema |
| `producer.py` | Zero unit tests | 🟠 High | S | Test `RetailEventGenerator.generate_event()` output schema; test `ProducerConfig` with missing env var raises `EnvironmentError` |
| `retail_pipeline_dag.py` | Zero unit tests | 🟠 High | M | Test `_send_slack_alert()` with missing webhook URL (no-op); test `run_data_quality_checks()` with mocked `SnowflakeHook`; test `alert_if_quality_fails()` PASS/FAIL XCom paths |
| dbt models | Only schema tests | 🟡 Medium | S | Add `dbt test` to CI; add `relationships` test from `fact_sales.TRANSACTION_ID` to `raw_retail_sales.TRANSACTION_ID` |
| Snowflake setup | No idempotency test | 🟢 Low | S | Run `snowflake/setup.sql` twice and verify no errors (all statements use `IF NOT EXISTS`) |

**Suggested test file structure:**
```
tests/
├── unit/
│   ├── test_stream_processor.py
│   ├── test_producer.py
│   └── test_dag.py
└── integration/
    └── test_pipeline_e2e.py   # run with live Kafka + Snowflake in staging
```

---

## Data Quality Controls (Task 8)

| # | Finding | Severity | Effort | Recommendation |
|---|---|---|---|---|
| Q1 | **No schema validation at ingestion** — Avro enforces field types, but `PERMISSIVE` mode silently drops bad messages with no counter | 🟠 High | M | Add a Spark metric counter for PERMISSIVE drops; emit to a DLQ topic |
| Q2 | **No row count reconciliation between Kafka and Snowflake** — no way to detect if Spark is dropping valid messages | 🟠 High | M | Track Kafka consumer group lag; compare `raw_retail_sales` row count delta to Kafka partition offset delta per DAG run |
| Q3 | **Source freshness not asserted in dbt** — see M3 above | 🟠 High | S | Add `freshness:` to `streaming.yml` sources |
| Q4 | **`agg_store_sales_5min.TOTAL_REVENUE` nullable** — Snowflake allows NULL; a window with all-NULL prices would silently produce NULL revenue | 🟡 Medium | S | Add `not_null` test if `agg_store_sales_5min` is added as a dbt source |
| Q5 | **No Great Expectations or Soda integration** — all checks are SQL-based; no column distribution or referential integrity checks | 🟢 Low | L | Consider `soda-core-snowflake` for automated quality scans; add as an Airflow `BashOperator` task after `run_dbt_models` |

---

## CI/CD Review (Task 9)

A `.github/workflows/ci.yml` has been added to this repo. Current coverage:

| Check | Status |
|---|---|
| Secret scanning (TruffleHog) | ✅ Added |
| Python linting (flake8) | ✅ Added |
| SQL linting (sqlfluff) | ✅ Added (non-blocking) |
| dbt compile | ✅ Added (mock profile) |
| Python unit tests (pytest) | ✅ Stub added — needs test files |
| Docker build validation | ✅ Added (Airflow + Superset) |
| Dependency CVE audit (pip-audit) | ✅ Added |
| dbt test (live Snowflake) | ❌ Missing — requires CI secrets; add as a separate scheduled workflow |
| Branch protection rules | ❌ Not in code — set in GitHub repo Settings → Branches → Require PR reviews + status checks |

**Recommended branch protection (configure in GitHub UI):**
- Require pull request reviews (1 approver)
- Require status checks: `lint-python`, `dbt-compile`, `secret-scan`
- Restrict force-push to `main`

---

## Monitoring Gap Analysis (Task 10)

| # | Gap | Severity | Effort | Recommendation |
|---|---|---|---|---|
| OB1 | **No Kafka consumer lag monitoring** — Spark consumer falling behind is invisible | 🔴 Critical | M | Add `kafka-consumer-groups --describe` metrics to Airflow health check; or integrate Burrow / Confluent Control Center |
| OB2 | **No Spark job metrics** — batch duration, rows processed, and memory usage are not tracked | 🟠 High | M | Enable Spark metrics with `spark.metrics.conf`; push to Prometheus/Grafana or log to Snowflake via `foreachBatch` |
| OB3 | **No Snowflake cost tracking** — `QUERY_HISTORY` is not queried; warehouse credits consumed per DAG run are unknown | 🟡 Medium | S | Add a DAG task that queries `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` and logs credits consumed |
| OB4 | **dbt run results not logged** — `dbt run` exit code is checked, but model-level timing and row counts are not captured | 🟡 Medium | S | Use `dbt run --log-format json` and parse the artifacts in `target/run_results.json` |
| OB5 | **No dead-letter queue handling** — failed Avro messages disappear silently | 🟠 High | M | See A5 — add DLQ topic + an Airflow task that counts DLQ messages and alerts if > 0 |
| OB6 | **No Airflow task duration alerting** — a task that normally takes 10s and suddenly takes 5min would not trigger any alert | 🟡 Medium | S | Use `execution_timeout` (already set on most tasks) + `SLA` + `sla_miss_callback` (see D4) |

---

## Security & Compliance Audit (Tasks 12–13)

### Secrets & Access Control

| # | Finding | Severity | Effort | Recommendation |
|---|---|---|---|---|
| S1 | **Weak default passwords** — Postgres uses `airflow/airflow`; Superset uses `admin123`; Airflow UI uses `airflow/airflow` | 🟠 High | S | Rotate all passwords before any non-local deployment; inject via a secrets manager (AWS Secrets Manager, HashiCorp Vault) |
| S2 | **Snowflake password in `.env` as plaintext** — `.env` is gitignored correctly, but transmitted in plain text to containers via environment variables | 🟡 Medium | M | For production, use Docker secrets or Kubernetes Secrets with `secretKeyRef`; the Helm chart already uses `secretRef` |
| S3 | **Kafka has no authentication (no ACLs)** — any container on the Docker network can produce or consume | 🟡 Medium | M | Enable SASL/SCRAM authentication for production Kafka; add ACLs restricting `retail_sales` topic to producer + consumer service accounts only |
| S4 | **Schema Registry has no authentication** — any client can register or delete schemas | 🟡 Medium | M | Enable Schema Registry basic auth or mTLS for production |
| S5 | **Superset debug server exposes debugger PIN** — `--debugger` flag in production allows remote code execution if the pin is leaked | 🔴 Critical | S | Replace `superset run --debugger` with Gunicorn in `docker-compose.yml` (see A7) |
| S6 | **No secret scanning in git history** — previous commits may contain secrets if `.env` was accidentally staged | 🟠 High | S | CI TruffleHog scan added; also run `git log --all --full-history -- .env` to check history |
| S7 | **`DATA_ENGINEER` role has `INSERT` + `UPDATE` on all tables** — broader than needed for read-only analytics consumers | 🟢 Low | S | Create a read-only `ANALYST` role with `SELECT` only; grant to BI tools (Superset) |

### PII & Data Governance

| # | Finding | Severity | Effort | Recommendation |
|---|---|---|---|---|
| P1 | **`PAYMENT_TYPE` is not PII but adjacent** — payment method type is low-sensitivity but could combine with other fields to infer financial behaviour | 🟢 Low | S | No masking required for payment_type alone; document sensitivity classification in DATA_DICTIONARY.md |
| P2 | **No data retention policy** — `raw_retail_sales` grows unbounded; no TTL or archival strategy | 🟡 Medium | M | Add a Snowflake Data Retention policy or a dbt snapshot-based archival job; define retention SLA (e.g. 90 days raw, 2 years aggregated) |
| P3 | **No audit logging** — Snowflake `ACCESS_HISTORY` is not reviewed; no record of who queried what | 🟡 Medium | S | Enable Snowflake Account Usage views and set up weekly `ACCESS_HISTORY` review for compliance |
| P4 | **Synthetic data only (current)** — the current producer generates fake data; adding real retail data requires a PII inventory and GDPR/CCPA assessment | 🟡 Medium | L | Before onboarding real data: conduct a PII scan, implement column-level masking for PII fields, add row-level security for store-level access control |

---

## Dependency & Version Hygiene (Task 14)

| # | Finding | Severity | Effort | Recommendation |
|---|---|---|---|---|
| V1 | **`confluent-kafka==2.3.0` is not the latest** (latest ~2.5.x as of early 2026) | 🟢 Low | S | Test with latest; update if no breaking changes |
| V2 | **`apache/superset:3.1.0` — 2+ major versions behind** (4.x available) | 🟡 Medium | M | Upgrade Superset; retest `cryptography` pin after upgrade |
| V3 | **`confluentinc/cp-kafka:7.4.0` uses Zookeeper** — Kafka 3.x+ supports KRaft (no Zookeeper) | 🟡 Medium | L | Migrate to KRaft mode to eliminate Zookeeper dependency; reduces ops complexity |
| V4 | **`requirements.txt` has `kafka-python==2.0.2` removed but `confluent-kafka` appears twice** — `confluent-kafka==2.3.0` and `confluent-kafka[avro]==2.3.0` are redundant | 🟢 Low | S | Keep only `confluent-kafka[avro]==2.3.0`; it includes the base package |
| V5 | **No `dbt-packages.yml` lock file** — dbt has no packages defined; if packages are added later, version locks are critical | 🟢 Low | S | Run `dbt deps --lock` after adding any dbt packages to generate `package-lock.yml` |

---

## Cost Optimization Review (Task 15)

| # | Finding | Severity | Effort | Recommendation |
|---|---|---|---|---|
| C1 | **XSMALL warehouse with `AUTO_SUSPEND=60s`** — well-sized for micro-batch loads and dbt incremental runs | ✅ Good | — | No change needed; cost is near-zero when idle |
| C2 | **dbt runs every 10 minutes** — triggers a warehouse resume on every DAG run; at $0.002/credit, 6 runs/hour × 24h = 144 warehouse resumes/day | 🟡 Medium | S | Consider reducing to `*/30 * * * *` or using `dbt build --select result:error+` to skip passing models |
| C3 | **`raw_retail_sales` grows unbounded** — no partition pruning beyond clustering; full table scans become expensive as data grows | 🟡 Medium | M | Add a `WHERE transaction_ts >= DATEADD('day', -90, CURRENT_TIMESTAMP())` partition filter in `fact_sales.sql`; archive older data to a cheap storage layer |
| C4 | **Spark worker always on** — `spark-worker` container runs 24/7 even when no jobs are active | 🟢 Low | S | For development, use `docker compose stop spark-worker` when not actively streaming; for production use Kubernetes HPA to scale workers to 0 |
| C5 | **Superset Postgres separate from Airflow Postgres** — currently shares the same Postgres container (airflow user, superset DB); this is fine at small scale | 🟢 Low | — | No change for dev; for production, separate into dedicated RDS instances |

---

## Environment Parity (Task 16)

| # | Finding | Severity | Effort | Recommendation |
|---|---|---|---|---|
| E1 | **Only one environment exists** — there is no dev/staging/prod separation; all development happens against the single `RETAIL_DB` Snowflake database | 🟠 High | M | Create `RETAIL_DB_STAGING` and `RETAIL_DB_PROD` in Snowflake; use dbt `targets` to switch between them |
| E2 | **dbt profile has only one `dev` target** — `profiles.yml` has a single output; no staging or prod target | 🟠 High | S | Add `staging` and `prod` targets in `profiles.yml` using different databases/schemas |
| E3 | **Helm `values.yaml` has no environment overrides** — the chart has one `values.yaml` but no `values-staging.yaml` or `values-prod.yaml` | 🟡 Medium | S | Create `helm/values-staging.yaml` and `helm/values-prod.yaml` with environment-specific image tags, replica counts, and resource limits |
| E4 | **Kafka topic name is hardcoded in multiple places** — `retail_sales` appears in `docker-compose.yml`, `.env`, and `kafka-producer/producer.py` fallbacks | 🟢 Low | S | `KAFKA_TOPIC` is already an env var; ensure all code reads from env only (no hardcoded strings) |

---

## Master Prioritised Backlog (Bonus Task)

### 🔴 Critical (fix immediately)

| ID | Finding | Effort |
|---|---|---|
| S5 | Replace Superset Flask dev server with Gunicorn | S |
| OB1 | Add Kafka consumer lag monitoring | M |
| A5 | Implement dead-letter queue for failed Avro messages | M |
| (Task 7) | Write unit tests for Spark, DAG, and producer | M |

### 🟠 High (fix this sprint)

| ID | Finding | Effort |
|---|---|---|
| A2 | Mount named volume for Spark checkpoint | S |
| A1 | Increase Kafka partitions to 3 | S |
| D2 | Make `check_spark_streaming_health` fail the task on 0 rows | S |
| M3 | Add source freshness assertions in `streaming.yml` | S |
| Q2 | Add Kafka lag vs. Snowflake row count reconciliation | M |
| S1 | Rotate weak default passwords | S |
| E1 | Create staging Snowflake database and dbt target | M |
| OB2 | Instrument Spark batch metrics | M |

### 🟡 Medium (next sprint)

| ID | Finding | Effort |
|---|---|---|
| A7 | Replace Superset Flask dev server with Gunicorn in docker-compose | S |
| A8 | Add dbt staging model layer | M |
| A9 | Fix incremental filter to include late-arriving data (lookback window) | S |
| M4 | Change dbt global default materialization to `view` | S |
| D1 | Isolate Kafka producer from Airflow (DockerOperator) | S |
| D4 | Add SLA miss callback | S |
| E2 | Add staging/prod targets to dbt profiles.yml | S |
| E3 | Add `values-staging.yaml` and `values-prod.yaml` to Helm chart | S |

### 🟢 Low (backlog)

| ID | Finding | Effort |
|---|---|---|
| V4 | Remove duplicate `confluent-kafka` entry in `requirements.txt` | S |
| V3 | Migrate Kafka from Zookeeper to KRaft mode | L |
| P2 | Define and implement data retention policy | M |
| C2 | Reduce dbt DAG run frequency or add `result:error+` filtering | S |
| D6 | Remove unused `DBT_DIR` env var or wire it properly | S |
