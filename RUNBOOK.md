# Runbook — Real-Time Retail Sales Pipeline

On-call reference for the top failure scenarios. Each entry includes symptoms,
diagnosis steps, root cause analysis, and recovery procedure.

---

## Failure 1: Spark Streaming Job Crashes / Exits

### Symptoms
- `docker compose ps` shows `spark-streaming` in **Restarting** state
- No new rows in `raw_retail_sales` for > 15 minutes
- Airflow `check_spark_streaming_health` task logs a warning

### Diagnosis

```bash
# 1. Check last 50 lines of logs
docker compose logs --tail=50 spark-streaming

# 2. Common error signatures
docker compose logs spark-streaming | grep -E "ERROR|Exception|FAILED|terminated"
```

| Log message | Root cause |
|---|---|
| `Avro decoding failed` | Schema mismatch between producer and consumer |
| `Partition X's offset was changed from Y to 0` | Topic deleted/recreated; checkpoint stale |
| `Task N failed 4 times; aborting job` | Snowflake connection rejected or credentials expired |
| `ConnectException: Connection refused` | Kafka broker not reachable |
| `No such file or directory: stream_processor.py` | Volume mount missing |

### Recovery

**Stale checkpoint after topic recreation:**
```bash
docker compose rm -sf spark-streaming
docker compose up -d spark-streaming
```

**Kafka unreachable:**
```bash
docker compose restart kafka
docker compose logs --tail=20 kafka
# Wait for healthy, then:
docker compose restart spark-streaming
```

**Snowflake credentials expired:**
1. Update `SNOWFLAKE_PASSWORD` in `.env`
2. `docker compose up -d spark-streaming` (reads env at startup)

**Schema mismatch (Avro decode errors):**
1. Check Schema Registry: `curl http://localhost:8082/subjects`
2. Compare schema in `producer.py:RETAIL_SALE_SCHEMA` with `stream_processor.py:RETAIL_AVRO_SCHEMA`
3. If diverged, align both files, delete old schema version from Registry, restart containers

### Prevention
- `failOnDataLoss=false` prevents crashes on offset resets
- `from_avro` PERMISSIVE mode silently drops undecodable messages
- `restart: unless-stopped` automatically recovers transient failures

---

## Failure 2: Airflow DAG Short-Circuits at `validate_snowflake_conn`

### Symptoms
- All DAG runs show `validate_snowflake_conn` → **skipped** or **failed**
- Downstream tasks `run_kafka_producer`, `run_dbt_models` never execute
- Airflow UI shows the DAG as greyed out (short-circuited)

### Diagnosis

```bash
# View task log
docker compose exec airflow-scheduler \
  airflow tasks logs retail_streaming_pipeline validate_snowflake_conn <run_id>

# Test connection manually
docker compose exec airflow-scheduler python3 -c "
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('snowflake_default')
print(conn.host, conn.schema, conn.extra)
"
```

Common causes:
1. `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` not set or malformed in `.env`
2. Snowflake account identifier format wrong (should be `ORGNAME-ACCOUNTNAME` or legacy format)
3. Snowflake warehouse suspended and `AUTO_RESUME=FALSE`
4. Network outage to Snowflake

### Recovery

**Fix connection string:**
```bash
# Correct format for AIRFLOW_CONN_SNOWFLAKE_DEFAULT:
# snowflake://USER:PASSWORD@/?account=ACCOUNT_ID&database=RETAIL_DB&schema=STREAMING&warehouse=RETAIL_WH&role=DATA_ENGINEER

# Re-inject env and restart scheduler
docker compose up -d airflow-scheduler airflow-webserver
```

**Resume Snowflake warehouse:**
```sql
ALTER WAREHOUSE RETAIL_WH RESUME;
```

**Manually trigger after fixing:**
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger retail_streaming_pipeline
```

---

## Failure 3: Kafka Producer Fails / No Events Reaching Snowflake

### Symptoms
- `run_kafka_producer` Airflow task fails or succeeds but Snowflake shows no new rows
- Schema Registry shows no subjects: `curl http://localhost:8082/subjects` → `[]`
- Spark streaming logs show empty batches

### Diagnosis

```bash
# Check producer task log in Airflow
# Or run manually:
docker compose exec airflow-scheduler \
  python3 /opt/airflow/kafka-producer/producer.py

# Verify messages are in Kafka
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic retail_sales --from-beginning --max-messages 1 \
  --timeout-ms 5000 | xxd | head -5
# First byte should be 0x00 (Confluent magic byte)

# Check Schema Registry
curl http://localhost:8082/subjects
curl http://localhost:8082/subjects/retail_sales-value/versions/latest
```

Common causes:
1. Schema Registry not healthy (producer fails to register schema)
2. `SCHEMA_REGISTRY_URL` pointing to wrong host/port
3. Kafka topic deleted (auto-create will recreate on next produce)
4. `confluent-kafka` not installed in Airflow container (wrong image)

### Recovery

**Schema Registry not reachable:**
```bash
docker compose restart schema-registry
# Wait for healthy (curl http://localhost:8082/ returns 200)
docker compose exec airflow-scheduler \
  python3 /opt/airflow/kafka-producer/producer.py
```

**Wrong image (missing confluent-kafka):**
```bash
docker compose build airflow-scheduler airflow-webserver
docker compose up -d airflow-scheduler airflow-webserver
```

**Stale JSON messages causing Spark Avro decode failure:**
```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --delete --topic retail_sales
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --create --topic retail_sales \
  --partitions 1 --replication-factor 1
docker compose rm -sf spark-streaming
docker compose up -d spark-streaming
# Re-run producer
docker compose exec airflow-scheduler \
  python3 /opt/airflow/kafka-producer/producer.py
```

---

## Failure 4: dbt Run Fails

### Symptoms
- `run_dbt_models` Airflow task fails
- `fact_sales` table not updated / stale
- Airflow logs show `dbt run` exit code 1

### Diagnosis

```bash
# Run dbt manually inside airflow-scheduler
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt run --profiles-dir /home/airflow/.dbt 2>&1"

# Check for compilation errors
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt compile --profiles-dir /home/airflow/.dbt 2>&1"
```

| Error message | Root cause |
|---|---|
| `Profile 'default' not found` | `profiles.yml` not mounted or env vars missing |
| `Database 'RETAIL_DB' does not exist` | Snowflake setup not run or wrong database name |
| `Insufficient privileges` | `DATA_ENGINEER` role missing grants; re-run `snowflake/setup.sql` |
| `SQL compilation error: Object 'RAW_RETAIL_SALES' does not exist` | Source table not yet created; run `snowflake/setup.sql` first |
| `Incremental model already exists as wrong type` | Table/view type mismatch; run `dbt run --full-refresh` |

### Recovery

**Profile not found / env vars missing:**
```bash
# Verify env vars are set in the container
docker compose exec airflow-scheduler env | grep SNOWFLAKE

# Check profiles.yml is mounted
docker compose exec airflow-scheduler cat /home/airflow/.dbt/profiles.yml
```

**Re-run with full refresh (rebuilds from scratch):**
```bash
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt run --full-refresh --profiles-dir /home/airflow/.dbt"
```

**Re-apply Snowflake grants:**
```sql
-- Run in Snowflake worksheet as SYSADMIN or ACCOUNTADMIN
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA RETAIL_DB.STREAMING TO ROLE DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE ON FUTURE TABLES IN SCHEMA RETAIL_DB.STREAMING TO ROLE DATA_ENGINEER;
```

---

## Failure 5: Data Quality Alert Fires (Slack / DAG task `FAIL`)

### Symptoms
- Airflow `run_data_quality_checks` task returns `FAIL`
- `send_alert_if_quality_fails` logs `ALERT_SENT` (or would, if Slack is configured)
- One of: null rows, duplicate transaction IDs, volume anomaly, revenue anomaly detected

### Diagnosis

Run each check manually in Snowflake to identify the failing check:

```sql
-- 1. Null check
SELECT COUNT(*) AS null_count
FROM RETAIL_DB.STREAMING.raw_retail_sales
WHERE transaction_id IS NULL OR store_id IS NULL
   OR product_id IS NULL OR quantity IS NULL
   OR price IS NULL OR transaction_ts IS NULL;

-- 2. Duplicate check
SELECT transaction_id, COUNT(*) AS cnt
FROM RETAIL_DB.STREAMING.raw_retail_sales
GROUP BY 1 HAVING cnt > 1
LIMIT 10;

-- 3. Recent volume
SELECT DATE_TRUNC('hour', transaction_ts) AS hr, COUNT(*) AS vol
FROM RETAIL_DB.STREAMING.raw_retail_sales
WHERE transaction_ts >= DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY 1 DESC;
```

### Recovery

**Null rows detected:**
- Indicates a bug in the producer or a schema change; check `producer.py` for missing fields
- Null rows in Snowflake are already written; fix the producer and monitor for recurrence

**Duplicate transaction IDs:**
- Indicates Spark checkpoint was lost and messages were reprocessed from `startingOffsets=earliest`
- Deduplication logic in Spark only covers the current watermark window; past duplicates can still land
- Fix: deduplicate in `fact_sales` dbt model by taking the first occurrence

**Volume / revenue anomaly:**
- First, check if it is a real problem (producer stopped, Spark crashed) or a false positive (traffic spike)
- Z-score > 3 is a real outlier; confirm by checking `raw_retail_sales` row counts
- If false positive due to cold start (< 7 days of history), anomaly will auto-resolve as history builds
- If real: diagnose upstream using Failure 1 or Failure 3 runbooks

**Re-run DAG after fixing:**
```bash
docker compose exec airflow-scheduler \
  airflow tasks clear retail_streaming_pipeline \
    --task-ids run_data_quality_checks,send_alert_if_quality_fails \
    --yes
```

---

## General Operational Commands

```bash
# Check all service health
docker compose ps --format "table {{.Name}}\t{{.Status}}"

# Tail all logs simultaneously
docker compose logs -f --tail=20

# Restart a specific service
docker compose restart <service-name>

# Trigger pipeline from CLI
docker compose exec airflow-scheduler \
  airflow dags trigger retail_streaming_pipeline

# Run producer manually (25 events)
docker compose exec airflow-scheduler \
  python3 /opt/airflow/kafka-producer/producer.py

# Run dbt manually
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt run --profiles-dir /home/airflow/.dbt"

# Check Kafka topic offsets
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --all-groups

# Check Schema Registry schemas
curl -s http://localhost:8082/subjects | python3 -m json.tool

# Full restart (preserves volumes)
docker compose down && docker compose up -d

# Full reset (destroys all data - use with caution)
docker compose down -v && docker compose up --build -d
```
