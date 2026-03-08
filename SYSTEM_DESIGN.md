# System Design Document: Real-Time Retail Sales Data Pipeline

## 1. Data Flow

1. **Event Generation**: Retail transaction events are simulated by a Kafka producer and published to the `retail_sales` Kafka topic.
2. **Streaming Ingestion**: PySpark Structured Streaming job consumes events from Kafka, parses and deduplicates them, and applies a 10-minute watermark for late data handling.
3. **Transformation & Aggregation**:
    - Raw events are written to the `raw_retail_sales` table in Snowflake.
    - 5-minute windowed aggregations (by store) are computed and written to the `agg_store_sales_5min` table.
4. **Analytics & Modeling**: dbt models transform raw data into business-ready fact tables (e.g., `fact_sales`).
5. **Orchestration & Monitoring**: Airflow DAG orchestrates the pipeline, runs data quality checks, and triggers alerts on failures.

## 2. Scaling Strategy

- **Kafka**: Scales horizontally by increasing partitions and brokers, supporting high-throughput ingestion.
- **Spark Streaming**: Scales by adding executors and nodes; can process more partitions in parallel.
- **Snowflake**: Scales compute (warehouse size) independently from storage; auto-suspend minimizes idle costs.
- **Airflow**: Can be deployed with multiple schedulers and workers for high availability.
- **Containerization**: Docker Compose enables local scaling and easy migration to Kubernetes for cloud-scale deployments.

## 3. Failure Scenarios & Handling

- **Kafka Broker Failure**: Producers/consumers retry; data is not lost if replication is configured.
- **Spark Executor Failure**: Spark retries failed tasks; checkpointing ensures recovery from last successful batch.
- **Snowflake Outage**: Writes are retried; pipeline can buffer data in Kafka or Spark until Snowflake is available.
- **Airflow Task Failure**: Retries are configured; on repeated failure, alerts are triggered for manual intervention.
- **Data Quality Failure**: Airflow triggers alerts and can halt downstream tasks to prevent bad data propagation.

## 4. Cost Implications

- **Kafka**: Costs scale with broker count, storage, and network usage. Cloud-managed Kafka (e.g., MSK, Confluent Cloud) can be more expensive but reduces ops overhead.
- **Spark**: Compute costs depend on cluster size and job duration. Streaming jobs can be more expensive than batch due to always-on resources.
- **Snowflake**: Compute (warehouse) costs are per-second; auto-suspend and XSMALL size minimize idle costs. Storage is billed separately.
- **Airflow**: Minimal cost for orchestration, but can increase with scale and high-availability setups.
- **Trade-off**: Real-time guarantees and low-latency analytics increase infrastructure costs compared to batch.

## 5. Streaming vs Micro-Batching Trade-offs

| Aspect                | Streaming (event-at-a-time) | Micro-Batching (mini-batch) |
|-----------------------|-----------------------------|-----------------------------|
| Latency               | Lowest (sub-second)         | Higher (seconds-minutes)    |
| Throughput            | Lower per node              | Higher per node             |
| Complexity            | Higher (state, ordering)    | Lower (batch semantics)     |
| Fault Tolerance       | Complex (state mgmt)        | Easier (batch recovery)     |
| Use Case Fit          | Alerts, dashboards          | Reporting, ETL              |

- **This pipeline uses micro-batching (PySpark Structured Streaming)** for a balance of latency, throughput, and operational simplicity. Windowed aggregations and checkpointing are easier to reason about in micro-batch mode.

## 6. Exactly-Once vs At-Least-Once Semantics

- **At-Least-Once**: Guarantees every event is processed at least once, but duplicates may occur (e.g., after retries or failures). Simpler to implement, but requires downstream deduplication.
- **Exactly-Once**: Guarantees each event is processed only once, even in the face of retries/failures. Requires idempotent writes and careful state management (e.g., deduplication by transaction_id, atomic writes to Snowflake).

- **This pipeline achieves exactly-once semantics** at the application level:
    - Deduplication by transaction_id in Spark ensures idempotency.
    - Checkpointing and transactional writes to Snowflake prevent duplicates.
    - However, true end-to-end exactly-once is challenging in distributed systems; careful monitoring and data quality checks are essential.

---
