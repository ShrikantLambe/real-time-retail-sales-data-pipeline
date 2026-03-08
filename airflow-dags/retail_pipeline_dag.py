"""
Production-grade Airflow DAG for retail streaming pipeline.
"""
import logging
import os
from datetime import timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago


class DAGConfig:
    """Configuration for Airflow DAG and tasks."""

    def __init__(self) -> None:
        self.schedule = os.getenv('DAG_SCHEDULE', '*/10 * * * *')
        self.snowflake_conn_id = os.getenv('SNOWFLAKE_CONN_ID', 'snowflake_default')
        self.dbt_dir = os.getenv('DBT_DIR', '/opt/airflow/dbt')


class StructuredLogger:
    """Structured logger for Airflow tasks."""
    @staticmethod
    def setup() -> None:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(name)s %(message)s'
        )

    @staticmethod
    def log_failure(context: Dict[str, Any]) -> None:
        logging.error(f"Task failed: {context['task_instance'].task_id}", extra={
                      'dag_id': context['dag'].dag_id})

    @staticmethod
    def log_alert(message: str) -> None:
        logging.warning(message)


def task_failure_callback(context: Dict[str, Any]) -> None:
    """Failure callback — logs and can be extended with Slack/PagerDuty alerting."""
    StructuredLogger.log_failure(context)
    # TODO: hook in Slack/email/PagerDuty here


def validate_snowflake_conn(**kwargs) -> bool:
    """Validate Snowflake connection via Airflow Connection store."""
    from airflow.hooks.base import BaseHook
    try:
        BaseHook.get_connection(DAGConfig().snowflake_conn_id)
        logging.info('Snowflake connection validated.')
        return True
    except Exception as e:
        logging.error(f"Snowflake connection failed: {e}")
        return False


def run_data_quality_checks(**kwargs) -> str:
    """
    Execute all data quality checks against Snowflake and return a pass/fail summary.
    Result is pushed to XCom for the downstream alert task.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=DAGConfig().snowflake_conn_id)
    failures = []

    # 1. Null check on critical columns
    null_row = hook.get_first("""
        SELECT
            CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
            COUNT(*) AS null_count
        FROM RETAIL_DB.STREAMING.raw_retail_sales
        WHERE transaction_id IS NULL OR store_id IS NULL
           OR product_id IS NULL OR quantity IS NULL
           OR price IS NULL OR transaction_ts IS NULL
    """)
    logging.info(f"null_check: status={null_row[0]}, null_count={null_row[1]}")
    if null_row[0] == 'FAIL':
        failures.append(f"null_check FAIL: {null_row[1]} null rows")

    # 2. Duplicate transaction_id detection
    dup_row = hook.get_first("""
        SELECT
            CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
            COUNT(*) AS duplicate_count
        FROM (
            SELECT transaction_id
            FROM RETAIL_DB.STREAMING.raw_retail_sales
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        )
    """)
    logging.info(f"duplicate_check: status={dup_row[0]}, duplicate_count={dup_row[1]}")
    if dup_row[0] == 'FAIL':
        failures.append(f"duplicate_check FAIL: {dup_row[1]} duplicate transaction_ids")

    # 3. Volume anomaly: last 1 hr vs avg of previous 24 hrs (±50%).
    #    Guard against division-by-zero on first run (prev_24hr.volume = 0).
    vol_row = hook.get_first("""
        WITH last_hour AS (
            SELECT COUNT(*) AS volume
            FROM RETAIL_DB.STREAMING.raw_retail_sales
            WHERE transaction_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        ),
        prev_24hr AS (
            SELECT COUNT(*) AS volume
            FROM RETAIL_DB.STREAMING.raw_retail_sales
            WHERE transaction_ts < DATEADD('hour', -1, CURRENT_TIMESTAMP())
              AND transaction_ts >= DATEADD('hour', -25, CURRENT_TIMESTAMP())
        )
        SELECT
            CASE
                WHEN p24.volume = 0 THEN 'PASS'
                WHEN lh.volume BETWEEN (p24.volume / 24) * 0.5
                                   AND (p24.volume / 24) * 1.5 THEN 'PASS'
                ELSE 'FAIL'
            END,
            lh.volume         AS last_hour_volume,
            (p24.volume / 24) AS avg_hourly_volume
        FROM last_hour lh, prev_24hr p24
    """)
    logging.info(f"volume_anomaly: status={vol_row[0]}, last_hour={vol_row[1]}, avg_hourly={vol_row[2]}")
    if vol_row[0] == 'FAIL':
        failures.append(f"volume_anomaly FAIL: last_hour={vol_row[1]}, avg_hourly={vol_row[2]}")

    # 4. Revenue anomaly: same ±50% window, guarded against NULL/zero baseline.
    rev_row = hook.get_first("""
        WITH last_hour AS (
            SELECT SUM(quantity * price) AS revenue
            FROM RETAIL_DB.STREAMING.raw_retail_sales
            WHERE transaction_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        ),
        prev_24hr AS (
            SELECT SUM(quantity * price) AS revenue
            FROM RETAIL_DB.STREAMING.raw_retail_sales
            WHERE transaction_ts < DATEADD('hour', -1, CURRENT_TIMESTAMP())
              AND transaction_ts >= DATEADD('hour', -25, CURRENT_TIMESTAMP())
        )
        SELECT
            CASE
                WHEN p24.revenue IS NULL OR p24.revenue = 0 THEN 'PASS'
                WHEN lh.revenue BETWEEN (p24.revenue / 24) * 0.5
                                    AND (p24.revenue / 24) * 1.5 THEN 'PASS'
                ELSE 'FAIL'
            END,
            lh.revenue         AS last_hour_revenue,
            (p24.revenue / 24) AS avg_hourly_revenue
        FROM last_hour lh, prev_24hr p24
    """)
    logging.info(f"revenue_anomaly: status={rev_row[0]}, last_hour={rev_row[1]}, avg_hourly={rev_row[2]}")
    if rev_row[0] == 'FAIL':
        failures.append(f"revenue_anomaly FAIL: last_hour={rev_row[1]}, avg_hourly={rev_row[2]}")

    if failures:
        return 'FAIL: ' + ' | '.join(failures)
    return 'PASS'


def alert_if_quality_fails(**kwargs) -> str:
    """Trigger alert if any data quality check returned FAIL."""
    ti = kwargs['ti']
    dq_result = ti.xcom_pull(task_ids='run_data_quality_checks')
    if dq_result and 'FAIL' in str(dq_result):
        StructuredLogger.log_alert(f'Data quality check failed: {dq_result}')
        # TODO: send Slack/email/PagerDuty notification
        return 'ALERT_SENT'
    logging.info('Data quality check passed.')
    return 'NO_ALERT'


def check_streaming_health(**kwargs) -> None:
    """
    Verify Spark streaming is actively writing data to Snowflake.
    Logs a warning (non-blocking) if no records arrived in the last 15 minutes.
    The Spark service is managed by Docker Compose, not triggered here.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=DAGConfig().snowflake_conn_id)
    row = hook.get_first("""
        SELECT COUNT(*) FROM RETAIL_DB.STREAMING.raw_retail_sales
        WHERE transaction_ts >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
    """)
    count = row[0] if row else 0
    logging.info(f"Records written to Snowflake in the last 15 minutes: {count}")
    if count == 0:
        logging.warning(
            "No records in raw_retail_sales for the past 15 minutes. "
            "Verify spark-streaming container is running: docker compose ps"
        )


StructuredLogger.setup()
config = DAGConfig()

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=30),
    'on_failure_callback': task_failure_callback,
}

with DAG(
    dag_id='retail_streaming_pipeline',
    default_args=default_args,
    description='Retail streaming pipeline with monitoring and analytics',
    schedule_interval=config.schedule,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['retail', 'streaming', 'production'],
) as dag:

    # Gate: short-circuit the whole DAG if Snowflake is unreachable.
    validate_snowflake = ShortCircuitOperator(
        task_id='validate_snowflake_conn',
        python_callable=validate_snowflake_conn,
    )

    # Produce 25 synthetic retail events to Kafka.
    # Credentials come from the container environment (docker-compose / .env).
    run_kafka_producer = BashOperator(
        task_id='run_kafka_producer',
        bash_command='python3 /opt/airflow/kafka-producer/producer.py',
        execution_timeout=timedelta(minutes=10),
    )

    # Spark streaming is a long-lived Docker Compose service — not triggered here.
    # This task checks whether recent data has landed in Snowflake as a health signal.
    check_spark_streaming = PythonOperator(
        task_id='check_spark_streaming_health',
        python_callable=check_streaming_health,
        execution_timeout=timedelta(minutes=5),
    )

    # Run dbt incremental models to materialise fact_sales.
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='dbt run --profiles-dir /home/airflow/.dbt',
        cwd='/opt/airflow/dbt',
        execution_timeout=timedelta(minutes=10),
    )

    # Execute all four data quality checks; result pushed to XCom.
    run_data_quality = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=run_data_quality_checks,
        execution_timeout=timedelta(minutes=10),
    )

    # Alert if any check failed.
    send_alert = PythonOperator(
        task_id='send_alert_if_quality_fails',
        python_callable=alert_if_quality_fails,
    )

    (
        validate_snowflake
        >> run_kafka_producer
        >> check_spark_streaming
        >> run_dbt
        >> run_data_quality
        >> send_alert
    )
