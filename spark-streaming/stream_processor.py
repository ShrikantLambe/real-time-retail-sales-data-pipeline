"""
PySpark Structured Streaming job for retail sales pipeline — Avro edition.

Schema evolution strategy:
  - The Kafka producer serialises messages using Confluent Avro wire format:
      [0x00 magic byte][4-byte schema_id][avro payload]
  - This job strips the 5-byte Confluent prefix with substring(value, 6, ...)
    then decodes the raw Avro bytes using PySpark's built-in from_avro().
  - The Avro schema string is kept in sync with the producer's RETAIL_SALE_SCHEMA.
    When the schema evolves, bump the version here and in the producer together.

Production best practices: modularity, error handling, structured logging,
config separation, docstrings, type hints.
"""
import json
import logging
import os
from typing import Any, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, window, sum as _sum, to_timestamp


# ---------------------------------------------------------------------------
# Avro schema — must stay in sync with the Kafka producer's RETAIL_SALE_SCHEMA
# ---------------------------------------------------------------------------
RETAIL_AVRO_SCHEMA: Dict[str, Any] = {
    "type": "record",
    "name": "RetailSale",
    "namespace": "com.retail.sales",
    "doc": "A single retail point-of-sale transaction.",
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "store_id",       "type": "string"},
        {"name": "product_id",     "type": "string"},
        {"name": "quantity",       "type": "int"},
        {"name": "price",          "type": "float"},
        {"name": "payment_type",   "type": ["null", "string"], "default": None},
        {"name": "transaction_ts", "type": "string"},
    ],
}
RETAIL_AVRO_SCHEMA_STR = json.dumps(RETAIL_AVRO_SCHEMA)


class SnowflakeConfig:
    """Configuration for Snowflake connection."""

    def __init__(self) -> None:
        self.options = {
            'sfURL':      os.getenv('SNOWFLAKE_URL'),
            'sfUser':     os.getenv('SNOWFLAKE_USER'),
            'sfPassword': os.getenv('SNOWFLAKE_PASSWORD'),
            'sfDatabase': os.getenv('SNOWFLAKE_DATABASE', 'RETAIL_DB'),
            'sfSchema':   os.getenv('SNOWFLAKE_SCHEMA', 'STREAMING'),
            'sfWarehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'RETAIL_WH'),
            'sfRole':     os.getenv('SNOWFLAKE_ROLE', 'DATA_ENGINEER'),
        }


class KafkaConfig:
    """Configuration for Kafka streaming."""

    def __init__(self) -> None:
        self.bootstrap_servers = os.getenv('KAFKA_BROKERS')
        self.topic = os.getenv('KAFKA_TOPIC', 'retail_sales')
        self.checkpoint_dir = os.getenv('CHECKPOINT_DIR', '/tmp/spark_checkpoint')
        self.starting_offsets = os.getenv('KAFKA_STARTING_OFFSETS', 'latest')


class StructuredLogger:
    """Structured logger for streaming job."""
    @staticmethod
    def setup() -> None:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(name)s %(message)s'
        )

    @staticmethod
    def log_batch(batch_id: int, message: str, extra: Dict[str, Any] = None) -> None:
        logging.info(f'Batch {batch_id}: {message}', extra=extra or {})

    @staticmethod
    def log_error(batch_id: int, error: Exception) -> None:
        logging.error(f'Batch {batch_id}: Error', extra={'error': str(error)})


class RetailStreamProcessor:
    """Retail streaming processor for PySpark."""

    def __init__(self, spark: SparkSession, kafka_cfg: KafkaConfig,
                 snowflake_cfg: SnowflakeConfig) -> None:
        self.spark = spark
        self.kafka_cfg = kafka_cfg
        self.snowflake_cfg = snowflake_cfg

    def parse_and_cast(self, df: DataFrame) -> DataFrame:
        """Decode Confluent Avro wire format and cast transaction_ts to Timestamp.

        Confluent wire format layout (all Kafka messages from this producer):
          Byte 0      : magic byte (0x00)
          Bytes 1–4   : schema_id (big-endian int32)
          Bytes 5–end : Avro-encoded payload

        We strip the 5-byte prefix with substring(value, 6, ...) before handing
        the raw Avro bytes to from_avro(), which uses the embedded schema string
        rather than a live Schema Registry call — keeping Spark decoupled from
        Registry availability at read time.
        """
        avro_payload = expr(
            "substring(value, 6, length(value) - 5)"
        )
        # PERMISSIVE: malformed messages produce null rows instead of crashing the job.
        # This handles stale non-Avro messages and unexpected wire-format changes gracefully.
        # Null rows (decode failures) are filtered out before writing to Snowflake.
        decoded = df.select(
            from_avro(avro_payload, RETAIL_AVRO_SCHEMA_STR, {"mode": "PERMISSIVE"}).alias("data")
        )
        return decoded.filter(col("data").isNotNull()).select(
            col("data.transaction_id"),
            col("data.store_id"),
            col("data.product_id"),
            col("data.quantity"),
            col("data.price"),
            col("data.payment_type"),
            to_timestamp(col("data.transaction_ts")).alias("transaction_ts"),
        )

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Deduplicate by transaction_id with watermark."""
        return df.withWatermark("transaction_ts", "10 minutes") \
                 .dropDuplicates(["transaction_id"])

    def aggregate_window(self, df: DataFrame) -> DataFrame:
        """5-minute window aggregation by store_id.

        The incoming df already has a watermark applied by deduplicate().
        Re-applying withWatermark here would create a second conflicting
        watermark — rely on the existing one.
        """
        agg = (
            df.groupBy(window(col("transaction_ts"), "5 minutes"), col("store_id"))
            .agg(
                _sum(col("quantity") * col("price")).alias("total_revenue"),
                _sum("quantity").alias("total_quantity"),
            )
        )
        return agg.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("store_id"),
            col("total_revenue"),
            col("total_quantity"),
        )

    def write_to_snowflake(self, df: DataFrame, table: str, batch_id: int) -> None:
        """Write a micro-batch DataFrame to a Snowflake table.

        Writing an empty DataFrame is a no-op in Snowflake — no need to
        call df.count() (which would trigger a full extra scan).
        """
        try:
            StructuredLogger.log_batch(batch_id, f"Writing batch to {table}.")
            df.write \
              .format("snowflake") \
              .options(**self.snowflake_cfg.options) \
              .option("dbtable", table) \
              .mode("append") \
              .save()
            StructuredLogger.log_batch(batch_id, f"{table} batch committed to Snowflake.")
        except Exception as exc:
            StructuredLogger.log_error(batch_id, exc)

    def run(self) -> None:
        """Run the streaming job."""
        # failOnDataLoss=false: skip missing offsets if topic is deleted/recreated or compacted.
        raw_stream = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_cfg.bootstrap_servers)
            .option("subscribe", self.kafka_cfg.topic)
            .option("startingOffsets", self.kafka_cfg.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed_stream = self.parse_and_cast(raw_stream)
        deduped_stream = self.deduplicate(parsed_stream)

        def write_raw(batch_df: DataFrame, batch_id: int) -> None:
            self.write_to_snowflake(batch_df, "raw_retail_sales", batch_id)

        _raw_query = (  # noqa: F841 — kept alive; awaitAnyTermination monitors it
            deduped_stream.writeStream
            .foreachBatch(write_raw)
            .outputMode("append")
            .option("checkpointLocation", f"{self.kafka_cfg.checkpoint_dir}/raw")
            .start()
        )

        agg_stream = self.aggregate_window(deduped_stream)

        def write_agg(batch_df: DataFrame, batch_id: int) -> None:
            self.write_to_snowflake(batch_df, "agg_store_sales_5min", batch_id)

        _agg_query = (  # noqa: F841 — kept alive; awaitAnyTermination monitors it
            agg_stream.writeStream
            .foreachBatch(write_agg)
            .outputMode("append")
            .option("checkpointLocation", f"{self.kafka_cfg.checkpoint_dir}/agg")
            .start()
        )

        # Both queries run concurrently. awaitAnyTermination() blocks until
        # either terminates (e.g. on error), at which point the container
        # restarts the job via docker-compose restart: unless-stopped.
        self.spark.streams.awaitAnyTermination()


def main() -> None:
    """Main entrypoint for retail streaming job."""
    StructuredLogger.setup()
    spark = SparkSession.builder.appName("RetailSalesStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    kafka_cfg = KafkaConfig()
    snowflake_cfg = SnowflakeConfig()
    processor = RetailStreamProcessor(spark, kafka_cfg, snowflake_cfg)
    processor.run()


if __name__ == "__main__":
    main()
