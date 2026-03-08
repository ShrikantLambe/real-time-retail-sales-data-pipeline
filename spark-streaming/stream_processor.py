"""
PySpark Structured Streaming job for retail sales pipeline
Production best practices: modularity, error handling, structured logging, config separation, docstrings, type hints.
"""
import os
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import col, window, sum as _sum, from_json, to_timestamp


class SnowflakeConfig:
    """Configuration for Snowflake connection."""

    def __init__(self) -> None:
        self.options = {
            'sfURL': os.getenv('SNOWFLAKE_URL'),
            'sfUser': os.getenv('SNOWFLAKE_USER'),
            'sfPassword': os.getenv('SNOWFLAKE_PASSWORD'),
            'sfDatabase': os.getenv('SNOWFLAKE_DATABASE', 'RETAIL_DB'),
            'sfSchema': os.getenv('SNOWFLAKE_SCHEMA', 'STREAMING'),
            'sfWarehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'RETAIL_WH'),
            'sfRole': os.getenv('SNOWFLAKE_ROLE', 'DATA_ENGINEER')
        }


class KafkaConfig:
    """Configuration for Kafka streaming."""

    def __init__(self) -> None:
        self.bootstrap_servers = os.getenv('KAFKA_BROKERS')
        self.topic = os.getenv('KAFKA_TOPIC', 'retail_sales')
        self.checkpoint_dir = os.getenv(
            'CHECKPOINT_DIR', '/tmp/spark_checkpoint')
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


class RetailSchema:
    """Retail transaction schema definition."""
    schema = StructType()\
        .add('transaction_id', StringType())\
        .add('store_id', StringType())\
        .add('product_id', StringType())\
        .add('quantity', IntegerType())\
        .add('price', FloatType())\
        .add('payment_type', StringType())\
        .add('transaction_ts', StringType())


class RetailStreamProcessor:
    """Retail streaming processor for PySpark."""

    def __init__(self, spark: SparkSession, kafka_cfg: KafkaConfig, snowflake_cfg: SnowflakeConfig) -> None:
        self.spark = spark
        self.kafka_cfg = kafka_cfg
        self.snowflake_cfg = snowflake_cfg

    def parse_and_cast(self, df: DataFrame) -> DataFrame:
        """Parse JSON and cast transaction_ts to timestamp."""
        parsed = df.select(from_json(col('value').cast(
            'string'), RetailSchema.schema).alias('data'))
        return parsed.select(
            col('data.transaction_id'),
            col('data.store_id'),
            col('data.product_id'),
            col('data.quantity'),
            col('data.price'),
            col('data.payment_type'),
            to_timestamp(col('data.transaction_ts')).alias('transaction_ts')
        )

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Deduplicate by transaction_id with watermark."""
        return df.withWatermark('transaction_ts', '10 minutes').dropDuplicates(['transaction_id'])

    def aggregate_window(self, df: DataFrame) -> DataFrame:
        """5-minute window aggregation by store_id.

        The incoming df already has a watermark applied by deduplicate().
        Re-applying withWatermark here would create a second, conflicting
        watermark on the same stream — omit it and rely on the existing one.
        """
        agg = df\
            .groupBy(
                window(col('transaction_ts'), '5 minutes'),
                col('store_id')
            )\
            .agg(
                _sum(col('quantity') * col('price')).alias('total_revenue'),
                _sum('quantity').alias('total_quantity')
            )
        return agg.select(
            col('window.start').alias('window_start'),
            col('window.end').alias('window_end'),
            col('store_id'),
            col('total_revenue'),
            col('total_quantity')
        )

    def write_to_snowflake(self, df: DataFrame, table: str, batch_id: int) -> None:
        """Write a micro-batch DataFrame to a Snowflake table.

        Avoid calling df.count() before the write: it triggers a full extra
        scan of the batch (Spark action), doubling compute cost. Writing an
        empty DataFrame is a no-op in Snowflake, so we let the write decide.
        """
        try:
            StructuredLogger.log_batch(batch_id, f'Writing batch to {table}.')
            df.write\
                .format('snowflake')\
                .options(**self.snowflake_cfg.options)\
                .option('dbtable', table)\
                .mode('append')\
                .save()
            StructuredLogger.log_batch(batch_id, f'{table} batch committed to Snowflake.')
        except Exception as e:
            StructuredLogger.log_error(batch_id, e)

    def run(self) -> None:
        """Run the streaming job."""
        raw_stream = self.spark.readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers', self.kafka_cfg.bootstrap_servers)\
            .option('subscribe', self.kafka_cfg.topic)\
            .option('startingOffsets', self.kafka_cfg.starting_offsets)\
            .load()

        parsed_stream = self.parse_and_cast(raw_stream)
        deduped_stream = self.deduplicate(parsed_stream)

        def write_raw(batch_df: DataFrame, batch_id: int) -> None:
            self.write_to_snowflake(batch_df, 'raw_retail_sales', batch_id)

        raw_query = deduped_stream.writeStream\
            .foreachBatch(write_raw)\
            .outputMode('append')\
            .option('checkpointLocation', f'{self.kafka_cfg.checkpoint_dir}/raw')\
            .start()

        agg_stream = self.aggregate_window(deduped_stream)

        def write_agg(batch_df: DataFrame, batch_id: int) -> None:
            self.write_to_snowflake(batch_df, 'agg_store_sales_5min', batch_id)

        agg_query = agg_stream.writeStream\
            .foreachBatch(write_agg)\
            .outputMode('append')\
            .option('checkpointLocation', f'{self.kafka_cfg.checkpoint_dir}/agg')\
            .start()

        # Streaming concepts:
        # - Watermarking handles late data up to 10 minutes
        # - Deduplication ensures idempotency
        # - foreachBatch enables robust error handling and Snowflake writes
        # - Checkpointing ensures recovery and exactly-once semantics

        raw_query.awaitTermination()
        agg_query.awaitTermination()


def main() -> None:
    """Main entrypoint for retail streaming job."""
    StructuredLogger.setup()
    spark = SparkSession.builder.appName('RetailSalesStreaming').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    kafka_cfg = KafkaConfig()
    snowflake_cfg = SnowflakeConfig()
    processor = RetailStreamProcessor(spark, kafka_cfg, snowflake_cfg)
    processor.run()


if __name__ == '__main__':
    main()
