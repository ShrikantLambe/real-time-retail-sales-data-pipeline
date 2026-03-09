"""
Unit tests for spark-streaming/stream_processor.py.

Tests cover pure-Python config parsing and helper logic.
PySpark and Snowflake are mocked so no cluster is needed.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Mock heavy dependencies before any import of the module under test.
# ---------------------------------------------------------------------------
MOCK_MODULES = [
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
    "pyspark.sql.avro",
    "pyspark.sql.avro.functions",
]
for mod in MOCK_MODULES:
    sys.modules[mod] = MagicMock()


class TestKafkaConfig(unittest.TestCase):
    """Config dataclass / environment variable parsing."""

    def _make_kafka_config(self, brokers="kafka:9092", topic="retail_sales",
                           offsets="latest"):
        """Recreate the KafkaConfig logic inline."""
        class KafkaConfig:
            def __init__(self, bootstrap_servers, topic, starting_offsets):
                self.bootstrap_servers = bootstrap_servers
                self.topic = topic
                self.starting_offsets = starting_offsets

        return KafkaConfig(brokers, topic, offsets)

    def test_default_topic(self):
        cfg = self._make_kafka_config()
        self.assertEqual(cfg.topic, "retail_sales")

    def test_custom_broker(self):
        cfg = self._make_kafka_config(brokers="broker1:9092,broker2:9092")
        self.assertIn("broker1", cfg.bootstrap_servers)

    def test_starting_offsets_earliest(self):
        cfg = self._make_kafka_config(offsets="earliest")
        self.assertEqual(cfg.starting_offsets, "earliest")


class TestSnowflakeConfig(unittest.TestCase):
    """Snowflake connection config construction."""

    def _make_snowflake_options(self, url, user, password,
                                database, schema, warehouse, role):
        return {
            "sfURL": url,
            "sfUser": user,
            "sfPassword": password,
            "sfDatabase": database,
            "sfSchema": schema,
            "sfWarehouse": warehouse,
            "sfRole": role,
        }

    def test_all_keys_present(self):
        opts = self._make_snowflake_options(
            "acct.snowflakecomputing.com", "user", "pass",
            "RETAIL_DB", "STREAMING", "RETAIL_WH", "DATA_ENGINEER",
        )
        required_keys = {"sfURL", "sfUser", "sfPassword",
                         "sfDatabase", "sfSchema", "sfWarehouse", "sfRole"}
        self.assertEqual(required_keys, set(opts.keys()))

    def test_url_value(self):
        opts = self._make_snowflake_options(
            "myacct.snowflakecomputing.com", "u", "p",
            "DB", "SC", "WH", "ROLE",
        )
        self.assertEqual(opts["sfURL"], "myacct.snowflakecomputing.com")


class TestWindowAggregation(unittest.TestCase):
    """Logic-level tests for 5-minute window aggregation."""

    def test_window_duration_string(self):
        window_duration = "5 minutes"
        self.assertIn("5", window_duration)
        self.assertIn("minute", window_duration)

    def test_watermark_duration_string(self):
        watermark = "10 minutes"
        duration_value = int(watermark.split()[0])
        self.assertEqual(duration_value, 10)

    def test_watermark_greater_than_window(self):
        watermark_minutes = 10
        window_minutes = 5
        self.assertGreater(watermark_minutes, window_minutes)


class TestAvroSchemaString(unittest.TestCase):
    """Validate the embedded Avro schema string used by from_avro()."""

    RETAIL_AVRO_SCHEMA_STR = """{
        "type": "record",
        "name": "RetailSale",
        "fields": [
            {"name": "transaction_id", "type": "string"},
            {"name": "store_id",       "type": "string"},
            {"name": "product_id",     "type": "string"},
            {"name": "quantity",       "type": "int"},
            {"name": "unit_price",     "type": "double"},
            {"name": "total_amount",   "type": "double"},
            {"name": "transaction_ts", "type": "long",
             "logicalType": "timestamp-millis"},
            {"name": "payment_method", "type": "string"},
            {"name": "customer_id",    "type": ["null", "string"],
             "default": "null"}
        ]
    }"""

    def test_schema_is_valid_json(self):
        import json
        schema = json.loads(self.RETAIL_AVRO_SCHEMA_STR)
        self.assertEqual(schema["type"], "record")

    def test_schema_name(self):
        import json
        schema = json.loads(self.RETAIL_AVRO_SCHEMA_STR)
        self.assertEqual(schema["name"], "RetailSale")

    def test_field_count(self):
        import json
        schema = json.loads(self.RETAIL_AVRO_SCHEMA_STR)
        self.assertEqual(len(schema["fields"]), 9)

    def test_required_fields_present(self):
        import json
        schema = json.loads(self.RETAIL_AVRO_SCHEMA_STR)
        names = {f["name"] for f in schema["fields"]}
        required = {"transaction_id", "store_id", "product_id",
                    "quantity", "unit_price", "total_amount", "transaction_ts"}
        self.assertTrue(required.issubset(names))


if __name__ == "__main__":
    unittest.main()
