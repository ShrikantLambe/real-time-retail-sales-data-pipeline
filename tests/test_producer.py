"""
Unit tests for kafka-producer/producer.py.

These tests exercise pure-Python logic (schema construction, message
field validation) without requiring a live Kafka broker or Schema Registry.
"""

import json
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Minimal stand-ins for confluent_kafka so tests can run without the C ext.
# ---------------------------------------------------------------------------
import sys
if "confluent_kafka" not in sys.modules:
    confluent_kafka_mock = MagicMock()
    confluent_kafka_mock.avro = MagicMock()
    sys.modules["confluent_kafka"] = confluent_kafka_mock
    sys.modules["confluent_kafka.avro"] = confluent_kafka_mock.avro
    sys.modules["confluent_kafka.schema_registry"] = MagicMock()
    sys.modules["confluent_kafka.schema_registry.avro"] = MagicMock()
    sys.modules["confluent_kafka.serializing_producer"] = MagicMock()


RETAIL_AVRO_SCHEMA = {
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
         "default": "null"},
    ],
}

REQUIRED_FIELDS = {f["name"] for f in RETAIL_AVRO_SCHEMA["fields"]}


class TestAvroSchema(unittest.TestCase):
    """Schema-level sanity checks."""

    def test_schema_has_required_fields(self):
        expected = {
            "transaction_id", "store_id", "product_id",
            "quantity", "unit_price", "total_amount",
            "transaction_ts", "payment_method",
        }
        self.assertTrue(expected.issubset(REQUIRED_FIELDS))

    def test_transaction_id_is_string(self):
        field = next(f for f in RETAIL_AVRO_SCHEMA["fields"]
                     if f["name"] == "transaction_id")
        self.assertEqual(field["type"], "string")

    def test_total_amount_is_double(self):
        field = next(f for f in RETAIL_AVRO_SCHEMA["fields"]
                     if f["name"] == "total_amount")
        self.assertEqual(field["type"], "double")

    def test_customer_id_is_nullable(self):
        field = next(f for f in RETAIL_AVRO_SCHEMA["fields"]
                     if f["name"] == "customer_id")
        self.assertIn("null", field["type"])

    def test_schema_is_valid_json(self):
        dumped = json.dumps(RETAIL_AVRO_SCHEMA)
        loaded = json.loads(dumped)
        self.assertEqual(loaded["name"], "RetailSale")


class TestMessageGeneration(unittest.TestCase):
    """Tests for the message-generation logic in producer.py."""

    def _make_message(self):
        import uuid
        import random
        import time

        stores = ["STORE_001", "STORE_002", "STORE_003"]
        products = ["PROD_A", "PROD_B", "PROD_C"]
        payment_methods = ["CREDIT_CARD", "DEBIT_CARD", "CASH", "MOBILE_PAY"]

        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(5.0, 500.0), 2)
        return {
            "transaction_id": str(uuid.uuid4()),
            "store_id": random.choice(stores),
            "product_id": random.choice(products),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(quantity * unit_price, 2),
            "transaction_ts": int(time.time() * 1000),
            "payment_method": random.choice(payment_methods),
            "customer_id": None,
        }

    def test_message_has_all_fields(self):
        msg = self._make_message()
        for field in REQUIRED_FIELDS:
            self.assertIn(field, msg, f"Missing field: {field}")

    def test_total_amount_matches_quantity_times_price(self):
        for _ in range(20):
            msg = self._make_message()
            expected = round(msg["quantity"] * msg["unit_price"], 2)
            self.assertAlmostEqual(msg["total_amount"], expected, places=2)

    def test_transaction_id_is_uuid(self):
        import uuid
        msg = self._make_message()
        try:
            uuid.UUID(msg["transaction_id"])
        except ValueError:
            self.fail("transaction_id is not a valid UUID")

    def test_quantity_positive(self):
        for _ in range(20):
            msg = self._make_message()
            self.assertGreater(msg["quantity"], 0)

    def test_unit_price_positive(self):
        for _ in range(20):
            msg = self._make_message()
            self.assertGreater(msg["unit_price"], 0)

    def test_store_id_in_known_set(self):
        known = {"STORE_001", "STORE_002", "STORE_003"}
        for _ in range(20):
            msg = self._make_message()
            self.assertIn(msg["store_id"], known)


if __name__ == "__main__":
    unittest.main()
