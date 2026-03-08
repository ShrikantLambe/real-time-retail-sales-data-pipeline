"""
Kafka Producer for Retail Transaction Events — Avro + Schema Registry edition.

Uses confluent-kafka with AvroSerializer so that:
  - The schema is registered in (and versioned by) Schema Registry on first run.
  - Every message is prefixed with the Confluent wire format
    [0x00][4-byte schema_id][avro payload], enabling safe schema evolution.
  - Downstream consumers (Spark, ksqlDB, etc.) can fetch the exact schema
    by schema_id rather than relying on in-band schema negotiation.
"""
import json
import logging
import os
import random
import time
import uuid
from typing import Any, Dict, Optional

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


# ---------------------------------------------------------------------------
# Avro schema — must stay in sync with the Spark consumer's RETAIL_AVRO_SCHEMA
# ---------------------------------------------------------------------------
RETAIL_SALE_SCHEMA = {
    "type": "record",
    "name": "RetailSale",
    "namespace": "com.retail.sales",
    "doc": "A single retail point-of-sale transaction.",
    "fields": [
        {"name": "transaction_id", "type": "string",  "doc": "UUID primary key"},
        {"name": "store_id",       "type": "string",  "doc": "Store identifier"},
        {"name": "product_id",     "type": "string",  "doc": "Product identifier"},
        {"name": "quantity",       "type": "int",     "doc": "Units sold"},
        {"name": "price",          "type": "float",   "doc": "Unit price"},
        {"name": "payment_type",   "type": ["null", "string"], "default": None,
         "doc": "credit_card | debit_card | cash | online"},
        {"name": "transaction_ts", "type": "string",
         "doc": "ISO-8601 UTC timestamp of the transaction"},
    ],
}
RETAIL_AVRO_SCHEMA_STR = json.dumps(RETAIL_SALE_SCHEMA)


class ProducerConfig:
    """Configuration for Kafka Producer and Schema Registry."""

    def __init__(self) -> None:
        brokers = os.getenv('KAFKA_BROKERS')
        if not brokers:
            raise EnvironmentError('KAFKA_BROKERS environment variable not set')
        self.topic = os.getenv('KAFKA_TOPIC', 'retail_sales')
        self.producer_settings: Dict[str, Any] = {
            'bootstrap.servers': brokers,
            'acks': 'all',
            'retries': 5,
        }
        self.schema_registry_url: str = os.getenv(
            'SCHEMA_REGISTRY_URL', 'http://schema-registry:8081'
        )


class RetailEventGenerator:
    """Generates realistic retail transaction events."""
    @staticmethod
    def generate_event() -> Dict[str, Any]:
        return {
            'transaction_id': str(uuid.uuid4()),
            'store_id':       f"store_{random.randint(1, 20)}",
            'product_id':     f"product_{random.randint(1, 100)}",
            'quantity':       random.randint(1, 5),
            'price':          round(random.uniform(5.0, 500.0), 2),
            'payment_type':   random.choice(['credit_card', 'debit_card', 'cash', 'online']),
            'transaction_ts': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        }


class StructuredLogger:
    """Structured logger for the producer."""
    @staticmethod
    def setup() -> None:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(name)s %(message)s'
        )

    @staticmethod
    def log_delivery(err: Optional[Exception], msg: Any) -> None:
        if err:
            logging.error('Delivery failed', extra={'error': str(err)})
        else:
            logging.info(
                'Message delivered',
                extra={'topic': msg.topic(), 'partition': msg.partition(),
                       'offset': msg.offset()}
            )


class RetailKafkaProducer:
    """Avro-serialising Kafka producer for retail events."""

    def __init__(self, config: ProducerConfig) -> None:
        self.config = config
        sr_client = SchemaRegistryClient({'url': config.schema_registry_url})
        self._serializer = AvroSerializer(sr_client, RETAIL_AVRO_SCHEMA_STR)
        self._producer = Producer(config.producer_settings)

    def send_event(self, event: Dict[str, Any]) -> None:
        ctx = SerializationContext(self.config.topic, MessageField.VALUE)
        try:
            serialized = self._serializer(event, ctx)
            self._producer.produce(
                self.config.topic,
                value=serialized,
                on_delivery=StructuredLogger.log_delivery,
            )
            # poll(0) delivers any pending delivery callbacks without blocking
            self._producer.poll(0)
        except Exception as exc:
            logging.error('Error sending event', extra={'error': str(exc)})

    def close(self) -> None:
        self._producer.flush()


def main() -> None:
    """Main entrypoint for Kafka producer."""
    StructuredLogger.setup()
    config = ProducerConfig()
    producer = RetailKafkaProducer(config)
    logging.info('Kafka producer started',
                 extra={'brokers': config.producer_settings['bootstrap.servers'],
                        'topic': config.topic,
                        'schema_registry': config.schema_registry_url})
    try:
        for _ in range(25):
            event = RetailEventGenerator.generate_event()
            logging.info('Producing event', extra={'event': event})
            producer.send_event(event)
            time.sleep(random.uniform(0.1, 0.5))
        logging.info('Completed sending 25 events. Producer exiting.')
    except KeyboardInterrupt:
        logging.info('Shutdown requested, flushing producer...')
    except Exception as exc:
        logging.error('Unexpected error', extra={'error': str(exc)})
    finally:
        producer.close()
        logging.info('Producer shutdown complete')


if __name__ == '__main__':
    main()
