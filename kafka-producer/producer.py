"""
Kafka Producer for Retail Transaction Events
Production best practices: structured logging, error handling, modularity, config separation, docstrings, type hints.
"""
import os
import json
import time
import random
import logging
import uuid
from typing import Dict, Any, Optional
from kafka import KafkaProducer


class ProducerConfig:
    """Configuration for Kafka Producer."""

    def __init__(self) -> None:
        self.brokers = os.getenv('KAFKA_BROKERS')
        if not self.brokers:
            raise EnvironmentError(
                'KAFKA_BROKERS environment variable not set')
        self.topic = os.getenv('KAFKA_TOPIC', 'retail_sales')
        self.producer_settings = {
            'bootstrap_servers': self.brokers.split(','),
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'acks': 'all',
            'retries': 5
        }


class RetailEventGenerator:
    """Generates realistic retail transaction events."""
    @staticmethod
    def generate_event() -> Dict[str, Any]:
        return {
            'transaction_id': str(uuid.uuid4()),
            'store_id': f"store_{random.randint(1, 20)}",
            'product_id': f"product_{random.randint(1, 100)}",
            'quantity': random.randint(1, 5),
            'price': round(random.uniform(5.0, 500.0), 2),
            'payment_type': random.choice(['credit_card', 'debit_card', 'cash', 'online']),
            'transaction_ts': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
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
    def log_event(event: Dict[str, Any]) -> None:
        logging.info('Producing event', extra={'event': event})

    @staticmethod
    def log_delivery(record_metadata: Optional[Any], exception: Optional[Exception] = None) -> None:
        if exception:
            logging.error('Delivery failed', extra={'error': str(exception)})
        elif record_metadata:
            logging.info(
                'Message delivered',
                extra={
                    'topic': record_metadata.topic,
                    'partition': record_metadata.partition,
                    'offset': record_metadata.offset
                }
            )


class RetailKafkaProducer:
    """Kafka producer for retail events."""

    def __init__(self, config: ProducerConfig) -> None:
        self.config = config
        self.producer = KafkaProducer(**self.config.producer_settings)

    def send_event(self, event: Dict[str, Any]) -> None:
        try:
            future = self.producer.send(self.config.topic, event)
            future.add_callback(
                lambda meta: StructuredLogger.log_delivery(meta))
            future.add_errback(
                lambda exc: StructuredLogger.log_delivery(None, exc))
        except Exception as e:
            logging.error('Error sending event', extra={'error': str(e)})

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()


def main() -> None:
    """Main entrypoint for Kafka producer."""
    StructuredLogger.setup()
    config = ProducerConfig()
    producer = RetailKafkaProducer(config)
    logging.info('Kafka producer started', extra={
                 'brokers': config.brokers, 'topic': config.topic})
    try:
        for i in range(25):
            event = RetailEventGenerator.generate_event()
            StructuredLogger.log_event(event)
            producer.send_event(event)
            delay = random.uniform(0.1, 0.5)
            time.sleep(delay)
        logging.info('Completed sending 25 events. Producer exiting.')
    except KeyboardInterrupt:
        logging.info('Shutdown requested, flushing producer...')
    except Exception as e:
        logging.error('Unexpected error', extra={'error': str(e)})
    finally:
        producer.close()
        logging.info('Producer shutdown complete')


if __name__ == '__main__':
    main()
