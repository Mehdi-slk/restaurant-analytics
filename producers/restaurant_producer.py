from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from data.extract_data import extract_restaurant_data
import logging
import  os
import json
import sys

TOPIC = 'restaurant_topic'
DLQ_TOPIC = 'restaurant_dlq'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = {
    'bootstrap.servers': os.getenv(
        'KAFKA_BOOTSTRAP_SERVERS',
        'localhost:9092'
    ),

    # Idempotent producer
    'enable.idempotence': True,
    'acks': 'all',

    # Reliability
    'retries': 2147483647,
    'max.in.flight.requests.per.connection': 5,
    'delivery.timeout.ms': 120000,
    'retry.backoff.ms': 1000,

    # Batching / compression
    'linger.ms': 10,
    'batch.size': 16384,
    'compression.type': 'lz4'
}

schema_registry_conf = {
    'url' : os.getenv(
        'SCHEMA_REGISTRY_URL',
        'http://localhost:8083'
    )
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

with open("/opt/airflow/schemas/restaurants.avsc", "r") as schema_file:
    restaurant_schema = schema_file.read()

avr_serializer = AvroSerializer(
    schema_registry_client,
    restaurant_schema
)

def delivery_report(err, msg, record):
    if err is None:
        logger.info(
            f"Record {msg.key()} successfully produced to "
            f"{msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )
        return

    logger.error(f"Delivery failed for record {msg.key()}: {err}")

    producer.produce(
        topic=DLQ_TOPIC,
        key=str(record["camis"]),
        value=avr_serializer(
            record,
            SerializationContext(DLQ_TOPIC, MessageField.VALUE)
        )
    )

    
producer = Producer(config)

data = extract_restaurant_data()

try:
    for record in data:

        while True:
            try:
                producer.produce(
                    topic=TOPIC,
                    key=str(record["camis"]),
                    value=avr_serializer(
                        record,
                        SerializationContext(TOPIC, MessageField.VALUE)
                    ),
                    on_delivery=lambda err, msg: delivery_report(
                        err, msg, record
                    )
                )

                producer.poll(0)
                break
            except BufferError:
                logger.warning("Buffer full, waiting for free space...")
                producer.poll(1)
    producer.flush()

    logger.info("All records have been produced successfully.")
finally:
    producer.flush()
    logger.info("Producer has been flushed and closed.")

