import os
import logging
from logging.handlers import RotatingFileHandler
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError

# Configuration for source and destination Kafka clusters
source_config = {
    'bootstrap.servers': '113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',
    'group.id': 'replicator-group',
    'auto.offset.reset': 'earliest'
}

dest_config = {
    'bootstrap.servers': 'localhost:9094,localhost:9194,localhost:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024', 
}

source_topic = 'product_view'
dest_topic = 'product_1'

# Ensure log directories exist
os.makedirs("logs/consumer", exist_ok=True)
os.makedirs("logs/producer", exist_ok=True)

# Configure Consumer Logger
consumer_logger = logging.getLogger("ConsumerLogger")
consumer_logger.setLevel(logging.INFO)
consumer_handler = RotatingFileHandler(
    filename="logs/consumer/consumer.log",
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=10
)
consumer_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
consumer_handler.setFormatter(consumer_formatter)
consumer_logger.addHandler(consumer_handler)

# Configure Producer Logger
producer_logger = logging.getLogger("ProducerLogger")
producer_logger.setLevel(logging.INFO)
producer_handler = RotatingFileHandler(
    filename="logs/producer/producer.log",
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=10
)
producer_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
producer_handler.setFormatter(producer_formatter)
producer_logger.addHandler(producer_handler)

def delivery_report(err, msg):
    if err is not None:
        producer_logger.error(f"Delivery failed: {err}")
    else:
        producer_logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def replicate_messages():
    consumer = Consumer(source_config)
    producer = Producer(dest_config)

    consumer.subscribe([source_topic])
    consumer_logger.info(f"Subscribed to topic: {source_topic}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    consumer_logger.info(f"End of partition reached {msg.topic()} [{msg.partition()}]")
                else:
                    consumer_logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                message_value = msg.value()
                producer.produce(dest_topic, message_value, callback=delivery_report)
                producer.poll(0)
            except Exception as e:
                producer_logger.exception(f"Error producing message: {e}")
    except KeyboardInterrupt:
        consumer_logger.info("Replication interrupted by user.")
    except Exception as e:
        consumer_logger.exception(f"Unexpected error during replication: {e}")
    finally:
        consumer.close()
        producer.flush()
        consumer_logger.info("Consumer closed.")
        producer_logger.info("Producer flushed and closed.")

if __name__ == "__main__":
    replicate_messages()
