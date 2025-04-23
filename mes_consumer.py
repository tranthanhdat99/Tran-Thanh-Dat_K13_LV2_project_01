from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import json
import logging
from logging.handlers import RotatingFileHandler
import os

# Ensure logs directory exists
log_dir = 'logs/consumer_2'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
    print(f"Created directory: {log_dir}")

# Setup Logging
log_file_path = os.path.join(log_dir, 'consumer.log')

logger = logging.getLogger()  # root logger
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(
    log_file_path,
    maxBytes=5 * 1024 * 1024,  # 5MB
    backupCount=10             # Keep up to 10 log files
)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Config Kafka Consumer
kafka_config = {
    'bootstrap.servers': 'localhost:9094,localhost:9194,localhost:9294',
    'group.id': 'mongo-writer-group',
    'auto.offset.reset': 'earliest',
    # SASL Configuration
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024'
}

topic = 'product_1'

# MongoDB Connection
try:
    mongo_client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000) # Added timeout
    logger.info("Successfully connected to MongoDB.")
    mongo_db = mongo_client["kafka_messages"]
    mongo_collection = mongo_db["product_view_logs"]
except PyMongoError as e:
    logger.error(f"Could not connect to MongoDB: {e}")
    print(f"Could not connect to MongoDB: {e}")
    exit(1) # Exit if DB connection fails

# Kafka Consumer
try:
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])
    logger.info(f"Subscribed to topic '{topic}'. Listening for messages...")
    print(f"Subscribed to topic '{topic}'. Listening for messages...")

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error(): # Check for Kafka errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.info(f"End of partition reached {msg.topic()} [{msg.partition()}]")
            else:
                logger.error(f"Consumer error: {msg.error()}")
            continue

        try:
            # Decode message value (assuming utf-8)
            message_str = msg.value().decode('utf-8')
            # Parse JSON
            message_data = json.loads(message_str)

            # Insert into MongoDB
            insert_result = mongo_collection.insert_one(message_data)
            logger.info(f"Inserted message to MongoDB (ID: {insert_result.inserted_id}) | Offset: {msg.offset()}")

        except UnicodeDecodeError as e:
            logger.error(f"Error decoding message value (offset {msg.offset()}): {e} - Skipping message.")
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON from message value (offset {msg.offset()}): {e} - Skipping message.")
        except PyMongoError as e:
            logger.exception(f"Error inserting message (offset {msg.offset()}) into MongoDB: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error processing message (offset {msg.offset()}): {e}")

except KeyboardInterrupt:
    logger.info("KeyboardInterrupt received. Stopping consumer...")
    print("Stopping consumer...")
except KafkaException as e:
    logger.error(f"Critical Kafka Exception: {e}")
    print(f"Critical Kafka Exception: {e}")
except Exception as e:
    logger.exception(f"An unexpected error occurred: {e}")
    print(f"An unexpected error occurred: {e}")
finally:
    consumer.unsubscribe()
    consumer.close()
    mongo_client.close()
    logger.info("Consumer closed.")
    logger.info("MongoDB connection closed.")