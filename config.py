import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_PAYMENT_TOPIC = os.getenv("KAFKA_PAYMENT_TOPIC", "payment-events")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "payment-events-dlq")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "payment-processors")
KAFKA_DLQ_CONSUMER_GROUP_ID = os.getenv(
    "KAFKA_DLQ_CONSUMER_GROUP_ID",
    "paymentdlq-processors",
)
KAFKA_TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "4"))
KAFKA_TOPIC_REPLICATION_FACTOR = int(
    os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "1")
)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "payments")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
