import logging

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_PAYMENT_TOPIC,
    KAFKA_TOPIC_PARTITIONS,
    KAFKA_TOPIC_REPLICATION_FACTOR,
)
from utils.logging_config import configure_logging

BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS
TOPIC_NAME = KAFKA_PAYMENT_TOPIC
NUM_PARTITIONS = KAFKA_TOPIC_PARTITIONS
REPLICATION_FACTOR = KAFKA_TOPIC_REPLICATION_FACTOR

logger = logging.getLogger(__name__)


def create_topic():
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    topic = NewTopic(
        topic=TOPIC_NAME,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR
    )

    futures = admin_client.create_topics([topic])

    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info("Topic '%s' created successfully", topic_name)
        except KafkaException as e:
            logger.warning(
                "Topic '%s' may already exist or an error occurred: %s",
                topic_name,
                e,
            )


if __name__ == "__main__":
    configure_logging()
    create_topic()
