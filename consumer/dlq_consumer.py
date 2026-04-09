import json
import logging

from confluent_kafka import Consumer

from config import KAFKA_BOOTSTRAP_SERVERS
from config import KAFKA_DLQ_CONSUMER_GROUP_ID
from config import KAFKA_DLQ_TOPIC
from utils.logging_config import configure_logging

BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS
DLQ_TOPIC = KAFKA_DLQ_TOPIC
GROUP_ID = KAFKA_DLQ_CONSUMER_GROUP_ID

logger = logging.getLogger(__name__)


def main():
    configure_logging()

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([DLQ_TOPIC])
    logger.info("Starting DLQ consumer for topic %s", DLQ_TOPIC)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error("DLQ consumer error: %s", msg.error())
                continue

            event = json.loads(msg.value().decode("utf-8"))
            logger.warning("DLQ event received: %s", json.dumps(event, indent=2))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
