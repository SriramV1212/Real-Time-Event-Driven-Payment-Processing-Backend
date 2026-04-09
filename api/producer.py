import json
import logging

from confluent_kafka import Producer

from config import KAFKA_BOOTSTRAP_SERVERS
from config import KAFKA_PAYMENT_TOPIC

logger = logging.getLogger(__name__)

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})

TOPIC = KAFKA_PAYMENT_TOPIC


def delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed: %s", err)
    else:
        logger.info(
            "Delivered to partition %s offset %s",
            msg.partition(),
            msg.offset(),
        )


def produce_event(event):
    producer.produce(
        topic=TOPIC,
        key=event["user_id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report
    )

    producer.poll(0)
    producer.flush()
