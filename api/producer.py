import json
import logging

from confluent_kafka import Producer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_PAYMENT_TOPIC

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
    delivery_error = {"error": None}

    def on_delivery(err, msg):
        delivery_report(err, msg)
        delivery_error["error"] = err

    producer.produce(
        topic=TOPIC,
        key=event["user_id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=on_delivery,
    )

    producer.flush()


    if delivery_error["error"] is not None:
        raise RuntimeError(
            f"Kafka delivery failed: {delivery_error['error']}"
        )
