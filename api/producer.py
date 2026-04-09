import json
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "127.0.0.1:9092"
})

TOPIC = "payment-events"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to partition {msg.partition()} offset {msg.offset()}")


def produce_event(event):
    producer.produce(
        topic=TOPIC,
        key=event["user_id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report
    )

    producer.poll(0)

    producer.flush()