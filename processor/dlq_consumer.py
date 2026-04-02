import json
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
DLQ_TOPIC = "payment-events-dlq"
GROUP_ID = "paymentdlq-processors"

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
})

consumer.subscribe([DLQ_TOPIC])

print("Starting DLQ consumer...")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Error:", msg.error())
        continue

    event = json.loads(msg.value().decode("utf-8"))

    print("\nDLQ EVENT RECEIVED:")
    print(json.dumps(event, indent=2))