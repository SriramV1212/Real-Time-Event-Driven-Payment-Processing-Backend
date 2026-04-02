import json
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
DLQ_TOPIC = "payment-events-dlq"

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "dlq-processors",   # separate group
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

    print("\n🚨 DLQ EVENT RECEIVED:")
    print(json.dumps(event, indent=2))