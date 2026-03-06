import json
import time
import random
import uuid
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "payment-events"

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS
})

users = ["user_1", "user_2", "user_3", "user_4", "user_5"]

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to partition {msg.partition()}")

def generate_payment_event():
    user_id = random.choice(users)
    payment_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())


    event = {
        "event_id": event_id,
        "payment_id": payment_id,
        "user_id": user_id,
        "amount": random.randint(10, 500),
        "event_type": "payment_captured",
        "timestamp": time.time()
    }



    return user_id, event

def main():
    while True:
        key, event = generate_payment_event()

        print("Producing:", event)

        producer.produce(
            topic=TOPIC_NAME,
            key=key,
            value=json.dumps(event),
            callback=delivery_report
        )

        # producer.poll(0)
        producer.flush(1)
        time.sleep(1)

if __name__ == "__main__":
    main()