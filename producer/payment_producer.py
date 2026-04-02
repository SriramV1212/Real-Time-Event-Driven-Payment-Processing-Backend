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

def delivery_report(err, msg, event):
    if err is not None:
        print(
            f"Delivery failed for event_id={event['event_id']} "
            f"payment_id={event['payment_id']}: {err}"
        )
    else:
        print(
            f"Delivered event_id={event['event_id']} "
            f"payment_id={event['payment_id']} "
            f"to partition={msg.partition()} offset={msg.offset()}"
        )

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

    start_time = time.time()
    try:
        for _ in range(1000): # while True:
            key, event = generate_payment_event()

            print("Producing:", event) 

            producer.produce(
                topic=TOPIC_NAME,
                key=key.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=lambda err, msg, event=event: delivery_report(err, msg, event)
            )

            producer.poll(0)   
            time.sleep(0.001)
        
        print(f"Sent 1000 events in {time.time() - start_time:.2f}s")
        
    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.flush()   # Makes sure any remaining messages are sent from producer buffer before exit

if __name__ == "__main__":
    main()
