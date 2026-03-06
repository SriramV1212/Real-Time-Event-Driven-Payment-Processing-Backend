import json
import psycopg2
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "payment-events"
GROUP_ID = "payment-processors-debug-1"

DB_CONFIG = {
    "dbname": "payments",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": 5432
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def process_event(event):
    print("Received event:", event)

    conn = get_db_connection()
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        event_id = event["event_id"]
        payment_id = event["payment_id"]
        user_id = event["user_id"]
        amount = event["amount"]

        cursor.execute(
            "INSERT INTO processed_events (event_id) VALUES (%s)",
            (event_id,)
        )

        cursor.execute(
            "INSERT INTO users (user_id, balance) VALUES (%s, %s) "
            "ON CONFLICT (user_id) DO NOTHING",
            (user_id, 0)
        )

        cursor.execute(
            "UPDATE users SET balance = balance - %s WHERE user_id = %s",
            (amount, user_id)
        )

        cursor.execute(
            "INSERT INTO payments (payment_id, user_id, amount, status) "
            "VALUES (%s, %s, %s, %s)",
            (payment_id, user_id, amount, "captured")
        )

        conn.commit()
        print(f"Processed payment {payment_id} for user {user_id}")

    except psycopg2.Error as e:
        conn.rollback()
        print("Database error:", e)

    finally:
        cursor.close()
        conn.close()

def main():
    print("Starting consumer...")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([TOPIC_NAME])
    print(f"Subscribed to topic: {TOPIC_NAME}")

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            print("No message yet...")
            continue

        if msg.error():
            print("Consumer error:", msg.error())
            continue

        print("Message received from Kafka")
        try:
            event = json.loads(msg.value().decode("utf-8"))

            required_fields = ["event_id", "payment_id", "user_id", "amount"]

            if not all(field in event for field in required_fields):
                print(f"Skipping invalid event: {event}")
                continue

            process_event(event)

        except json.JSONDecodeError:
            print("Skipping malformed JSON message")
            continue

if __name__ == "__main__":
    main()