import json
import os
import subprocess
import sys
import time
import uuid

from confluent_kafka import Consumer
from psycopg2.extras import Json

from db.connection import get_connection


def start_crashing_relay(event_id):
    env = os.environ.copy()

    env["FAULT_INJECT_CRASH_AFTER_KAFKA_PUBLISH_EVENT_ID"] = event_id

    return subprocess.Popen(
        [sys.executable, "outbox_relay.py"],
        env=env,
    )

def start_relay():
    return subprocess.Popen(
        [sys.executable, "outbox_relay.py"],
    )

def stop_process_if_running(process):
    if process is not None and process.poll() is None:
        process.terminate()
        process.wait(timeout=5)

def wait_until_published(conn, event_id, timeout=10):
    deadline = time.time() + timeout

    while time.time() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT published_at
                FROM outbox
                WHERE event_id = %s
                """,
                (event_id,),
            )

            published_at = cur.fetchone()[0]

        if published_at is not None:
            return True

        time.sleep(0.2)

    return False

def count_kafka_events(event_id, timeout=5):
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "outbox-duplicate-verification",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe(["payment-events"])

    count = 0
    deadline = time.time() + timeout

    try:
        while time.time() < deadline:
            message = consumer.poll(0.2)

            if message is None:
                continue

            if message.error():
                continue

            event = json.loads(message.value().decode("utf-8"))

            if event.get("event_id") == event_id:
                count += 1

    finally:
        consumer.close()

    return count

def test_relay_crash_after_kafka_publish_leaves_event_unpublished():
    event_id = str(uuid.uuid4())
    payment_id = str(uuid.uuid4())

    event = {
        "event_id": event_id,
        "payment_id": payment_id,
        "user_id": "user_relay_crash_test",
        "amount": 100,
        "event_type": "payment_created",
    }

    conn = get_connection()
    crashing_relay_process = None
    recovery_relay_process = None

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO outbox (
                    event_id,
                    aggregate_id,
                    event_type,
                    payload
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    event_id,
                    payment_id,
                    event["event_type"],
                    Json(event),
                ),
            )

        conn.commit()

        crashing_relay_process = start_crashing_relay(event_id)

        crashing_relay_process.wait(timeout=10)

        assert crashing_relay_process.returncode == 1

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT published_at
                FROM outbox
                WHERE event_id = %s
                """,
                (event_id,),
            )

            published_at = cur.fetchone()[0]

        assert published_at is None

        recovery_relay_process = start_relay()

        assert wait_until_published(conn, event_id)
        assert count_kafka_events(event_id) == 2

    finally:
        stop_process_if_running(crashing_relay_process)
        stop_process_if_running(recovery_relay_process)

        conn.rollback()

        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM outbox
                WHERE event_id = %s
                """,
                (event_id,),
            )

        conn.commit()
        conn.close()