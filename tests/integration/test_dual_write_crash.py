import http.client
import json
import os
import socket
import subprocess
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaException

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_PAYMENT_TOPIC
from db.connection import get_connection


def find_payment_by_user_id(user_id):
    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT payment_id, status
                FROM payments
                WHERE user_id = %s
                """,
                (user_id,),
            )
            return cursor.fetchone()
    finally:
        conn.close()
        

def kafka_has_payment_event(payment_id, timeout_seconds=5):
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "dual-write-verification",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    try:
        consumer.subscribe([KAFKA_PAYMENT_TOPIC])

        deadline = time.monotonic() + timeout_seconds

        while time.monotonic() < deadline:
            message = consumer.poll(0.5)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            event = json.loads(message.value().decode("utf-8"))

            if event.get("payment_id") == payment_id:
                return True

        return False

    finally:
        consumer.close()


def start_crash_api():
    env = os.environ.copy()
    env["FAULT_INJECT_CRASH_AFTER_PAYMENT_COMMIT"] = "true"

    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "api.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8001",
        ],
        env=env,
    )


def wait_for_api(port=8001, timeout_seconds=5):
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)

    raise RuntimeError("Crash-test API did not start in time")


def send_payment_expect_disconnect(user_id, amount=100):
    connection = http.client.HTTPConnection(
        "127.0.0.1",
        8001,
        timeout=2,
    )

    body = json.dumps(
        {
            "user_id": user_id,
            "amount": amount,
        }
    )

    try:
        connection.request(
            "POST",
            "/payments",
            body=body,
            headers={"Content-Type": "application/json"},
        )

        response = connection.getresponse()

    except (http.client.RemoteDisconnected, ConnectionResetError):
        return

    finally:
        connection.close()

    raise AssertionError(
        f"Expected API connection to terminate, but received HTTP {response.status}"
    )


def assert_crash_api_exited(api_process, timeout_seconds=2):
    try:
        return_code = api_process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        api_process.terminate()
        api_process.wait(timeout=2)
        raise AssertionError(
            "Crash-test API did not exit after the payment request"
        )

    assert return_code == 1, (
        f"Expected crash-test API to exit with code 1, got {return_code}"
    )


def stop_process_if_running(api_process):
    if api_process.poll() is None:
        api_process.terminate()
        api_process.wait(timeout=2)


def test_dual_write_crash_does_not_strand_payment():
    user_id = f"user_crash_{uuid.uuid4().hex}"

    api_process = start_crash_api()

    try:
        wait_for_api()

        send_payment_expect_disconnect(user_id)

        assert_crash_api_exited(api_process)

        payment = find_payment_by_user_id(user_id)

        # Proves PostgreSQL committed the payment before the process died.
        assert payment is not None

        payment_id, status = payment

        assert status == "pending"

        # Protects the invariant that every committed payment must have
        # an event available for downstream processing.
        assert kafka_has_payment_event(payment_id)

    finally:
        stop_process_if_running(api_process)