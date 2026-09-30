import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests

from db.connection import get_connection


def start_api():
    env = os.environ.copy()
    env["FAULT_INJECT_IDEMPOTENCY_RACE_DELAY_MS"] = "500"

    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "api.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8002",
        ],
        env=env,
    )


def wait_for_api(timeout_seconds=5):
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        try:
            with socket.create_connection(
                ("127.0.0.1", 8002),
                timeout=0.2,
            ):
                return

        except OSError:
            time.sleep(0.1)

    raise RuntimeError("Concurrency-test API did not start in time")


def stop_process_if_running(process):
    if process is not None and process.poll() is None:
        process.terminate()
        process.wait(timeout=5)


def send_payment(barrier, idempotency_key, user_id):
    barrier.wait(timeout=5)

    return requests.post(
        "http://127.0.0.1:8002/payments",
        json={
            "user_id": user_id,
            "amount": 100,
        },
        headers={
            "Idempotency-Key": idempotency_key,
        },
        timeout=10,
    )


def test_concurrent_requests_with_same_idempotency_key_expose_race():
    request_count = 10

    idempotency_key = f"concurrent-{uuid.uuid4().hex}"
    user_id = f"user_conc_{uuid.uuid4().hex}"

    barrier = threading.Barrier(request_count)

    api_process = None
    conn = get_connection()

    try:
        api_process = start_api()
        wait_for_api()

        with ThreadPoolExecutor(max_workers=request_count) as executor:
            futures = [
                executor.submit(
                    send_payment,
                    barrier,
                    idempotency_key,
                    user_id,
                )
                for _ in range(request_count)
            ]

            responses = [
                future.result()
                for future in futures
            ]

        status_codes = [
            response.status_code
            for response in responses
        ]

        print(f"status codes: {status_codes}")

        assert all(
            status_code == 200
            for status_code in status_codes
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM idempotency_keys
                WHERE idempotency_key = %s
                """,
                (idempotency_key,),
            )

            idempotency_count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM payments
                WHERE user_id = %s
                """,
                (user_id,),
            )

            payment_count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM outbox
                WHERE aggregate_id IN (
                    SELECT payment_id
                    FROM payments
                    WHERE user_id = %s
                )
                """,
                (user_id,),
            )

            outbox_count = cur.fetchone()[0]

        print(f"idempotency rows: {idempotency_count}")
        print(f"payments created: {payment_count}")
        print(f"outbox rows: {outbox_count}")

        assert idempotency_count > 1
        assert payment_count > 1
        assert outbox_count > 1

    finally:
        stop_process_if_running(api_process)

        conn.rollback()

        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM idempotency_keys
                WHERE idempotency_key = %s
                """,
                (idempotency_key,),
            )

            cur.execute(
                """
                DELETE FROM outbox
                WHERE aggregate_id IN (
                    SELECT payment_id
                    FROM payments
                    WHERE user_id = %s
                )
                """,
                (user_id,),
            )

            cur.execute(
                """
                DELETE FROM payments
                WHERE user_id = %s
                """,
                (user_id,),
            )

        conn.commit()
        conn.close()