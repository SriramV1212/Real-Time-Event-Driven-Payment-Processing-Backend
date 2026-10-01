import uuid

from fastapi.testclient import TestClient

from api.main import app
from db.connection import get_connection

client = TestClient(app)


def test_same_idempotency_key_replays_original_response():
    user_id = f"user_idem_{uuid.uuid4().hex}"
    idempotency_key = f"idem-{uuid.uuid4()}"

    payload = {
        "user_id": user_id,
        "amount": 100,
    }

    headers = {
        "Idempotency-Key": idempotency_key,
    }

    first_response = client.post(
        "/payments",
        json=payload,
        headers=headers,
    )

    second_response = client.post(
        "/payments",
        json=payload,
        headers=headers,
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 200

    assert first_response.content == second_response.content

    payment_id = first_response.json()["payment_id"]

    conn = get_connection()

    try:
        with conn.cursor() as cur:
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
                FROM idempotency_keys
                WHERE idempotency_key = %s
                """,
                (idempotency_key,),
            )
            idempotency_count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM outbox
                WHERE aggregate_id = %s
                """,
                (payment_id,),
            )
            outbox_count = cur.fetchone()[0]

        assert payment_count == 1
        assert idempotency_count == 1
        assert outbox_count == 1

    finally:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM idempotency_keys WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            cur.execute(
                "DELETE FROM outbox WHERE aggregate_id = %s",
                (payment_id,),
            )
            cur.execute(
                "DELETE FROM payments WHERE payment_id = %s",
                (payment_id,),
            )

        conn.commit()
        conn.close()


def test_same_idempotency_key_with_different_payload_returns_409():
    user_id = f"user_idem_{uuid.uuid4().hex}"
    idempotency_key = f"idem-{uuid.uuid4()}"

    headers = {
        "Idempotency-Key": idempotency_key,
    }

    first_response = client.post(
        "/payments",
        json={
            "user_id": user_id,
            "amount": 100,
        },
        headers=headers,
    )

    conflicting_response = client.post(
        "/payments",
        json={
            "user_id": user_id,
            "amount": 500,
        },
        headers=headers,
    )

    assert first_response.status_code == 200
    assert conflicting_response.status_code == 409

    payment_id = first_response.json()["payment_id"]

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM payments
                WHERE user_id = %s
                """,
                (user_id,),
            )
            payment_count = cur.fetchone()[0]

        assert payment_count == 1

    finally:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM idempotency_keys WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            cur.execute(
                "DELETE FROM outbox WHERE aggregate_id = %s",
                (payment_id,),
            )
            cur.execute(
                "DELETE FROM payments WHERE payment_id = %s",
                (payment_id,),
            )

        conn.commit()
        conn.close()