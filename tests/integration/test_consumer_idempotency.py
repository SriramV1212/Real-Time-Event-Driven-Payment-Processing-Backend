import time
import uuid

from consumer.payment_consumer import process_event
from db.connection import get_connection


def test_duplicate_event_does_not_apply_payment_twice():
    event_id = str(uuid.uuid4())
    payment_id = str(uuid.uuid4())
    user_id = f"user_idempotency_{uuid.uuid4().hex}"

    event = {
        "event_id": event_id,
        "payment_id": payment_id,
        "user_id": user_id,
        "amount": 100,
        "event_type": "payment_created",
    }

    conn = get_connection()

    try:
        # Create the pending payment that the consumer expects.
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payments (
                    payment_id,
                    user_id,
                    amount,
                    status
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    payment_id,
                    user_id,
                    100,
                    "pending",
                ),
            )

        conn.commit()

        # First delivery.
        process_event(event, time.time())

        # Duplicate delivery with the exact same event_id.
        process_event(event, time.time())

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT balance
                FROM users
                WHERE user_id = %s
                """,
                (user_id,),
            )
            balance = cur.fetchone()[0]

            cur.execute(
                """
                SELECT status
                FROM payments
                WHERE payment_id = %s
                """,
                (payment_id,),
            )
            status = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM processed_events
                WHERE event_id = %s
                """,
                (event_id,),
            )
            processed_count = cur.fetchone()[0]

        assert balance == -100
        assert status == "processed"
        assert processed_count == 1

    finally:
        conn.rollback()

        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            cur.execute(
                "DELETE FROM payments WHERE payment_id = %s",
                (payment_id,),
            )
            cur.execute(
                "DELETE FROM users WHERE user_id = %s",
                (user_id,),
            )

        conn.commit()
        conn.close()