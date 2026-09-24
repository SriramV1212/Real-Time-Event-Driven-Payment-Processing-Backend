import uuid

import pytest
from confluent_kafka import Producer
from psycopg2.extras import Json

import api.producer as producer_module
import outbox_relay
from config import KAFKA_BOOTSTRAP_SERVERS
from db.connection import get_connection


def test_outbox_event_survives_kafka_failure_and_retries(monkeypatch):
    event_id = str(uuid.uuid4())
    payment_id = str(uuid.uuid4())

    event = {
        "event_id": event_id,
        "payment_id": payment_id,
        "user_id": "user_outbox_failure_test",
        "amount": 100,
        "event_type": "payment_created",
    }

    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE outbox (
                id BIGSERIAL PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                aggregate_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                published_at TIMESTAMP NULL
            ) ON COMMIT PRESERVE ROWS
        """)

    conn.commit()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pg_temp.outbox (
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

        failing_producer = Producer({
            "bootstrap.servers": "127.0.0.1:1",
            "message.timeout.ms": 1000,
        })

        monkeypatch.setattr(
            producer_module,
            "producer",
            failing_producer,
        )

        with pytest.raises(RuntimeError, match="Kafka delivery failed"):
            outbox_relay.publish_next_event(conn)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT published_at
                FROM pg_temp.outbox
                WHERE event_id = %s
                """,
                (event_id,),
            )

            assert cur.fetchone()[0] is None

        working_producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        })

        monkeypatch.setattr(
            producer_module,
            "producer",
            working_producer,
        )

        assert outbox_relay.publish_next_event(conn) is True

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT published_at
                FROM pg_temp.outbox
                WHERE event_id = %s
                """,
                (event_id,),
            )

            assert cur.fetchone()[0] is not None

    finally:
        conn.close()