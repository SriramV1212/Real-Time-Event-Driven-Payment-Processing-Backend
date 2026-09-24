import outbox_relay
from db.connection import get_connection


def test_oldest_unpublished_age_uses_oldest_pending_row():
    conn = get_connection()

    try:
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

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pg_temp.outbox (
                    event_id,
                    aggregate_id,
                    event_type,
                    payload,
                    created_at
                )
                VALUES
                    (
                        'event-old',
                        'payment-old',
                        'payment_created',
                        '{}',
                        CURRENT_TIMESTAMP - INTERVAL '10 seconds'
                    ),
                    (
                        'event-new',
                        'payment-new',
                        'payment_created',
                        '{}',
                        CURRENT_TIMESTAMP - INTERVAL '2 seconds'
                    )
            """)

        conn.commit()

        age = outbox_relay.get_oldest_unpublished_age_seconds(conn)

        assert age >= 10

    finally:
        conn.close()


def test_oldest_unpublished_age_is_zero_when_no_pending_rows():
    conn = get_connection()

    try:
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

        assert outbox_relay.get_oldest_unpublished_age_seconds(conn) == 0.0

    finally:
        conn.close()