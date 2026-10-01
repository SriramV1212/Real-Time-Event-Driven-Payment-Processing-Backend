from datetime import UTC, datetime, timedelta

from db.connection import get_connection
from idempotency_cleanup import delete_expired_batch


def test_cleanup_deletes_expired_key_and_keeps_active_key():
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE idempotency_keys (
                    id BIGSERIAL PRIMARY KEY,
                    idempotency_key TEXT NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL
                )
                """
            )

            now = datetime.now(UTC)

            cur.execute(
                """
                INSERT INTO idempotency_keys (
                    idempotency_key,
                    expires_at
                )
                VALUES
                    (%s, %s),
                    (%s, %s)
                """,
                (
                    "expired-test-key",
                    now - timedelta(hours=1),
                    "active-test-key",
                    now + timedelta(hours=1),
                ),
            )

        conn.commit()

        deleted_count = delete_expired_batch(conn)

        assert deleted_count == 1

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT idempotency_key
                FROM idempotency_keys
                ORDER BY idempotency_key
                """
            )

            remaining_keys = [
                row[0]
                for row in cur.fetchall()
            ]

        assert remaining_keys == ["active-test-key"]

    finally:
        conn.close()