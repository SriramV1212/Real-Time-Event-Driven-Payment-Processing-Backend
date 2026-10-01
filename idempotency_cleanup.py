import logging
import time

from db.connection import get_connection
from utils.logging_config import configure_logging

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000
POLL_INTERVAL_SECONDS = 60


def delete_expired_batch(conn):
    cur = conn.cursor()

    try:
        cur.execute(
            """
            WITH expired_rows AS (
                SELECT id
                FROM idempotency_keys
                WHERE expires_at <= CURRENT_TIMESTAMP
                ORDER BY expires_at
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM idempotency_keys
            WHERE id IN (
                SELECT id
                FROM expired_rows
            )
            RETURNING id
            """,
            (BATCH_SIZE,),
        )

        deleted_count = len(cur.fetchall())

        conn.commit()

        return deleted_count

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()


def main():
    configure_logging()

    conn = get_connection()

    try:
       while True:
            try:
                deleted_count = delete_expired_batch(conn)

                if deleted_count > 0:
                    logger.info(
                        "Deleted %s expired idempotency records",
                        deleted_count,
                    )

                if deleted_count < BATCH_SIZE:
                    time.sleep(POLL_INTERVAL_SECONDS)

            except Exception:
                logger.exception(
                    "Failed to clean expired idempotency records"
                )
                time.sleep(POLL_INTERVAL_SECONDS)

    finally:
        conn.close()


if __name__ == "__main__":
    main()