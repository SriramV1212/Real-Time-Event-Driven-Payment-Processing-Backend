import logging
import os
import time

from api.producer import produce_event
from db.connection import get_connection
from utils.logging_config import configure_logging

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 1


def publish_next_event(conn):
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT id, payload
            FROM outbox
            WHERE published_at IS NULL
            ORDER BY id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """)

        row = cur.fetchone()

        if row is None:
            conn.commit()
            return False

        outbox_id, payload = row

        produce_event(payload)

        crash_event_id = os.getenv(
            "FAULT_INJECT_CRASH_AFTER_KAFKA_PUBLISH_EVENT_ID"
        )

        if crash_event_id == payload["event_id"]:
            os._exit(1)

        cur.execute("""
            UPDATE outbox
            SET published_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (outbox_id,))

        conn.commit()

        logger.info("Published outbox event %s", outbox_id)

        return True

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()


def get_oldest_unpublished_age_seconds(conn):
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT EXTRACT(
                EPOCH FROM (
                    CURRENT_TIMESTAMP - MIN(created_at)
                )
            )
            FROM outbox
            WHERE published_at IS NULL
        """)

        age = cur.fetchone()[0]

        conn.commit()

        if age is None:
            return 0.0

        return float(age)

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
                published = publish_next_event(conn)

            except Exception:
                logger.exception("Failed to publish outbox event")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            if not published:
                time.sleep(POLL_INTERVAL_SECONDS)

    finally:
        conn.close()


if __name__ == "__main__":
    main()