import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import pytest
from psycopg2.extras import Json

from consumer import payment_consumer
from db.connection import get_connection


def attempt_capture(payment_id, entries, barrier):
    conn = get_connection(database="payments_test")

    try:
        barrier.wait(timeout=10)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT post_ledger_transaction(%s, %s, %s)
                """,
                (payment_id, "payment_capture", Json(entries)),
            )
            transaction_id = cur.fetchone()[0]

        conn.commit()
        return ("success", transaction_id)

    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return ("duplicate", None)

    finally:
        conn.close()


def test_successful_ledger_posting():
    payment_id = str(uuid.uuid4())
    conn = get_connection(database="payments_test")

    entries = [
        {
            "account": "held_at_stripe",
            "currency": "USD",
            "direction": "debit",
            "amount_minor": 4800,
        },
        {
            "account": "stripe_fees",
            "currency": "USD",
            "direction": "debit",
            "amount_minor": 200,
        },
        {
            "account": "revenue",
            "currency": "USD",
            "direction": "credit",
            "amount_minor": 5000,
        },
    ]

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payments
                    (payment_id, user_id, amount, status)
                VALUES (%s, %s, %s, %s)
                """,
                (payment_id, "user_test", 5000, "pending"),
            )

            cur.execute("SELECT name, balance_minor FROM ledger_accounts")
            balances_before = dict(cur.fetchall())

            cur.execute(
                """
                SELECT post_ledger_transaction(
                    %s, %s, %s
                )
                """,
                (payment_id, "payment_capture", Json(entries)),
            )

            transaction_id = cur.fetchone()[0]
            assert transaction_id is not None

            cur.execute(
                """
                SELECT account_id, direction, amount_minor
                FROM ledger_entries
                WHERE transaction_id = %s
                """,
                (transaction_id,),
            )
            assert len(cur.fetchall()) == 3

            cur.execute(
                """
                SELECT name, balance_minor
                FROM ledger_accounts
                """
            )
            balances = dict(cur.fetchall())

            assert balances["held_at_stripe"] == balances_before["held_at_stripe"] + 4800
            assert balances["stripe_fees"] == balances_before["stripe_fees"] + 200
            assert balances["revenue"] == balances_before["revenue"] + 5000

    finally:
        conn.rollback()
        conn.close()


def test_ledger_reversal():
    payment_id = str(uuid.uuid4())
    conn = get_connection(database="payments_test")

    entries = [
        {"account": "held_at_stripe", "currency": "USD", "direction": "debit", "amount_minor": 4800},
        {"account": "stripe_fees", "currency": "USD", "direction": "debit", "amount_minor": 200},
        {"account": "revenue", "currency": "USD", "direction": "credit", "amount_minor": 5000},
    ]

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payments (payment_id, user_id, amount, status)
                VALUES (%s, %s, %s, %s)
                """,
                (payment_id, "user_test", 5000, "pending"),
            )

            cur.execute(
                "SELECT name, balance_minor FROM ledger_accounts"
            )
            balances_before = dict(cur.fetchall())

            cur.execute(
                "SELECT post_ledger_transaction(%s, %s, %s)",
                (payment_id, "payment_capture", Json(entries)),
            )
            original_id = cur.fetchone()[0]

            cur.execute(
                "SELECT post_ledger_transaction(%s, %s, %s, %s)",
                (payment_id, "reversal", None, original_id),
            )
            reversal_id = cur.fetchone()[0]

            # Confirm the reversal links to the original.
            cur.execute(
                """
                SELECT reverses_transaction_id
                FROM ledger_transactions
                WHERE id = %s
                """,
                (reversal_id,),
            )
            assert cur.fetchone()[0] == original_id

            # Confirm the original entries remain unchanged.
            cur.execute(
                """
                SELECT direction, amount_minor
                FROM ledger_entries
                WHERE transaction_id = %s
                ORDER BY account_id
                """,
                (original_id,),
            )
            original_entries = cur.fetchall()

            cur.execute(
                """
                SELECT direction, amount_minor
                FROM ledger_entries
                WHERE transaction_id = %s
                ORDER BY account_id
                """,
                (reversal_id,),
            )
            reversal_entries = cur.fetchall()

            assert len(original_entries) == len(reversal_entries) == 3

            for original, reversal in zip(original_entries, reversal_entries):
                assert original[1] == reversal[1]
                assert original[0] != reversal[0]

            # Reversal should restore balances to their initial values.
            cur.execute(
                "SELECT name, balance_minor FROM ledger_accounts"
            )
            assert dict(cur.fetchall()) == balances_before

    finally:
        conn.rollback()
        conn.close()

def test_duplicate_payment_capture_rejected():
    payment_id = str(uuid.uuid4())
    conn = get_connection(database="payments_test")

    entries = [
        {"account": "held_at_stripe", "currency": "USD", "direction": "debit", "amount_minor": 4800},
        {"account": "stripe_fees", "currency": "USD", "direction": "debit", "amount_minor": 200},
        {"account": "revenue", "currency": "USD", "direction": "credit", "amount_minor": 5000},
    ]

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payments (payment_id, user_id, amount, status)
                VALUES (%s, %s, %s, %s)
                """,
                (payment_id, "user_test", 5000, "pending"),
            )

            cur.execute(
                "SELECT post_ledger_transaction(%s, %s, %s)",
                (payment_id, "payment_capture", Json(entries)),
            )

            with pytest.raises(psycopg2.errors.UniqueViolation):
                cur.execute(
                    "SELECT post_ledger_transaction(%s, %s, %s)",
                    (payment_id, "payment_capture", Json(entries)),
                )

    finally:
        conn.rollback()
        conn.close()


def test_concurrent_duplicate_capture():
    payment_id = str(uuid.uuid4())

    entries = [
        {"account": "held_at_stripe", "currency": "USD",
         "direction": "debit", "amount_minor": 4800},
        {"account": "stripe_fees", "currency": "USD",
         "direction": "debit", "amount_minor": 200},
        {"account": "revenue", "currency": "USD",
         "direction": "credit", "amount_minor": 5000},
    ]

    conn = get_connection(database="payments_test")

    try:
        # The payment must exist before either worker starts.
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payments (payment_id, user_id, amount, status)
                VALUES (%s, %s, %s, %s)
                """,
                (payment_id, "user_test", 5000, "pending"),
            )

        conn.commit()

        barrier = threading.Barrier(2)

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    attempt_capture, payment_id, entries, barrier
                )
                for _ in range(2)
            ]
            results = [f.result(timeout=15) for f in futures]

        assert sorted(result[0] for result in results) == [
            "duplicate", "success"
        ]

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM ledger_transactions
                WHERE payment_id = %s
                  AND operation = 'payment_capture'
                """,
                (payment_id,),
            )
            assert cur.fetchone()[0] == 1

            cur.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(amount_minor), 0)
                FROM ledger_entries
                WHERE transaction_id IN (
                    SELECT id
                    FROM ledger_transactions
                    WHERE payment_id = %s
                    AND operation = 'payment_capture'
                )
                """,
                (payment_id,),
            )

            entry_count, total_amount = cur.fetchone()

            assert entry_count == 3
            assert total_amount == 10000

    finally:
        conn.rollback()
        conn.close()

def test_stored_balances_match_ledger_entries():
    conn = get_connection(database="payments_test")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    a.name,
                    a.balance_minor AS stored_balance,
                    COALESCE(
                        SUM(
                            CASE
                                WHEN a.type IN ('asset', 'expense')
                                     AND e.direction = 'debit'
                                    THEN e.amount_minor
                                WHEN a.type = 'revenue'
                                     AND e.direction = 'credit'
                                    THEN e.amount_minor
                                ELSE -e.amount_minor
                            END
                        ), 0
                    ) AS computed_balance
                FROM ledger_accounts a
                LEFT JOIN ledger_entries e ON e.account_id = a.id
                GROUP BY a.id, a.name, a.type, a.balance_minor
            """)

            for name, stored, computed in cur.fetchall():
                assert stored == computed, (
                    f"Balance mismatch for {name}: "
                    f"stored={stored}, computed={computed}"
                )

    finally:
        conn.close()

def test_consumer_posts_ledger_transaction(monkeypatch):
    monkeypatch.setattr(
        payment_consumer,
        "get_db_connection",
        lambda: get_connection(database="payments_test"),
    )

    payment_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())

    event = {
        "event_id": event_id,
        "payment_id": payment_id,
        "user_id": "consumer_test_user",
        "amount": 5000,
    }

    # Create the payment that the consumer will process.
    conn = get_connection(database="payments_test")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payments (payment_id, user_id, amount, status)
                VALUES (%s, %s, %s, %s)
                """,
                (payment_id, event["user_id"], event["amount"], "pending"),
            )

        conn.commit()
    finally:
        conn.close()

    # Run the real consumer business logic.
    payment_consumer.process_event(event, time.time())

    conn = get_connection(database="payments_test")

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM payments WHERE payment_id = %s",
                (payment_id,),
            )
            assert cur.fetchone()[0] == "processed"

            cur.execute(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            assert cur.fetchone()[0] == 1

            cur.execute(
                """
                SELECT direction, amount_minor
                FROM ledger_entries
                WHERE transaction_id = (
                    SELECT id
                    FROM ledger_transactions
                    WHERE payment_id = %s
                    AND operation = 'payment_capture'
                )
                """,
                (payment_id,),
            )
            entries = cur.fetchall()

            assert sorted(entries) == [
                ("credit", 5000),
                ("debit", 5000),
            ]
    finally:
        conn.close()


def test_consumer_duplicate_event_is_idempotent(monkeypatch):
    monkeypatch.setattr(
        payment_consumer,
        "get_db_connection",
        lambda: get_connection(database="payments_test"),
    )

    payment_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())

    event = {
        "event_id": event_id,
        "payment_id": payment_id,
        "user_id": "duplicate_test_user",
        "amount": 5000,
    }

    conn = get_connection(database="payments_test")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payments (payment_id, user_id, amount, status)
                VALUES (%s, %s, %s, %s)
                """,
                (payment_id, event["user_id"], event["amount"], "pending"),
            )

        conn.commit()
    finally:
        conn.close()

    payment_consumer.process_event(event, time.time())
    payment_consumer.process_event(event, time.time())

    conn = get_connection(database="payments_test")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM processed_events
                WHERE event_id = %s
                """,
                (event_id,),
            )
            assert cur.fetchone()[0] == 1

            cur.execute(
                """
                SELECT COUNT(*)
                FROM ledger_transactions
                WHERE payment_id = %s
                AND operation = 'payment_capture'
                """,
                (payment_id,),
            )
            assert cur.fetchone()[0] == 1

            cur.execute(
                """
                SELECT COUNT(*)
                FROM ledger_entries
                WHERE transaction_id = (
                    SELECT id
                    FROM ledger_transactions
                    WHERE payment_id = %s
                    AND operation = 'payment_capture'
                )
                """,
                (payment_id,),
            )
            assert cur.fetchone()[0] == 2
    finally:
        conn.close()

def test_consumer_ledger_failure_rolls_back(monkeypatch):
    monkeypatch.setattr(
        payment_consumer,
        "get_db_connection",
        lambda: get_connection(database="payments_test"),
    )

    payment_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())

    event = {
        "event_id": event_id,
        "payment_id": payment_id,
        "user_id": "rollback_test_user",
        "amount": 5000,
    }

    entries = [
        {
            "account": "held_at_stripe",
            "currency": "USD",
            "direction": "debit",
            "amount_minor": 5000,
        },
        {
            "account": "revenue",
            "currency": "USD",
            "direction": "credit",
            "amount_minor": 5000,
        },
    ]

    conn = get_connection(database="payments_test")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payments (payment_id, user_id, amount, status)
                VALUES (%s, %s, %s, %s)
                """,
                (payment_id, event["user_id"], event["amount"], "pending"),
            )

            # Create an existing capture to force a duplicate error.
            cur.execute(
                """
                SELECT post_ledger_transaction(%s, %s, %s)
                """,
                (payment_id, "payment_capture", Json(entries)),
            )

        conn.commit()
    finally:
        conn.close()

    # Consumer attempts to capture the same payment again.
    with pytest.raises(psycopg2.errors.UniqueViolation):
        payment_consumer.process_event(event, time.time())

    conn = get_connection(database="payments_test")

    try:
        with conn.cursor() as cur:
            # Payment must remain pending.
            cur.execute(
                "SELECT status FROM payments WHERE payment_id = %s",
                (payment_id,),
            )
            assert cur.fetchone()[0] == "pending"

            # The failed consumer attempt must roll back event tracking.
            cur.execute(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            assert cur.fetchone()[0] == 0

            # Only the original capture should exist.
            cur.execute(
                """
                SELECT COUNT(*)
                FROM ledger_transactions
                WHERE payment_id = %s
                  AND operation = 'payment_capture'
                """,
                (payment_id,),
            )
            assert cur.fetchone()[0] == 1
    finally:
        conn.close()


def test_consumer_posts_with_restricted_role(monkeypatch):
    # Keep the actual payment_app connection, but target payments_test.
    monkeypatch.setitem(
        payment_consumer.DB_CONFIG,
        "dbname",
        "payments_test",
    )

    conn = payment_consumer.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_user, current_database()")
            assert cur.fetchone() == ("payment_app", "payments_test")
    finally:
        conn.close()

    payment_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())

    event = {
        "event_id": event_id,
        "payment_id": payment_id,
        "user_id": "restricted_role_test_user",
        "amount": 5000,
    }

    # Create the pending payment as admin.
    conn = get_connection(database="payments_test")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payments (payment_id, user_id, amount, status)
                VALUES (%s, %s, %s, %s)
                """,
                (payment_id, event["user_id"], 5000, "pending"),
            )
        conn.commit()
    finally:
        conn.close()

    # Process the payment using the real payment_app connection.
    payment_consumer.process_event(event, time.time())

    conn = payment_consumer.get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM payments WHERE payment_id = %s",
                (payment_id,),
            )
            assert cur.fetchone()[0] == "processed"

            cur.execute(
                """
                SELECT COUNT(*)
                FROM ledger_transactions
                WHERE payment_id = %s
                  AND operation = 'payment_capture'
                """,
                (payment_id,),
            )
            assert cur.fetchone()[0] == 1

            cur.execute(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            assert cur.fetchone()[0] == 1
    finally:
        conn.close()

@pytest.mark.parametrize("invalid_amount", [
    -5000,
    0,
    "5000",
    5000.0,
    True,
])
def test_consumer_rejects_invalid_amount(monkeypatch, invalid_amount):
    monkeypatch.setattr(
        payment_consumer,
        "get_db_connection",
        lambda: get_connection(database="payments_test"),
    )

    payment_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())

    conn = get_connection(database="payments_test")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payments (payment_id, user_id, amount, status)
                VALUES (%s, %s, %s, %s)
                """,
                (payment_id, "validation_test_user", 5000, "pending"),
            )
        conn.commit()
    finally:
        conn.close()

    event = {
        "event_id": event_id,
        "payment_id": payment_id,
        "user_id": "validation_test_user",
        "amount": invalid_amount,
    }

    with pytest.raises(ValueError):
        payment_consumer.process_event(event, time.time())

    conn = get_connection(database="payments_test")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM payments WHERE payment_id = %s",
                (payment_id,),
            )
            assert cur.fetchone()[0] == "pending"

            cur.execute(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            assert cur.fetchone()[0] == 0

            cur.execute(
                "SELECT COUNT(*) FROM ledger_transactions WHERE payment_id = %s",
                (payment_id,),
            )
            assert cur.fetchone()[0] == 0
    finally:
        conn.close()

    
