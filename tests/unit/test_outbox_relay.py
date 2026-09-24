import pytest

import outbox_relay


class FakeCursor:
    def __init__(self):
        self.row = (
            1,
            {
                "event_id": "event-123",
                "payment_id": "payment-123",
                "user_id": "user_123",
                "amount": 100,
                "event_type": "payment_created",
            },
        )
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return self.row

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()
        self.commit_called = False
        self.rollback_called = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commit_called = True

    def rollback(self):
        self.rollback_called = True


def test_publish_failure_rolls_back_and_leaves_event_unpublished(monkeypatch):
    conn = FakeConnection()

    def fail_publish(event):
        raise RuntimeError("Kafka delivery failed")

    monkeypatch.setattr(
        outbox_relay,
        "produce_event",
        fail_publish,
    )

    with pytest.raises(RuntimeError, match="Kafka delivery failed"):
        outbox_relay.publish_next_event(conn)

    assert conn.rollback_called is True
    assert conn.commit_called is False

    update_queries = [
        query
        for query, _ in conn.cursor_instance.executed
        if "UPDATE outbox" in query
    ]

    assert update_queries == []