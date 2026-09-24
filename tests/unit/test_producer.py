import json

import pytest

import api.producer as producer_module


class FakeMessage:
    def partition(self):
        return 0

    def offset(self):
        return 0


class FakeProducer:
    def __init__(self, delivery_error=None):
        self.produced_message = None
        self.flush_called = False
        self.delivery_error = delivery_error

    def produce(self, **kwargs):
        self.produced_message = kwargs

    def flush(self):
        self.flush_called = True

        callback = self.produced_message["callback"]
        callback(self.delivery_error, FakeMessage())

        return 0


def test_produce_event_uses_expected_topic(monkeypatch):
    fake_producer = FakeProducer()

    monkeypatch.setattr(
        producer_module,
        "producer",
        fake_producer
    )

    event = {
        "event_id": "event-123",
        "payment_id": "payment-123",
        "user_id": "user_123",
        "amount": 100,
        "event_type": "payment_created"
    }

    producer_module.produce_event(event)

    # Verifies that produce_event():
    # - publishes to the configured payment topic
    # - uses user_id as the Kafka message key
    # - serializes the event payload correctly
    # - polls producer callbacks
    # - flushes queued messages before returning


    message = fake_producer.produced_message

    assert message["topic"] == producer_module.TOPIC
    assert message["key"] == b"user_123"
    assert json.loads(message["value"].decode("utf-8")) == event
    assert fake_producer.flush_called is True

def test_produce_event_raises_when_kafka_delivery_fails(monkeypatch):
    fake_producer = FakeProducer(delivery_error="delivery failed")

    monkeypatch.setattr(
        producer_module,
        "producer",
        fake_producer,
    )

    event = {
        "event_id": "event-123",
        "payment_id": "payment-123",
        "user_id": "user_123",
        "amount": 100,
        "event_type": "payment_created",
    }

    with pytest.raises(RuntimeError, match="Kafka delivery failed"):
        producer_module.produce_event(event)
