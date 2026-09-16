import json

import api.producer as producer_module


class FakeProducer:
    def __init__(self):
        self.produced_message = None
        self.poll_called_with = None
        self.flush_called = False

    def produce(self, **kwargs):
        self.produced_message = kwargs

    def poll(self, timeout):
        self.poll_called_with = timeout

    def flush(self):
        self.flush_called = True


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
    assert fake_producer.poll_called_with == 0
    assert fake_producer.flush_called is True

def test_produce_event_attaches_delivery_callback(monkeypatch):
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

    message = fake_producer.produced_message

    # Verifies that produce_event() registers the delivery_report callback.
    # If this callback were removed, Kafka messages could still be published,
    # but the application would no longer receive delivery success/failure results.
    # So, this test basically checks if call back still points to delivery_report
    assert message["callback"] is producer_module.delivery_report
