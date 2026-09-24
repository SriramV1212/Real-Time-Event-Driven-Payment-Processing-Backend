# Postmortem: PostgreSQL-to-Kafka Dual-Write Failure

## Summary

The payment API originally committed payment state to PostgreSQL and then published a `payment_created` event to Kafka as a separate operation.

A crash between those two operations could leave a payment committed in PostgreSQL without a corresponding Kafka event.

This failure was reproduced deterministically and later fixed using a transactional outbox.

## Original Flow

The original payment creation path was:

1. Insert payment into PostgreSQL.
2. Commit the PostgreSQL transaction.
3. Publish the event to Kafka.

The database commit and Kafka publish were independent operations.

## Failure Scenario

The problematic sequence was:

```text
payment inserted
    ↓
PostgreSQL commit succeeds
    ↓
API process crashes
    ↓
Kafka publish never happens
```

The result was:

- the payment existed in PostgreSQL
- the payment remained `pending`
- no corresponding event existed in Kafka
- the consumer had nothing to process

The system therefore had durable payment state but had lost the event required to continue processing it.

## How It Was Reproduced

A deterministic fault-injection point was added immediately after the PostgreSQL commit and before Kafka publication.

When enabled, the API terminated using `os._exit(1)`.

The integration test verified three things:

1. The API process exited.
2. The payment row existed in PostgreSQL.
3. Kafka did not contain the corresponding event.

This proved the failure was caused by the database-to-Kafka dual-write gap rather than by an unrelated request or networking failure.

## Root Cause

The system attempted to coordinate two independent systems:

- PostgreSQL
- Kafka

without a shared atomic transaction.

A successful PostgreSQL commit did not guarantee that Kafka publication would also occur.

Changing the order to publish to Kafka first would not eliminate the problem. It would create the opposite failure mode, where Kafka could contain an event for database state that never committed.

## Resolution

The payment API was changed to use a transactional outbox.

The API now writes:

- the payment row
- the complete event payload in the outbox

inside the same PostgreSQL transaction.

A separate relay process later reads unpublished outbox rows and publishes them to Kafka.

After Kafka delivery succeeds, the relay marks the outbox row as published.

The new flow is:

```text
API
  ↓
payment insert
  ↓
outbox insert
  ↓
single PostgreSQL commit
  ↓
API may crash
  ↓
outbox row remains durable
  ↓
relay publishes event later
```

## Verification

The original crash test was rerun after implementing the outbox.

The API was again terminated immediately after the PostgreSQL commit.

This time:

- the payment row remained committed
- the corresponding outbox row remained committed
- the independently running relay discovered the row
- the event was successfully published to Kafka

The regression test was later made self-contained by starting the relay subprocess from the test itself.

## New Failure Mode Identified

The outbox removes the original lost-event failure window, but it introduces an expected at-least-once publication behavior.

The relay can experience this sequence:

```text
publish event to Kafka
    ↓
Kafka acknowledges delivery
    ↓
relay crashes
    ↓
published_at was not committed
    ↓
relay restarts
    ↓
same event is published again
```

A deterministic event-specific failpoint was used to reproduce this window.

The integration test confirmed that two Kafka records with the same `event_id` could be produced.

## Consumer Protection

Because duplicate publication is possible, consumer idempotency is required for correctness.

The payment consumer stores processed `event_id` values in `processed_events`.

If the same logical event is processed again, the duplicate `event_id` causes the consumer to return before applying the payment side effect again.

An integration test verified that processing the same event twice resulted in:

- one `processed_events` row
- one payment status transition
- one balance deduction

## Remaining Limitations

The implementation intentionally keeps several production concerns out of scope:

- the relay publishes one row at a time
- Kafka publication uses synchronous `flush()`
- retry behavior is basic
- there is no exponential backoff or retry scheduling
- a permanently failing oldest event could delay later events
- published outbox rows are not yet cleaned up
- strict global publication ordering is not guaranteed with concurrent workers
- the Kafka duplicate-count verification test scans topic history with a timeout and therefore does not provide bounded verification as the topic grows

A stronger duplicate-verification test could snapshot Kafka partition offsets before and after the test and inspect only that bounded range.

## Observability

The relay can query the age of the oldest unpublished outbox row.

A growing age indicates that publication may be falling behind because of Kafka unavailability, relay failure, or a repeatedly failing event.

## Outcome

The original architecture could permanently lose the event needed to continue processing a committed payment.

The transactional outbox changed that failure mode from possible event loss to possible duplicate publication.

Duplicate publication is recoverable because consumers are idempotent, making the overall design at-least-once and failure-tolerant.