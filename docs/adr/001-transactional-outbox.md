# ADR 001: Transactional Outbox for Payment Events

## Status

Accepted

## Context

The payment API originally performed two independent operations:

1. Commit the payment to PostgreSQL.
2. Publish the `payment_created` event to Kafka.

This created a dual-write failure window. If the API process crashed after the PostgreSQL commit but before Kafka publication, the payment remained in the database with no corresponding Kafka event.

A deterministic integration test reproduced this failure by terminating the API process immediately after the database commit.

## Decision

Use the transactional outbox pattern.

When a payment is created, the API now writes both:

- the payment row
- an outbox row containing the event to publish

inside the same PostgreSQL transaction.

The outbox stores the complete event payload rather than reconstructing the event later from the payment table.

A separate relay process polls unpublished outbox rows, publishes them to Kafka, and sets `published_at` only after Kafka delivery succeeds.

The relay selects work using:

```sql
WHERE published_at IS NULL
ORDER BY id
LIMIT 1
FOR UPDATE SKIP LOCKED
```

This keeps the initial implementation simple while allowing multiple relay workers to safely claim different rows in the future.

## Guarantees

The design prevents the original failure mode where a committed payment permanently loses its corresponding event.

If PostgreSQL commits successfully, the event publication obligation is durably stored in the outbox.

Kafka publication is at-least-once, not exactly-once.

If the relay publishes successfully and then crashes before committing `published_at`, the event may be published again after restart.

Consumers therefore need to be idempotent. The payment consumer uses `event_id` in the `processed_events` table to prevent duplicate business effects.

## Alternatives Considered

### Direct PostgreSQL commit followed by Kafka publish

Simple, but creates the dual-write failure window demonstrated by the Phase 2 crash test.

### Kafka publish before PostgreSQL commit

Moves the failure window rather than eliminating it. Kafka could contain an event for a payment whose database transaction never committed.

### CDC / Debezium

A production system could stream committed database changes using change data capture instead of polling an application-managed outbox.

This reduces application polling logic but introduces additional infrastructure and operational complexity. A polling relay was chosen for this implementation because it keeps the reliability mechanism explicit and within project scope.

## Consequences

### Benefits

- Payment state and publication intent are committed atomically.
- Kafka outages do not destroy the event publication obligation.
- Failed publications remain retryable.
- Relay work can be scaled using row locking and `SKIP LOCKED`.
- The architecture makes failure recovery explicit and testable.

### Tradeoffs

- Publication is asynchronous, adding polling latency.
- Duplicate Kafka records are possible.
- Consumer idempotency becomes a correctness requirement.
- The current relay publishes one event at a time and waits synchronously for Kafka delivery, prioritizing simplicity over throughput.
- A permanently failing oldest event could repeatedly retry and delay later rows.
- Outbox rows require retention or cleanup in a long-running production system.
- `ORDER BY id` provides deterministic lower-ID preference but does not guarantee strict global commit or publication ordering under concurrency.

## Observability

The relay can measure the age of the oldest unpublished outbox row.

A growing value indicates that publication is falling behind, Kafka may be unavailable, or a row may be repeatedly failing.

## Future Improvements

Potential production improvements include:

- batched or asynchronous Kafka publication
- finite delivery timeouts
- exponential retry backoff
- retry counts and `next_attempt_at`
- poison-event isolation
- outbox retention and cleanup
- multiple relay workers
- CDC-based publication with Debezium
- exporting outbox lag as a Prometheus metric