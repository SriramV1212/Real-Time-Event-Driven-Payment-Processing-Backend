# Repository Audit

## 1. API Request Flow

The FastAPI application exposes `POST /payments` for creating a payment.

The request body is validated using `CreatePaymentRequest`. After validation, the endpoint performs an additional check requiring `user_id` to start with `user_`.

The payment creation flow is:

```text
Client
  ↓
POST /payments
  ↓
Validate request
  ↓
Validate user_id prefix
  ↓
Generate payment_id and event_id
  ↓
Open PostgreSQL connection
  ↓
INSERT payment with status = "pending"
  ↓
COMMIT PostgreSQL transaction
  ↓
Create payment_created event
  ↓
Publish event to Kafka
  ↓
Return payment_id and "pending" status
```

The payment row is committed to PostgreSQL before the Kafka event is published.

The generated event contains:

- `event_id`
- `payment_id`
- `user_id`
- `amount`
- `event_type`
- `timestamp`

If an exception occurs, the endpoint calls `rollback()`, logs the error, and returns HTTP 500.

The database cursor and connection are closed in the `finally` block.

The API also exposes `GET /payments/{payment_id}`. This endpoint reads the payment directly from PostgreSQL and returns its current status. It returns HTTP 404 if the payment does not exist.

## 2. Kafka Producer Flow

After the payment row is committed, the API calls `produce_event(event)`.

The producer serializes the event to JSON and publishes it to the `payment-events` Kafka topic.

The Kafka message key is the payment's `user_id`.

```text
payment_created event
        ↓
serialize to JSON
        ↓
key = user_id
        ↓
publish to payment-events
        ↓
Kafka selects partition based on key
```

Using `user_id` as the message key means events for the same user are routed consistently to the same partition. This helps preserve event ordering for a given user while still allowing different users to be processed in parallel across partitions.

The producer uses a delivery callback to log whether Kafka successfully delivered the message.

After producing the event, the producer calls `flush()` so queued messages are given a chance to be delivered before the function returns.

## 3. Kafka Consumer Group and Partition Flow

The payment consumer subscribes to the `payment-events` topic using the consumer group:

`payment-processors`

The topic has four partitions.

```text
payment-events

P0   P1   P2   P3
 \    |    |   /
  \   |    |  /
   consumer group
  payment-processors
```

Kafka assigns partitions among the active consumers in the group.

For example, with two consumers:

```text
Consumer A → P0, P1
Consumer B → P2, P3
```

With four consumers:

```text
Consumer A → P0
Consumer B → P1
Consumer C → P2
Consumer D → P3
```

Because a partition is assigned to only one consumer in the group at a time, consumers can process different partitions in parallel while preserving ordering within each partition.

The consumer disables automatic offset commits, so offsets are committed manually by the application after processing decisions are made.

## 4. Consumer Processing Flow

After a valid Kafka message is received, the consumer calls `process_event()`.

```text
Kafka message
    ↓
decode JSON
    ↓
validate required fields
    ↓
process_event()
    ↓
open PostgreSQL transaction
    ↓
insert event_id into processed_events
    ↓
duplicate?
 ┌───────────────┐
 │ yes           │ no
 ↓               ↓
rollback         update user balance
return           mark payment processed
                increment metrics
                     ↓
                  COMMIT
                     ↓
             commit Kafka offset
```

The consumer first inserts the event's `event_id` into `processed_events`.

If that `event_id` already exists, PostgreSQL does not insert another row. The consumer treats the message as a duplicate, rolls back the transaction, and returns without applying the payment changes again.

For a new event, the consumer updates the user's balance, changes the payment status from `pending` to `processed`, increments the processing metric, and commits those database changes.

After `process_event()` returns successfully, the consumer manually commits the Kafka offset.

## 5. Consumer-Side Idempotency

The consumer uses the `processed_events` table to prevent the same Kafka event from applying its business changes more than once.

Before processing an event, the consumer attempts to insert its `event_id` into `processed_events`.

```text
receive event
    ↓
insert event_id into processed_events
    ↓
already exists?
 ┌──────────────┐
 │ yes          │ no
 ↓              ↓
skip event      process payment
```

The `event_id` is protected by a database uniqueness constraint, so PostgreSQL determines whether the event has already been seen.

If the event was already processed, the consumer returns without updating the user's balance or payment status again.

This makes repeated delivery of the same Kafka event safe for the current processing path.

## 6. Kafka Offset Handling

Automatic offset commits are disabled for the payment consumer.

The consumer manually commits the Kafka offset only after `process_event()` returns successfully.

```text
receive Kafka message
        ↓
process event
        ↓
commit PostgreSQL transaction
        ↓
commit Kafka offset
```

This ordering ensures the message is not acknowledged to Kafka before the database work completes.

If the consumer crashes after the database transaction commits but before the Kafka offset is committed, Kafka may deliver the message again when processing resumes.

The `processed_events` idempotency check protects against applying the same event's business changes twice in that case.

## 7. Consumer Failure and DLQ Flow

If `process_event()` raises an exception, the consumer does not continue normal processing for that message.

Instead, it creates a dead-letter message containing the original event and failure context.

```text
Kafka message
    ↓
process_event()
    ↓
exception
    ↓
build DLQ payload
    ↓
publish to payment-events-dlq
    ↓
commit original Kafka offset
```

The DLQ payload currently includes:

- the original event
- the error message
- the failure timestamp
- the original partition
- the original offset

After attempting to publish the failure to the DLQ, the consumer commits the original message's Kafka offset so that the failed message does not keep being delivered repeatedly on the main processing path.

A separate DLQ consumer reads from the DLQ topic and logs the failed messages for inspection.

## 8. Current Database Tables

The current PostgreSQL schema contains three tables used by the payment-processing logic and one auxiliary benchmarking table.

### `payments`

Stores each payment created through the API.

Important fields include:

- `payment_id`
- `user_id`
- `amount`
- `status`
- `created_at`

A newly created payment starts with status `pending`. The consumer later changes it to `processed` or `failed`.

### `users`

Stores a user's current balance.

Important fields include:

- `user_id`
- `balance`

The consumer currently changes money state by directly updating this balance.

### `processed_events`

Stores Kafka `event_id` values that have already been processed.

This table is used for consumer-side idempotency so a redelivered Kafka event does not apply its business changes twice.

### `metrics` (benchmarking only)

This table stores a `total_processed` counter that was added to help measure how long it took the consumer setup to process a fixed number of events during load testing.

It is not part of the core payment-processing domain model.

## 9. Consumer Database Transaction Boundary

The consumer processes a new event inside a PostgreSQL transaction.

The transaction includes:

```text
insert event_id into processed_events
        ↓
create user if needed
        ↓
update user balance
        ↓
update payment status
        ↓
increment benchmark counter
        ↓
COMMIT
```

These database changes are committed together.

If an error occurs before the commit, the consumer calls `rollback()`, so the uncommitted changes from that transaction are discarded.

This prevents partial database state such as marking an event as processed while failing to update the corresponding payment or balance.

## 10. Reliability Gap: Database and Kafka Dual Write

The payment creation path performs two separate writes:

```text
write payment to PostgreSQL
        ↓
commit database transaction
        ↓
publish payment_created event to Kafka
```

These operations are not part of one atomic transaction.

A failure can therefore occur after PostgreSQL commits but before Kafka receives the event.

```text
PostgreSQL commit succeeds
        ↓
application crashes or Kafka publish fails
        ↓
payment remains in PostgreSQL
        ↓
no corresponding Kafka event exists
```

In this situation, the payment may remain in `pending` state because the consumer never receives an event to process it.

The exception handler calls `rollback()`, but a rollback cannot undo a transaction that has already been committed.

This is the current database-to-Kafka dual-write reliability gap.

## 11. Reliability Gap: Duplicate API Requests

The API currently has no request-level idempotency mechanism.

Each call to `POST /payments` generates a new `payment_id` and a new `event_id`.

If a client retries the same logical payment request, the API creates another payment rather than recognizing it as a retry.

```text
first request
    ↓
payment A + event E1

same request retried
    ↓
payment B + event E2
```

Consumer-side idempotency does not prevent this because the two requests produce different event IDs.

The system therefore currently protects against duplicate Kafka event processing, but not against duplicate client requests.

## 12. Reliability Gap: No Retry Before DLQ

The consumer currently sends a message to the DLQ when `process_event()` raises an exception.

There is no retry step before this happens.

```text
process event
    ↓
failure
    ↓
DLQ
```

This means a valid event can be dead-lettered because of a temporary infrastructure failure, such as a short PostgreSQL outage.

The current implementation does not distinguish between transient failures that may succeed on retry and permanent failures that should not be retried.

## 13. Reliability Gap: Non-Atomic DLQ Handoff

The current failure path performs DLQ publication and source-offset commit as separate operations.

```text
processing fails
    ↓
publish event to DLQ
    ↓
commit original Kafka offset
```

These two steps are not atomic.

A failure between them can produce inconsistent outcomes.

If the DLQ publish succeeds but the source offset is not committed, Kafka may redeliver the original event and the failure may be dead-lettered again.

If the DLQ publish fails but the source offset is still committed, the event may no longer be available on the main topic and may also be missing from the DLQ.

The current implementation therefore does not guarantee atomic handoff from the main topic to the DLQ.

## 14. Reliability Gap: Limited `processed_events` Metadata

The `processed_events` table currently stores only the `event_id`.

This is sufficient for basic duplicate detection because the consumer can determine whether an event has already been seen.

However, the table does not record additional processing context such as:

- processing status
- payment ID
- processing timestamp
- failure information
- replay count

As a result, the table can answer whether an event ID already exists, but it cannot describe what happened during previous processing attempts.

The table will also continue growing unless a retention strategy is introduced later.

## 15. Code Quality / Performance Concern: Connection Per Request

The API currently opens a new PostgreSQL connection for each request and closes it after the request completes.

```text
request
    ↓
open PostgreSQL connection
    ↓
execute database work
    ↓
close connection
```

This is functionally correct, but repeatedly creating database connections adds overhead and can become a performance bottleneck under higher concurrency.

A connection pool could later allow requests to reuse a bounded set of existing database connections instead of creating a new connection for every request.

This is a performance and resource-management concern rather than a correctness bug.

## 16. Validation Concern: Weak `user_id` Format Check

The API currently validates `user_id` using a simple prefix check:

```python
user_id.startswith("user_")
```

This verifies only that the value begins with `user_`.

It does not enforce a stricter identifier format beyond the existing length validation in the request model.

The current validation is therefore minimal and may accept identifiers that are syntactically unexpected but still begin with the required prefix.

## 17. Financial Correctness Gap: Mutable Balance Without Funds Validation

The current consumer directly mutates a user's stored balance.

If a user does not already exist, the consumer creates the user with a balance of `0` and then subtracts the payment amount.

```text
new user
balance = 0
    ↓
payment amount = 100
    ↓
balance = -100
```

There is currently no insufficient-funds check and no database constraint preventing a negative balance.

The schema also stores `amount` and `balance` without an associated currency.

This means the current balance model is not sufficient for a financially correct payment system.

## 18. Event Format Concern

The current event payload uses `time.time()` for timestamps.

This produces a Unix timestamp rather than a human-readable UTC timestamp.

The event also has no explicit schema or version field.

As the event format evolves, the lack of versioning could make it harder for consumers to distinguish between older and newer payload structures.

## 19. Code Quality Concerns Relevant to Future Work

A few parts of the current structure will make testing and future reliability work harder.

### Global Kafka producer

The Kafka producer is created at module level and reused globally.

This works, but it makes isolated tests harder because application code is directly tied to a real Kafka client.

### Duplicated database connection setup

Database connection logic is defined in more than one place, including separate connection handling in the API and consumer.

This creates duplicated configuration and makes integration testing harder to manage consistently.

### Consumer loop has multiple responsibilities

The main consumer loop currently handles several concerns together:

- polling Kafka
- decoding JSON
- validating messages
- calling business processing
- building DLQ messages
- publishing to the DLQ
- committing offsets
- logging

This makes individual behaviors harder to test in isolation.

These are not reasons for a large refactor now. Small boundaries should only be extracted later when tests or new reliability work require them.

## 20. Current Architecture Summary

```text
Client
  ↓
FastAPI
  ↓
POST /payments
  ↓
PostgreSQL
insert payment as pending
  ↓
COMMIT
  ↓
Kafka producer
  ↓
payment-events
4 partitions, keyed by user_id
  ↓
consumer group: payment-processors
  ↓
payment consumer
  ↓
PostgreSQL transaction
  ├─ processed_events
  ├─ users balance update
  ├─ payment status update
  └─ benchmark counter
  ↓
COMMIT
  ↓
commit Kafka offset

On processing failure:

payment consumer
  ↓
payment-events-dlq
  ↓
DLQ consumer
  ↓
log failed message
```

## 21. Reliability Summary

The current system already provides several useful reliability properties:

- consumer-side idempotency using `processed_events`
- manual Kafka offset commits
- database rollback on consumer-processing failure
- duplicate Kafka event protection
- parallel processing through Kafka partitions and a consumer group
- DLQ isolation for processing failures

The main gaps identified in the current implementation are:

- PostgreSQL and Kafka are written separately, creating a dual-write failure window
- duplicate client requests can create multiple payments
- transient failures are sent directly to the DLQ without retry classification
- DLQ publication and source-offset commit are not atomic
- `processed_events` stores limited processing metadata
- balances are mutated directly with no insufficient-funds protection
- amounts and balances have no currency
- database connections are created per request
- event payloads are unversioned
- some business and infrastructure concerns are tightly coupled, making isolated testing harder

These gaps define the main reliability and correctness work for the next phases.