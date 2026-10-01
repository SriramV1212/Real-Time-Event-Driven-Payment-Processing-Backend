# ADR 002: API Idempotency and Concurrency Safety

## Status

Accepted

## Context

The payment creation API must tolerate client retries without creating multiple logical payments.

A client may retry a request when the original response is lost because of a timeout, network failure, or process crash. Without API-level idempotency, each retry could create a new payment.

The API therefore accepts a client-provided `Idempotency-Key`.

For each request, the API canonicalizes the request payload and computes a SHA-256 request hash. The idempotency key identifies the logical request, while the request hash is used to detect reuse of the same key with different request data.

The initial implementation used an application-level check:

```text
look up idempotency key
    ↓
key does not exist
    ↓
create payment
    ↓
create outbox row
    ↓
insert idempotency record
    ↓
commit
```

This worked correctly for sequential retries.

However, the check and insert were separate operations. Multiple concurrent requests could all observe the same key as absent before any request inserted it.

A deterministic concurrency test reproduced this race by sending 10 simultaneous requests with the same idempotency key and request payload.

Before database uniqueness was enforced, the test created:

- 10 payment rows
- 10 outbox rows
- 10 idempotency records

## Decision

Use PostgreSQL as the final authority for idempotency-key uniqueness.

The `idempotency_key` column has a database `UNIQUE` constraint.

The application-level lookup remains as the normal fast path for retries, but correctness does not depend on the lookup and insert happening atomically in application code.

When concurrent requests race:

```text
Request A                    Request B

lookup K1 → absent           lookup K1 → absent

create P1                    create P2
create outbox E1             create outbox E2

insert K1 succeeds           insert K1
commit                           ↓
                             UniqueViolation
                                 ↓
                             rollback
```

The losing transaction rolls back its payment and outbox writes.

After rollback, it reads the persisted idempotency record.

If the stored request hash matches the current request hash, the original response is replayed.

If the hashes differ, the API returns `409 Conflict`.

### Request fingerprinting

The request fingerprint is produced through:

```text
Pydantic request object
    ↓
Python dictionary
    ↓
canonical JSON string
    ↓
UTF-8 bytes
    ↓
SHA-256
    ↓
hexadecimal hash
```

Canonical JSON uses sorted keys and compact separators so equivalent request payloads produce the same serialized representation before hashing.

### Response replay

The original serialized response body is stored as `TEXT`.

The response is treated as an opaque replay value rather than structured data that needs JSON field queries.

This avoids relying on PostgreSQL `JSONB` representation when the requirement is to replay the serialized response body.

### Retention

Idempotency records have a 24-hour retention period.

The API treats a key as reserved for as long as its row physically exists.

The request path therefore uses:

```text
row exists + same request hash
    → replay stored response

row exists + different request hash
    → 409 Conflict

row does not exist
    → key may be used for a new request
```

The API does not use `expires_at` to decide whether a key exists.

Instead, `expires_at` is used by a separate cleanup worker.

This means 24 hours is a minimum retention period. A key may remain reserved slightly longer until asynchronous cleanup removes its row.

### Cleanup

A background Python worker removes expired idempotency records.

Rows are deleted in batches rather than with one unbounded delete.

The worker selects expired rows using:

```sql
WHERE expires_at <= CURRENT_TIMESTAMP
ORDER BY expires_at
LIMIT ...
FOR UPDATE SKIP LOCKED
```

`FOR UPDATE SKIP LOCKED` allows multiple cleanup workers to divide work safely if cleanup needs to be scaled later.

An index on `expires_at` supports the cleanup query.

If a full batch is deleted, the worker immediately attempts another batch. Once fewer than a full batch are found, it waits before polling again.

## Guarantees

During the retention period:

- the same idempotency key cannot create multiple committed idempotency records
- concurrent retries with the same key and payload result in one committed payment
- losing concurrent transactions roll back their payment and outbox writes
- retries with the same key and payload replay the stored response
- reuse of the same key with a different payload returns `409 Conflict`

The payment, outbox row, and idempotency record for the successful request are committed in the same PostgreSQL transaction.

## Alternatives Considered

### Application-level check only

Simple, but unsafe under concurrency.

Two requests can both observe a key as absent before either transaction commits.

### Delete expired rows in the API request path

Expired rows could be removed lazily when a request encounters them.

This keeps reuse closer to the exact expiry time but adds cleanup work and additional concurrency logic to the payment request path.

### Reuse expired rows with UPDATE

An expired row could be updated with the new request information instead of being deleted.

This would require additional locking and state-transition logic and was not needed for the current design.

### Never delete idempotency records

This provides stronger protection against very late retries but causes unbounded table growth.

### Time partitioning

Partitioning could make large-scale retention cleanup cheaper by dropping old partitions.

It was not chosen for the current implementation because it adds schema complexity, particularly when combined with global idempotency-key uniqueness.

## Consequences

### Benefits

- Duplicate client retries are handled safely.
- PostgreSQL enforces the concurrency invariant.
- Sequential and concurrent retry behavior use the same API semantics.
- Losing transactions leave no payment or outbox side effects.
- Cleanup is removed from the payment request path.
- Cleanup work can scale to multiple workers with `SKIP LOCKED`.
- Expired-row lookup is supported by an index on `expires_at`.

### Tradeoffs

- Additional indexes increase storage use and write cost.
- Idempotency records must be retained and cleaned up.
- Keys may remain reserved slightly longer than 24 hours because cleanup is asynchronous.
- A retry arriving after its idempotency record has been physically deleted can be interpreted as a new request and may create another logical payment.
- The retention period therefore needs to be longer than the expected client retry window.

## Observability

Useful future metrics include:

- number of retained idempotency records
- number of expired records awaiting cleanup
- age of the oldest expired record
- cleanup batch size
- cleanup failures
- number of idempotency conflicts
- number of concurrent uniqueness conflicts recovered by replay

## Future Improvements

Potential production improvements include:

- configurable retention duration
- configurable cleanup batch size and polling interval
- cleanup-worker metrics
- alerting when expired-row backlog grows
- multiple cleanup workers
- bounded cleanup rate during database load
- longer retention for clients with longer retry policies
- archival or partitioning if idempotency volume becomes very large