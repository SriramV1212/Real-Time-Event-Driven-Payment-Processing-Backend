# Postmortem: API Idempotency Race Under Concurrent Requests

## Summary

The first API idempotency implementation correctly handled sequential retries but was unsafe under concurrent requests.

The implementation checked whether an idempotency key existed before creating a payment and inserting the idempotency record.

Because the key was not protected by a database uniqueness constraint, multiple requests could observe the same key as absent and independently create payments.

A deterministic concurrency test reproduced the race.

The issue was fixed by enforcing idempotency-key uniqueness in PostgreSQL and recovering from concurrent `UniqueViolation` errors.

## Original Flow

The initial flow was:

```text
receive request
    ↓
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

For sequential retries this behaved correctly.

A later request would find the existing idempotency record and either:

- replay the stored response when the request hash matched
- return `409 Conflict` when the same key was used with a different request payload

## Failure Scenario

Under concurrency, two requests could execute:

```text
Request A                    Request B

SELECT K1 → absent           SELECT K1 → absent

create payment P1            create payment P2

create outbox E1             create outbox E2

insert K1                    insert K1

commit                       commit
```

Each request made its decision before the other transaction had committed an idempotency record.

Because the database allowed duplicate `idempotency_key` values, both transactions could succeed.

## How It Was Reproduced

A real Uvicorn process was started for the integration test.

The test created 10 client threads using the same:

- idempotency key
- user
- amount
- request payload

A thread barrier was used so the requests were released together.

A test-only delay was added immediately after the initial idempotency lookup.

The delay widened the existing race window:

```text
SELECT key → absent
    ↓
test-only delay
    ↓
create payment and insert key
```

The delay did not create the race. It made an existing timing-dependent failure deterministic and reproducible.

## Impact

Before the fix, one test run with 10 concurrent requests produced:

- 10 HTTP `200` responses
- 10 idempotency records for the same key
- 10 different payment records
- 10 outbox records

The same logical request was therefore accepted as 10 independent payments.

This behavior was reproduced intentionally during development before the concurrency fix was added.

## Root Cause

The system attempted to enforce a concurrency invariant using application-level check-then-act logic.

The invariant was:

```text
one idempotency key
    →
one logical payment
```

But the database schema did not enforce uniqueness.

Under concurrent database transactions, one request does not treat another request's uncommitted work as an already committed idempotency record.

The separate lookup and insert operations therefore left a race window.

Sequential tests did not expose this behavior because each request completed before the next request checked the key.

## Resolution

A `UNIQUE` constraint was added to `idempotency_key`.

The database now acts as the final authority for ownership of a key.

The corrected concurrent flow is:

```text
Request A                    Request B

SELECT K1 → absent           SELECT K1 → absent

create P1                    create P2
create E1                    create E2

INSERT K1 succeeds           INSERT K1
COMMIT                           ↓
                             UniqueViolation
                                 ↓
                             ROLLBACK
                                 ↓
                             P2 removed
                             E2 removed
                                 ↓
                             read persisted K1
                                 ↓
                             replay stored response
```

The losing transaction must call `rollback()` before querying again because PostgreSQL marks the transaction as failed after the constraint violation.

The rollback also removes the losing request's payment and outbox writes because they were part of the same transaction.

After rollback:

- matching request hash → replay stored response
- different request hash → `409 Conflict`

## Verification

The same concurrency test was reused after the fix.

The final behavior for 10 concurrent requests with the same key and payload was:

- all requests returned HTTP `200`
- all returned the same serialized response body
- exactly one idempotency record existed
- exactly one payment existed
- exactly one outbox record existed

The test-only race delay remains enabled in the concurrency test so the previously unsafe timing window is still deliberately exercised.

## Retention Issue Identified

Adding uniqueness exposed another design question.

The initial lookup ignored records after `expires_at`.

However, an expired record still physically exists in PostgreSQL and therefore still participates in the unique constraint.

This could produce contradictory behavior:

```text
API:
expired key is treated as absent

database:
key still physically exists and must remain unique
```

The API semantics were changed so physical row existence determines whether a key is reserved.

`expires_at` is now used only by the cleanup process.

## Cleanup Design

A background Python worker removes expired idempotency rows asynchronously.

The worker deletes rows in batches and uses:

```sql
FOR UPDATE SKIP LOCKED
```

to allow multiple workers to safely claim different cleanup rows if horizontal scaling is needed later.

An index on `expires_at` supports efficient selection of expired records.

The 24-hour retention period is therefore a minimum retention guarantee rather than an exact key-reuse timestamp.

## Remaining Limitation

Once an idempotency record is physically deleted, the server no longer remembers that key's previous request.

A very late retry using the deleted key could therefore be interpreted as a new request and create another logical payment.

The probability of an unrelated new operation randomly generating the same high-entropy key is extremely small when clients use UUID-style keys.

The more relevant risk is a late retry or a client that incorrectly reuses old keys.

The retention period should therefore be chosen based on the longest realistic retry window.

## Additional Debugging Finding

During regression testing, an earlier failed relay crash test left background `outbox_relay.py` processes running.

A stale relay process later consumed the crash test's outbox row before the intended relay process could reach it, causing the test to time out.

The issue was diagnosed by:

1. observing the subprocess timeout
2. checking that no unpublished outbox rows remained
3. checking active OS processes
4. discovering multiple relay processes
5. terminating the stale processes
6. rerunning the test successfully

The relay integration test was updated so subprocesses started by the test are always terminated in cleanup.

This reinforced the importance of explicit ownership and cleanup of background processes in integration tests.

## Lessons Learned

1. Sequential correctness does not imply concurrent correctness.
2. Check-then-act logic is unsafe for invariants that must hold across concurrent transactions.
3. Database constraints should enforce database-level invariants.
4. Application checks are useful fast paths but should not replace authoritative constraints.
5. Deterministic race injection is valuable for testing timing-dependent failures.
6. A transaction that encounters a PostgreSQL constraint violation must be rolled back before the connection can be reused.
7. Transaction rollback protects against partial side effects from the losing request.
8. Retention policy affects correctness semantics, not only storage usage.
9. Indexes should match important access patterns such as expiration cleanup.
10. Integration tests that start subprocesses must guarantee subprocess cleanup even when the test fails.