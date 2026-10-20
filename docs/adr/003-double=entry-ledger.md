# ADR 003: Double-Entry Ledger for Merchant Payment Processing

- **Status:** Accepted for Phase 5 (subject to final repository review)
- **Scope:** Phase 5, merchant ledger core and temporary simulated-capture consumer bridge

## Context and problem

The original Kafka payment consumer updated `users.balance` directly. That wallet-style mutation did not represent the chosen business: a **single merchant accepting card payments**, with Stripe as the external payment processor. A mutable balance alone does not explain which financial events produced it, nor enforce balanced accounting.

The project needs an auditable record of payment captures, protection against duplicate Kafka delivery and duplicate business operations, and database transactions that cannot leave event tracking, payment status, and accounting out of sync.

## Options considered

1. **Keep updating `users.balance`:** smallest change, but no double-entry audit trail or accounting invariant. Rejected.
2. **Compute balances exclusively by summing journal entries:** simple source of truth but increasingly costly to read on every balance lookup. Not chosen for the initial implementation.
3. **Append-only journal plus cached account balances:** retain a full record of entries and update cached balances in the same database transaction, with an independent recomputation check. **Chosen.**

For the domain model, a custodial wallet and a multi-merchant/Stripe Connect platform were considered but deferred in favor of one merchant and Stripe test-mode charging.

## Decision

### Data model

- `ledger_accounts`: account name, type, currency, and cached `balance_minor`; unique `(name, currency)`.
- `ledger_transactions`: one logical accounting operation, optionally associated with a `payment_id`; unique `(payment_id, operation)` and optional `reverses_transaction_id`.
- `ledger_entries`: positive integer minor-unit amounts, debit/credit direction, account, currency, and parent ledger transaction.
- Initial USD account types: `held_at_stripe` (asset), `stripe_fees` (expense), and `revenue` (revenue). `bank_cash`/payout handling is deferred.

### Posting contract

All normal ledger postings go through PostgreSQL `post_ledger_transaction(...)`. It validates the entry JSON structure, known accounts/currency, strictly positive integer amounts, and debit/credit equality per currency; creates the transaction and entries; locks affected accounts in a consistent order; and updates cached balances within the caller's PostgreSQL transaction.

For asset and expense accounts, debits increase the cached balance; for revenue accounts, credits increase it. The original journal is retained. A reversal creates compensating entries with flipped directions, references its original transaction, and is limited by a unique index to one reversal per original transaction.

Database constraints complement the function, particularly `UNIQUE(payment_id, operation)` for business-operation idempotency beyond the consumer's `processed_events(event_id)` safeguard.

### Temporary consumer bridge

Until Stripe is integrated in Phase 6, the Kafka consumer simulates a successful **gross capture**: for 5,000 cents it debits `held_at_stripe` 5,000 and credits `revenue` 5,000. **It does not claim money was charged or held by Stripe.** It does not invent a processing fee. Once Stripe is integrated, accounting entries must follow confirmed Stripe outcomes and actual fee records.

In one PostgreSQL transaction, the consumer records the processed event, validates and locks the pending payment, verifies event user/amount against the stored payment, posts the ledger transaction, updates payment status, and updates metrics. Kafka offset commit occurs after database commit. An unsuccessful DB operation rolls back database work rather than marking the payment as a business failure.

### Permission boundary

The runtime consumer connects as restricted `payment_app`, not `admin`. It can read ledger records and execute `post_ledger_transaction`, but cannot directly insert, update, or delete ledger records. The function uses `SECURITY DEFINER` and a controlled `search_path` to perform validated writes with its owner's authority. Execution is revoked from `PUBLIC` and granted explicitly to `payment_app`.

The function currently belongs to the powerful development `admin` role. **A dedicated least-privileged function owner and additional privilege hardening are future security improvements.** The present design must not be described as tamper-proof against database administrators or table owners.

## Evidence and verification

Phase 5 tests exercised balanced postings, reversal and original-history preservation, duplicate capture rejection, concurrent duplicate capture, stored-versus-derived account balances, successful consumer posting, duplicate-event handling, rollback after a deliberate duplicate-capture SQL error, invalid amount handling, and a real consumer DB connection using `payment_app` in `payments_test`.

A previous assertion expected an account's final balance to be exactly 4,800 cents. It failed because a concurrency test committed earlier postings in the shared test database. The assertion was corrected to verify **balance after = balance before + posting delta**; the disposable test database is also reset before pytest sessions. Full pytest regression and Ruff checks subsequently passed, as reported during Phase 5.

These tests do **not** demonstrate real Stripe settlement, Kafka redelivery across a live broker for every case, high-availability operation, or a complete immutable-ledger threat model.

## Tradeoffs and consequences

- Cached balances improve reads but create a second representation that must remain consistent with journal entries. Phase 5 includes a recomputation test; the **continuous periodic invariant checker** is planned for a later phase.
- Locking account rows serializes competing updates to affected accounts and may become a throughput bottleneck; no scaling claim is made.
- Validations occur in both the consumer (event shape and agreement with the payment row) and PostgreSQL (accounting correctness). Neither layer replaces the other.
- Ledger entries are append-only **for the restricted application role**; corrections use reversal entries instead of mutating posted history. Administrators still have broader powers.
- The consumer's current general failure path sends events to the DLQ and commits Kafka offsets, so infrastructure failures do **not** automatically retry. Retries, backoff, classification, and replay belong to the later reliability phase.
- Account names, simulated-capture state and payment statuses are transitional until Stripe lifecycle integration; this ADR does not claim Stripe-connected behavior already exists.
- Test setup resets only the disposable `payments_test` schema once per pytest session. Concurrent test sessions targeting the same database remain unsafe.

## Follow-up work

- Phase 6: Stripe test-mode charging, webhook processing, lifecycle state transitions, and actual Stripe-based ledger amounts/fees; revisit Kafka keying by `payment_id`.
- Later reliability/observability: bounded retries, DLQ replay, property-based tests, continuous ledger invariant checks and metrics.
- Security hardening: dedicated function-owner role, review schema/function execution grants and function input permissions, and automate reliable database initialization/migration in all environments.
- Final project README: setup instructions, tested behavior, limitations and run commands.