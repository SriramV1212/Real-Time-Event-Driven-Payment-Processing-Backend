-- Allow the application to access objects in public.
GRANT USAGE ON SCHEMA public TO payment_app;

-- Allow read-only access to ledger tables.
GRANT SELECT ON
    ledger_accounts,
    ledger_transactions,
    ledger_entries
TO payment_app;

-- Remove public execution privileges.
REVOKE ALL ON FUNCTION post_ledger_transaction(
    TEXT, TEXT, JSONB, BIGINT
) FROM PUBLIC;

-- Allow the application to post through the function.
GRANT EXECUTE ON FUNCTION post_ledger_transaction(
    TEXT, TEXT, JSONB, BIGINT
) TO payment_app;

-- Permissions required for Kafka payment processing.
GRANT SELECT, INSERT ON processed_events TO payment_app;

GRANT SELECT, UPDATE ON payments TO payment_app;

GRANT SELECT, UPDATE ON metrics TO payment_app;