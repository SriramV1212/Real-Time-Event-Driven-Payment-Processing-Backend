CREATE TABLE IF NOT EXISTS ledger_accounts (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('asset', 'expense', 'revenue')),
    currency CHAR(3) NOT NULL,
    balance_minor BIGINT NOT NULL DEFAULT 0,

    UNIQUE(name, currency)
);

INSERT INTO ledger_accounts (name, type, currency)
VALUES
    ('held_at_stripe', 'asset', 'USD'),
    ('stripe_fees', 'expense', 'USD'),
    ('revenue', 'revenue', 'USD')
ON CONFLICT (name, currency) DO NOTHING;

CREATE TABLE IF NOT EXISTS ledger_transactions (
    id BIGSERIAL PRIMARY KEY,
    payment_id TEXT REFERENCES payments(payment_id),
    operation TEXT NOT NULL,
    reverses_transaction_id BIGINT REFERENCES ledger_transactions(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(payment_id, operation),

    CHECK (
        operation <> 'payment_capture'
        OR payment_id IS NOT NULL
    )
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT NOT NULL REFERENCES ledger_transactions(id),
    account_id BIGINT NOT NULL REFERENCES ledger_accounts(id),
    direction TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency CHAR(3) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS
    idx_ledger_unique_reversal
ON ledger_transactions (reverses_transaction_id)
WHERE reverses_transaction_id IS NOT NULL;