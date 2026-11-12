CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    balance NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (balance >= 0),
    is_hot BOOLEAN NOT NULL DEFAULT FALSE,
    active_epoch BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS account_credit_slots (
    -- Avoid FK-generated row locks on hot merchant accounts during incoming credits.
    account_id TEXT NOT NULL,
    epoch BIGINT NOT NULL,
    slot_id SMALLINT NOT NULL CHECK (slot_id BETWEEN 0 AND 63),
    amount NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (amount >= 0),
    PRIMARY KEY (account_id, epoch, slot_id)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    key_hash TEXT NOT NULL,
    payment_id TEXT,
    state TEXT NOT NULL CHECK (state IN ('processing', 'completed', 'cancelled')),
    request_hash TEXT,
    response_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, key_hash)
);

CREATE TABLE IF NOT EXISTS transactions (
    payment_id TEXT PRIMARY KEY,
    sender_id TEXT NOT NULL REFERENCES accounts(account_id),
    receiver_id TEXT NOT NULL,
    amount NUMERIC(18,4) NOT NULL CHECK (amount > 0),
    status TEXT NOT NULL CHECK (status IN ('processing', 'completed', 'failed')),
    idempotency_key_hash TEXT NOT NULL,
    request_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS transactions_sender_created_idx
    ON transactions(sender_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ledger_entries (
    entry_id BIGSERIAL PRIMARY KEY,
    payment_id TEXT NOT NULL REFERENCES transactions(payment_id),
    -- Account existence is checked by the transfer CTE to keep hot credits lock-free.
    account_id TEXT NOT NULL,
    entry_type TEXT NOT NULL CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    amount NUMERIC(18,4) NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (payment_id, entry_type)
);

CREATE TABLE IF NOT EXISTS transaction_events (
    event_id BIGSERIAL PRIMARY KEY,
    payment_id TEXT NOT NULL REFERENCES transactions(payment_id),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
    event_id BIGINT PRIMARY KEY REFERENCES transaction_events(event_id),
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    claim_token TEXT,
    claimed_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox(created_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS worker_leases (
    lease_name TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_merkle_chain (
    partition_id SMALLINT NOT NULL CHECK (partition_id BETWEEN 0 AND 31),
    sequence BIGINT NOT NULL,
    event_id BIGINT NOT NULL UNIQUE,
    previous_hash BYTEA NOT NULL,
    event_hash BYTEA NOT NULL,
    mac BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (partition_id, sequence)
);

INSERT INTO accounts(account_id, balance, is_hot) VALUES
    ('system_treasury', 0.0000, FALSE),
    ('acc_asha', 100000.0000, FALSE),
    ('acc_ravi', 100000.0000, FALSE),
    ('acc_merchant_diwali', 0.0000, TRUE)
ON CONFLICT (account_id) DO NOTHING;

INSERT INTO transactions(payment_id, sender_id, receiver_id, amount, status,
                         idempotency_key_hash, request_id, completed_at)
VALUES
    ('seed_opening_asha', 'system_treasury', 'acc_asha', 100000.0000, 'completed', 'seed', 'seed', now()),
    ('seed_opening_ravi', 'system_treasury', 'acc_ravi', 100000.0000, 'completed', 'seed', 'seed', now())
ON CONFLICT (payment_id) DO NOTHING;

INSERT INTO ledger_entries(payment_id, account_id, entry_type, amount)
VALUES
    ('seed_opening_asha', 'system_treasury', 'DEBIT', 100000.0000),
    ('seed_opening_asha', 'acc_asha', 'CREDIT', 100000.0000),
    ('seed_opening_ravi', 'system_treasury', 'DEBIT', 100000.0000),
    ('seed_opening_ravi', 'acc_ravi', 'CREDIT', 100000.0000)
ON CONFLICT (payment_id, entry_type) DO NOTHING;
