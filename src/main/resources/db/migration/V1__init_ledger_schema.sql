CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    owner_name VARCHAR(120) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    balance NUMERIC(19, 4) NOT NULL CHECK (balance >= 0),
    version BIGINT NOT NULL DEFAULT 0, -- Optimistic Locking column
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key VARCHAR(100) UNIQUE NOT NULL,
    source_account_id UUID NOT NULL REFERENCES accounts(id),
    target_account_id UUID NOT NULL REFERENCES accounts(id),
    amount NUMERIC(19, 4) NOT NULL CHECK (amount > 0),
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Double-entry immutable accounting table (Sum of entries per transaction MUST equal 0)
CREATE TABLE ledger_entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id UUID NOT NULL REFERENCES transactions(id),
    account_id UUID NOT NULL REFERENCES accounts(id),
    entry_type VARCHAR(10) NOT NULL CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    amount NUMERIC(19, 4) NOT NULL, -- Negative for DEBIT, Positive for CREDIT
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Transactional Outbox for Guaranteed At-Least-Once Kafka Delivery
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_outbox_status ON outbox_events(status) WHERE status = 'PENDING';
CREATE INDEX idx_ledger_account ON ledger_entries(account_id);
CREATE INDEX idx_tx_idempotency ON transactions(idempotency_key);

