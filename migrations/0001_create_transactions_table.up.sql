CREATE TABLE transactions (
    id           BIGSERIAL PRIMARY KEY,
    event_id     TEXT NOT NULL UNIQUE,
    account_id   TEXT NOT NULL,
    type         TEXT NOT NULL CHECK (type IN ('debit', 'credit')),
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    currency     TEXT NOT NULL,
    status       TEXT NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_transactions_account_id ON transactions (account_id);
