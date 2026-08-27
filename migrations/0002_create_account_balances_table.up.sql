CREATE TABLE account_balances (
    account_id          TEXT PRIMARY KEY,
    balance_cents        BIGINT NOT NULL DEFAULT 0,
    applied_event_count  BIGINT NOT NULL DEFAULT 0,
    last_event_at        TIMESTAMPTZ
);
