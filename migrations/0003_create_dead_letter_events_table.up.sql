CREATE TABLE dead_letter_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT NULL,
    topic           TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    payload         BYTEA NOT NULL,
    failure_reason  TEXT NOT NULL,
    attempt_count   INT NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic, kafka_partition, kafka_offset)
);

CREATE INDEX idx_dead_letter_events_event_id ON dead_letter_events (event_id);
