-- processed_events stores every Salesforce event the service successfully
-- decodes and persists. event_uuid is the idempotency key.
CREATE TABLE processed_events (
    id            BIGSERIAL PRIMARY KEY,
    event_uuid    TEXT NOT NULL UNIQUE,
    topic         TEXT NOT NULL,
    schema_id     TEXT NOT NULL,
    replay_id     BYTEA NOT NULL,
    payload       JSONB NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at  TIMESTAMPTZ
);

-- Operational debugging: list recent events per topic.
CREATE INDEX idx_processed_events_topic_received_at
    ON processed_events (topic, received_at DESC);

-- replay_state stores the last successfully processed replay_id per topic so
-- the subscriber can resume after a restart instead of starting at LATEST.
CREATE TABLE replay_state (
    topic       TEXT PRIMARY KEY,
    replay_id   BYTEA NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
