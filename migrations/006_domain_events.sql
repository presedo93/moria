CREATE TABLE IF NOT EXISTS domain_events (
    id UUID PRIMARY KEY,
    producer TEXT NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_domain_events_created_at
    ON domain_events (created_at);

CREATE INDEX IF NOT EXISTS idx_domain_events_event_type
    ON domain_events (event_type, created_at);
