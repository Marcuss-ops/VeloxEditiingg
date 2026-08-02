-- Signed InstaEdit -> Velox delivery events.
CREATE TABLE IF NOT EXISTS instaedit_delivery_events (
    event_id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    UNIQUE (delivery_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_instaedit_delivery_events_delivery
    ON instaedit_delivery_events(delivery_id, sequence);
