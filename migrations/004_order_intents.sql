CREATE TABLE IF NOT EXISTS order_intents (
    id UUID PRIMARY KEY,
    signal_id UUID NOT NULL UNIQUE REFERENCES signals(id),
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    qty NUMERIC(20, 8) NOT NULL,
    status TEXT NOT NULL DEFAULT 'Pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_order_intents_status_next_attempt
    ON order_intents (status, next_attempt_at, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_signal_id_unique
    ON trades (signal_id);
