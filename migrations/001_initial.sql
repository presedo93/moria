CREATE TABLE IF NOT EXISTS signals (
    id UUID PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    qty NUMERIC(20, 8) NOT NULL,
    strategy TEXT NOT NULL DEFAULT 'sma_crossover',
    approved BOOLEAN NOT NULL,
    reject_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trades (
    id UUID PRIMARY KEY,
    signal_id UUID NOT NULL REFERENCES signals(id),
    order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    qty NUMERIC(20, 8) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS positions (
    symbol TEXT PRIMARY KEY,
    qty NUMERIC(20, 8) NOT NULL DEFAULT 0,
    avg_entry_price NUMERIC(20, 8) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
