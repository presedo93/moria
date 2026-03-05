CREATE INDEX IF NOT EXISTS idx_trades_status_created_at
    ON trades (status, created_at);
