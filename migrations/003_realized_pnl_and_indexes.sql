-- Add realized_pnl column to trades for proper PnL tracking
ALTER TABLE trades ADD COLUMN IF NOT EXISTS realized_pnl NUMERIC(20, 8) NOT NULL DEFAULT 0;

-- Indexes for query-critical columns
CREATE INDEX IF NOT EXISTS idx_trades_symbol_date_status
    ON trades (symbol, created_at, status);

CREATE INDEX IF NOT EXISTS idx_trades_signal_id
    ON trades (signal_id);

CREATE INDEX IF NOT EXISTS idx_signals_created_at
    ON signals (created_at);
