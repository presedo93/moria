CREATE TABLE IF NOT EXISTS backtest_runs (
    id UUID PRIMARY KEY,
    strategy TEXT NOT NULL,
    symbol TEXT NOT NULL,
    params JSONB NOT NULL,
    total_return_pct NUMERIC(20, 8) NOT NULL,
    max_drawdown_pct NUMERIC(20, 8) NOT NULL,
    sharpe_ratio NUMERIC(20, 8) NOT NULL,
    trades_count INTEGER NOT NULL,
    win_rate NUMERIC(20, 8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_created_at
    ON backtest_runs (created_at DESC);
