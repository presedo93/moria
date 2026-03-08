mod engine;
mod sma;
mod strategy;

use anyhow::{Result, bail};
use moria_common::config::StrategyConfig;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = StrategyConfig::from_env()?;
    moria_common::telemetry::init_tracing("strategy")?;
    moria_common::telemetry::init_metrics("strategy", config.telemetry.metrics_addr.as_deref())?;

    let strategy = strategy::create_strategy(
        &config.strategy_type,
        config.sma_short_period,
        config.sma_long_period,
    );

    let strategy = match strategy {
        Some(s) => s,
        None => bail!("Unknown strategy type: {}", config.strategy_type),
    };

    info!(
        pair = %config.trading_pair,
        strategy = %config.strategy_type,
        qty = %config.order_qty,
        "Starting strategy service"
    );

    let mut engine = engine::StrategyEngine::new(
        config.trading_pair,
        config.kline_interval,
        config.order_qty,
        strategy,
        config.market_data_grpc_addr,
        config.risk_grpc_addr,
        config.bybit_rest_url,
        config.sma_long_period,
        config.signal_queue_capacity,
        config.signal_max_inflight,
        config.account_equity_usd,
        config.risk_budget_pct,
        config.max_notional_per_trade,
        config.volatility_window,
        config.min_volatility,
        config.auth.internal_service_token,
    );

    // Run engine with graceful shutdown on SIGTERM/SIGINT
    tokio::select! {
        result = engine.run() => result,
        _ = moria_common::signal::shutdown_signal("strategy") => {
            moria_common::telemetry::shutdown_tracing();
            info!("Strategy service shut down gracefully");
            Ok(())
        }
    }
}
