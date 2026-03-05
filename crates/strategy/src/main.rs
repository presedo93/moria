mod engine;
mod sma;
mod strategy;

use anyhow::{Result, bail};
use moria_common::Config;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
    config.validate_for_service("strategy")?;
    moria_common::telemetry::init_tracing("strategy")?;
    moria_common::telemetry::init_metrics("strategy", config.metrics_addr.as_deref())?;

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
        config.internal_service_token,
    );

    // Run engine with graceful shutdown on SIGTERM/SIGINT
    tokio::select! {
        result = engine.run() => result,
        _ = shutdown_signal() => {
            moria_common::telemetry::shutdown_tracing();
            info!("Strategy service shut down gracefully");
            Ok(())
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    tokio::select! {
        _ = ctrl_c => info!("strategy: received SIGINT, shutting down"),
        _ = sigterm.recv() => info!("strategy: received SIGTERM, shutting down"),
    }
}
