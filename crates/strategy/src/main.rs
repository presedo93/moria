mod engine;
mod sma;
mod strategy;

use anyhow::{Result, bail};
use moria_common::Config;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
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
    );

    engine.run().await
}
