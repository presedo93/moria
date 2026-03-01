mod engine;
mod sma;

use anyhow::Result;
use moria_common::Config;
use rust_decimal::Decimal;
use std::str::FromStr;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
    moria_common::telemetry::init_tracing("strategy")?;

    info!(
        pair = %config.trading_pair,
        short = config.sma_short_period,
        long = config.sma_long_period,
        "Starting strategy service"
    );

    let mut engine = engine::StrategyEngine::new(
        config.trading_pair,
        config.kline_interval,
        Decimal::from_str("0.001").expect("valid decimal literal"),
        config.sma_short_period,
        config.sma_long_period,
        config.market_data_grpc_addr,
        config.risk_grpc_addr,
    );

    engine.run().await
}
