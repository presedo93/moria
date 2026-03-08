mod bybit_ws;
mod server;

use anyhow::Result;
use moria_common::config::MarketDataConfig;
use moria_proto::market_data::{Kline, OrderbookSnapshot, Trade};
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<()> {
    let config = MarketDataConfig::from_env()?;
    moria_common::telemetry::init_tracing("market_data")?;
    moria_common::telemetry::init_metrics("market_data", config.telemetry.metrics_addr.as_deref())?;

    let (kline_tx, _) = broadcast::channel::<Kline>(1024);
    let (trade_tx, _) = broadcast::channel::<Trade>(1024);
    let (orderbook_tx, _) = broadcast::channel::<OrderbookSnapshot>(256);

    // Start WebSocket connection in background
    let ws = bybit_ws::BybitWs::new(
        config.bybit_ws_url.clone(),
        config.trading_pair.clone(),
        config.kline_interval.clone(),
        kline_tx.clone(),
        trade_tx.clone(),
        orderbook_tx.clone(),
    );
    let ws_handle = tokio::spawn(async move { ws.run().await });

    // Start gRPC server
    let grpc_server = server::MarketDataServer::new(kline_tx, trade_tx, orderbook_tx);
    let addr = config.market_data_grpc_addr.parse()?;
    moria_common::grpc::serve_grpc(grpc_server.into_service(), addr, "market-data").await?;

    ws_handle.abort();
    Ok(())
}
