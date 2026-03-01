mod bybit_ws;
mod server;

use anyhow::Result;
use moria_common::Config;
use moria_proto::market_data::{Kline, OrderbookSnapshot, Trade};
use tokio::sync::broadcast;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
    moria_common::telemetry::init_tracing("market_data")?;
    moria_common::telemetry::init_metrics("market_data", config.metrics_addr.as_deref())?;

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
    tokio::spawn(async move { ws.run().await });

    // Start gRPC server
    let grpc_server = server::MarketDataServer::new(kline_tx, trade_tx, orderbook_tx);
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<moria_proto::market_data::market_data_service_server::MarketDataServiceServer<server::MarketDataServer>>()
        .await;
    let addr = config.market_data_grpc_addr.parse()?;
    info!(%addr, "Starting market-data gRPC server");

    Server::builder()
        .add_service(health_service)
        .add_service(grpc_server.into_service())
        .serve(addr)
        .await?;

    Ok(())
}
