mod bybit_rest;
mod server;

use anyhow::Result;
use moria_common::Config;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
    moria_common::telemetry::init_tracing("order")?;
    moria_common::telemetry::init_metrics("order", config.metrics_addr.as_deref())?;

    let rest_client = bybit_rest::BybitRestClient::new(
        config.bybit_rest_url,
        config.bybit_api_key,
        config.bybit_api_secret,
    );

    let grpc_server = server::OrderServer::new(rest_client);
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<moria_proto::order::order_service_server::OrderServiceServer<server::OrderServer>>()
        .await;
    let addr = config.order_grpc_addr.parse()?;
    info!(%addr, "Starting order gRPC server");

    Server::builder()
        .add_service(health_service)
        .add_service(grpc_server.into_service())
        .serve(addr)
        .await?;

    Ok(())
}
