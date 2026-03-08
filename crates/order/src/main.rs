mod bybit_rest;
mod server;

use anyhow::Result;
use moria_common::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
    config.validate_for_service("order")?;
    moria_common::telemetry::init_tracing("order")?;
    moria_common::telemetry::init_metrics("order", config.metrics_addr.as_deref())?;

    let rest_client = bybit_rest::BybitRestClient::new(
        config.bybit_rest_url,
        config.bybit_api_key,
        config.bybit_api_secret,
    );

    let grpc_server = server::OrderServer::new(rest_client, config.internal_service_token.clone());
    let addr = config.order_grpc_addr.parse()?;
    moria_common::grpc::serve_grpc(grpc_server.into_service(), addr, "order").await
}