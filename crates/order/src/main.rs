mod bybit_rest;
mod server;

use anyhow::Result;
use moria_common::config::OrderConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let config = OrderConfig::from_env()?;
    moria_common::telemetry::init_tracing("order")?;
    moria_common::telemetry::init_metrics("order", config.telemetry.metrics_addr.as_deref())?;

    let rest_client = bybit_rest::BybitRestClient::new(
        config.bybit_rest_url,
        config.bybit_api_key,
        config.bybit_api_secret,
    );

    let grpc_server = server::OrderServer::new(rest_client, config.auth.internal_service_token.clone());
    let addr = config.order_grpc_addr.parse()?;
    moria_common::grpc::serve_grpc(grpc_server.into_service(), addr, "order").await
}
