mod bybit_rest;
mod server;

use anyhow::Result;
use moria_common::Config;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tonic_reflection::server::Builder as ReflectionBuilder;
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

    let reflection_service = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(moria_proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    Server::builder()
        .add_service(health_service)
        .add_service(reflection_service)
        .add_service(grpc_server.into_service())
        .serve_with_shutdown(addr, shutdown_signal("order"))
        .await?;

    moria_common::telemetry::shutdown_tracing();
    info!("Order service shut down gracefully");
    Ok(())
}

async fn shutdown_signal(service: &str) {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    tokio::select! {
        _ = ctrl_c => info!("{service}: received SIGINT, shutting down"),
        _ = sigterm.recv() => info!("{service}: received SIGTERM, shutting down"),
    }
}
