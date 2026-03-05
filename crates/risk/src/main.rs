mod db;
mod server;
mod validator;
mod worker;

use anyhow::{Context, Result};
use moria_common::Config;
use moria_proto::order::order_service_client::OrderServiceClient;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;
use std::time::Duration;
use tonic::transport::{Channel, Server};
use tonic_health::server::health_reporter;
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
    config.validate_for_service("risk")?;
    moria_common::telemetry::init_tracing("risk")?;
    moria_common::telemetry::init_metrics("risk", config.metrics_addr.as_deref())?;

    // Connect to PostgreSQL with retry
    // Set statement_timeout at connection level to prevent indefinite query blocking
    let connect_options = PgConnectOptions::from_str(&config.database_url)?
        .options([("statement_timeout", "5000")]); // 5 second query timeout

    let pool = retry_connect("PostgreSQL", 5, Duration::from_secs(3), || {
        let opts = connect_options.clone();
        async move {
            PgPoolOptions::new()
                .max_connections(5)
                .acquire_timeout(Duration::from_secs(5))
                .connect_with(opts)
                .await
        }
    })
    .await
    .context("Failed to connect to PostgreSQL")?;

    db::run_migrations(&pool).await?;

    // Connect to order service with retry
    let order_endpoint = Channel::from_shared(format!("http://{}", config.order_grpc_addr))
        .context("Invalid order service address")?
        .connect_timeout(Duration::from_secs(5));
    let order_channel = retry_connect("order service", 5, Duration::from_secs(3), || {
        let endpoint = order_endpoint.clone();
        async move { endpoint.connect().await }
    })
    .await
    .context("Failed to connect to order service")?;
    let order_client = OrderServiceClient::new(order_channel);
    worker::spawn_order_execution_worker(
        pool.clone(),
        order_client.clone(),
        config.internal_service_token.clone(),
    );

    let risk_validator = validator::RiskValidator::new(
        config.max_position_size,
        config.max_daily_loss,
        config.max_portfolio_notional,
        config.max_drawdown,
    );

    let grpc_server = server::RiskServer::new(
        pool,
        risk_validator,
        order_client,
        config.internal_service_token.clone(),
    );
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<moria_proto::risk::risk_service_server::RiskServiceServer<server::RiskServer>>()
        .await;
    let addr = config.risk_grpc_addr.parse()?;
    info!(%addr, "Starting risk gRPC server");

    let reflection_service = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(moria_proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    Server::builder()
        .add_service(health_service)
        .add_service(reflection_service)
        .add_service(grpc_server.into_service())
        .serve_with_shutdown(addr, shutdown_signal("risk"))
        .await?;

    moria_common::telemetry::shutdown_tracing();
    info!("Risk service shut down gracefully");
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

async fn retry_connect<F, Fut, T, E>(
    name: &str,
    max_attempts: u32,
    base_delay: Duration,
    f: F,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    for attempt in 1..=max_attempts {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                if attempt == max_attempts {
                    anyhow::bail!("Failed to connect to {name} after {max_attempts} attempts: {e}");
                }
                let delay = base_delay * attempt;
                warn!(%attempt, %name, %e, ?delay, "Connection failed, retrying");
                tokio::time::sleep(delay).await;
            }
        }
    }
    unreachable!()
}
