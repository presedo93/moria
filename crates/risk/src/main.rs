mod db;
mod server;
mod validator;

use anyhow::{Context, Result};
use moria_common::Config;
use moria_proto::order::order_service_client::OrderServiceClient;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tonic::transport::{Channel, Server};
use tonic_health::server::health_reporter;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
    moria_common::telemetry::init_tracing("risk")?;
    moria_common::telemetry::init_metrics("risk", config.metrics_addr.as_deref())?;

    // Connect to PostgreSQL with retry
    let pool = retry_connect("PostgreSQL", 5, Duration::from_secs(3), || async {
        PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&config.database_url)
            .await
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

    let risk_validator = validator::RiskValidator::new(
        config.max_position_size,
        config.max_daily_loss,
        config.max_portfolio_notional,
        config.max_drawdown,
    );

    let grpc_server = server::RiskServer::new(pool, risk_validator, order_client);
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<moria_proto::risk::risk_service_server::RiskServiceServer<server::RiskServer>>()
        .await;
    let addr = config.risk_grpc_addr.parse()?;
    info!(%addr, "Starting risk gRPC server");

    Server::builder()
        .add_service(health_service)
        .add_service(grpc_server.into_service())
        .serve(addr)
        .await?;

    Ok(())
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
