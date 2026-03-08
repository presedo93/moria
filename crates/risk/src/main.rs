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
use tonic::transport::Channel;

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

    let pool = moria_common::retry::retry_connect("PostgreSQL", 5, Duration::from_secs(3), || {
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
    let order_channel = moria_common::retry::retry_connect("order service", 5, Duration::from_secs(3), || {
        let endpoint = order_endpoint.clone();
        async move { endpoint.connect().await }
    })
    .await
    .context("Failed to connect to order service")?;
    let order_client = OrderServiceClient::new(order_channel);
    worker::spawn_order_execution_worker(
        pool.clone(),
        config.database_url.clone(),
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
    let addr = config.risk_grpc_addr.parse()?;
    moria_common::grpc::serve_grpc(grpc_server.into_service(), addr, "risk").await
}
