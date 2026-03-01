mod db;
mod server;
mod validator;

use anyhow::{Context, Result};
use moria_common::Config;
use moria_proto::order::order_service_client::OrderServiceClient;
use sqlx::postgres::PgPoolOptions;
use tonic::transport::{Channel, Server};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
    moria_common::telemetry::init_tracing("risk")?;

    // Connect to PostgreSQL
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .context("Failed to connect to PostgreSQL")?;

    db::run_migrations(&pool).await?;

    // Connect to order service
    let order_channel = Channel::from_shared(format!("http://{}", config.order_grpc_addr))?
        .connect()
        .await
        .context("Failed to connect to order service")?;
    let order_client = OrderServiceClient::new(order_channel);

    let risk_validator = validator::RiskValidator::new(
        config.max_position_size,
        config.max_daily_loss,
    );

    let grpc_server = server::RiskServer::new(pool, risk_validator, order_client);
    let addr = config.risk_grpc_addr.parse()?;
    info!(%addr, "Starting risk gRPC server");

    Server::builder()
        .add_service(grpc_server.into_service())
        .serve(addr)
        .await?;

    Ok(())
}
