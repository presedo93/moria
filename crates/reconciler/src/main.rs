use anyhow::{Context, Result};
use metrics::counter;
use moria_common::Config;
use moria_proto::order::{OrderStatusRequest, order_service_client::OrderServiceClient};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{info, warn};
use uuid::Uuid;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const MAX_BATCH_SIZE: i64 = 100;

#[derive(sqlx::FromRow, Debug)]
struct PendingTrade {
    signal_id: Uuid,
    order_id: String,
    symbol: String,
    status: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env();
    moria_common::telemetry::init_tracing("reconciler")?;
    moria_common::telemetry::init_metrics("reconciler", config.metrics_addr.as_deref())?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&config.database_url)
        .await
        .context("failed to connect to postgres")?;

    let order_channel = Channel::from_shared(format!("http://{}", config.order_grpc_addr))
        .context("invalid order service address")?
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .context("failed to connect to order service")?;
    let mut order_client = OrderServiceClient::new(order_channel);

    info!("Order reconciliation service started");

    let mut ticker = tokio::time::interval(RECONCILE_INTERVAL);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(e) = reconcile_once(&pool, &mut order_client).await {
                    warn!(?e, "Reconciliation tick failed");
                }
            }
            _ = shutdown_signal() => {
                info!("Reconciler shutting down gracefully");
                break;
            }
        }
    }

    moria_common::telemetry::shutdown_tracing();
    Ok(())
}

async fn reconcile_once(
    pool: &PgPool,
    order_client: &mut OrderServiceClient<Channel>,
) -> Result<()> {
    let pending = fetch_pending_trades(pool).await?;
    if pending.is_empty() {
        return Ok(());
    }

    for trade in pending {
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            order_client.get_order_status(OrderStatusRequest {
                order_id: trade.order_id.clone(),
                symbol: trade.symbol.clone(),
            }),
        )
        .await;

        let status_resp = match response {
            Ok(Ok(resp)) => resp.into_inner(),
            Ok(Err(e)) => {
                counter!("reconciler_lookup_failures_total").increment(1);
                warn!(?e, order_id = %trade.order_id, "Order status lookup failed");
                continue;
            }
            Err(_) => {
                counter!("reconciler_lookup_timeouts_total").increment(1);
                warn!(order_id = %trade.order_id, "Order status lookup timed out");
                continue;
            }
        };

        if status_resp.status == trade.status {
            continue;
        }

        counter!("reconciler_status_updates_total", "status" => status_resp.status.clone()).increment(1);
        sqlx::query("UPDATE trades SET status = $2 WHERE signal_id = $1")
            .bind(trade.signal_id)
            .bind(&status_resp.status)
            .execute(pool)
            .await?;

        if status_resp.status == "Rejected" {
            sqlx::query("UPDATE signals SET reject_reason = $2 WHERE id = $1")
                .bind(trade.signal_id)
                .bind(&status_resp.message)
                .execute(pool)
                .await?;
        }
    }

    Ok(())
}

async fn fetch_pending_trades(pool: &PgPool) -> Result<Vec<PendingTrade>> {
    let rows = sqlx::query_as::<_, PendingTrade>(
        "SELECT signal_id, order_id, symbol, status
         FROM trades
         WHERE status IN ('Submitted', 'New', 'PartiallyFilled')
         ORDER BY created_at ASC
         LIMIT $1",
    )
    .bind(MAX_BATCH_SIZE)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    tokio::select! {
        _ = ctrl_c => info!("reconciler: received SIGINT"),
        _ = sigterm.recv() => info!("reconciler: received SIGTERM"),
    }
}
