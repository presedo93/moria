use anyhow::{Context, Result};
use metrics::counter;
use moria_common::config::ReconcilerConfig;
use moria_proto::order::{OrderStatusRequest, order_service_client::OrderServiceClient};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{info, warn};
use uuid::Uuid;
use serde_json::json;

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
    let config = ReconcilerConfig::from_env()?;
    moria_common::telemetry::init_tracing("reconciler")?;
    moria_common::telemetry::init_metrics("reconciler", config.telemetry.metrics_addr.as_deref())?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&config.db.database_url)
        .await
        .context("failed to connect to postgres")?;

    moria_common::migrate::run_migrations(&pool).await?;

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
                if let Err(e) = reconcile_once(&pool, &mut order_client, config.auth.internal_service_token.as_deref()).await {
                    warn!(?e, "Reconciliation tick failed");
                }
            }
            _ = moria_common::signal::shutdown_signal("reconciler") => {
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
    internal_token: Option<&str>,
) -> Result<()> {
    let pending = fetch_pending_trades(pool).await?;
    if pending.is_empty() {
        return Ok(());
    }

    for trade in pending {
        let mut grpc_request = tonic::Request::new(OrderStatusRequest {
            order_id: trade.order_id.clone(),
            symbol: trade.symbol.clone(),
        });
        moria_common::auth::attach_internal_token(&mut grpc_request, internal_token)?;
        let response =
            tokio::time::timeout(Duration::from_secs(5), order_client.get_order_status(grpc_request))
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

        moria_common::db::append_domain_event(
            pool,
            "reconciler",
            "OrderStatusReconciled",
            &trade.signal_id.to_string(),
            json!({
                "signal_id": trade.signal_id.to_string(),
                "order_id": trade.order_id,
                "old_status": trade.status,
                "new_status": status_resp.status,
                "message": status_resp.message
            }),
        )
        .await?;
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