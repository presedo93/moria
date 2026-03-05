use crate::db;
use anyhow::Result;
use metrics::counter;
use moria_proto::order::order_service_client::OrderServiceClient;
use sqlx::PgPool;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{info, warn};

const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const MAX_ATTEMPTS: i32 = 8;

pub fn spawn_order_execution_worker(pool: PgPool, order_client: OrderServiceClient<Channel>) {
    tokio::spawn(async move {
        if let Err(e) = run_order_execution_worker(pool, order_client).await {
            warn!(?e, "Order execution worker exited");
        }
    });
}

async fn run_order_execution_worker(
    pool: PgPool,
    mut order_client: OrderServiceClient<Channel>,
) -> Result<()> {
    info!("Order execution worker started");

    loop {
        let Some(intent) = db::claim_pending_order_intent(&pool).await? else {
            tokio::time::sleep(IDLE_POLL_INTERVAL).await;
            continue;
        };

        counter!("risk_outbox_claimed_total").increment(1);
        let request = moria_proto::order::OrderRequest {
            signal_id: intent.signal_id.to_string(),
            symbol: intent.symbol.clone(),
            side: intent.side.clone(),
            order_type: intent.order_type.clone(),
            price: intent.price.to_string(),
            qty: intent.qty.to_string(),
        };

        match tokio::time::timeout(Duration::from_secs(10), order_client.place_order(request)).await {
            Ok(Ok(response)) => {
                let result = response.into_inner();
                let status = result.status.clone();
                let rejected = (status == "Rejected").then_some(result.message.as_str());
                db::upsert_trade_execution(
                    &pool,
                    intent.signal_id,
                    &result.order_id,
                    &intent.symbol,
                    &intent.side,
                    intent.price,
                    intent.qty,
                    &status,
                    rejected,
                )
                .await?;
                db::mark_order_intent_succeeded(&pool, intent.id).await?;
                counter!("risk_outbox_submitted_total", "status" => status).increment(1);
            }
            Ok(Err(e)) => {
                counter!("risk_outbox_submit_errors_total").increment(1);
                let reason = format!("order service call failed: {e}");
                db::mark_order_intent_retry(&pool, intent.id, &reason, intent.attempts, MAX_ATTEMPTS)
                    .await?;
            }
            Err(_) => {
                counter!("risk_outbox_submit_timeouts_total").increment(1);
                db::mark_order_intent_retry(
                    &pool,
                    intent.id,
                    "order service call timed out",
                    intent.attempts,
                    MAX_ATTEMPTS,
                )
                .await?;
            }
        }
    }
}
