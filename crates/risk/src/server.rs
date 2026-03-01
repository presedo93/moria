use crate::{db, validator::RiskValidator};
use moria_proto::{
    order::{OrderRequest, order_service_client::OrderServiceClient},
    risk::{RiskDecision, risk_service_server::{RiskService, RiskServiceServer}},
};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::str::FromStr;
use tonic::{Request, Response, Status, transport::Channel};
use tracing::{info, warn};
use uuid::Uuid;

pub struct RiskServer {
    pool: PgPool,
    validator: RiskValidator,
    order_client: OrderServiceClient<Channel>,
}

impl RiskServer {
    pub fn new(
        pool: PgPool,
        validator: RiskValidator,
        order_client: OrderServiceClient<Channel>,
    ) -> Self {
        Self {
            pool,
            validator,
            order_client,
        }
    }

    pub fn into_service(self) -> RiskServiceServer<Self> {
        RiskServiceServer::new(self)
    }
}

#[allow(clippy::result_large_err)]
fn parse_decimal(field: &str, value: &str) -> Result<Decimal, Status> {
    Decimal::from_str(value)
        .map_err(|e| Status::invalid_argument(format!("invalid {field}: {e}")))
}

#[tonic::async_trait]
impl RiskService for RiskServer {
    async fn validate_order(
        &self,
        request: Request<OrderRequest>,
    ) -> Result<Response<RiskDecision>, Status> {
        let order = request.into_inner();
        let signal_id = order.signal_id.clone();

        let signal_uuid = Uuid::parse_str(&signal_id)
            .map_err(|e| Status::invalid_argument(format!("invalid signal_id: {e}")))?;

        let price = parse_decimal("price", &order.price)?;
        let qty = parse_decimal("qty", &order.qty)?;

        // Fetch current risk state
        let current_position = db::get_position_qty(&self.pool, &order.symbol)
            .await
            .map_err(|e| Status::internal(format!("db error: {e}")))?;

        let daily_pnl = db::get_daily_realized_pnl(&self.pool, &order.symbol)
            .await
            .map_err(|e| Status::internal(format!("db error: {e}")))?;

        // Validate
        let decision = match self.validator.validate(current_position, qty, daily_pnl) {
            Ok(()) => {
                info!(signal_id, symbol = %order.symbol, side = %order.side, "Order approved");

                // Persist approved signal
                db::insert_signal(
                    &self.pool,
                    signal_uuid,
                    &order.symbol,
                    &order.side,
                    &order.order_type,
                    price,
                    qty,
                    true,
                    None,
                )
                .await
                .map_err(|e| Status::internal(format!("db error: {e}")))?;

                // Forward to order service
                let order_req = OrderRequest {
                    signal_id: signal_id.clone(),
                    symbol: order.symbol,
                    side: order.side,
                    order_type: order.order_type,
                    price: price.to_string(),
                    qty: qty.to_string(),
                };
                let mut order_client = self.order_client.clone();
                match order_client.place_order(order_req).await {
                    Ok(resp) => {
                        let order_resp = resp.into_inner();
                        info!(
                            signal_id,
                            order_id = %order_resp.order_id,
                            status = %order_resp.status,
                            "Order placed"
                        );
                    }
                    Err(e) => {
                        warn!(?e, signal_id, "Failed to place order");
                    }
                }

                RiskDecision {
                    approved: true,
                    reason: String::new(),
                    signal_id,
                }
            }
            Err(e) => {
                let reason = e.to_string();
                warn!(signal_id, %reason, "Order rejected");

                // Persist rejected signal
                db::insert_signal(
                    &self.pool,
                    signal_uuid,
                    &order.symbol,
                    &order.side,
                    &order.order_type,
                    price,
                    qty,
                    false,
                    Some(&reason),
                )
                .await
                .map_err(|e| Status::internal(format!("db error: {e}")))?;

                RiskDecision {
                    approved: false,
                    reason,
                    signal_id,
                }
            }
        };

        Ok(Response::new(decision))
    }
}
