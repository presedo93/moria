use crate::{db, validator::RiskValidator};
use metrics::{counter, histogram};
use moria_proto::{
    order::{OrderRequest, order_service_client::OrderServiceClient},
    risk::{
        RiskDecision,
        risk_service_server::{RiskService, RiskServiceServer},
    },
};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::str::FromStr;
use std::time::Instant;
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
    Decimal::from_str(value).map_err(|e| Status::invalid_argument(format!("invalid {field}: {e}")))
}

#[tonic::async_trait]
impl RiskService for RiskServer {
    async fn validate_order(
        &self,
        request: Request<OrderRequest>,
    ) -> Result<Response<RiskDecision>, Status> {
        let started = Instant::now();
        counter!("risk_validate_order_requests_total").increment(1);

        let order = request.into_inner();
        let signal_id = order.signal_id.clone();

        let signal_uuid = Uuid::parse_str(&signal_id)
            .map_err(|e| Status::invalid_argument(format!("invalid signal_id: {e}")))?;

        if let Some(existing) = db::get_signal_decision(&self.pool, signal_uuid)
            .await
            .map_err(|e| Status::internal(format!("db error: {e}")))?
        {
            counter!("risk_duplicate_signal_total").increment(1);
            let decision = if existing.approved {
                RiskDecision {
                    approved: true,
                    reason: String::new(),
                    signal_id,
                }
            } else {
                RiskDecision {
                    approved: false,
                    reason: existing
                        .reject_reason
                        .unwrap_or_else(|| "duplicate signal already rejected".to_string()),
                    signal_id,
                }
            };
            histogram!("risk_validate_order_latency_seconds")
                .record(started.elapsed().as_secs_f64());
            return Ok(Response::new(decision));
        }

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
        let decision = match self
            .validator
            .validate(current_position, &order.side, qty, daily_pnl)
        {
            Ok(()) => {
                // Forward to order service
                let order_req = OrderRequest {
                    signal_id: signal_id.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side.clone(),
                    order_type: order.order_type.clone(),
                    price: price.to_string(),
                    qty: qty.to_string(),
                };
                let mut order_client = self.order_client.clone();
                match order_client.place_order(order_req).await {
                    Ok(resp) => {
                        let order_resp = resp.into_inner();
                        let is_approved =
                            order_resp.status == "Submitted" || order_resp.status == "Filled";
                        let reject_reason = (!is_approved).then_some(order_resp.message.as_str());

                        db::persist_signal_and_trade(
                            &self.pool,
                            signal_uuid,
                            &order.symbol,
                            &order.side,
                            &order.order_type,
                            price,
                            qty,
                            is_approved,
                            reject_reason,
                            Some(&order_resp.order_id),
                            Some(&order_resp.status),
                        )
                        .await
                        .map_err(|e| Status::internal(format!("db error: {e}")))?;

                        if is_approved {
                            counter!("risk_approved_total").increment(1);
                            info!(
                                signal_id,
                                order_id = %order_resp.order_id,
                                status = %order_resp.status,
                                "Order approved and persisted"
                            );
                            RiskDecision {
                                approved: true,
                                reason: String::new(),
                                signal_id,
                            }
                        } else {
                            counter!("risk_rejected_total", "reason" => "downstream_execution")
                                .increment(1);
                            warn!(
                                signal_id,
                                order_id = %order_resp.order_id,
                                status = %order_resp.status,
                                reason = %order_resp.message,
                                "Order rejected by downstream execution"
                            );
                            RiskDecision {
                                approved: false,
                                reason: order_resp.message,
                                signal_id,
                            }
                        }
                    }
                    Err(e) => {
                        counter!("risk_rejected_total", "reason" => "order_service_error")
                            .increment(1);
                        let reason = format!("order service call failed: {e}");
                        warn!(?e, signal_id, "Failed to place order");

                        db::persist_signal_and_trade(
                            &self.pool,
                            signal_uuid,
                            &order.symbol,
                            &order.side,
                            &order.order_type,
                            price,
                            qty,
                            false,
                            Some(&reason),
                            None,
                            None,
                        )
                        .await
                        .map_err(|db_err| Status::internal(format!("db error: {db_err}")))?;

                        RiskDecision {
                            approved: false,
                            reason,
                            signal_id,
                        }
                    }
                }
            }
            Err(e) => {
                counter!("risk_rejected_total", "reason" => "risk_validation").increment(1);
                let reason = e.to_string();
                warn!(signal_id, %reason, "Order rejected");

                db::persist_signal_and_trade(
                    &self.pool,
                    signal_uuid,
                    &order.symbol,
                    &order.side,
                    &order.order_type,
                    price,
                    qty,
                    false,
                    Some(&reason),
                    None,
                    None,
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

        histogram!("risk_validate_order_latency_seconds").record(started.elapsed().as_secs_f64());
        Ok(Response::new(decision))
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::db;
    use moria_proto::order::{
        OrderRequest, OrderResponse,
        order_service_server::{OrderService, OrderServiceServer},
    };
    use rust_decimal::Decimal;
    use sqlx::PgPool;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use tokio::sync::oneshot;
    use tonic::transport::{Channel, Server};
    use tonic::{Request, Response, Status};
    use uuid::Uuid;

    struct MockOrderService {
        status: &'static str,
        message: &'static str,
    }

    #[tonic::async_trait]
    impl OrderService for MockOrderService {
        async fn place_order(
            &self,
            request: Request<OrderRequest>,
        ) -> Result<Response<OrderResponse>, Status> {
            let req = request.into_inner();
            Ok(Response::new(OrderResponse {
                order_id: format!("mock-{}", req.signal_id),
                status: self.status.to_string(),
                message: self.message.to_string(),
            }))
        }
    }

    async fn start_mock_order_server(
        status: &'static str,
        message: &'static str,
    ) -> (SocketAddr, oneshot::Sender<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test port");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let svc = OrderServiceServer::new(MockOrderService { status, message });
        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("mock order server should run");
        });

        (addr, shutdown_tx)
    }

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).expect("valid decimal")
    }

    #[sqlx::test]
    #[ignore = "requires DATABASE_URL to a Postgres server for sqlx::test"]
    async fn filled_order_persists_signal_trade_and_updates_position(pool: PgPool) {
        db::run_migrations(&pool)
            .await
            .expect("migrations should apply");

        let symbol = "ITEST_FILLED_BTCUSDT";

        let (order_addr, shutdown) = start_mock_order_server("Filled", "ok").await;
        let order_channel = Channel::from_shared(format!("http://{order_addr}"))
            .expect("valid order URL")
            .connect()
            .await
            .expect("connect to mock order");

        let server = RiskServer::new(
            pool.clone(),
            RiskValidator::new(dec("1.0"), dec("100.0")),
            OrderServiceClient::new(order_channel),
        );

        let buy_signal_id = Uuid::new_v4().to_string();
        let buy_req = OrderRequest {
            signal_id: buy_signal_id.clone(),
            symbol: symbol.to_string(),
            side: "Buy".to_string(),
            order_type: "Market".to_string(),
            price: "100".to_string(),
            qty: "0.4".to_string(),
        };

        let buy_resp = server
            .validate_order(Request::new(buy_req))
            .await
            .expect("buy validate call")
            .into_inner();
        assert!(buy_resp.approved);

        let sell_signal_id = Uuid::new_v4().to_string();
        let sell_req = OrderRequest {
            signal_id: sell_signal_id.clone(),
            symbol: symbol.to_string(),
            side: "Sell".to_string(),
            order_type: "Market".to_string(),
            price: "110".to_string(),
            qty: "0.1".to_string(),
        };

        let sell_resp = server
            .validate_order(Request::new(sell_req))
            .await
            .expect("sell validate call")
            .into_inner();
        assert!(sell_resp.approved);

        let buy_signal_uuid = Uuid::parse_str(&buy_signal_id).expect("uuid");
        let sell_signal_uuid = Uuid::parse_str(&sell_signal_id).expect("uuid");

        let buy_signal_row: (bool,) = sqlx::query_as("SELECT approved FROM signals WHERE id = $1")
            .bind(buy_signal_uuid)
            .fetch_one(&pool)
            .await
            .expect("buy signal row");
        let sell_signal_row: (bool,) = sqlx::query_as("SELECT approved FROM signals WHERE id = $1")
            .bind(sell_signal_uuid)
            .fetch_one(&pool)
            .await
            .expect("sell signal row");
        assert!(buy_signal_row.0);
        assert!(sell_signal_row.0);

        let trade_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM trades WHERE symbol = $1 AND status = 'Filled'")
                .bind(symbol)
                .fetch_one(&pool)
                .await
                .expect("filled trades");
        assert_eq!(trade_count.0, 2);

        let position: (Decimal, Decimal) =
            sqlx::query_as("SELECT qty, avg_entry_price FROM positions WHERE symbol = $1")
                .bind(symbol)
                .fetch_one(&pool)
                .await
                .expect("position row");
        assert_eq!(position.0, dec("0.3"));
        assert_eq!(position.1, dec("100"));

        let _ = shutdown.send(());
    }

    #[sqlx::test]
    #[ignore = "requires DATABASE_URL to a Postgres server for sqlx::test"]
    async fn rejected_execution_returns_not_approved_and_persists_reason(pool: PgPool) {
        db::run_migrations(&pool)
            .await
            .expect("migrations should apply");

        let symbol = "ITEST_REJECT_BTCUSDT";

        let (order_addr, shutdown) =
            start_mock_order_server("Rejected", "insufficient margin").await;
        let order_channel = Channel::from_shared(format!("http://{order_addr}"))
            .expect("valid order URL")
            .connect()
            .await
            .expect("connect to mock order");

        let server = RiskServer::new(
            pool.clone(),
            RiskValidator::new(dec("1.0"), dec("100.0")),
            OrderServiceClient::new(order_channel),
        );

        let signal_id = Uuid::new_v4().to_string();
        let req = OrderRequest {
            signal_id: signal_id.clone(),
            symbol: symbol.to_string(),
            side: "Buy".to_string(),
            order_type: "Market".to_string(),
            price: "100".to_string(),
            qty: "0.2".to_string(),
        };

        let resp = server
            .validate_order(Request::new(req))
            .await
            .expect("validate call")
            .into_inner();
        assert!(!resp.approved);
        assert_eq!(resp.reason, "insufficient margin");

        let signal_uuid = Uuid::parse_str(&signal_id).expect("uuid");

        let signal_row: (bool, Option<String>) =
            sqlx::query_as("SELECT approved, reject_reason FROM signals WHERE id = $1")
                .bind(signal_uuid)
                .fetch_one(&pool)
                .await
                .expect("signal row");
        assert!(!signal_row.0);
        assert_eq!(signal_row.1.as_deref(), Some("insufficient margin"));

        let trade_row: (String,) = sqlx::query_as("SELECT status FROM trades WHERE signal_id = $1")
            .bind(signal_uuid)
            .fetch_one(&pool)
            .await
            .expect("trade row");
        assert_eq!(trade_row.0, "Rejected");

        let _ = shutdown.send(());
    }

    #[sqlx::test]
    #[ignore = "requires DATABASE_URL to a Postgres server for sqlx::test"]
    async fn duplicate_signal_id_returns_existing_decision_without_duplicate_trade(pool: PgPool) {
        db::run_migrations(&pool)
            .await
            .expect("migrations should apply");

        let symbol = "ITEST_DUP_BTCUSDT";
        let (order_addr, shutdown) = start_mock_order_server("Filled", "ok").await;
        let order_channel = Channel::from_shared(format!("http://{order_addr}"))
            .expect("valid order URL")
            .connect()
            .await
            .expect("connect to mock order");

        let server = RiskServer::new(
            pool.clone(),
            RiskValidator::new(dec("1.0"), dec("100.0")),
            OrderServiceClient::new(order_channel),
        );

        let signal_id = Uuid::new_v4().to_string();
        let req = OrderRequest {
            signal_id: signal_id.clone(),
            symbol: symbol.to_string(),
            side: "Buy".to_string(),
            order_type: "Market".to_string(),
            price: "100".to_string(),
            qty: "0.2".to_string(),
        };

        let first = server
            .validate_order(Request::new(req.clone()))
            .await
            .expect("first validate call")
            .into_inner();
        let second = server
            .validate_order(Request::new(req))
            .await
            .expect("second validate call")
            .into_inner();

        assert_eq!(first.approved, second.approved);
        assert_eq!(first.signal_id, second.signal_id);

        let signal_uuid = Uuid::parse_str(&signal_id).expect("uuid");
        let signal_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM signals WHERE id = $1")
            .bind(signal_uuid)
            .fetch_one(&pool)
            .await
            .expect("signal count");
        assert_eq!(signal_count.0, 1);

        let trade_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM trades WHERE signal_id = $1")
                .bind(signal_uuid)
                .fetch_one(&pool)
                .await
                .expect("trade count");
        assert_eq!(trade_count.0, 1);

        let _ = shutdown.send(());
    }
}
