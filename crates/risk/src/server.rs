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
use serde_json::json;
use sqlx::PgPool;
use std::str::FromStr;
use std::time::Instant;
use tonic::{Request, Response, Status, transport::Channel};
use tracing::{info, warn};
use uuid::Uuid;

pub struct RiskServer {
    pool: PgPool,
    validator: RiskValidator,
    _order_client: OrderServiceClient<Channel>,
    internal_service_token: Option<String>,
}

impl RiskServer {
    pub fn new(
        pool: PgPool,
        validator: RiskValidator,
        order_client: OrderServiceClient<Channel>,
        internal_service_token: Option<String>,
    ) -> Self {
        Self {
            pool,
            validator,
            _order_client: order_client,
            internal_service_token,
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
    #[tracing::instrument(skip_all)]
    async fn validate_order(
        &self,
        request: Request<OrderRequest>,
    ) -> Result<Response<RiskDecision>, Status> {
        moria_common::auth::authorize_request(&request, self.internal_service_token.as_deref())?;
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

        // Fetch current risk state under a transaction lock (prevents TOCTOU races)
        let (mut risk_tx, risk_state) = db::begin_risk_check(&self.pool, &order.symbol)
            .await
            .map_err(|e| Status::internal(format!("db error: {e}")))?;

        // Compute unrealized PnL from current position and market price
        let unrealized_pnl = moria_common::position::unrealized_pnl(
            risk_state.current_position,
            risk_state.avg_entry_price,
            price,
        );

        let total_daily_pnl = risk_state.daily_realized_pnl + unrealized_pnl;

        // Validate against the locked snapshot (using total PnL including unrealized)
        let decision = match self.validator.validate(
            risk_state.current_position,
            &order.side,
            qty,
            price,
            total_daily_pnl,
            risk_state.portfolio_notional,
            risk_state.daily_peak_pnl,
        ) {
            Ok(()) => {
                // Update daily peak high-water mark if PnL improved (atomic conditional update)
                if total_daily_pnl > risk_state.daily_peak_pnl {
                    if let Err(e) = db::update_daily_peak_pnl_in_tx(&mut risk_tx, total_daily_pnl).await {
                        warn!(?e, "Failed to update daily peak PnL");
                    }
                }

                // Persist risk decision and enqueue downstream execution intent atomically.
                db::persist_signal_and_trade_in_tx(
                    &mut risk_tx,
                    signal_uuid,
                    &order.symbol,
                    &order.side,
                    &order.order_type,
                    price,
                    qty,
                    true,
                    None,
                    None,
                    None,
                )
                .await
                .map_err(|e| Status::internal(format!("db error: {e}")))?;

                db::enqueue_order_intent_in_tx(
                    &mut risk_tx,
                    Uuid::new_v4(),
                    signal_uuid,
                    &order.symbol,
                    &order.side,
                    &order.order_type,
                    price,
                    qty,
                )
                .await
                .map_err(|e| Status::internal(format!("db error: {e}")))?;

                db::append_domain_event_in_tx(
                    &mut risk_tx,
                    "risk",
                    "RiskOrderAccepted",
                    &signal_id,
                    json!({
                        "signal_id": signal_id,
                        "symbol": order.symbol,
                        "side": order.side,
                        "order_type": order.order_type,
                        "price": price,
                        "qty": qty
                    }),
                )
                .await
                .map_err(|e| Status::internal(format!("db error: {e}")))?;

                risk_tx.commit().await.map_err(|e| Status::internal(format!("db error: {e}")))?;

                counter!("risk_approved_total").increment(1);
                info!(signal_id, "Order accepted by risk and queued for async execution");
                RiskDecision {
                    approved: true,
                    reason: String::new(),
                    signal_id,
                }
            }
            Err(e) => {
                if matches!(e, crate::validator::RiskError::DrawdownBreached { .. }) {
                    counter!("risk_drawdown_breaker_tripped_total").increment(1);
                }
                counter!("risk_rejected_total", "reason" => "risk_validation").increment(1);
                let reason = e.to_string();
                warn!(signal_id, %reason, "Order rejected");

                db::persist_signal_and_trade_in_tx(
                    &mut risk_tx,
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

                db::append_domain_event_in_tx(
                    &mut risk_tx,
                    "risk",
                    "RiskOrderRejected",
                    &signal_id,
                    json!({
                        "signal_id": signal_id,
                        "symbol": order.symbol,
                        "side": order.side,
                        "reason": reason
                    }),
                )
                .await
                .map_err(|e| Status::internal(format!("db error: {e}")))?;

                risk_tx.commit().await.map_err(|e| Status::internal(format!("db error: {e}")))?;

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
        OrderRequest, OrderResponse, OrderStatusRequest, OrderStatusResponse,
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

        async fn get_order_status(
            &self,
            _request: Request<OrderStatusRequest>,
        ) -> Result<Response<OrderStatusResponse>, Status> {
            Ok(Response::new(OrderStatusResponse {
                order_id: "mock-order-status".to_string(),
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
        moria_common::migrate::run_migrations(&pool)
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
            RiskValidator::new(dec("1.0"), dec("100.0"), dec("10000.0"), dec("500.0")),
            OrderServiceClient::new(order_channel),
            None,
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
        moria_common::migrate::run_migrations(&pool)
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
            RiskValidator::new(dec("1.0"), dec("100.0"), dec("10000.0"), dec("500.0")),
            OrderServiceClient::new(order_channel),
            None,
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
        moria_common::migrate::run_migrations(&pool)
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
            RiskValidator::new(dec("1.0"), dec("100.0"), dec("10000.0"), dec("500.0")),
            OrderServiceClient::new(order_channel),
            None,
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
