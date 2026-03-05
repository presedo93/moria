use crate::bybit_rest::BybitRestClient;
use metrics::{counter, histogram};
use moria_proto::order::{
    OrderRequest, OrderResponse, OrderStatusRequest, OrderStatusResponse,
    order_service_server::{OrderService, OrderServiceServer},
};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::info;

pub struct OrderServer {
    rest_client: Arc<BybitRestClient>,
}

impl OrderServer {
    pub fn new(rest_client: BybitRestClient) -> Self {
        Self {
            rest_client: Arc::new(rest_client),
        }
    }

    pub fn into_service(self) -> OrderServiceServer<Self> {
        OrderServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl OrderService for OrderServer {
    #[tracing::instrument(skip_all)]
    async fn place_order(
        &self,
        request: Request<OrderRequest>,
    ) -> Result<Response<OrderResponse>, Status> {
        let started = Instant::now();
        counter!("order_place_order_requests_total").increment(1);

        let order = request.into_inner();
        info!(
            signal_id = %order.signal_id,
            symbol = %order.symbol,
            side = %order.side,
            order_type = %order.order_type,
            qty = %order.qty,
            "Received order request"
        );

        let price = Decimal::from_str(&order.price)
            .map_err(|e| Status::invalid_argument(format!("invalid price: {e}")))?;
        let qty = Decimal::from_str(&order.qty)
            .map_err(|e| Status::invalid_argument(format!("invalid qty: {e}")))?;

        let result = self
            .rest_client
            .place_order(&order.symbol, &order.side, &order.order_type, price, qty)
            .await
            .map_err(|e| Status::internal(format!("order placement failed: {e}")))?;

        counter!("order_place_order_result_total", "status" => result.status.clone()).increment(1);
        histogram!("order_place_order_latency_seconds").record(started.elapsed().as_secs_f64());

        Ok(Response::new(OrderResponse {
            order_id: result.order_id,
            status: result.status,
            message: result.message,
        }))
    }

    #[tracing::instrument(skip_all)]
    async fn get_order_status(
        &self,
        request: Request<OrderStatusRequest>,
    ) -> Result<Response<OrderStatusResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .rest_client
            .get_order_status(&req.symbol, &req.order_id)
            .await
            .map_err(|e| Status::internal(format!("order status lookup failed: {e}")))?;

        Ok(Response::new(OrderStatusResponse {
            order_id: result.order_id,
            status: result.status,
            message: result.message,
        }))
    }
}
