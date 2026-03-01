use crate::bybit_rest::BybitRestClient;
use moria_proto::order::{
    OrderRequest, OrderResponse,
    order_service_server::{OrderService, OrderServiceServer},
};
use std::sync::Arc;
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
    async fn place_order(
        &self,
        request: Request<OrderRequest>,
    ) -> Result<Response<OrderResponse>, Status> {
        let order = request.into_inner();
        info!(
            signal_id = %order.signal_id,
            symbol = %order.symbol,
            side = %order.side,
            order_type = %order.order_type,
            qty = order.qty,
            "Received order request"
        );

        let result = self
            .rest_client
            .place_order(&order.symbol, &order.side, &order.order_type, order.price, order.qty)
            .await
            .map_err(|e| Status::internal(format!("order placement failed: {e}")))?;

        Ok(Response::new(OrderResponse {
            order_id: result.order_id,
            status: result.status,
            message: result.message,
        }))
    }
}
