use moria_proto::market_data::{
    Kline, OrderbookSnapshot, StreamRequest, Trade,
    market_data_service_server::{MarketDataService, MarketDataServiceServer},
};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};
use tracing::info;

pub struct MarketDataServer {
    kline_tx: broadcast::Sender<Kline>,
    trade_tx: broadcast::Sender<Trade>,
    orderbook_tx: broadcast::Sender<OrderbookSnapshot>,
}

impl MarketDataServer {
    pub fn new(
        kline_tx: broadcast::Sender<Kline>,
        trade_tx: broadcast::Sender<Trade>,
        orderbook_tx: broadcast::Sender<OrderbookSnapshot>,
    ) -> Self {
        Self {
            kline_tx,
            trade_tx,
            orderbook_tx,
        }
    }

    pub fn into_service(self) -> MarketDataServiceServer<Self> {
        MarketDataServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl MarketDataService for MarketDataServer {
    type StreamKlinesStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<Kline, Status>> + Send>>;
    type StreamTradesStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<Trade, Status>> + Send>>;
    type StreamOrderbookStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<OrderbookSnapshot, Status>> + Send>>;

    async fn stream_klines(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamKlinesStream>, Status> {
        let req = request.into_inner();
        info!(symbol = %req.symbol, interval = %req.interval, "Client subscribing to klines");

        let rx = self.kline_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|result| match result {
            Ok(kline) => Some(Ok(kline)),
            Err(_) => None, // lagged receiver, skip
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn stream_trades(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamTradesStream>, Status> {
        let req = request.into_inner();
        info!(symbol = %req.symbol, "Client subscribing to trades");

        let rx = self.trade_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|result| match result {
            Ok(trade) => Some(Ok(trade)),
            Err(_) => None,
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn stream_orderbook(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamOrderbookStream>, Status> {
        let req = request.into_inner();
        info!(symbol = %req.symbol, "Client subscribing to orderbook");

        let rx = self.orderbook_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|result| match result {
            Ok(snapshot) => Some(Ok(snapshot)),
            Err(_) => None,
        });

        Ok(Response::new(Box::pin(stream)))
    }
}

use tokio_stream::StreamExt;
