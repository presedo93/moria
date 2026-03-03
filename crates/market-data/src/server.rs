use metrics::counter;
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

fn matches_requested(actual: &str, requested: &str) -> bool {
    requested.is_empty() || actual.eq_ignore_ascii_case(requested)
}

#[tonic::async_trait]
impl MarketDataService for MarketDataServer {
    type StreamKlinesStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<Kline, Status>> + Send>>;
    type StreamTradesStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<Trade, Status>> + Send>>;
    type StreamOrderbookStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<OrderbookSnapshot, Status>> + Send>,
    >;

    #[tracing::instrument(skip_all)]
    async fn stream_klines(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamKlinesStream>, Status> {
        let req = request.into_inner();
        info!(symbol = %req.symbol, interval = %req.interval, "Client subscribing to klines");
        let symbol = req.symbol;
        let interval = req.interval;

        let rx = self.kline_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
            Ok(kline)
                if matches_requested(&kline.symbol, &symbol)
                    && matches_requested(&kline.interval, &interval) =>
            {
                counter!("market_data_klines_streamed_total").increment(1);
                Some(Ok(kline))
            }
            Err(_) => {
                counter!("market_data_stream_lagged_total", "stream" => "klines").increment(1);
                None
            } // lagged receiver, skip
            _ => {
                counter!("market_data_filtered_out_total", "stream" => "klines").increment(1);
                None
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(skip_all)]
    async fn stream_trades(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamTradesStream>, Status> {
        let req = request.into_inner();
        info!(symbol = %req.symbol, "Client subscribing to trades");
        let symbol = req.symbol;

        let rx = self.trade_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
            Ok(trade) if matches_requested(&trade.symbol, &symbol) => {
                counter!("market_data_trades_streamed_total").increment(1);
                Some(Ok(trade))
            }
            Err(_) => {
                counter!("market_data_stream_lagged_total", "stream" => "trades").increment(1);
                None
            }
            _ => {
                counter!("market_data_filtered_out_total", "stream" => "trades").increment(1);
                None
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(skip_all)]
    async fn stream_orderbook(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamOrderbookStream>, Status> {
        let req = request.into_inner();
        info!(symbol = %req.symbol, "Client subscribing to orderbook");
        let symbol = req.symbol;

        let rx = self.orderbook_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
            Ok(snapshot) if matches_requested(&snapshot.symbol, &symbol) => {
                counter!("market_data_orderbook_streamed_total").increment(1);
                Some(Ok(snapshot))
            }
            Err(_) => {
                counter!("market_data_stream_lagged_total", "stream" => "orderbook").increment(1);
                None
            }
            _ => {
                counter!("market_data_filtered_out_total", "stream" => "orderbook").increment(1);
                None
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }
}

use tokio_stream::StreamExt;

#[cfg(test)]
mod tests {
    use super::matches_requested;

    #[test]
    fn matches_when_requested_is_empty() {
        assert!(matches_requested("BTCUSDT", ""));
    }

    #[test]
    fn matches_symbol_case_insensitive() {
        assert!(matches_requested("BTCUSDT", "btcusdt"));
    }

    #[test]
    fn does_not_match_different_symbol() {
        assert!(!matches_requested("BTCUSDT", "ETHUSDT"));
    }
}
