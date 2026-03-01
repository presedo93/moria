use crate::sma::{CrossoverSignal, SmaCrossover};
use anyhow::{Context, Result};
use moria_proto::{
    market_data::{StreamRequest, market_data_service_client::MarketDataServiceClient},
    order::OrderRequest,
    risk::{risk_service_client::RiskServiceClient},
};
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::{info, warn};

pub struct StrategyEngine {
    symbol: String,
    interval: String,
    qty: f64,
    sma: SmaCrossover,
    market_data_addr: String,
    risk_addr: String,
}

impl StrategyEngine {
    pub fn new(
        symbol: String,
        interval: String,
        qty: f64,
        short_period: usize,
        long_period: usize,
        market_data_addr: String,
        risk_addr: String,
    ) -> Self {
        Self {
            symbol,
            interval,
            qty,
            sma: SmaCrossover::new(short_period, long_period),
            market_data_addr,
            risk_addr,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let market_channel = Channel::from_shared(format!("http://{}", self.market_data_addr))?
            .connect()
            .await
            .context("Failed to connect to market-data service")?;
        let mut market_client = MarketDataServiceClient::new(market_channel);

        let risk_channel = Channel::from_shared(format!("http://{}", self.risk_addr))?
            .connect()
            .await
            .context("Failed to connect to risk service")?;
        let risk_client = RiskServiceClient::new(risk_channel);

        let request = StreamRequest {
            symbol: self.symbol.clone(),
            interval: self.interval.clone(),
        };

        let mut stream = market_client
            .stream_klines(request)
            .await
            .context("Failed to start kline stream")?
            .into_inner();

        info!(symbol = %self.symbol, "Strategy engine started, consuming klines");

        while let Some(kline) = stream.next().await {
            let kline = match kline {
                Ok(k) => k,
                Err(e) => {
                    warn!(?e, "Error receiving kline");
                    continue;
                }
            };

            let signal = self.sma.push(kline.close);

            if let Some(short) = self.sma.short_sma()
                && let Some(long) = self.sma.long_sma()
            {
                tracing::debug!(
                    close = kline.close,
                    short_sma = short,
                    long_sma = long,
                    "Kline processed"
                );
            }

            match signal {
                CrossoverSignal::Buy => {
                    info!(price = kline.close, "BUY signal detected");
                    self.send_signal("Buy", kline.close, risk_client.clone())
                        .await;
                }
                CrossoverSignal::Sell => {
                    info!(price = kline.close, "SELL signal detected");
                    self.send_signal("Sell", kline.close, risk_client.clone())
                        .await;
                }
                CrossoverSignal::None => {}
            }
        }

        Ok(())
    }

    async fn send_signal(
        &self,
        side: &str,
        price: f64,
        mut risk_client: RiskServiceClient<Channel>,
    ) {
        let signal_id = uuid::Uuid::new_v4().to_string();
        let request = OrderRequest {
            signal_id: signal_id.clone(),
            symbol: self.symbol.clone(),
            side: side.to_string(),
            order_type: "Market".to_string(),
            price,
            qty: self.qty,
        };

        match risk_client.validate_order(request).await {
            Ok(response) => {
                let decision = response.into_inner();
                if decision.approved {
                    info!(signal_id, "Order approved by risk service");
                } else {
                    warn!(signal_id, reason = %decision.reason, "Order rejected by risk service");
                }
            }
            Err(e) => {
                warn!(?e, signal_id, "Failed to contact risk service");
            }
        }
    }
}
