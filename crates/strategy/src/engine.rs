use crate::sma::{CrossoverSignal, SmaCrossover};
use anyhow::{Context, Result};
use metrics::counter;
use moria_proto::{
    market_data::{StreamRequest, market_data_service_client::MarketDataServiceClient},
    order::OrderRequest,
    risk::risk_service_client::RiskServiceClient,
};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::{info, warn};

pub struct StrategyEngine {
    symbol: String,
    interval: String,
    qty: Decimal,
    sma: SmaCrossover,
    market_data_addr: String,
    risk_addr: String,
}

impl StrategyEngine {
    pub fn new(
        symbol: String,
        interval: String,
        qty: Decimal,
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
        const RECONNECT_BASE_DELAY: Duration = Duration::from_secs(1);
        const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
        let mut attempt = 0u32;

        loop {
            match self.run_once().await {
                Ok(()) => {
                    attempt = 0;
                    counter!("strategy_reconnect_total", "reason" => "stream_closed").increment(1);
                    warn!("Kline stream ended, reconnecting");
                }
                Err(e) => {
                    attempt = attempt.saturating_add(1);
                    counter!("strategy_reconnect_total", "reason" => "error").increment(1);
                    let delay = std::cmp::min(
                        RECONNECT_BASE_DELAY * 2u32.saturating_pow(attempt - 1),
                        MAX_RECONNECT_DELAY,
                    );
                    warn!(?e, attempt, ?delay, "Strategy loop failed, reconnecting");
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn run_once(&mut self) -> Result<()> {
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

            let close = match Decimal::from_str(&kline.close) {
                Ok(d) => d,
                Err(e) => {
                    warn!(?e, close = %kline.close, "Failed to parse kline close price");
                    continue;
                }
            };

            let signal = self.sma.push(close);

            if let Some(short) = self.sma.short_sma()
                && let Some(long) = self.sma.long_sma()
            {
                tracing::debug!(
                    %close,
                    %short,
                    %long,
                    "Kline processed"
                );
            }

            match signal {
                CrossoverSignal::Buy => {
                    counter!("strategy_signals_total", "side" => "Buy").increment(1);
                    info!(%close, "BUY signal detected");
                    self.send_signal("Buy", close, risk_client.clone()).await;
                }
                CrossoverSignal::Sell => {
                    counter!("strategy_signals_total", "side" => "Sell").increment(1);
                    info!(%close, "SELL signal detected");
                    self.send_signal("Sell", close, risk_client.clone()).await;
                }
                CrossoverSignal::None => {}
            }
        }

        Ok(())
    }

    async fn send_signal(
        &self,
        side: &str,
        price: Decimal,
        mut risk_client: RiskServiceClient<Channel>,
    ) {
        let signal_id = uuid::Uuid::new_v4().to_string();
        let request = OrderRequest {
            signal_id: signal_id.clone(),
            symbol: self.symbol.clone(),
            side: side.to_string(),
            order_type: "Market".to_string(),
            price: price.to_string(),
            qty: self.qty.to_string(),
        };

        match risk_client.validate_order(request).await {
            Ok(response) => {
                let decision = response.into_inner();
                if decision.approved {
                    counter!("strategy_risk_decision_total", "decision" => "approved").increment(1);
                    info!(signal_id, "Order approved by risk service");
                } else {
                    counter!("strategy_risk_decision_total", "decision" => "rejected").increment(1);
                    warn!(signal_id, reason = %decision.reason, "Order rejected by risk service");
                }
            }
            Err(e) => {
                counter!("strategy_risk_decision_total", "decision" => "error").increment(1);
                warn!(?e, signal_id, "Failed to contact risk service");
            }
        }
    }
}
