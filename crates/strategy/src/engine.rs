use crate::strategy::{Signal, Strategy};
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
    strategy: Box<dyn Strategy>,
    market_data_addr: String,
    risk_addr: String,
    bybit_rest_url: String,
    warm_up_period: usize,
}

impl StrategyEngine {
    pub fn new(
        symbol: String,
        interval: String,
        qty: Decimal,
        strategy: Box<dyn Strategy>,
        market_data_addr: String,
        risk_addr: String,
        bybit_rest_url: String,
        warm_up_period: usize,
    ) -> Self {
        Self {
            symbol,
            interval,
            qty,
            strategy,
            market_data_addr,
            risk_addr,
            bybit_rest_url,
            warm_up_period,
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

    /// Fetch historical klines from Bybit REST API to pre-fill the strategy's price window,
    /// avoiding the cold-start blind period where no signals can be generated.
    async fn bootstrap_historical(&mut self) -> Result<()> {
        if self.warm_up_period == 0 {
            return Ok(());
        }

        let url = format!(
            "{}/v5/market/kline?category=linear&symbol={}&interval={}&limit={}",
            self.bybit_rest_url, self.symbol, self.interval, self.warm_up_period
        );

        info!(url = %url, count = self.warm_up_period, "Bootstrapping strategy with historical klines");

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        let resp: BybitKlineResponse = client.get(&url).send().await?.json().await?;

        if resp.ret_code != 0 {
            warn!(ret_code = resp.ret_code, msg = %resp.ret_msg, "Bybit kline API error during bootstrap");
            return Ok(()); // Non-fatal: strategy will warm up naturally
        }

        // Bybit returns klines newest-first, reverse to chronological order
        let mut klines = resp.result.list;
        klines.reverse();

        let mut count = 0;
        for kline in &klines {
            // Kline format: [start_time, open, high, low, close, volume, turnover]
            if kline.len() >= 5 {
                if let Ok(close) = Decimal::from_str(&kline[4]) {
                    self.strategy.push(close);
                    count += 1;
                }
            }
        }

        info!(count, "Strategy bootstrapped with historical prices");
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(symbol = %self.symbol))]
    async fn run_once(&mut self) -> Result<()> {
        // Bootstrap strategy with historical data to avoid cold-start blind period
        if let Err(e) = self.bootstrap_historical().await {
            warn!(?e, "Failed to bootstrap historical klines (will warm up naturally)");
        }

        let market_channel = Channel::from_shared(format!("http://{}", self.market_data_addr))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .context("Failed to connect to market-data service")?;
        let mut market_client = MarketDataServiceClient::new(market_channel);

        let risk_channel = Channel::from_shared(format!("http://{}", self.risk_addr))?
            .connect_timeout(Duration::from_secs(5))
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

        let strategy_name = self.strategy.name().to_owned();
        info!(symbol = %self.symbol, strategy = %strategy_name, "Strategy engine started, consuming klines");

        while let Some(kline) = stream.next().await {
            let kline = match kline {
                Ok(k) => k,
                Err(e) => {
                    warn!(?e, "Error receiving kline");
                    continue;
                }
            };

            // Only evaluate strategy on confirmed (closed) candles to avoid
            // false signals from mid-candle price spikes
            if !kline.confirm {
                tracing::trace!(close = %kline.close, "Skipping unconfirmed kline");
                continue;
            }

            let close = match Decimal::from_str(&kline.close) {
                Ok(d) => d,
                Err(e) => {
                    warn!(?e, close = %kline.close, "Failed to parse kline close price");
                    continue;
                }
            };

            let signal = self.strategy.push(close);

            tracing::debug!(%close, strategy = %strategy_name, "Kline processed");

            match signal {
                Signal::Buy => {
                    counter!("strategy_signals_total", "side" => "Buy").increment(1);
                    info!(%close, "BUY signal detected");
                    Self::spawn_signal(
                        "Buy", close, risk_client.clone(),
                        self.symbol.clone(), self.qty,
                    );
                }
                Signal::Sell => {
                    counter!("strategy_signals_total", "side" => "Sell").increment(1);
                    info!(%close, "SELL signal detected");
                    Self::spawn_signal(
                        "Sell", close, risk_client.clone(),
                        self.symbol.clone(), self.qty,
                    );
                }
                Signal::None => {}
            }
        }

        Ok(())
    }

    /// Dispatch signal to risk service asynchronously so kline processing isn't blocked.
    fn spawn_signal(
        side: &str,
        price: Decimal,
        risk_client: RiskServiceClient<Channel>,
        symbol: String,
        qty: Decimal,
    ) {
        let side = side.to_string();
        tokio::spawn(async move {
            Self::send_signal(&side, price, risk_client, &symbol, qty).await;
        });
    }

    async fn send_signal(
        side: &str,
        price: Decimal,
        mut risk_client: RiskServiceClient<Channel>,
        symbol: &str,
        qty: Decimal,
    ) {
        let signal_id = uuid::Uuid::new_v4().to_string();
        let request = OrderRequest {
            signal_id: signal_id.clone(),
            symbol: symbol.to_string(),
            side: side.to_string(),
            order_type: "Market".to_string(),
            price: price.to_string(),
            qty: qty.to_string(),
        };

        match tokio::time::timeout(
            Duration::from_secs(5),
            risk_client.validate_order(request),
        )
        .await
        {
            Ok(Ok(response)) => {
                let decision = response.into_inner();
                if decision.approved {
                    counter!("strategy_risk_decision_total", "decision" => "approved").increment(1);
                    info!(signal_id, "Order approved by risk service");
                } else {
                    counter!("strategy_risk_decision_total", "decision" => "rejected").increment(1);
                    warn!(signal_id, reason = %decision.reason, "Order rejected by risk service");
                }
            }
            Ok(Err(e)) => {
                counter!("strategy_risk_decision_total", "decision" => "error").increment(1);
                warn!(?e, signal_id, "Failed to contact risk service");
            }
            Err(_) => {
                counter!("strategy_risk_decision_total", "decision" => "timeout").increment(1);
                warn!(signal_id, "Risk validation timed out");
            }
        }
    }
}

// --- Bybit REST API types for historical kline bootstrap ---

#[derive(serde::Deserialize)]
struct BybitKlineResponse {
    #[serde(rename = "retCode")]
    ret_code: i32,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: BybitKlineResult,
}

#[derive(serde::Deserialize)]
struct BybitKlineResult {
    list: Vec<Vec<String>>,
}
