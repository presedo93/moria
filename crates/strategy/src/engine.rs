use crate::strategy::{Signal, Strategy};
use anyhow::{Context, Result};
use metrics::counter;
use moria_proto::{
    market_data::{StreamRequest, market_data_service_client::MarketDataServiceClient},
    order::OrderRequest,
    risk::risk_service_client::RiskServiceClient,
};
use moria_common::math::RollingVolatility;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
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
    signal_queue_capacity: usize,
    signal_max_inflight: usize,
    account_equity_usd: Decimal,
    risk_budget_pct: Decimal,
    max_notional_per_trade: Decimal,
    min_volatility: Decimal,
    internal_service_token: Option<String>,
    volatility: RollingVolatility,
}

struct PendingSignal {
    side: &'static str,
    price: Decimal,
    qty: Decimal,
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
        signal_queue_capacity: usize,
        signal_max_inflight: usize,
        account_equity_usd: Decimal,
        risk_budget_pct: Decimal,
        max_notional_per_trade: Decimal,
        volatility_window: usize,
        min_volatility: Decimal,
        internal_service_token: Option<String>,
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
            signal_queue_capacity,
            signal_max_inflight,
            account_equity_usd,
            risk_budget_pct,
            max_notional_per_trade,
            min_volatility,
            internal_service_token,
            volatility: RollingVolatility::new(volatility_window),
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
        let (signal_tx, mut signal_rx) = mpsc::channel::<PendingSignal>(self.signal_queue_capacity);
        let max_inflight = self.signal_max_inflight.max(1);
        let inflight = Arc::new(Semaphore::new(max_inflight));
        let risk_client_for_dispatch = risk_client.clone();
        let symbol_for_dispatch = self.symbol.clone();
        let internal_token_for_dispatch = self.internal_service_token.clone();
        let inflight_for_dispatch = inflight.clone();

        tokio::spawn(async move {
            while let Some(pending) = signal_rx.recv().await {
                let permit = match inflight_for_dispatch.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break,
                };
                let risk_client = risk_client_for_dispatch.clone();
                let symbol = symbol_for_dispatch.clone();
                let internal_token = internal_token_for_dispatch.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    Self::send_signal_with_retry(
                        pending.side,
                        pending.price,
                        risk_client,
                        &symbol,
                        pending.qty,
                        internal_token.as_deref(),
                    ).await;
                });
            }
        });

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
            self.record_close(close);

            let signal = self.strategy.push(close);

            tracing::debug!(%close, strategy = %strategy_name, "Kline processed");

            match signal {
                Signal::Buy => {
                    counter!("strategy_signals_total", "side" => "Buy").increment(1);
                    let qty = self.sized_qty(close);
                    info!(%close, %qty, "BUY signal detected");
                    if signal_tx.try_send(PendingSignal { side: "Buy", price: close, qty }).is_err() {
                        counter!("strategy_signal_enqueue_dropped_total").increment(1);
                        warn!("Signal queue full; dropping BUY signal");
                    }
                }
                Signal::Sell => {
                    counter!("strategy_signals_total", "side" => "Sell").increment(1);
                    let qty = self.sized_qty(close);
                    info!(%close, %qty, "SELL signal detected");
                    if signal_tx.try_send(PendingSignal { side: "Sell", price: close, qty }).is_err() {
                        counter!("strategy_signal_enqueue_dropped_total").increment(1);
                        warn!("Signal queue full; dropping SELL signal");
                    }
                }
                Signal::None => {}
            }
        }

        Ok(())
    }

    async fn send_signal_once(
        side: &str,
        price: Decimal,
        mut risk_client: RiskServiceClient<Channel>,
        symbol: &str,
        qty: Decimal,
        signal_id: &str,
        internal_token: Option<&str>,
    ) -> bool {
        let request = OrderRequest {
            signal_id: signal_id.to_string(),
            symbol: symbol.to_string(),
            side: side.to_string(),
            order_type: "Market".to_string(),
            price: price.to_string(),
            qty: qty.to_string(),
        };
        let mut grpc_request = tonic::Request::new(request);
        if moria_common::auth::attach_internal_token(&mut grpc_request, internal_token).is_err() {
            warn!(signal_id, "Failed to attach internal auth token");
            return false;
        }

        match tokio::time::timeout(Duration::from_secs(5), risk_client.validate_order(grpc_request)).await {
            Ok(Ok(response)) => {
                let decision = response.into_inner();
                if decision.approved {
                    counter!("strategy_risk_decision_total", "decision" => "approved").increment(1);
                    info!(signal_id, "Order approved by risk service");
                } else {
                    counter!("strategy_risk_decision_total", "decision" => "rejected").increment(1);
                    warn!(signal_id, reason = %decision.reason, "Order rejected by risk service");
                }
                true
            }
            Ok(Err(e)) => {
                counter!("strategy_risk_decision_total", "decision" => "error").increment(1);
                warn!(?e, signal_id, "Failed to contact risk service");
                false
            }
            Err(_) => {
                counter!("strategy_risk_decision_total", "decision" => "timeout").increment(1);
                warn!(signal_id, "Risk validation timed out");
                false
            }
        }
    }

    async fn send_signal_with_retry(
        side: &str,
        price: Decimal,
        risk_client: RiskServiceClient<Channel>,
        symbol: &str,
        qty: Decimal,
        internal_token: Option<&str>,
    ) {
        let signal_id = uuid::Uuid::new_v4().to_string();
        const MAX_ATTEMPTS: u32 = 3;
        for attempt in 1..=MAX_ATTEMPTS {
            let delivered = Self::send_signal_once(
                side,
                price,
                risk_client.clone(),
                symbol,
                qty,
                &signal_id,
                internal_token,
            )
            .await;
            if delivered {
                if attempt > 1 {
                    counter!("strategy_signal_retry_total", "result" => "recovered").increment(1);
                }
                return;
            }

            if attempt < MAX_ATTEMPTS {
                let backoff_ms = 250 * (1_u64 << (attempt - 1));
                let jitter_ms = (signal_id.as_bytes()[0] as u64) % 120;
                tokio::time::sleep(Duration::from_millis(backoff_ms + jitter_ms)).await;
                counter!("strategy_signal_retry_total", "result" => "retry").increment(1);
            }
        }

        counter!("strategy_signal_retry_total", "result" => "exhausted").increment(1);
        warn!(signal_id, "Exhausted signal delivery retries");
    }

    fn record_close(&mut self, close: Decimal) {
        self.volatility.push(close);
    }

    fn sized_qty(&self, price: Decimal) -> Decimal {
        moria_common::math::sized_qty(
            self.qty,
            price,
            self.rolling_volatility(),
            self.min_volatility,
            self.account_equity_usd,
            self.risk_budget_pct,
            self.max_notional_per_trade,
        )
    }

    fn rolling_volatility(&self) -> Decimal {
        self.volatility.stddev().unwrap_or(self.min_volatility)
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
