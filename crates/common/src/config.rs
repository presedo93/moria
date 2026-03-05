use rust_decimal::Decimal;
use serde::Deserialize;
use std::env;
use std::str::FromStr;
use anyhow::{Result, bail};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub bybit_api_key: String,
    pub bybit_api_secret: String,
    pub bybit_ws_url: String,
    pub bybit_rest_url: String,
    pub trading_pair: String,
    pub kline_interval: String,
    pub strategy_type: String,
    pub sma_short_period: usize,
    pub sma_long_period: usize,
    pub order_qty: Decimal,
    pub signal_queue_capacity: usize,
    pub signal_max_inflight: usize,
    pub max_position_size: Decimal,
    pub max_daily_loss: Decimal,
    pub max_portfolio_notional: Decimal,
    pub max_drawdown: Decimal,
    pub database_url: String,
    pub otel_endpoint: String,
    pub metrics_addr: Option<String>,
    pub market_data_grpc_addr: String,
    pub strategy_grpc_addr: String,
    pub risk_grpc_addr: String,
    pub order_grpc_addr: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            bybit_api_key: env::var("BYBIT_API_KEY").unwrap_or_default(),
            bybit_api_secret: env::var("BYBIT_API_SECRET").unwrap_or_default(),
            bybit_ws_url: env::var("BYBIT_WS_URL")
                .unwrap_or_else(|_| "wss://stream-testnet.bybit.com/v5/public/linear".into()),
            bybit_rest_url: env::var("BYBIT_REST_URL")
                .unwrap_or_else(|_| "https://api-testnet.bybit.com".into()),
            trading_pair: env::var("TRADING_PAIR").unwrap_or_else(|_| "BTCUSDT".into()),
            kline_interval: env::var("KLINE_INTERVAL").unwrap_or_else(|_| "1".into()),
            strategy_type: env::var("STRATEGY_TYPE")
                .unwrap_or_else(|_| "sma_crossover".into()),
            sma_short_period: env::var("SMA_SHORT_PERIOD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
            sma_long_period: env::var("SMA_LONG_PERIOD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
            order_qty: env::var("ORDER_QTY")
                .ok()
                .and_then(|v| Decimal::from_str(&v).ok())
                .unwrap_or_else(|| Decimal::from_str("0.001").expect("valid decimal")),
            signal_queue_capacity: env::var("SIGNAL_QUEUE_CAPACITY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(256),
            signal_max_inflight: env::var("SIGNAL_MAX_INFLIGHT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(16),
            max_position_size: env::var("MAX_POSITION_SIZE")
                .ok()
                .and_then(|v| Decimal::from_str(&v).ok())
                .unwrap_or_else(|| Decimal::from(1)),
            max_daily_loss: env::var("MAX_DAILY_LOSS")
                .ok()
                .and_then(|v| Decimal::from_str(&v).ok())
                .unwrap_or_else(|| Decimal::from(100)),
            max_portfolio_notional: env::var("MAX_PORTFOLIO_NOTIONAL")
                .ok()
                .and_then(|v| Decimal::from_str(&v).ok())
                .unwrap_or_else(|| Decimal::from(10_000)),
            max_drawdown: env::var("MAX_DRAWDOWN")
                .ok()
                .and_then(|v| Decimal::from_str(&v).ok())
                .unwrap_or_else(|| Decimal::from(500)),
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://moria:moria@localhost:5432/moria".into()),
            otel_endpoint: env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:4317".into()),
            metrics_addr: env::var("METRICS_ADDR").ok().and_then(|v| {
                let trimmed = v.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }),
            market_data_grpc_addr: env::var("MARKET_DATA_GRPC_ADDR")
                .unwrap_or_else(|_| "[::1]:50051".into()),
            strategy_grpc_addr: env::var("STRATEGY_GRPC_ADDR")
                .unwrap_or_else(|_| "[::1]:50052".into()),
            risk_grpc_addr: env::var("RISK_GRPC_ADDR").unwrap_or_else(|_| "[::1]:50053".into()),
            order_grpc_addr: env::var("ORDER_GRPC_ADDR").unwrap_or_else(|_| "[::1]:50054".into()),
        }
    }

    pub fn validate_for_service(&self, service: &str) -> Result<()> {
        if self.trading_pair.trim().is_empty() {
            bail!("TRADING_PAIR must not be empty");
        }
        if self.kline_interval.trim().is_empty() {
            bail!("KLINE_INTERVAL must not be empty");
        }
        if self.sma_short_period == 0 || self.sma_long_period == 0 {
            bail!("SMA periods must be > 0");
        }
        if self.sma_short_period >= self.sma_long_period {
            bail!("SMA_SHORT_PERIOD must be smaller than SMA_LONG_PERIOD");
        }
        if self.order_qty <= Decimal::ZERO {
            bail!("ORDER_QTY must be > 0");
        }
        if self.max_position_size <= Decimal::ZERO {
            bail!("MAX_POSITION_SIZE must be > 0");
        }
        if self.max_daily_loss <= Decimal::ZERO {
            bail!("MAX_DAILY_LOSS must be > 0");
        }
        if self.max_portfolio_notional <= Decimal::ZERO {
            bail!("MAX_PORTFOLIO_NOTIONAL must be > 0");
        }
        if self.max_drawdown <= Decimal::ZERO {
            bail!("MAX_DRAWDOWN must be > 0");
        }
        if self.signal_queue_capacity == 0 {
            bail!("SIGNAL_QUEUE_CAPACITY must be > 0");
        }
        if self.signal_max_inflight == 0 {
            bail!("SIGNAL_MAX_INFLIGHT must be > 0");
        }

        match service {
            "order" => {
                if self.bybit_api_key.trim().is_empty() || self.bybit_api_secret.trim().is_empty() {
                    bail!("BYBIT_API_KEY and BYBIT_API_SECRET are required for order service");
                }
            }
            "risk" | "reconciler" => {
                if self.database_url.trim().is_empty() {
                    bail!("DATABASE_URL is required for {service} service");
                }
            }
            _ => {}
        }

        Ok(())
    }
}
