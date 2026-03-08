use anyhow::{Result, bail};
use rust_decimal::Decimal;
use std::env;
use std::str::FromStr;

// --- Shared fragments (used by 3+ services) ---

#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    pub otel_endpoint: String,
    pub metrics_addr: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub database_url: String,
}

#[derive(Debug, Clone)]
pub struct ServiceAuthConfig {
    pub internal_service_token: Option<String>,
}

fn telemetry_from_env() -> TelemetryConfig {
    TelemetryConfig {
        otel_endpoint: env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:4317".into()),
        metrics_addr: optional_trimmed_env("METRICS_ADDR"),
    }
}

fn database_from_env() -> DatabaseConfig {
    DatabaseConfig {
        database_url: env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://moria:moria@localhost:5432/moria".into()),
    }
}

fn auth_from_env() -> ServiceAuthConfig {
    ServiceAuthConfig {
        internal_service_token: optional_trimmed_env("INTERNAL_SERVICE_TOKEN"),
    }
}

fn optional_trimmed_env(key: &str) -> Option<String> {
    env::var(key).ok().and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
    })
}

fn env_decimal(key: &str, default: Decimal) -> Decimal {
    env::var(key)
        .ok()
        .and_then(|v| Decimal::from_str(&v).ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// --- Per-service configs ---

#[derive(Debug, Clone)]
pub struct MarketDataConfig {
    pub bybit_ws_url: String,
    pub trading_pair: String,
    pub kline_interval: String,
    pub market_data_grpc_addr: String,
    pub telemetry: TelemetryConfig,
}

impl MarketDataConfig {
    pub fn from_env() -> Result<Self> {
        let config = Self {
            bybit_ws_url: env::var("BYBIT_WS_URL")
                .unwrap_or_else(|_| "wss://stream-testnet.bybit.com/v5/public/linear".into()),
            trading_pair: env::var("TRADING_PAIR").unwrap_or_else(|_| "BTCUSDT".into()),
            kline_interval: env::var("KLINE_INTERVAL").unwrap_or_else(|_| "1".into()),
            market_data_grpc_addr: env::var("MARKET_DATA_GRPC_ADDR")
                .unwrap_or_else(|_| "[::1]:50051".into()),
            telemetry: telemetry_from_env(),
        };
        if config.trading_pair.trim().is_empty() {
            bail!("TRADING_PAIR must not be empty");
        }
        if config.kline_interval.trim().is_empty() {
            bail!("KLINE_INTERVAL must not be empty");
        }
        Ok(config)
    }
}

#[derive(Debug, Clone)]
pub struct OrderConfig {
    pub bybit_rest_url: String,
    pub bybit_api_key: String,
    pub bybit_api_secret: String,
    pub order_grpc_addr: String,
    pub auth: ServiceAuthConfig,
    pub telemetry: TelemetryConfig,
}

impl OrderConfig {
    pub fn from_env() -> Result<Self> {
        let api_key = env::var("BYBIT_API_KEY").unwrap_or_default();
        let api_secret = env::var("BYBIT_API_SECRET").unwrap_or_default();
        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            bail!("BYBIT_API_KEY and BYBIT_API_SECRET are required for order service");
        }
        Ok(Self {
            bybit_rest_url: env::var("BYBIT_REST_URL")
                .unwrap_or_else(|_| "https://api-testnet.bybit.com".into()),
            bybit_api_key: api_key,
            bybit_api_secret: api_secret,
            order_grpc_addr: env::var("ORDER_GRPC_ADDR")
                .unwrap_or_else(|_| "[::1]:50054".into()),
            auth: auth_from_env(),
            telemetry: telemetry_from_env(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub trading_pair: String,
    pub kline_interval: String,
    pub strategy_type: String,
    pub sma_short_period: usize,
    pub sma_long_period: usize,
    pub order_qty: Decimal,
    pub account_equity_usd: Decimal,
    pub risk_budget_pct: Decimal,
    pub max_notional_per_trade: Decimal,
    pub volatility_window: usize,
    pub min_volatility: Decimal,
    pub signal_queue_capacity: usize,
    pub signal_max_inflight: usize,
    pub market_data_grpc_addr: String,
    pub risk_grpc_addr: String,
    pub bybit_rest_url: String,
    pub auth: ServiceAuthConfig,
    pub telemetry: TelemetryConfig,
}

impl StrategyConfig {
    pub fn from_env() -> Result<Self> {
        let config = Self {
            trading_pair: env::var("TRADING_PAIR").unwrap_or_else(|_| "BTCUSDT".into()),
            kline_interval: env::var("KLINE_INTERVAL").unwrap_or_else(|_| "1".into()),
            strategy_type: env::var("STRATEGY_TYPE").unwrap_or_else(|_| "sma_crossover".into()),
            sma_short_period: env_usize("SMA_SHORT_PERIOD", 10),
            sma_long_period: env_usize("SMA_LONG_PERIOD", 30),
            order_qty: env_decimal("ORDER_QTY", Decimal::from_str("0.001").expect("valid")),
            account_equity_usd: env_decimal("ACCOUNT_EQUITY_USD", Decimal::from(10_000)),
            risk_budget_pct: env_decimal("RISK_BUDGET_PCT", Decimal::from_str("0.01").expect("valid")),
            max_notional_per_trade: env_decimal("MAX_NOTIONAL_PER_TRADE", Decimal::from(2_500)),
            volatility_window: env_usize("VOLATILITY_WINDOW", 20),
            min_volatility: env_decimal("MIN_VOLATILITY", Decimal::from_str("0.001").expect("valid")),
            signal_queue_capacity: env_usize("SIGNAL_QUEUE_CAPACITY", 256),
            signal_max_inflight: env_usize("SIGNAL_MAX_INFLIGHT", 16),
            market_data_grpc_addr: env::var("MARKET_DATA_GRPC_ADDR")
                .unwrap_or_else(|_| "[::1]:50051".into()),
            risk_grpc_addr: env::var("RISK_GRPC_ADDR").unwrap_or_else(|_| "[::1]:50053".into()),
            bybit_rest_url: env::var("BYBIT_REST_URL")
                .unwrap_or_else(|_| "https://api-testnet.bybit.com".into()),
            auth: auth_from_env(),
            telemetry: telemetry_from_env(),
        };
        validate_strategy_fields(&config)?;
        Ok(config)
    }
}

fn validate_strategy_fields(c: &StrategyConfig) -> Result<()> {
    if c.trading_pair.trim().is_empty() {
        bail!("TRADING_PAIR must not be empty");
    }
    if c.kline_interval.trim().is_empty() {
        bail!("KLINE_INTERVAL must not be empty");
    }
    if c.sma_short_period == 0 || c.sma_long_period == 0 {
        bail!("SMA periods must be > 0");
    }
    if c.sma_short_period >= c.sma_long_period {
        bail!("SMA_SHORT_PERIOD must be smaller than SMA_LONG_PERIOD");
    }
    if c.order_qty <= Decimal::ZERO {
        bail!("ORDER_QTY must be > 0");
    }
    if c.account_equity_usd <= Decimal::ZERO {
        bail!("ACCOUNT_EQUITY_USD must be > 0");
    }
    if c.risk_budget_pct <= Decimal::ZERO || c.risk_budget_pct >= Decimal::ONE {
        bail!("RISK_BUDGET_PCT must be in (0, 1)");
    }
    if c.max_notional_per_trade <= Decimal::ZERO {
        bail!("MAX_NOTIONAL_PER_TRADE must be > 0");
    }
    if c.volatility_window < 2 {
        bail!("VOLATILITY_WINDOW must be >= 2");
    }
    if c.min_volatility <= Decimal::ZERO {
        bail!("MIN_VOLATILITY must be > 0");
    }
    if c.signal_queue_capacity == 0 {
        bail!("SIGNAL_QUEUE_CAPACITY must be > 0");
    }
    if c.signal_max_inflight == 0 {
        bail!("SIGNAL_MAX_INFLIGHT must be > 0");
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct RiskConfig {
    pub max_position_size: Decimal,
    pub max_daily_loss: Decimal,
    pub max_portfolio_notional: Decimal,
    pub max_drawdown: Decimal,
    pub risk_grpc_addr: String,
    pub order_grpc_addr: String,
    pub db: DatabaseConfig,
    pub auth: ServiceAuthConfig,
    pub telemetry: TelemetryConfig,
}

impl RiskConfig {
    pub fn from_env() -> Result<Self> {
        let db = database_from_env();
        if db.database_url.trim().is_empty() {
            bail!("DATABASE_URL is required for risk service");
        }
        let config = Self {
            max_position_size: env_decimal("MAX_POSITION_SIZE", Decimal::from(1)),
            max_daily_loss: env_decimal("MAX_DAILY_LOSS", Decimal::from(100)),
            max_portfolio_notional: env_decimal("MAX_PORTFOLIO_NOTIONAL", Decimal::from(10_000)),
            max_drawdown: env_decimal("MAX_DRAWDOWN", Decimal::from(500)),
            risk_grpc_addr: env::var("RISK_GRPC_ADDR").unwrap_or_else(|_| "[::1]:50053".into()),
            order_grpc_addr: env::var("ORDER_GRPC_ADDR").unwrap_or_else(|_| "[::1]:50054".into()),
            db,
            auth: auth_from_env(),
            telemetry: telemetry_from_env(),
        };
        if config.max_position_size <= Decimal::ZERO {
            bail!("MAX_POSITION_SIZE must be > 0");
        }
        if config.max_daily_loss <= Decimal::ZERO {
            bail!("MAX_DAILY_LOSS must be > 0");
        }
        if config.max_portfolio_notional <= Decimal::ZERO {
            bail!("MAX_PORTFOLIO_NOTIONAL must be > 0");
        }
        if config.max_drawdown <= Decimal::ZERO {
            bail!("MAX_DRAWDOWN must be > 0");
        }
        Ok(config)
    }
}

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    pub order_grpc_addr: String,
    pub db: DatabaseConfig,
    pub auth: ServiceAuthConfig,
    pub telemetry: TelemetryConfig,
}

impl ReconcilerConfig {
    pub fn from_env() -> Result<Self> {
        let db = database_from_env();
        if db.database_url.trim().is_empty() {
            bail!("DATABASE_URL is required for reconciler service");
        }
        Ok(Self {
            order_grpc_addr: env::var("ORDER_GRPC_ADDR")
                .unwrap_or_else(|_| "[::1]:50054".into()),
            db,
            auth: auth_from_env(),
            telemetry: telemetry_from_env(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct BacktestConfig {
    pub trading_pair: String,
    pub sma_short_period: usize,
    pub sma_long_period: usize,
    pub order_qty: Decimal,
    pub account_equity_usd: Decimal,
    pub risk_budget_pct: Decimal,
    pub max_notional_per_trade: Decimal,
    pub volatility_window: usize,
    pub min_volatility: Decimal,
    pub database_url: String,
    pub telemetry: TelemetryConfig,
}

impl BacktestConfig {
    pub fn from_env() -> Result<Self> {
        let config = Self {
            trading_pair: env::var("TRADING_PAIR").unwrap_or_else(|_| "BTCUSDT".into()),
            sma_short_period: env_usize("SMA_SHORT_PERIOD", 10),
            sma_long_period: env_usize("SMA_LONG_PERIOD", 30),
            order_qty: env_decimal("ORDER_QTY", Decimal::from_str("0.001").expect("valid")),
            account_equity_usd: env_decimal("ACCOUNT_EQUITY_USD", Decimal::from(10_000)),
            risk_budget_pct: env_decimal("RISK_BUDGET_PCT", Decimal::from_str("0.01").expect("valid")),
            max_notional_per_trade: env_decimal("MAX_NOTIONAL_PER_TRADE", Decimal::from(2_500)),
            volatility_window: env_usize("VOLATILITY_WINDOW", 20),
            min_volatility: env_decimal("MIN_VOLATILITY", Decimal::from_str("0.001").expect("valid")),
            database_url: env::var("DATABASE_URL").unwrap_or_default(),
            telemetry: telemetry_from_env(),
        };
        validate_backtest_fields(&config)?;
        Ok(config)
    }
}

fn validate_backtest_fields(c: &BacktestConfig) -> Result<()> {
    if c.trading_pair.trim().is_empty() {
        bail!("TRADING_PAIR must not be empty");
    }
    if c.sma_short_period == 0 || c.sma_long_period == 0 {
        bail!("SMA periods must be > 0");
    }
    if c.sma_short_period >= c.sma_long_period {
        bail!("SMA_SHORT_PERIOD must be smaller than SMA_LONG_PERIOD");
    }
    if c.order_qty <= Decimal::ZERO {
        bail!("ORDER_QTY must be > 0");
    }
    if c.account_equity_usd <= Decimal::ZERO {
        bail!("ACCOUNT_EQUITY_USD must be > 0");
    }
    if c.risk_budget_pct <= Decimal::ZERO || c.risk_budget_pct >= Decimal::ONE {
        bail!("RISK_BUDGET_PCT must be in (0, 1)");
    }
    if c.max_notional_per_trade <= Decimal::ZERO {
        bail!("MAX_NOTIONAL_PER_TRADE must be > 0");
    }
    if c.volatility_window < 2 {
        bail!("VOLATILITY_WINDOW must be >= 2");
    }
    if c.min_volatility <= Decimal::ZERO {
        bail!("MIN_VOLATILITY must be > 0");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Env vars are process-global, so serialize tests that set them
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
        let _lock = ENV_LOCK.lock().unwrap();
        let mut old: Vec<(&str, Option<String>)> = Vec::new();
        for (k, v) in vars {
            old.push((k, env::var(k).ok()));
            // SAFETY: tests are serialized via ENV_LOCK so no concurrent access
            unsafe { env::set_var(k, v) };
        }
        f();
        for (k, prev) in old {
            // SAFETY: tests are serialized via ENV_LOCK so no concurrent access
            unsafe {
                match prev {
                    Some(v) => env::set_var(k, v),
                    None => env::remove_var(k),
                }
            }
        }
    }

    #[test]
    fn market_data_config_valid() {
        with_env(&[], || {
            let cfg = MarketDataConfig::from_env().unwrap();
            assert_eq!(cfg.trading_pair, "BTCUSDT");
        });
    }

    #[test]
    fn order_config_rejects_missing_keys() {
        with_env(&[("BYBIT_API_KEY", ""), ("BYBIT_API_SECRET", "")], || {
            assert!(OrderConfig::from_env().is_err());
        });
    }

    #[test]
    fn order_config_valid() {
        with_env(&[("BYBIT_API_KEY", "key"), ("BYBIT_API_SECRET", "secret")], || {
            let cfg = OrderConfig::from_env().unwrap();
            assert_eq!(cfg.bybit_api_key, "key");
        });
    }

    #[test]
    fn strategy_config_rejects_bad_sma() {
        with_env(&[("SMA_SHORT_PERIOD", "50"), ("SMA_LONG_PERIOD", "30")], || {
            assert!(StrategyConfig::from_env().is_err());
        });
    }

    #[test]
    fn risk_config_valid() {
        with_env(&[], || {
            let cfg = RiskConfig::from_env().unwrap();
            assert_eq!(cfg.max_position_size, Decimal::from(1));
        });
    }

    #[test]
    fn reconciler_config_valid() {
        with_env(&[], || {
            let cfg = ReconcilerConfig::from_env().unwrap();
            assert!(!cfg.db.database_url.is_empty());
        });
    }
}
