use anyhow::{Context, Result, bail};
use chrono::Utc;
use moria_common::config::BacktestConfig;
use moria_common::math::{RollingSma, RollingVolatility};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::fs::File;
use std::io::{BufRead, BufReader};
use uuid::Uuid;

#[derive(Default)]
struct PortfolioState {
    qty: Decimal,
    avg_entry: Decimal,
    realized_pnl: Decimal,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = BacktestConfig::from_env()?;
    moria_common::telemetry::init_tracing("backtest")?;

    let csv_path = std::env::var("BACKTEST_CSV_PATH")
        .context("BACKTEST_CSV_PATH is required (CSV with a 'close' column)")?;
    let fee_bps = std::env::var("BACKTEST_FEE_BPS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(5.0);
    let closes = load_closes(&csv_path)?;
    if closes.len() < config.sma_long_period + 2 {
        bail!("not enough rows in backtest CSV for configured SMA_LONG_PERIOD");
    }

    let (metrics, params) = run_backtest(&config, &closes, fee_bps)?;
    println!(
        "backtest_complete strategy=sma_crossover symbol={} total_return_pct={} max_drawdown_pct={} sharpe={} trades={} win_rate={}",
        config.trading_pair,
        metrics.total_return_pct,
        metrics.max_drawdown_pct,
        metrics.sharpe_ratio,
        metrics.trades_count,
        metrics.win_rate
    );

    if !config.database_url.is_empty() {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&config.database_url)
            .await
            .context("failed to connect to postgres for leaderboard write")?;
        sqlx::query(
            "INSERT INTO backtest_runs
                (id, strategy, symbol, params, total_return_pct, max_drawdown_pct, sharpe_ratio, trades_count, win_rate, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(Uuid::new_v4())
        .bind("sma_crossover")
        .bind(&config.trading_pair)
        .bind(params)
        .bind(metrics.total_return_pct)
        .bind(metrics.max_drawdown_pct)
        .bind(metrics.sharpe_ratio)
        .bind(metrics.trades_count)
        .bind(metrics.win_rate)
        .bind(Utc::now())
        .execute(&pool)
        .await?;
    }

    moria_common::telemetry::shutdown_tracing();
    Ok(())
}

struct BacktestMetrics {
    total_return_pct: Decimal,
    max_drawdown_pct: Decimal,
    sharpe_ratio: Decimal,
    trades_count: i32,
    win_rate: Decimal,
}

fn run_backtest(config: &BacktestConfig, closes: &[Decimal], fee_bps: f64) -> Result<(BacktestMetrics, serde_json::Value)> {
    let mut short_sma = RollingSma::new(config.sma_short_period);
    let mut long_sma = RollingSma::new(config.sma_long_period);
    let mut volatility = RollingVolatility::new(config.volatility_window);
    let mut prev_short_above_long: Option<bool> = None;
    let mut state = PortfolioState::default();
    let mut equity_curve = Vec::with_capacity(closes.len());
    let mut closed_trade_pnls: Vec<Decimal> = Vec::new();

    for close in closes {
        let short = short_sma.push(*close);
        let long = long_sma.push(*close);
        volatility.push(*close);

        if let (Some(short), Some(long)) = (short, long) {
            let short_above = short > long;
            let signal = match prev_short_above_long {
                Some(prev) if prev != short_above => {
                    if short_above { Some("Buy") } else { Some("Sell") }
                }
                _ => None,
            };
            prev_short_above_long = Some(short_above);

            if let Some(side) = signal {
                let vol = volatility.stddev().unwrap_or(config.min_volatility);
                let qty = moria_common::math::sized_qty(
                    config.order_qty,
                    *close,
                    vol,
                    config.min_volatility,
                    config.account_equity_usd,
                    config.risk_budget_pct,
                    config.max_notional_per_trade,
                );
                let trade_realized = apply_fill(&mut state, side, qty, *close);
                let fee = *close * qty * Decimal::from_f64(fee_bps / 10_000.0).unwrap_or(Decimal::ZERO);
                state.realized_pnl -= fee;
                if trade_realized != Decimal::ZERO {
                    closed_trade_pnls.push(trade_realized - fee);
                }
            }
        }

        let unrealized = moria_common::position::unrealized_pnl(state.qty, state.avg_entry, *close);
        equity_curve.push(config.account_equity_usd + state.realized_pnl + unrealized);
    }

    let first = *equity_curve.first().unwrap_or(&config.account_equity_usd);
    let last = *equity_curve.last().unwrap_or(&config.account_equity_usd);
    let total_return_pct = if first > Decimal::ZERO {
        ((last - first) / first) * Decimal::from(100)
    } else {
        Decimal::ZERO
    };
    let max_drawdown_pct = max_drawdown_pct(&equity_curve);
    let sharpe_ratio = sharpe_ratio(&equity_curve);
    let trades_count = closed_trade_pnls.len() as i32;
    let wins = closed_trade_pnls.iter().filter(|p| **p > Decimal::ZERO).count() as i32;
    let win_rate = if trades_count > 0 {
        Decimal::from(wins) / Decimal::from(trades_count)
    } else {
        Decimal::ZERO
    };

    let params = json!({
        "short_period": config.sma_short_period,
        "long_period": config.sma_long_period,
        "order_qty": config.order_qty.to_string(),
        "account_equity_usd": config.account_equity_usd.to_string(),
        "risk_budget_pct": config.risk_budget_pct.to_string(),
        "max_notional_per_trade": config.max_notional_per_trade.to_string(),
        "volatility_window": config.volatility_window,
        "fee_bps": fee_bps
    });

    Ok((BacktestMetrics {
        total_return_pct,
        max_drawdown_pct,
        sharpe_ratio,
        trades_count,
        win_rate,
    }, params))
}

fn apply_fill(state: &mut PortfolioState, side: &str, qty: Decimal, price: Decimal) -> Decimal {
    let result = moria_common::position::apply_fill(state.qty, state.avg_entry, side, price, qty);
    state.qty = result.new_qty;
    state.avg_entry = result.new_avg_entry;
    state.realized_pnl += result.realized_pnl;
    result.realized_pnl
}

fn max_drawdown_pct(equity: &[Decimal]) -> Decimal {
    if equity.is_empty() {
        return Decimal::ZERO;
    }
    let mut peak = equity[0];
    let mut max_dd = Decimal::ZERO;
    for value in equity {
        if *value > peak {
            peak = *value;
        }
        if peak > Decimal::ZERO {
            let dd = (peak - *value) / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }
    max_dd * Decimal::from(100)
}

fn sharpe_ratio(equity: &[Decimal]) -> Decimal {
    if equity.len() < 3 {
        return Decimal::ZERO;
    }
    let mut rets = Vec::with_capacity(equity.len() - 1);
    for pair in equity.windows(2) {
        let prev = pair[0].to_f64().unwrap_or(0.0);
        let next = pair[1].to_f64().unwrap_or(0.0);
        if prev > 0.0 && next > 0.0 {
            rets.push((next / prev) - 1.0);
        }
    }
    if rets.len() < 2 {
        return Decimal::ZERO;
    }
    let mean = rets.iter().sum::<f64>() / rets.len() as f64;
    let variance = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (rets.len() - 1) as f64;
    let stddev = variance.sqrt();
    if stddev <= f64::EPSILON {
        return Decimal::ZERO;
    }
    Decimal::from_f64((mean / stddev) * (252.0_f64).sqrt()).unwrap_or(Decimal::ZERO)
}

fn load_closes(path: &str) -> Result<Vec<Decimal>> {
    let file = File::open(path).with_context(|| format!("failed to open {path}"))?;
    let mut lines = BufReader::new(file).lines();
    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("CSV is empty"))??;
    let columns: Vec<&str> = header.split(',').collect();
    let close_idx = columns
        .iter()
        .position(|c| c.trim().eq_ignore_ascii_case("close"))
        .ok_or_else(|| anyhow::anyhow!("CSV must contain a 'close' column"))?;

    let mut closes = Vec::new();
    for line in lines {
        let line = line?;
        let parts: Vec<&str> = line.split(',').collect();
        if let Some(raw) = parts.get(close_idx) {
            if let Ok(price) = Decimal::from_str_exact(raw.trim()) {
                closes.push(price);
            }
        }
    }

    Ok(closes)
}
