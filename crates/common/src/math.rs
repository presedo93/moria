use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use std::collections::VecDeque;

/// Computes the log-return between two consecutive prices.
/// Returns `None` if either price is non-positive.
pub fn price_return(prev: Decimal, next: Decimal) -> Option<f64> {
    let prev = prev.to_f64()?;
    let next = next.to_f64()?;
    (prev > 0.0 && next > 0.0).then_some((next / prev) - 1.0)
}

/// Rolling standard deviation of price returns over a fixed window of close prices.
///
/// Uses an incremental sum/sum-of-squares approach so that `push` is O(1).
pub struct RollingVolatility {
    max_closes: usize,
    closes: VecDeque<Decimal>,
    returns_sum: f64,
    returns_sum_sq: f64,
    returns_count: usize,
}

impl RollingVolatility {
    pub fn new(max_closes: usize) -> Self {
        Self {
            max_closes,
            closes: VecDeque::with_capacity(max_closes.saturating_add(1)),
            returns_sum: 0.0,
            returns_sum_sq: 0.0,
            returns_count: 0,
        }
    }

    pub fn push(&mut self, close: Decimal) {
        if self.max_closes == 0 {
            return;
        }

        if self.closes.len() == self.max_closes {
            let removed = self.closes.pop_front().unwrap_or(Decimal::ZERO);
            if let Some(next) = self.closes.front().copied() {
                self.remove_return(removed, next);
            }
        }

        if let Some(prev) = self.closes.back().copied() {
            self.add_return(prev, close);
        }

        self.closes.push_back(close);
    }

    /// Returns the sample standard deviation of returns, or `None` if fewer than 2 returns exist.
    pub fn stddev(&self) -> Option<Decimal> {
        if self.returns_count < 2 {
            return None;
        }

        let mean = self.returns_sum / self.returns_count as f64;
        let variance = (self.returns_sum_sq - (self.returns_sum * mean))
            / (self.returns_count - 1) as f64;
        let stddev = variance.max(0.0).sqrt();
        Decimal::from_f64(stddev)
    }

    fn add_return(&mut self, prev: Decimal, next: Decimal) {
        let Some(value) = price_return(prev, next) else {
            return;
        };
        self.returns_sum += value;
        self.returns_sum_sq += value * value;
        self.returns_count += 1;
    }

    fn remove_return(&mut self, prev: Decimal, next: Decimal) {
        let Some(value) = price_return(prev, next) else {
            return;
        };
        self.returns_sum -= value;
        self.returns_sum_sq -= value * value;
        self.returns_count = self.returns_count.saturating_sub(1);
    }
}

/// Simple rolling moving average over a fixed window.
pub struct RollingSma {
    period: usize,
    window: VecDeque<Decimal>,
    sum: Decimal,
}

impl RollingSma {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            window: VecDeque::with_capacity(period.saturating_add(1)),
            sum: Decimal::ZERO,
        }
    }

    /// Push a value and return the SMA if the window is full.
    pub fn push(&mut self, value: Decimal) -> Option<Decimal> {
        if self.period == 0 {
            return None;
        }

        self.window.push_back(value);
        self.sum += value;

        if self.window.len() > self.period {
            if let Some(expired) = self.window.pop_front() {
                self.sum -= expired;
            }
        }

        (self.window.len() == self.period).then(|| self.sum / Decimal::from(self.period))
    }
}

/// Compute volatility-adjusted order quantity.
///
/// Given equity, risk budget, and current volatility, produces a position size
/// that targets a fixed dollar risk. The result is clamped to 0.25×–3.0× the base
/// quantity and rounded to 6 decimal places.
pub fn sized_qty(
    base_qty: Decimal,
    price: Decimal,
    volatility: Decimal,
    min_volatility: Decimal,
    account_equity_usd: Decimal,
    risk_budget_pct: Decimal,
    max_notional_per_trade: Decimal,
) -> Decimal {
    if price <= Decimal::ZERO {
        return base_qty;
    }

    let base_notional = base_qty * price;
    if base_notional <= Decimal::ZERO {
        return base_qty;
    }

    let vol = volatility.max(min_volatility);
    let risk_budget_usd = account_equity_usd * risk_budget_pct;
    let target_notional = (risk_budget_usd / vol).min(max_notional_per_trade);
    let ratio = target_notional / base_notional;
    let ratio_f = ratio.to_f64().unwrap_or(1.0).clamp(0.25, 3.0);
    let scaled = base_qty * Decimal::from_f64(ratio_f).unwrap_or(Decimal::ONE);
    (scaled * Decimal::from(1_000_000)).round() / Decimal::from(1_000_000)
}
