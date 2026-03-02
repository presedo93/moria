use rust_decimal::Decimal;

use crate::sma::SmaCrossover;

/// Signal emitted by a strategy after processing a price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Buy,
    Sell,
    None,
}

/// Core trait for all trading strategies.
///
/// A strategy receives sequential close prices and emits trading signals.
pub trait Strategy: Send {
    /// Human-readable name stored in the database `strategy` column.
    fn name(&self) -> &str;

    /// Feed the next close price and return a signal.
    fn push(&mut self, close: Decimal) -> Signal;
}

/// Create a strategy from its type name and configuration.
///
/// # Supported types
/// - `"sma_crossover"` — Simple Moving Average crossover
///
/// Returns `None` if `strategy_type` is unknown.
pub fn create_strategy(
    strategy_type: &str,
    short_period: usize,
    long_period: usize,
) -> Option<Box<dyn Strategy>> {
    match strategy_type {
        "sma_crossover" => Some(Box::new(SmaCrossover::new(short_period, long_period))),
        _ => None,
    }
}
