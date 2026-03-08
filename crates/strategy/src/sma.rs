use crate::strategy::{Signal, Strategy};
use rust_decimal::Decimal;
use std::collections::VecDeque;

pub struct SmaCrossover {
    short_period: usize,
    long_period: usize,
    prices: VecDeque<Decimal>,
    short_sum: Decimal,
    long_sum: Decimal,
    prev_short_above_long: Option<bool>,
}

impl SmaCrossover {
    pub fn new(short_period: usize, long_period: usize) -> Self {
        assert!(
            short_period < long_period,
            "short period must be less than long period"
        );
        Self {
            short_period,
            long_period,
            prices: VecDeque::with_capacity(long_period + 1),
            short_sum: Decimal::ZERO,
            long_sum: Decimal::ZERO,
            prev_short_above_long: None,
        }
    }

    fn push_price(&mut self, close: Decimal) -> Signal {
        self.prices.push_back(close);
        self.short_sum += close;
        self.long_sum += close;

        if self.prices.len() > self.short_period {
            let exited_short_idx = self.prices.len() - self.short_period - 1;
            self.short_sum -= self.prices[exited_short_idx];
        }

        if self.prices.len() > self.long_period {
            if let Some(expired) = self.prices.pop_front() {
                self.long_sum -= expired;
            }
        }

        // Need at least long_period prices to compute both SMAs
        if self.prices.len() < self.long_period {
            return Signal::None;
        }

        let short_sma = self.short_sum / Decimal::from(self.short_period);
        let long_sma = self.long_sum / Decimal::from(self.long_period);
        let short_above = short_sma > long_sma;

        let signal = match self.prev_short_above_long {
            Some(was_above) if was_above != short_above => {
                if short_above {
                    Signal::Buy
                } else {
                    Signal::Sell
                }
            }
            _ => Signal::None,
        };

        self.prev_short_above_long = Some(short_above);
        signal
    }

    fn compute_sma(&self, period: usize) -> Decimal {
        match period {
            p if p == self.short_period => self.short_sum / Decimal::from(period),
            p if p == self.long_period => self.long_sum / Decimal::from(period),
            _ => {
                let len = self.prices.len();
                let sum: Decimal = self.prices.iter().skip(len - period).copied().sum();
                sum / Decimal::from(period)
            }
        }
    }

    #[cfg(test)]
    pub fn short_sma(&self) -> Option<Decimal> {
        if self.prices.len() >= self.short_period {
            Some(self.compute_sma(self.short_period))
        } else {
            None
        }
    }

    #[cfg(test)]
    pub fn long_sma(&self) -> Option<Decimal> {
        if self.prices.len() >= self.long_period {
            Some(self.compute_sma(self.long_period))
        } else {
            None
        }
    }
}

impl Strategy for SmaCrossover {
    fn name(&self) -> &str {
        "sma_crossover"
    }

    fn push(&mut self, close: Decimal) -> Signal {
        self.push_price(close)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn no_signal_until_enough_data() {
        let mut sma = SmaCrossover::new(2, 4);
        assert_eq!(sma.push(dec("10")), Signal::None);
        assert_eq!(sma.push(dec("11")), Signal::None);
        assert_eq!(sma.push(dec("12")), Signal::None);
        // 4th price: first time we have both SMAs, establishes baseline
        assert_eq!(sma.push(dec("13")), Signal::None);
    }

    #[test]
    fn detects_buy_crossover() {
        let mut sma = SmaCrossover::new(2, 4);

        // Declining prices: short SMA < long SMA
        sma.push(dec("20"));
        sma.push(dec("18"));
        sma.push(dec("16"));
        assert_eq!(sma.push(dec("14")), Signal::None); // baseline: short < long

        // Now price jumps up
        assert_eq!(sma.push(dec("25")), Signal::Buy);
    }

    #[test]
    fn detects_sell_crossover() {
        let mut sma = SmaCrossover::new(2, 4);

        // Rising prices: short SMA > long SMA
        sma.push(dec("10"));
        sma.push(dec("12"));
        sma.push(dec("14"));
        assert_eq!(sma.push(dec("16")), Signal::None); // baseline: short > long

        // Price drops
        assert_eq!(sma.push(dec("5")), Signal::Sell);
    }

    #[test]
    fn no_signal_when_no_crossover() {
        let mut sma = SmaCrossover::new(2, 4);

        // Consistently rising: short always above long
        for price in ["10", "11", "12", "13", "14", "15", "16"] {
            let signal = sma.push(dec(price));
            assert_eq!(signal, Signal::None);
        }
    }

    #[test]
    fn sma_values_correct() {
        let mut sma = SmaCrossover::new(2, 3);
        sma.push(dec("10"));
        sma.push(dec("20"));
        sma.push(dec("30"));

        assert_eq!(sma.short_sma(), Some(dec("25"))); // (20+30)/2
        assert_eq!(sma.long_sma(), Some(dec("20"))); // (10+20+30)/3
    }

    #[test]
    fn window_slides_correctly() {
        let mut sma = SmaCrossover::new(2, 3);
        sma.push(dec("10"));
        sma.push(dec("20"));
        sma.push(dec("30"));
        sma.push(dec("40")); // window is now [20, 30, 40]

        assert_eq!(sma.short_sma(), Some(dec("35"))); // (30+40)/2
        assert_eq!(sma.long_sma(), Some(dec("30"))); // (20+30+40)/3
    }

    #[test]
    #[should_panic(expected = "short period must be less than long period")]
    fn panics_on_invalid_periods() {
        SmaCrossover::new(10, 5);
    }
}
