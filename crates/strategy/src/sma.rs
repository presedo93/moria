use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossoverSignal {
    Buy,
    Sell,
    None,
}

pub struct SmaCrossover {
    short_period: usize,
    long_period: usize,
    prices: VecDeque<f64>,
    prev_short_above_long: Option<bool>,
}

impl SmaCrossover {
    pub fn new(short_period: usize, long_period: usize) -> Self {
        assert!(short_period < long_period, "short period must be less than long period");
        Self {
            short_period,
            long_period,
            prices: VecDeque::with_capacity(long_period + 1),
            prev_short_above_long: None,
        }
    }

    /// Push a new close price and return a crossover signal if one occurred.
    pub fn push(&mut self, close: f64) -> CrossoverSignal {
        self.prices.push_back(close);
        if self.prices.len() > self.long_period {
            self.prices.pop_front();
        }

        // Need at least long_period prices to compute both SMAs
        if self.prices.len() < self.long_period {
            return CrossoverSignal::None;
        }

        let short_sma = self.compute_sma(self.short_period);
        let long_sma = self.compute_sma(self.long_period);
        let short_above = short_sma > long_sma;

        let signal = match self.prev_short_above_long {
            Some(was_above) if was_above != short_above => {
                if short_above {
                    CrossoverSignal::Buy
                } else {
                    CrossoverSignal::Sell
                }
            }
            _ => CrossoverSignal::None,
        };

        self.prev_short_above_long = Some(short_above);
        signal
    }

    fn compute_sma(&self, period: usize) -> f64 {
        let len = self.prices.len();
        let sum: f64 = self.prices.iter().skip(len - period).sum();
        sum / period as f64
    }

    pub fn short_sma(&self) -> Option<f64> {
        if self.prices.len() >= self.short_period {
            Some(self.compute_sma(self.short_period))
        } else {
            None
        }
    }

    pub fn long_sma(&self) -> Option<f64> {
        if self.prices.len() >= self.long_period {
            Some(self.compute_sma(self.long_period))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_signal_until_enough_data() {
        let mut sma = SmaCrossover::new(2, 4);
        assert_eq!(sma.push(10.0), CrossoverSignal::None);
        assert_eq!(sma.push(11.0), CrossoverSignal::None);
        assert_eq!(sma.push(12.0), CrossoverSignal::None);
        // 4th price: first time we have both SMAs, establishes baseline
        assert_eq!(sma.push(13.0), CrossoverSignal::None);
    }

    #[test]
    fn detects_buy_crossover() {
        let mut sma = SmaCrossover::new(2, 4);

        // Declining prices: short SMA < long SMA
        // Prices: 20, 18, 16, 14 → short_sma(2)=(16+14)/2=15, long_sma(4)=17
        sma.push(20.0);
        sma.push(18.0);
        sma.push(16.0);
        assert_eq!(sma.push(14.0), CrossoverSignal::None); // baseline: short < long

        // Now price jumps up: 14, 16, 14, 25 → short_sma(2)=(14+25)/2=19.5, long_sma(4)=17.25
        assert_eq!(sma.push(25.0), CrossoverSignal::Buy);
    }

    #[test]
    fn detects_sell_crossover() {
        let mut sma = SmaCrossover::new(2, 4);

        // Rising prices: short SMA > long SMA
        // Prices: 10, 12, 14, 16 → short=(14+16)/2=15, long=13
        sma.push(10.0);
        sma.push(12.0);
        sma.push(14.0);
        assert_eq!(sma.push(16.0), CrossoverSignal::None); // baseline: short > long

        // Price drops: 12, 14, 16, 5 → short=(16+5)/2=10.5, long=(12+14+16+5)/4=11.75
        assert_eq!(sma.push(5.0), CrossoverSignal::Sell);
    }

    #[test]
    fn no_signal_when_no_crossover() {
        let mut sma = SmaCrossover::new(2, 4);

        // Consistently rising: short always above long
        for price in [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0] {
            let signal = sma.push(price);
            assert_eq!(signal, CrossoverSignal::None);
        }
    }

    #[test]
    fn sma_values_correct() {
        let mut sma = SmaCrossover::new(2, 3);
        sma.push(10.0);
        sma.push(20.0);
        sma.push(30.0);

        assert_eq!(sma.short_sma(), Some(25.0)); // (20+30)/2
        assert_eq!(sma.long_sma(), Some(20.0));   // (10+20+30)/3
    }

    #[test]
    fn window_slides_correctly() {
        let mut sma = SmaCrossover::new(2, 3);
        sma.push(10.0);
        sma.push(20.0);
        sma.push(30.0);
        sma.push(40.0); // window is now [20, 30, 40]

        assert_eq!(sma.short_sma(), Some(35.0)); // (30+40)/2
        assert_eq!(sma.long_sma(), Some(30.0));   // (20+30+40)/3
    }

    #[test]
    #[should_panic(expected = "short period must be less than long period")]
    fn panics_on_invalid_periods() {
        SmaCrossover::new(10, 5);
    }
}
