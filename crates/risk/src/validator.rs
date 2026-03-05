use rust_decimal::Decimal;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RiskError {
    #[error("position size {current} + {requested} exceeds max {max}")]
    PositionLimitExceeded {
        current: Decimal,
        requested: Decimal,
        max: Decimal,
    },
    #[error("daily loss {current} exceeds max {max}")]
    DailyLossExceeded { current: Decimal, max: Decimal },
    #[error("portfolio notional {current} + {additional} exceeds max {max}")]
    PortfolioExposureExceeded {
        current: Decimal,
        additional: Decimal,
        max: Decimal,
    },
    #[error("drawdown {drawdown} from peak {peak} exceeds max {max}")]
    DrawdownBreached {
        peak: Decimal,
        current: Decimal,
        drawdown: Decimal,
        max: Decimal,
    },
    #[error("invalid side '{side}', expected Buy or Sell")]
    InvalidSide { side: String },
}

pub struct RiskValidator {
    pub max_position_size: Decimal,
    pub max_daily_loss: Decimal,
    pub max_portfolio_notional: Decimal,
    pub max_drawdown: Decimal,
}

impl RiskValidator {
    pub fn new(
        max_position_size: Decimal,
        max_daily_loss: Decimal,
        max_portfolio_notional: Decimal,
        max_drawdown: Decimal,
    ) -> Self {
        Self {
            max_position_size,
            max_daily_loss,
            max_portfolio_notional,
            max_drawdown,
        }
    }

    pub fn validate(
        &self,
        current_position: Decimal,
        side: &str,
        requested_qty: Decimal,
        request_price: Decimal,
        daily_pnl: Decimal,
        portfolio_notional: Decimal,
        daily_peak_pnl: Decimal,
    ) -> Result<(), RiskError> {
        let signed_requested = match side {
            "Buy" => requested_qty,
            "Sell" => -requested_qty,
            _ => return Err(RiskError::InvalidSide { side: side.to_string() }),
        };

        let new_position = current_position + signed_requested;
        if new_position > self.max_position_size {
            return Err(RiskError::PositionLimitExceeded {
                current: current_position,
                requested: requested_qty,
                max: self.max_position_size,
            });
        }
        if new_position < -self.max_position_size {
            return Err(RiskError::PositionLimitExceeded {
                current: current_position,
                requested: requested_qty,
                max: self.max_position_size,
            });
        }

        // Daily loss check: if PnL is negative and exceeds limit
        if daily_pnl < Decimal::ZERO && daily_pnl.abs() > self.max_daily_loss {
            return Err(RiskError::DailyLossExceeded {
                current: daily_pnl,
                max: self.max_daily_loss,
            });
        }

        // Portfolio notional exposure check
        let additional_notional = requested_qty * request_price;
        if portfolio_notional + additional_notional > self.max_portfolio_notional {
            return Err(RiskError::PortfolioExposureExceeded {
                current: portfolio_notional,
                additional: additional_notional,
                max: self.max_portfolio_notional,
            });
        }

        // Drawdown circuit breaker
        let drawdown = daily_peak_pnl - daily_pnl;
        if drawdown > self.max_drawdown {
            return Err(RiskError::DrawdownBreached {
                peak: daily_peak_pnl,
                current: daily_pnl,
                drawdown,
                max: self.max_drawdown,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn validator() -> RiskValidator {
        RiskValidator::new(dec("1.0"), dec("100.0"), dec("10000.0"), dec("500.0"))
    }

    #[test]
    fn approves_within_limits() {
        assert!(
            validator()
                .validate(dec("0.0"), "Buy", dec("0.5"), dec("100.0"), dec("0.0"), dec("0.0"), dec("0.0"))
                .is_ok()
        );
    }

    #[test]
    fn rejects_position_over_limit() {
        let err = validator()
            .validate(dec("0.8"), "Buy", dec("0.5"), dec("100.0"), dec("0.0"), dec("0.0"), dec("0.0"))
            .unwrap_err();
        assert!(matches!(err, RiskError::PositionLimitExceeded { .. }));
    }

    #[test]
    fn rejects_daily_loss_exceeded() {
        let err = validator()
            .validate(dec("0.0"), "Buy", dec("0.1"), dec("100.0"), dec("-150.0"), dec("0.0"), dec("0.0"))
            .unwrap_err();
        assert!(matches!(err, RiskError::DailyLossExceeded { .. }));
    }

    #[test]
    fn approves_with_positive_pnl() {
        assert!(
            validator()
                .validate(dec("0.0"), "Buy", dec("0.5"), dec("100.0"), dec("50.0"), dec("0.0"), dec("0.0"))
                .is_ok()
        );
    }

    #[test]
    fn approves_at_exactly_max_position() {
        assert!(
            validator()
                .validate(dec("0.5"), "Buy", dec("0.5"), dec("100.0"), dec("0.0"), dec("0.0"), dec("0.0"))
                .is_ok()
        );
    }

    #[test]
    fn approves_at_exactly_max_loss() {
        assert!(
            validator()
                .validate(dec("0.0"), "Buy", dec("0.1"), dec("100.0"), dec("-100.0"), dec("0.0"), dec("0.0"))
                .is_ok()
        );
    }

    #[test]
    fn allows_reducing_long_with_sell() {
        assert!(
            validator()
                .validate(dec("0.8"), "Sell", dec("0.5"), dec("100.0"), dec("0.0"), dec("0.0"), dec("0.0"))
                .is_ok()
        );
    }

    #[test]
    fn rejects_short_over_limit_with_sell() {
        let err = validator()
            .validate(dec("-0.8"), "Sell", dec("0.5"), dec("100.0"), dec("0.0"), dec("0.0"), dec("0.0"))
            .unwrap_err();
        assert!(matches!(err, RiskError::PositionLimitExceeded { .. }));
    }

    #[test]
    fn rejects_portfolio_exposure_exceeded() {
        let err = validator()
            .validate(dec("0.0"), "Buy", dec("0.5"), dec("100.0"), dec("0.0"), dec("9960.0"), dec("0.0"))
            .unwrap_err();
        assert!(matches!(err, RiskError::PortfolioExposureExceeded { .. }));
    }

    #[test]
    fn approves_portfolio_within_limit() {
        assert!(validator()
            .validate(dec("0.0"), "Buy", dec("0.5"), dec("100.0"), dec("0.0"), dec("5000.0"), dec("0.0"))
            .is_ok());
    }

    #[test]
    fn approves_portfolio_at_exactly_max() {
        assert!(validator()
            .validate(dec("0.0"), "Buy", dec("0.5"), dec("100.0"), dec("0.0"), dec("9950.0"), dec("0.0"))
            .is_ok());
    }

    #[test]
    fn rejects_drawdown_breached() {
        let err = validator()
            .validate(dec("0.0"), "Buy", dec("0.1"), dec("100.0"), dec("-50.0"), dec("0.0"), dec("500.0"))
            .unwrap_err();
        assert!(matches!(err, RiskError::DrawdownBreached { .. }));
    }

    #[test]
    fn approves_drawdown_within_limit() {
        assert!(validator()
            .validate(dec("0.0"), "Buy", dec("0.1"), dec("100.0"), dec("-50.0"), dec("0.0"), dec("200.0"))
            .is_ok());
    }

    #[test]
    fn approves_drawdown_at_exactly_max() {
        assert!(validator()
            .validate(dec("0.0"), "Buy", dec("0.1"), dec("100.0"), dec("-50.0"), dec("0.0"), dec("450.0"))
            .is_ok());
    }
}
