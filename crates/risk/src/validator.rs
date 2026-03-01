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
}

pub struct RiskValidator {
    pub max_position_size: Decimal,
    pub max_daily_loss: Decimal,
}

impl RiskValidator {
    pub fn new(max_position_size: Decimal, max_daily_loss: Decimal) -> Self {
        Self {
            max_position_size,
            max_daily_loss,
        }
    }

    pub fn validate(
        &self,
        current_position: Decimal,
        requested_qty: Decimal,
        daily_pnl: Decimal,
    ) -> Result<(), RiskError> {
        let new_position = current_position.abs() + requested_qty.abs();
        if new_position > self.max_position_size {
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
        RiskValidator::new(dec("1.0"), dec("100.0"))
    }

    #[test]
    fn approves_within_limits() {
        assert!(validator().validate(dec("0.0"), dec("0.5"), dec("0.0")).is_ok());
    }

    #[test]
    fn rejects_position_over_limit() {
        let err = validator().validate(dec("0.8"), dec("0.5"), dec("0.0")).unwrap_err();
        assert!(matches!(err, RiskError::PositionLimitExceeded { .. }));
    }

    #[test]
    fn rejects_daily_loss_exceeded() {
        let err = validator().validate(dec("0.0"), dec("0.1"), dec("-150.0")).unwrap_err();
        assert!(matches!(err, RiskError::DailyLossExceeded { .. }));
    }

    #[test]
    fn approves_with_positive_pnl() {
        assert!(validator().validate(dec("0.0"), dec("0.5"), dec("50.0")).is_ok());
    }

    #[test]
    fn approves_at_exactly_max_position() {
        assert!(validator().validate(dec("0.5"), dec("0.5"), dec("0.0")).is_ok());
    }

    #[test]
    fn approves_at_exactly_max_loss() {
        assert!(validator().validate(dec("0.0"), dec("0.1"), dec("-100.0")).is_ok());
    }
}
