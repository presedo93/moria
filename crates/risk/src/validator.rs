use thiserror::Error;

#[derive(Debug, Error)]
pub enum RiskError {
    #[error("position size {current} + {requested} exceeds max {max}")]
    PositionLimitExceeded {
        current: f64,
        requested: f64,
        max: f64,
    },
    #[error("daily loss {current} exceeds max {max}")]
    DailyLossExceeded { current: f64, max: f64 },
}

pub struct RiskValidator {
    pub max_position_size: f64,
    pub max_daily_loss: f64,
}

impl RiskValidator {
    pub fn new(max_position_size: f64, max_daily_loss: f64) -> Self {
        Self {
            max_position_size,
            max_daily_loss,
        }
    }

    pub fn validate(
        &self,
        current_position: f64,
        requested_qty: f64,
        daily_pnl: f64,
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
        if daily_pnl < 0.0 && daily_pnl.abs() > self.max_daily_loss {
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

    fn validator() -> RiskValidator {
        RiskValidator::new(1.0, 100.0)
    }

    #[test]
    fn approves_within_limits() {
        assert!(validator().validate(0.0, 0.5, 0.0).is_ok());
    }

    #[test]
    fn rejects_position_over_limit() {
        let err = validator().validate(0.8, 0.5, 0.0).unwrap_err();
        assert!(matches!(err, RiskError::PositionLimitExceeded { .. }));
    }

    #[test]
    fn rejects_daily_loss_exceeded() {
        let err = validator().validate(0.0, 0.1, -150.0).unwrap_err();
        assert!(matches!(err, RiskError::DailyLossExceeded { .. }));
    }

    #[test]
    fn approves_with_positive_pnl() {
        assert!(validator().validate(0.0, 0.5, 50.0).is_ok());
    }

    #[test]
    fn approves_at_exactly_max_position() {
        assert!(validator().validate(0.5, 0.5, 0.0).is_ok());
    }

    #[test]
    fn approves_at_exactly_max_loss() {
        assert!(validator().validate(0.0, 0.1, -100.0).is_ok());
    }
}
