use std::fmt;
use std::str::FromStr;

/// Trading side: Buy or Sell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Buy => write!(f, "Buy"),
            Side::Sell => write!(f, "Sell"),
        }
    }
}

impl FromStr for Side {
    type Err = InvalidEnumValue;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Buy" => Ok(Side::Buy),
            "Sell" => Ok(Side::Sell),
            _ => Err(InvalidEnumValue {
                value: s.to_string(),
                expected: "Buy or Sell",
            }),
        }
    }
}

/// Order type: Market or Limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Market,
    Limit,
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderType::Market => write!(f, "Market"),
            OrderType::Limit => write!(f, "Limit"),
        }
    }
}

impl FromStr for OrderType {
    type Err = InvalidEnumValue;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Market" => Ok(OrderType::Market),
            "Limit" => Ok(OrderType::Limit),
            _ => Err(InvalidEnumValue {
                value: s.to_string(),
                expected: "Market or Limit",
            }),
        }
    }
}

/// Trade/order execution status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Submitted,
    Filled,
    Rejected,
    Uncertain,
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderStatus::Submitted => write!(f, "Submitted"),
            OrderStatus::Filled => write!(f, "Filled"),
            OrderStatus::Rejected => write!(f, "Rejected"),
            OrderStatus::Uncertain => write!(f, "Uncertain"),
        }
    }
}

impl FromStr for OrderStatus {
    type Err = InvalidEnumValue;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Submitted" => Ok(OrderStatus::Submitted),
            "Filled" => Ok(OrderStatus::Filled),
            "Rejected" => Ok(OrderStatus::Rejected),
            "Uncertain" => Ok(OrderStatus::Uncertain),
            _ => Err(InvalidEnumValue {
                value: s.to_string(),
                expected: "Submitted, Filled, Rejected, or Uncertain",
            }),
        }
    }
}

impl OrderStatus {
    pub fn is_successful(&self) -> bool {
        matches!(self, OrderStatus::Submitted | OrderStatus::Filled)
    }
}

/// Error type for invalid enum string values.
#[derive(Debug, Clone)]
pub struct InvalidEnumValue {
    pub value: String,
    pub expected: &'static str,
}

impl fmt::Display for InvalidEnumValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid value '{}', expected {}",
            self.value, self.expected
        )
    }
}

impl std::error::Error for InvalidEnumValue {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_round_trips() {
        assert_eq!(Side::Buy.to_string(), "Buy");
        assert_eq!(Side::Sell.to_string(), "Sell");
        assert_eq!("Buy".parse::<Side>().unwrap(), Side::Buy);
        assert_eq!("Sell".parse::<Side>().unwrap(), Side::Sell);
        assert!("buy".parse::<Side>().is_err());
    }

    #[test]
    fn order_type_round_trips() {
        assert_eq!(OrderType::Market.to_string(), "Market");
        assert_eq!("Limit".parse::<OrderType>().unwrap(), OrderType::Limit);
        assert!("market".parse::<OrderType>().is_err());
    }

    #[test]
    fn order_status_round_trips() {
        assert_eq!(OrderStatus::Filled.to_string(), "Filled");
        assert_eq!("Uncertain".parse::<OrderStatus>().unwrap(), OrderStatus::Uncertain);
        assert!(OrderStatus::Submitted.is_successful());
        assert!(OrderStatus::Filled.is_successful());
        assert!(!OrderStatus::Rejected.is_successful());
        assert!(!OrderStatus::Uncertain.is_successful());
    }
}
