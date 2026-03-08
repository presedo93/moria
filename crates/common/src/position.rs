use rust_decimal::Decimal;
use rust_decimal::prelude::Signed;

/// Result of applying a fill to a position.
pub struct FillResult {
    pub new_qty: Decimal,
    pub new_avg_entry: Decimal,
    pub realized_pnl: Decimal,
}

/// Compute new position state after a fill.
///
/// Handles all cases: opening from flat, increasing, partial close, full close,
/// and flipping direction. Returns the new quantity, new average entry price,
/// and any realized PnL from reducing/closing the position.
pub fn apply_fill(
    current_qty: Decimal,
    current_avg_entry: Decimal,
    side: &str,
    fill_price: Decimal,
    fill_qty: Decimal,
) -> FillResult {
    let signed_delta = match side {
        "Buy" => fill_qty,
        "Sell" => -fill_qty,
        _ => return FillResult {
            new_qty: current_qty,
            new_avg_entry: current_avg_entry,
            realized_pnl: Decimal::ZERO,
        },
    };

    let new_qty = current_qty + signed_delta;

    // Compute realized PnL for reducing trades
    let is_reducing = (side == "Buy" && current_qty < Decimal::ZERO)
        || (side == "Sell" && current_qty > Decimal::ZERO);

    let realized_pnl = if is_reducing
        && current_qty != Decimal::ZERO
        && current_avg_entry != Decimal::ZERO
    {
        let qty_closed = fill_qty.min(current_qty.abs());
        if current_qty > Decimal::ZERO {
            (fill_price - current_avg_entry) * qty_closed
        } else {
            (current_avg_entry - fill_price) * qty_closed
        }
    } else {
        Decimal::ZERO
    };

    // Compute new average entry price
    let new_avg_entry = if new_qty == Decimal::ZERO {
        Decimal::ZERO
    } else if current_qty == Decimal::ZERO || current_qty.signum() == signed_delta.signum() {
        // Opening from flat or increasing in same direction
        ((current_avg_entry * current_qty.abs()) + (fill_price * fill_qty.abs())) / new_qty.abs()
    } else if current_qty.signum() == new_qty.signum() {
        // Reducing without flipping — keep prior average
        current_avg_entry
    } else {
        // Flipped direction — remainder opened at fill price
        fill_price
    };

    FillResult {
        new_qty,
        new_avg_entry,
        realized_pnl,
    }
}

/// Unrealized PnL given current position and market price.
pub fn unrealized_pnl(
    position_qty: Decimal,
    avg_entry: Decimal,
    market_price: Decimal,
) -> Decimal {
    if position_qty == Decimal::ZERO || avg_entry == Decimal::ZERO {
        return Decimal::ZERO;
    }
    if position_qty > Decimal::ZERO {
        (market_price - avg_entry) * position_qty
    } else {
        (avg_entry - market_price) * position_qty.abs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    // --- apply_fill: long positions ---

    #[test]
    fn open_long_from_flat() {
        let r = apply_fill(dec("0"), dec("0"), "Buy", dec("100"), dec("1"));
        assert_eq!(r.new_qty, dec("1"));
        assert_eq!(r.new_avg_entry, dec("100"));
        assert_eq!(r.realized_pnl, dec("0"));
    }

    #[test]
    fn increase_long() {
        let r = apply_fill(dec("1"), dec("100"), "Buy", dec("120"), dec("1"));
        assert_eq!(r.new_qty, dec("2"));
        assert_eq!(r.new_avg_entry, dec("110")); // (100*1 + 120*1) / 2
        assert_eq!(r.realized_pnl, dec("0"));
    }

    #[test]
    fn partial_close_long_at_profit() {
        let r = apply_fill(dec("2"), dec("100"), "Sell", dec("150"), dec("1"));
        assert_eq!(r.new_qty, dec("1"));
        assert_eq!(r.new_avg_entry, dec("100")); // avg unchanged on reduce
        assert_eq!(r.realized_pnl, dec("50")); // (150-100)*1
    }

    #[test]
    fn full_close_long() {
        let r = apply_fill(dec("1"), dec("100"), "Sell", dec("120"), dec("1"));
        assert_eq!(r.new_qty, dec("0"));
        assert_eq!(r.new_avg_entry, dec("0"));
        assert_eq!(r.realized_pnl, dec("20")); // (120-100)*1
    }

    #[test]
    fn flip_long_to_short() {
        let r = apply_fill(dec("1"), dec("100"), "Sell", dec("120"), dec("3"));
        assert_eq!(r.new_qty, dec("-2"));
        assert_eq!(r.new_avg_entry, dec("120")); // remainder at fill price
        assert_eq!(r.realized_pnl, dec("20")); // closed 1 unit: (120-100)*1
    }

    // --- apply_fill: short positions ---

    #[test]
    fn open_short_from_flat() {
        let r = apply_fill(dec("0"), dec("0"), "Sell", dec("100"), dec("1"));
        assert_eq!(r.new_qty, dec("-1"));
        assert_eq!(r.new_avg_entry, dec("100"));
        assert_eq!(r.realized_pnl, dec("0"));
    }

    #[test]
    fn increase_short() {
        let r = apply_fill(dec("-1"), dec("100"), "Sell", dec("80"), dec("1"));
        assert_eq!(r.new_qty, dec("-2"));
        assert_eq!(r.new_avg_entry, dec("90")); // (100*1 + 80*1) / 2
        assert_eq!(r.realized_pnl, dec("0"));
    }

    #[test]
    fn partial_close_short_at_profit() {
        let r = apply_fill(dec("-2"), dec("100"), "Buy", dec("80"), dec("1"));
        assert_eq!(r.new_qty, dec("-1"));
        assert_eq!(r.new_avg_entry, dec("100")); // avg unchanged on reduce
        assert_eq!(r.realized_pnl, dec("20")); // (100-80)*1
    }

    #[test]
    fn full_close_short() {
        let r = apply_fill(dec("-1"), dec("100"), "Buy", dec("80"), dec("1"));
        assert_eq!(r.new_qty, dec("0"));
        assert_eq!(r.new_avg_entry, dec("0"));
        assert_eq!(r.realized_pnl, dec("20")); // (100-80)*1
    }

    #[test]
    fn flip_short_to_long() {
        let r = apply_fill(dec("-1"), dec("100"), "Buy", dec("80"), dec("3"));
        assert_eq!(r.new_qty, dec("2"));
        assert_eq!(r.new_avg_entry, dec("80")); // remainder at fill price
        assert_eq!(r.realized_pnl, dec("20")); // closed 1 unit: (100-80)*1
    }

    #[test]
    fn close_long_at_loss() {
        let r = apply_fill(dec("1"), dec("100"), "Sell", dec("80"), dec("1"));
        assert_eq!(r.new_qty, dec("0"));
        assert_eq!(r.realized_pnl, dec("-20")); // (80-100)*1
    }

    #[test]
    fn close_short_at_loss() {
        let r = apply_fill(dec("-1"), dec("100"), "Buy", dec("120"), dec("1"));
        assert_eq!(r.new_qty, dec("0"));
        assert_eq!(r.realized_pnl, dec("-20")); // (100-120)*1
    }

    // --- unrealized_pnl ---

    #[test]
    fn unrealized_long_profit() {
        assert_eq!(unrealized_pnl(dec("2"), dec("100"), dec("110")), dec("20"));
    }

    #[test]
    fn unrealized_long_loss() {
        assert_eq!(unrealized_pnl(dec("2"), dec("100"), dec("90")), dec("-20"));
    }

    #[test]
    fn unrealized_short_profit() {
        assert_eq!(unrealized_pnl(dec("-2"), dec("100"), dec("90")), dec("20"));
    }

    #[test]
    fn unrealized_short_loss() {
        assert_eq!(unrealized_pnl(dec("-2"), dec("100"), dec("110")), dec("-20"));
    }

    #[test]
    fn unrealized_flat_is_zero() {
        assert_eq!(unrealized_pnl(dec("0"), dec("100"), dec("110")), dec("0"));
    }

    #[test]
    fn unrealized_zero_entry_is_zero() {
        assert_eq!(unrealized_pnl(dec("1"), dec("0"), dec("110")), dec("0"));
    }
}
