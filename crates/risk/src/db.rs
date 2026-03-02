use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal::prelude::Signed;
use sqlx::PgPool;
use uuid::Uuid;

pub struct SignalDecision {
    pub approved: bool,
    pub reject_reason: Option<String>,
}

pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    let migration_001 = include_str!("../../../migrations/001_initial.sql");
    sqlx::raw_sql(migration_001).execute(pool).await?;
    let migration_002 = include_str!("../../../migrations/002_daily_equity.sql");
    sqlx::raw_sql(migration_002).execute(pool).await?;
    tracing::info!("Database migrations applied");
    Ok(())
}

pub async fn get_signal_decision(pool: &PgPool, signal_id: Uuid) -> Result<Option<SignalDecision>> {
    let row: Option<(bool, Option<String>)> =
        sqlx::query_as("SELECT approved, reject_reason FROM signals WHERE id = $1")
            .bind(signal_id)
            .fetch_optional(pool)
            .await?;

    Ok(row.map(|(approved, reject_reason)| SignalDecision {
        approved,
        reject_reason,
    }))
}

#[allow(dead_code, clippy::too_many_arguments)]
pub async fn insert_signal(
    pool: &PgPool,
    id: Uuid,
    symbol: &str,
    side: &str,
    order_type: &str,
    price: Decimal,
    qty: Decimal,
    approved: bool,
    reject_reason: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO signals (id, symbol, side, order_type, price, qty, approved, reject_reason)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(id)
    .bind(symbol)
    .bind(side)
    .bind(order_type)
    .bind(price)
    .bind(qty)
    .bind(approved)
    .bind(reject_reason)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(dead_code, clippy::too_many_arguments)]
pub async fn insert_trade(
    pool: &PgPool,
    signal_id: Uuid,
    order_id: &str,
    symbol: &str,
    side: &str,
    price: Decimal,
    qty: Decimal,
    status: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO trades (id, signal_id, order_id, symbol, side, price, qty, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(Uuid::new_v4())
    .bind(signal_id)
    .bind(order_id)
    .bind(symbol)
    .bind(side)
    .bind(price)
    .bind(qty)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn persist_signal_and_trade(
    pool: &PgPool,
    signal_id: Uuid,
    symbol: &str,
    side: &str,
    order_type: &str,
    price: Decimal,
    qty: Decimal,
    approved: bool,
    reject_reason: Option<&str>,
    trade_order_id: Option<&str>,
    trade_status: Option<&str>,
) -> Result<()> {
    let mut tx = pool.begin().await?;

    sqlx::query(
        "INSERT INTO signals (id, symbol, side, order_type, price, qty, approved, reject_reason)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(signal_id)
    .bind(symbol)
    .bind(side)
    .bind(order_type)
    .bind(price)
    .bind(qty)
    .bind(approved)
    .bind(reject_reason)
    .execute(&mut *tx)
    .await?;

    if let (Some(order_id), Some(status)) = (trade_order_id, trade_status) {
        sqlx::query(
            "INSERT INTO trades (id, signal_id, order_id, symbol, side, price, qty, status)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(Uuid::new_v4())
        .bind(signal_id)
        .bind(order_id)
        .bind(symbol)
        .bind(side)
        .bind(price)
        .bind(qty)
        .bind(status)
        .execute(&mut *tx)
        .await?;

        if status == "Filled" {
            apply_filled_trade_to_position(&mut tx, symbol, side, price, qty).await?;
        }
    }

    tx.commit().await?;
    Ok(())
}

async fn apply_filled_trade_to_position(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    symbol: &str,
    side: &str,
    price: Decimal,
    qty: Decimal,
) -> Result<()> {
    let current: Option<(Decimal, Decimal)> =
        sqlx::query_as("SELECT qty, avg_entry_price FROM positions WHERE symbol = $1")
            .bind(symbol)
            .fetch_optional(&mut **tx)
            .await?;

    let (current_qty, current_avg_price) = current.unwrap_or((Decimal::ZERO, Decimal::ZERO));

    let signed_delta = match side {
        "Buy" => qty,
        "Sell" => -qty,
        _ => qty,
    };

    let new_qty = current_qty + signed_delta;

    let new_avg_price = if new_qty == Decimal::ZERO {
        Decimal::ZERO
    } else if current_qty == Decimal::ZERO || current_qty.signum() == signed_delta.signum() {
        // Increasing an existing position in the same direction (or opening from flat).
        ((current_avg_price * current_qty.abs()) + (price * qty.abs())) / new_qty.abs()
    } else if current_qty.signum() == new_qty.signum() {
        // Reducing a position without flipping side keeps prior average entry.
        current_avg_price
    } else {
        // Flipped side; the remaining position opened at the latest fill price.
        price
    };

    sqlx::query(
        "INSERT INTO positions (symbol, qty, avg_entry_price, updated_at)
         VALUES ($1, $2, $3, now())
         ON CONFLICT (symbol)
         DO UPDATE SET
             qty = EXCLUDED.qty,
             avg_entry_price = EXCLUDED.avg_entry_price,
             updated_at = now()",
    )
    .bind(symbol)
    .bind(new_qty)
    .bind(new_avg_price)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn get_position_qty(pool: &PgPool, symbol: &str) -> Result<Decimal> {
    let row: Option<(Decimal,)> = sqlx::query_as("SELECT qty FROM positions WHERE symbol = $1")
        .bind(symbol)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.0).unwrap_or(Decimal::ZERO))
}

pub async fn get_daily_realized_pnl(pool: &PgPool, symbol: &str) -> Result<Decimal> {
    // Simplified: sum of (sell_price - buy_price) * qty for today's trades
    // In a real system, this would be more sophisticated
    let row: Option<(Decimal,)> = sqlx::query_as(
        "SELECT COALESCE(SUM(
            CASE WHEN side = 'Sell' THEN price * qty ELSE -(price * qty) END
        ), 0) FROM trades
        WHERE symbol = $1 AND created_at >= CURRENT_DATE AND status = 'Filled'",
    )
    .bind(symbol)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0).unwrap_or(Decimal::ZERO))
}

pub async fn get_portfolio_notional(pool: &PgPool) -> Result<Decimal> {
    let row: Option<(Decimal,)> = sqlx::query_as(
        "SELECT COALESCE(SUM(ABS(qty) * avg_entry_price), 0) FROM positions WHERE qty != 0",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0).unwrap_or(Decimal::ZERO))
}

pub async fn get_daily_peak_pnl(pool: &PgPool) -> Result<Decimal> {
    let row: Option<(Decimal,)> =
        sqlx::query_as("SELECT peak_pnl FROM daily_equity WHERE date = CURRENT_DATE")
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.0).unwrap_or(Decimal::ZERO))
}

pub async fn update_daily_peak_pnl(pool: &PgPool, new_peak: Decimal) -> Result<()> {
    sqlx::query(
        "INSERT INTO daily_equity (date, peak_pnl, updated_at)
         VALUES (CURRENT_DATE, $1, now())
         ON CONFLICT (date)
         DO UPDATE SET peak_pnl = $1, updated_at = now()",
    )
    .bind(new_peak)
    .execute(pool)
    .await?;
    Ok(())
}
