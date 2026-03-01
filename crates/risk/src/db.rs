use anyhow::Result;
use sqlx::PgPool;
use uuid::Uuid;

pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    let migration_sql = include_str!("../../../migrations/001_initial.sql");
    sqlx::raw_sql(migration_sql).execute(pool).await?;
    tracing::info!("Database migrations applied");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_signal(
    pool: &PgPool,
    id: Uuid,
    symbol: &str,
    side: &str,
    order_type: &str,
    price: f64,
    qty: f64,
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
    price: f64,
    qty: f64,
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

pub async fn get_position_qty(pool: &PgPool, symbol: &str) -> Result<f64> {
    let row: Option<(f64,)> =
        sqlx::query_as("SELECT qty FROM positions WHERE symbol = $1")
            .bind(symbol)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.0).unwrap_or(0.0))
}

pub async fn get_daily_realized_pnl(pool: &PgPool, symbol: &str) -> Result<f64> {
    // Simplified: sum of (sell_price - buy_price) * qty for today's trades
    // In a real system, this would be more sophisticated
    let row: Option<(f64,)> = sqlx::query_as(
        "SELECT COALESCE(SUM(
            CASE WHEN side = 'Sell' THEN price * qty ELSE -(price * qty) END
        ), 0) FROM trades
        WHERE symbol = $1 AND created_at >= CURRENT_DATE AND status = 'Filled'",
    )
    .bind(symbol)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0).unwrap_or(0.0))
}
