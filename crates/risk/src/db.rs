use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::Signed;
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

pub const ORDER_INTENTS_NOTIFY_CHANNEL: &str = "order_intents_ready";

pub struct SignalDecision {
    pub approved: bool,
    pub reject_reason: Option<String>,
}

/// Snapshot of all risk-relevant state, fetched atomically within a transaction.
pub struct RiskState {
    pub current_position: Decimal,
    pub avg_entry_price: Decimal,
    pub daily_realized_pnl: Decimal,
    pub portfolio_notional: Decimal,
    pub daily_peak_pnl: Decimal,
}

#[derive(Debug, Clone)]
pub struct OrderIntent {
    pub id: Uuid,
    pub signal_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: Decimal,
    pub qty: Decimal,
    pub attempts: i32,
}

/// Begin a serializable risk check: acquires a row-level lock on the position row
/// (or an advisory lock for new symbols) so concurrent orders for the same symbol
/// are serialized. Returns the locked transaction and a consistent risk state snapshot.
pub async fn begin_risk_check(
    pool: &PgPool,
    symbol: &str,
) -> Result<(sqlx::Transaction<'static, sqlx::Postgres>, RiskState)> {
    let mut tx = pool.begin().await?;

    // Lock the position row for this symbol. If no row exists yet, we use an
    // advisory lock keyed on the symbol hash to prevent concurrent inserts.
    let position: Option<(Decimal, Decimal)> = sqlx::query_as(
        "SELECT qty, avg_entry_price FROM positions WHERE symbol = $1 FOR UPDATE",
    )
    .bind(symbol)
    .fetch_optional(&mut *tx)
    .await?;

    if position.is_none() {
        // Advisory lock on symbol hash to serialize new-position creation
        let lock_key = symbol_lock_key(symbol);
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_key)
            .execute(&mut *tx)
            .await?;
    }

    let (current_position, avg_entry_price) = position.unwrap_or((Decimal::ZERO, Decimal::ZERO));

    let daily_realized_pnl = {
        let row: Option<(Decimal,)> = sqlx::query_as(
            "SELECT COALESCE(SUM(realized_pnl), 0) FROM trades
            WHERE symbol = $1
              AND created_at >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
              AND status = 'Filled'",
        )
        .bind(symbol)
        .fetch_optional(&mut *tx)
        .await?;
        row.map(|r| r.0).unwrap_or(Decimal::ZERO)
    };

    let portfolio_notional = {
        let row: Option<(Decimal,)> = sqlx::query_as(
            "SELECT COALESCE(SUM(ABS(qty) * avg_entry_price), 0) FROM positions WHERE qty != 0",
        )
        .fetch_optional(&mut *tx)
        .await?;
        row.map(|r| r.0).unwrap_or(Decimal::ZERO)
    };

    let daily_peak_pnl = {
        let row: Option<(Decimal,)> = sqlx::query_as(
            "SELECT peak_pnl FROM daily_equity WHERE date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date",
        )
        .fetch_optional(&mut *tx)
        .await?;
        row.map(|r| r.0).unwrap_or(Decimal::ZERO)
    };

    Ok((tx, RiskState {
        current_position,
        avg_entry_price,
        daily_realized_pnl,
        portfolio_notional,
        daily_peak_pnl,
    }))
}

fn symbol_lock_key(symbol: &str) -> i64 {
    // Simple hash for advisory lock — collisions just cause unnecessary serialization
    let mut hash: i64 = 0;
    for byte in symbol.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(byte as i64);
    }
    hash
}

pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    let migration_001 = include_str!("../../../migrations/001_initial.sql");
    sqlx::raw_sql(migration_001).execute(pool).await?;
    let migration_002 = include_str!("../../../migrations/002_daily_equity.sql");
    sqlx::raw_sql(migration_002).execute(pool).await?;
    let migration_003 = include_str!("../../../migrations/003_realized_pnl_and_indexes.sql");
    sqlx::raw_sql(migration_003).execute(pool).await?;
    let migration_004 = include_str!("../../../migrations/004_order_intents.sql");
    sqlx::raw_sql(migration_004).execute(pool).await?;
    let migration_005 = include_str!("../../../migrations/005_trade_reconciliation_index.sql");
    sqlx::raw_sql(migration_005).execute(pool).await?;
    let migration_006 = include_str!("../../../migrations/006_domain_events.sql");
    sqlx::raw_sql(migration_006).execute(pool).await?;
    let migration_007 = include_str!("../../../migrations/007_backtest_leaderboard.sql");
    sqlx::raw_sql(migration_007).execute(pool).await?;
    tracing::info!("Database migrations applied");
    Ok(())
}

pub use moria_common::db::{append_domain_event, append_domain_event_in_tx};

#[allow(clippy::too_many_arguments)]
pub async fn enqueue_order_intent_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
    signal_id: Uuid,
    symbol: &str,
    side: &str,
    order_type: &str,
    price: Decimal,
    qty: Decimal,
) -> Result<()> {
    let result = sqlx::query(
        "INSERT INTO order_intents
            (id, signal_id, symbol, side, order_type, price, qty, status, attempts, next_attempt_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'Pending', 0, now(), now())
         ON CONFLICT (signal_id) DO NOTHING",
    )
    .bind(id)
    .bind(signal_id)
    .bind(symbol)
    .bind(side)
    .bind(order_type)
    .bind(price)
    .bind(qty)
    .execute(&mut **tx)
    .await?;

    if result.rows_affected() > 0 {
        notify_order_intents_in_tx(tx).await?;
    }
    Ok(())
}

pub async fn claim_pending_order_intent(pool: &PgPool) -> Result<Option<OrderIntent>> {
    let row = sqlx::query_as::<
        _,
        (
            Uuid,
            Uuid,
            String,
            String,
            String,
            Decimal,
            Decimal,
            i32,
            DateTime<Utc>,
        ),
    >(
        "WITH candidate AS (
            SELECT id
            FROM order_intents
            WHERE status IN ('Pending', 'Retry')
              AND next_attempt_at <= now()
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE order_intents oi
        SET status = 'InProgress',
            attempts = oi.attempts + 1,
            updated_at = now(),
            last_error = NULL
        FROM candidate
        WHERE oi.id = candidate.id
        RETURNING
            oi.id,
            oi.signal_id,
            oi.symbol,
            oi.side,
            oi.order_type,
            oi.price,
            oi.qty,
            oi.attempts,
            oi.updated_at",
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(
        |(id, signal_id, symbol, side, order_type, price, qty, attempts, _updated_at)| {
            OrderIntent {
                id,
                signal_id,
                symbol,
                side,
                order_type,
                price,
                qty,
                attempts,
            }
        },
    ))
}

pub async fn next_order_intent_due_in(pool: &PgPool) -> Result<Option<Duration>> {
    let next_attempt_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
        "SELECT MIN(next_attempt_at)
         FROM order_intents
         WHERE status IN ('Pending', 'Retry')",
    )
    .fetch_one(pool)
    .await?;

    Ok(next_attempt_at.map(|ts| {
        let now = Utc::now();
        ts.signed_duration_since(now)
            .to_std()
            .unwrap_or(Duration::ZERO)
    }))
}

pub async fn mark_order_intent_succeeded(pool: &PgPool, intent_id: Uuid) -> Result<()> {
    sqlx::query(
        "UPDATE order_intents
         SET status = 'Succeeded', updated_at = now()
         WHERE id = $1",
    )
    .bind(intent_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_order_intent_retry(
    pool: &PgPool,
    intent_id: Uuid,
    error: &str,
    attempts: i32,
    max_attempts: i32,
) -> Result<()> {
    let delay_seconds = i64::from(2_i32.pow(attempts.min(6) as u32));
    let next_status = if attempts >= max_attempts {
        "Failed"
    } else {
        "Retry"
    };

    sqlx::query(
        "UPDATE order_intents
         SET status = $2,
             next_attempt_at = now() + ($3::bigint * interval '1 second'),
             last_error = $4,
             updated_at = now()
         WHERE id = $1",
    )
    .bind(intent_id)
    .bind(next_status)
    .bind(delay_seconds)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

async fn notify_order_intents_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(ORDER_INTENTS_NOTIFY_CHANNEL)
        .bind("ready")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert_trade_execution(
    pool: &PgPool,
    signal_id: Uuid,
    order_id: &str,
    symbol: &str,
    side: &str,
    price: Decimal,
    qty: Decimal,
    status: &str,
    reject_reason: Option<&str>,
) -> Result<()> {
    let mut tx = pool.begin().await?;

    let realized_pnl = if status == "Filled" {
        compute_realized_pnl(&mut tx, symbol, side, price, qty).await?
    } else {
        Decimal::ZERO
    };

    sqlx::query(
        "INSERT INTO trades (id, signal_id, order_id, symbol, side, price, qty, status, realized_pnl)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (signal_id) DO UPDATE
         SET order_id = EXCLUDED.order_id,
             symbol = EXCLUDED.symbol,
             side = EXCLUDED.side,
             price = EXCLUDED.price,
             qty = EXCLUDED.qty,
             status = EXCLUDED.status,
             realized_pnl = EXCLUDED.realized_pnl,
             created_at = now()",
    )
    .bind(Uuid::new_v4())
    .bind(signal_id)
    .bind(order_id)
    .bind(symbol)
    .bind(side)
    .bind(price)
    .bind(qty)
    .bind(status)
    .bind(realized_pnl)
    .execute(&mut *tx)
    .await?;

    if status == "Filled" {
        apply_filled_trade_to_position(&mut tx, symbol, side, price, qty).await?;
    }

    if let Some(reason) = reject_reason {
        sqlx::query(
            "UPDATE signals
             SET reject_reason = $2
             WHERE id = $1",
        )
        .bind(signal_id)
        .bind(reason)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

#[tracing::instrument(skip(pool))]
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
        "INSERT INTO trades (id, signal_id, order_id, symbol, side, price, qty, status, realized_pnl)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0)",
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

#[allow(dead_code)]
#[tracing::instrument(skip(pool, price, qty, reject_reason, trade_order_id, trade_status))]
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
    persist_signal_and_trade_in_tx(&mut tx, signal_id, symbol, side, order_type, price, qty, approved, reject_reason, trade_order_id, trade_status).await?;
    tx.commit().await?;
    Ok(())
}

/// Persist signal and trade within an existing transaction (used by the locked risk path).
#[tracing::instrument(skip(tx, price, qty, reject_reason, trade_order_id, trade_status))]
#[allow(clippy::too_many_arguments)]
pub async fn persist_signal_and_trade_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
    .execute(&mut **tx)
    .await?;

    if let (Some(order_id), Some(status)) = (trade_order_id, trade_status) {
        // Calculate realized PnL for filled trades
        let realized_pnl = if status == "Filled" {
            compute_realized_pnl(tx, symbol, side, price, qty).await?
        } else {
            Decimal::ZERO
        };

        sqlx::query(
            "INSERT INTO trades (id, signal_id, order_id, symbol, side, price, qty, status, realized_pnl)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(Uuid::new_v4())
        .bind(signal_id)
        .bind(order_id)
        .bind(symbol)
        .bind(side)
        .bind(price)
        .bind(qty)
        .bind(status)
        .bind(realized_pnl)
        .execute(&mut **tx)
        .await?;

        if status == "Filled" {
            apply_filled_trade_to_position(tx, symbol, side, price, qty).await?;
        }
    }

    Ok(())
}

/// Compute realized PnL for a closing/reducing trade based on the current average entry price.
/// Returns zero for position-increasing trades (no PnL is realized when adding to a position).
async fn compute_realized_pnl(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    symbol: &str,
    side: &str,
    fill_price: Decimal,
    fill_qty: Decimal,
) -> Result<Decimal> {
    let current: Option<(Decimal, Decimal)> =
        sqlx::query_as("SELECT qty, avg_entry_price FROM positions WHERE symbol = $1")
            .bind(symbol)
            .fetch_optional(&mut **tx)
            .await?;

    let (current_qty, avg_entry) = current.unwrap_or((Decimal::ZERO, Decimal::ZERO));

    if current_qty == Decimal::ZERO || avg_entry == Decimal::ZERO {
        return Ok(Decimal::ZERO);
    }

    // Determine if this trade is reducing the position
    let is_reducing = match side {
        "Buy" => current_qty < Decimal::ZERO,  // Buying to cover a short
        "Sell" => current_qty > Decimal::ZERO,  // Selling to close a long
        _ => anyhow::bail!("invalid side: {side}"),
    };

    if !is_reducing {
        return Ok(Decimal::ZERO);
    }

    // PnL = (fill_price - avg_entry) * qty_closed for longs
    // PnL = (avg_entry - fill_price) * qty_closed for shorts
    let qty_closed = fill_qty.min(current_qty.abs());
    let pnl = if current_qty > Decimal::ZERO {
        // Closing a long: profit when sell price > entry price
        (fill_price - avg_entry) * qty_closed
    } else {
        // Closing a short: profit when entry price > buy price
        (avg_entry - fill_price) * qty_closed
    };

    Ok(pnl)
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
        _ => anyhow::bail!("invalid side: {side}"),
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

#[allow(dead_code)]
#[tracing::instrument(skip(pool))]
pub async fn get_position_qty(pool: &PgPool, symbol: &str) -> Result<Decimal> {
    let row: Option<(Decimal,)> = sqlx::query_as("SELECT qty FROM positions WHERE symbol = $1")
        .bind(symbol)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.0).unwrap_or(Decimal::ZERO))
}

#[allow(dead_code)]
#[tracing::instrument(skip(pool))]
pub async fn get_daily_realized_pnl(pool: &PgPool, symbol: &str) -> Result<Decimal> {
    let row: Option<(Decimal,)> = sqlx::query_as(
        "SELECT COALESCE(SUM(realized_pnl), 0) FROM trades
        WHERE symbol = $1
          AND created_at >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date
          AND status = 'Filled'",
    )
    .bind(symbol)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0).unwrap_or(Decimal::ZERO))
}

#[allow(dead_code)]
pub async fn get_portfolio_notional(pool: &PgPool) -> Result<Decimal> {
    let row: Option<(Decimal,)> = sqlx::query_as(
        "SELECT COALESCE(SUM(ABS(qty) * avg_entry_price), 0) FROM positions WHERE qty != 0",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0).unwrap_or(Decimal::ZERO))
}

#[allow(dead_code)]
pub async fn get_daily_peak_pnl(pool: &PgPool) -> Result<Decimal> {
    let row: Option<(Decimal,)> = sqlx::query_as(
        "SELECT peak_pnl FROM daily_equity WHERE date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0).unwrap_or(Decimal::ZERO))
}

#[allow(dead_code)]
/// Atomically update the daily peak PnL high-water mark.
/// Uses a conditional UPDATE to prevent concurrent writers from lowering the peak.
pub async fn update_daily_peak_pnl(pool: &PgPool, new_peak: Decimal) -> Result<()> {
    sqlx::query(
        "INSERT INTO daily_equity (date, peak_pnl, updated_at)
         VALUES ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date, $1, now())
         ON CONFLICT (date)
         DO UPDATE SET peak_pnl = $1, updated_at = now()
         WHERE daily_equity.peak_pnl < $1",
    )
    .bind(new_peak)
    .execute(pool)
    .await?;
    Ok(())
}

/// Update peak PnL within an existing transaction.
pub async fn update_daily_peak_pnl_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    new_peak: Decimal,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO daily_equity (date, peak_pnl, updated_at)
         VALUES ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date, $1, now())
         ON CONFLICT (date)
         DO UPDATE SET peak_pnl = $1, updated_at = now()
         WHERE daily_equity.peak_pnl < $1",
    )
    .bind(new_peak)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
