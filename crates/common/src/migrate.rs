use anyhow::Result;
use sqlx::PgPool;

pub struct Migration {
    pub name: &'static str,
    pub sql: &'static str,
}

/// All workspace migrations, in order.
pub const MIGRATIONS: &[Migration] = &[
    Migration { name: "001_initial", sql: include_str!("../../../migrations/001_initial.sql") },
    Migration { name: "002_daily_equity", sql: include_str!("../../../migrations/002_daily_equity.sql") },
    Migration { name: "003_realized_pnl_and_indexes", sql: include_str!("../../../migrations/003_realized_pnl_and_indexes.sql") },
    Migration { name: "004_order_intents", sql: include_str!("../../../migrations/004_order_intents.sql") },
    Migration { name: "005_trade_reconciliation_index", sql: include_str!("../../../migrations/005_trade_reconciliation_index.sql") },
    Migration { name: "006_domain_events", sql: include_str!("../../../migrations/006_domain_events.sql") },
    Migration { name: "007_backtest_leaderboard", sql: include_str!("../../../migrations/007_backtest_leaderboard.sql") },
];

/// Run unapplied migrations, tracking progress in `_migrations` table.
///
/// Idempotent: existing databases with all tables already created will
/// harmlessly re-run the IF NOT EXISTS SQL on first use, then record
/// all migrations as applied.
pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    // Ensure tracking table exists
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS _migrations (
            name TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )"
    )
    .execute(pool)
    .await?;

    // Fetch already-applied migration names
    let applied: Vec<String> = sqlx::query_scalar("SELECT name FROM _migrations")
        .fetch_all(pool)
        .await?;

    let mut count = 0;
    for migration in MIGRATIONS {
        if applied.contains(&migration.name.to_string()) {
            continue;
        }

        // Run each migration in a transaction: execute SQL then record it
        let mut tx = pool.begin().await?;
        sqlx::raw_sql(migration.sql).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO _migrations (name) VALUES ($1)")
            .bind(migration.name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        count += 1;
        tracing::info!(migration = migration.name, "Applied migration");
    }

    if count > 0 {
        tracing::info!(count, "Database migrations applied");
    } else {
        tracing::debug!("All migrations already applied");
    }

    Ok(())
}
