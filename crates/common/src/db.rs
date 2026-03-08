use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

/// Append a domain event to the `domain_events` table.
pub async fn append_domain_event(
    pool: &PgPool,
    producer: &str,
    event_type: &str,
    aggregate_id: &str,
    payload: Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO domain_events (id, producer, event_type, aggregate_id, payload)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(Uuid::new_v4())
    .bind(producer)
    .bind(event_type)
    .bind(aggregate_id)
    .bind(payload)
    .execute(pool)
    .await?;
    Ok(())
}

/// Append a domain event within an existing transaction.
pub async fn append_domain_event_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    producer: &str,
    event_type: &str,
    aggregate_id: &str,
    payload: Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO domain_events (id, producer, event_type, aggregate_id, payload)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(Uuid::new_v4())
    .bind(producer)
    .bind(event_type)
    .bind(aggregate_id)
    .bind(payload)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
