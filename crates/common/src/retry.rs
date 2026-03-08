use anyhow::Result;
use std::time::Duration;
use tracing::warn;

/// Retry an async connection factory with linear backoff.
///
/// On each failure the delay increases by `base_delay` (i.e. `base_delay * attempt`).
/// After `max_attempts` failures, returns an error.
pub async fn retry_connect<F, Fut, T, E>(
    name: &str,
    max_attempts: u32,
    base_delay: Duration,
    f: F,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    for attempt in 1..=max_attempts {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                if attempt == max_attempts {
                    anyhow::bail!("Failed to connect to {name} after {max_attempts} attempts: {e}");
                }
                let delay = base_delay * attempt;
                warn!(%attempt, %name, %e, ?delay, "Connection failed, retrying");
                tokio::time::sleep(delay).await;
            }
        }
    }
    unreachable!()
}
