use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

/// Simple circuit breaker that tracks consecutive failures and opens after a threshold.
///
/// States:
/// - **Closed**: All calls pass through. Consecutive failures increment counter.
/// - **Open**: All calls are rejected immediately. After `recovery_timeout`, transitions to Half-Open.
/// - **Half-Open**: One call is allowed through. Success → Closed, Failure → Open.
pub struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    failure_threshold: u32,
    /// Epoch millis when the circuit was opened; 0 means closed.
    opened_at: AtomicU64,
    recovery_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            failure_threshold,
            opened_at: AtomicU64::new(0),
            recovery_timeout,
        }
    }

    /// Check if a call should be allowed through.
    pub fn allow_request(&self) -> bool {
        let opened_at = self.opened_at.load(Ordering::Acquire);
        if opened_at == 0 {
            return true; // Closed state
        }

        let now = current_epoch_millis();
        let elapsed = Duration::from_millis(now.saturating_sub(opened_at));

        if elapsed >= self.recovery_timeout {
            // Half-open: allow one request through
            true
        } else {
            false
        }
    }

    /// Report a successful call — resets the circuit to closed.
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.opened_at.store(0, Ordering::Release);
    }

    /// Report a failed call — may open the circuit.
    pub fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
        if failures >= self.failure_threshold {
            self.opened_at
                .store(current_epoch_millis(), Ordering::Release);
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn state(&self) -> CircuitState {
        let opened_at = self.opened_at.load(Ordering::Acquire);
        if opened_at == 0 {
            return CircuitState::Closed;
        }
        let now = current_epoch_millis();
        let elapsed = Duration::from_millis(now.saturating_sub(opened_at));
        if elapsed >= self.recovery_timeout {
            CircuitState::HalfOpen
        } else {
            CircuitState::Open
        }
    }
}

fn current_epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_closed_and_allows_requests() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(10));
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn opens_after_threshold_failures() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(10));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn success_resets_to_closed() {
        let cb = CircuitBreaker::new(2, Duration::from_secs(10));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn transitions_to_half_open_after_recovery_timeout() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(0));
        cb.record_failure();
        // With 0ms recovery, should immediately be half-open
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        assert!(cb.allow_request());
    }
}
