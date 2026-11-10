use crate::types::{CircuitBreakerConfig, CircuitState};
use std::sync::Mutex;
use std::time::Instant;

struct Sample {
    timestamp: Instant,
    success: bool,
}

struct CircuitBreakerInner {
    state: CircuitState,
    state_changed_at: Instant,
    samples: Vec<Sample>,
    half_open_successes: usize,
    half_open_in_flight: usize,
}

pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    inner: Mutex<CircuitBreakerInner>,
}

fn poison_ok<T>(result: Result<T, std::sync::PoisonError<T>>) -> T {
    result.unwrap_or_else(|e| e.into_inner())
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(CircuitBreakerInner {
                state: CircuitState::Closed,
                state_changed_at: Instant::now(),
                samples: Vec::new(),
                half_open_successes: 0,
                half_open_in_flight: 0,
            }),
        }
    }

    /// Pure eligibility check — does not claim a half-open probe.
    pub fn can_execute(&self) -> bool {
        let inner = poison_ok(self.inner.lock());
        match inner.state {
            CircuitState::Open => {
                inner.state_changed_at.elapsed().as_millis() as u64 >= self.config.cooling_off_duration_ms
            }
            CircuitState::HalfOpen => inner.half_open_in_flight < 1,
            CircuitState::Closed => true,
        }
    }

    /// Claims the single half-open probe (or allows CLOSED traffic).
    pub fn try_claim_probe(&self) -> bool {
        let mut inner = poison_ok(self.inner.lock());
        match inner.state {
            CircuitState::Open => {
                if (inner.state_changed_at.elapsed().as_millis() as u64) < self.config.cooling_off_duration_ms {
                    return false;
                }
                inner.state = CircuitState::HalfOpen;
                inner.state_changed_at = Instant::now();
                inner.half_open_successes = 0;
                inner.half_open_in_flight = 1;
                true
            }
            CircuitState::HalfOpen => {
                if inner.half_open_in_flight < 1 {
                    inner.half_open_in_flight = 1;
                    true
                } else {
                    false
                }
            }
            CircuitState::Closed => true,
        }
    }

    pub fn release_probe_claim(&self) {
        let mut inner = poison_ok(self.inner.lock());
        if inner.state == CircuitState::HalfOpen && inner.half_open_in_flight > 0 {
            inner.half_open_in_flight -= 1;
        }
    }

    pub fn record_success(&self) {
        let mut inner = poison_ok(self.inner.lock());
        let now = Instant::now();

        Self::clean_samples(&mut inner.samples, now, self.config.sampling_window_ms);
        inner.samples.push(Sample {
            timestamp: now,
            success: true,
        });

        if inner.state == CircuitState::HalfOpen {
            if inner.half_open_in_flight > 0 {
                inner.half_open_in_flight -= 1;
            }

            inner.half_open_successes += 1;
            if inner.half_open_successes >= self.config.half_open_trial_successes {
                inner.state = CircuitState::Closed;
                inner.state_changed_at = now;
                inner.samples.clear();
                inner.half_open_successes = 0;
            }
        }
    }

    pub fn record_failure(&self) {
        let mut inner = poison_ok(self.inner.lock());
        let now = Instant::now();

        Self::clean_samples(&mut inner.samples, now, self.config.sampling_window_ms);
        inner.samples.push(Sample {
            timestamp: now,
            success: false,
        });

        if inner.state == CircuitState::HalfOpen {
            if inner.half_open_in_flight > 0 {
                inner.half_open_in_flight -= 1;
            }
            inner.state = CircuitState::Open;
            inner.state_changed_at = now;
            return;
        }

        if inner.state == CircuitState::Closed && inner.samples.len() >= self.config.minimum_request_volume {
            let failures = inner.samples.iter().filter(|s| !s.success).count();
            let failure_rate = (failures as f64 / inner.samples.len() as f64) * 100.0;
            if failure_rate >= self.config.failure_threshold_percentage {
                inner.state = CircuitState::Open;
                inner.state_changed_at = now;
            }
        }
    }

    fn clean_samples(samples: &mut Vec<Sample>, now: Instant, window_ms: u64) {
        let window = std::time::Duration::from_millis(window_ms);
        samples.retain(|s| now.duration_since(s.timestamp) <= window);
    }

    /// Read-only state; does not transition OPEN → HALF_OPEN.
    pub fn get_state(&self) -> CircuitState {
        poison_ok(self.inner.lock()).state
    }

    /// Milliseconds until OPEN cool-off elapses; 0 if already eligible / not OPEN.
    pub fn cool_off_remaining_ms(&self) -> u64 {
        let inner = poison_ok(self.inner.lock());
        if inner.state != CircuitState::Open {
            return 0;
        }
        let elapsed = inner.state_changed_at.elapsed().as_millis() as u64;
        self.config.cooling_off_duration_ms.saturating_sub(elapsed)
    }
}
