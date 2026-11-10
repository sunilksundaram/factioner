use std::sync::Mutex;
use std::time::Instant;

/// TokenBucketRateLimiter implements the classic token bucket algorithm for simple single-dimension RPS limits.
///
/// For service pool and endpoint rate limiting, prefer [`super::resource_budget::MultiResourceLimiter`]
/// which supports multi-dimensional quotas (RPM, TPM, Pages/min, RPS) with atomic check-and-acquire.
#[deprecated(
    note = "Use MultiResourceLimiter for multi-dimensional resource quota enforcement"
)]
pub struct TokenBucketRateLimiter {
    capacity: f64,
    refill_rate: f64,
    state: Mutex<(f64, Instant)>,
}

#[allow(deprecated)]
impl TokenBucketRateLimiter {
    pub fn new(rate_limit_rps: f64) -> Self {
        let capacity = rate_limit_rps.max(1.0);
        Self {
            capacity,
            refill_rate: rate_limit_rps.max(0.1),
            state: Mutex::new((capacity, Instant::now())),
        }
    }

    pub fn try_acquire(&self, count: f64) -> bool {
        let mut state = self.state.lock().unwrap();
        let (tokens, last_refill) = &mut *state;

        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill).as_secs_f64();
        *tokens = (*tokens + elapsed * self.refill_rate).min(self.capacity);
        *last_refill = now;

        if *tokens >= count {
            *tokens -= count;
            true
        } else {
            false
        }
    }

    pub fn get_tokens(&self) -> f64 {
        let mut state = self.state.lock().unwrap();
        let (tokens, last_refill) = &mut *state;
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill).as_secs_f64();
        *tokens = (*tokens + elapsed * self.refill_rate).min(self.capacity);
        *last_refill = now;
        (tokens.floor() * 100.0).round() / 100.0
    }
}

