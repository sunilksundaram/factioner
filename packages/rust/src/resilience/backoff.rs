use crate::types::{BackoffConfig, BackoffStrategy};
use rand::Rng;

pub struct BackoffCalculator {
    config: BackoffConfig,
}

impl BackoffCalculator {
    pub fn new(config: BackoffConfig) -> Self {
        Self { config }
    }

    /// All delays are in milliseconds. `retry_after_ms` is milliseconds when present.
    pub fn calculate_delay(
        &self,
        attempt: usize,
        retry_after_ms: Option<u64>,
        previous_job_delay: Option<u64>,
    ) -> (u64, u64) {
        if self.config.respect_retry_after {
            if let Some(delay_ms) = retry_after_ms {
                let d = delay_ms.min(self.config.max_delay_ms);
                return (d, d);
            }
        }

        let base = (self.config.initial_delay_ms as f64
            * self.config.factor.powi(attempt as i32)) as u64;
        let capped = base.min(self.config.max_delay_ms);

        let mut rng = rand::thread_rng();

        match self.config.strategy {
            BackoffStrategy::FullJitter => {
                let delay = if capped == 0 {
                    0
                } else {
                    rng.gen_range(0..=capped)
                };
                (delay, delay)
            }
            BackoffStrategy::EqualJitter => {
                let half = capped / 2;
                let delay = if half == 0 {
                    0
                } else {
                    half + rng.gen_range(0..=half)
                };
                (delay, delay)
            }
            BackoffStrategy::Decorrelated => {
                let prev = previous_job_delay.unwrap_or(self.config.initial_delay_ms);
                let low = self.config.initial_delay_ms;
                let high = (prev * 3).max(low + 1);
                let delay = rng.gen_range(low..=high).min(self.config.max_delay_ms);
                (delay, delay)
            }
            BackoffStrategy::Exponential => (capped, capped),
        }
    }
}
