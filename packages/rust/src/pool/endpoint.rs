use crate::backpressure::MultiResourceLimiter;
use crate::resilience::CircuitBreaker;
use crate::types::{
    BudgetDeduction, BudgetUsageMetrics, CircuitBreakerConfig, EndpointConfig, EndpointHealth,
    EndpointMetrics, FailureClass, ResourceBudgetConfig,
};
use std::sync::Mutex;

fn poison_ok<T>(result: Result<T, std::sync::PoisonError<T>>) -> T {
    result.unwrap_or_else(|e| e.into_inner())
}

struct EndpointInner {
    active_leases: usize,
    total_requests: usize,
    successful_requests: usize,
    failed_requests: usize,
    ema_latency_ms: f64,
    recent_latencies: Vec<u64>,
    health_status: EndpointHealth,
}

pub struct EndpointInstance {
    pub config: EndpointConfig,
    pub circuit_breaker: CircuitBreaker,
    pub budget_limiter: MultiResourceLimiter,
    inner: Mutex<EndpointInner>,
}

impl EndpointInstance {
    pub fn new(config: EndpointConfig, cb_config: Option<CircuitBreakerConfig>) -> Self {
        let circuit_breaker = CircuitBreaker::new(cb_config.unwrap_or_default());
        // max_concurrency is source of truth; budget.concurrency ignored
        let budget = config.budget.clone().unwrap_or(ResourceBudgetConfig::default());

        Self {
            config,
            circuit_breaker,
            budget_limiter: MultiResourceLimiter::new(budget),
            inner: Mutex::new(EndpointInner {
                active_leases: 0,
                total_requests: 0,
                successful_requests: 0,
                failed_requests: 0,
                ema_latency_ms: 0.0,
                recent_latencies: Vec::new(),
                health_status: EndpointHealth::Healthy,
            }),
        }
    }

    pub fn is_available(&self, estimate: &BudgetDeduction) -> bool {
        let inner = poison_ok(self.inner.lock());
        if inner.health_status == EndpointHealth::Unhealthy {
            return false;
        }
        if inner.active_leases >= self.config.max_concurrency {
            return false;
        }
        drop(inner);

        if !self.circuit_breaker.can_execute() {
            return false;
        }
        if !self.budget_limiter.has_available(estimate) {
            return false;
        }
        true
    }

    pub fn cool_off_remaining_ms(&self) -> u64 {
        self.circuit_breaker.cool_off_remaining_ms()
    }

    /// Atomic acquire: probe claim + budget + lease under one lock for lease counts.
    pub fn acquire_slot(&self, estimate: &BudgetDeduction) -> bool {
        let mut inner = poison_ok(self.inner.lock());
        if inner.health_status == EndpointHealth::Unhealthy {
            return false;
        }
        if inner.active_leases >= self.config.max_concurrency {
            return false;
        }
        if !self.budget_limiter.has_available(estimate) {
            return false;
        }
        if !self.circuit_breaker.try_claim_probe() {
            return false;
        }
        if !self.budget_limiter.try_acquire(estimate) {
            self.circuit_breaker.release_probe_claim();
            return false;
        }

        inner.active_leases += 1;
        inner.total_requests += 1;
        true
    }

    pub fn release_slot(
        &self,
        success: bool,
        duration_ms: u64,
        estimated: Option<&BudgetDeduction>,
        actual: Option<&BudgetDeduction>,
        failure_class: Option<FailureClass>,
    ) {
        let mut inner = poison_ok(self.inner.lock());
        if inner.active_leases > 0 {
            inner.active_leases -= 1;
        }

        if success {
            inner.successful_requests += 1;
            self.circuit_breaker.record_success();
            if let (Some(est), Some(act)) = (estimated, actual) {
                self.budget_limiter.reconcile(est, act);
            }
            let cb = self.circuit_breaker.get_state();
            if cb == crate::types::CircuitState::Closed {
                inner.health_status = EndpointHealth::Healthy;
            } else if cb == crate::types::CircuitState::HalfOpen {
                inner.health_status = EndpointHealth::Degraded;
            }
        } else {
            inner.failed_requests += 1;
            let trips = matches!(
                failure_class,
                None | Some(FailureClass::Transient)
                    | Some(FailureClass::Overloaded)
                    | Some(FailureClass::Unavailable)
            );
            if trips {
                self.circuit_breaker.record_failure();
            } else {
                self.circuit_breaker.release_probe_claim();
            }
            if let Some(est) = estimated {
                if actual.is_none() {
                    self.budget_limiter.refund(est);
                }
            }
            match self.circuit_breaker.get_state() {
                crate::types::CircuitState::Open => {
                    inner.health_status = EndpointHealth::CoolingOff;
                }
                crate::types::CircuitState::HalfOpen => {
                    inner.health_status = EndpointHealth::Degraded;
                }
                _ => {}
            }
        }

        if duration_ms > 0 {
            inner.recent_latencies.push(duration_ms);
            if inner.recent_latencies.len() > 50 {
                inner.recent_latencies.remove(0);
            }
            if inner.ema_latency_ms == 0.0 {
                inner.ema_latency_ms = duration_ms as f64;
            } else {
                inner.ema_latency_ms = 0.2 * (duration_ms as f64) + 0.8 * inner.ema_latency_ms;
            }
        }
    }

    pub fn set_health(&self, health: EndpointHealth) {
        poison_ok(self.inner.lock()).health_status = health;
    }

    pub fn get_active_leases(&self) -> usize {
        poison_ok(self.inner.lock()).active_leases
    }

    pub fn get_latency_ema(&self) -> u64 {
        poison_ok(self.inner.lock()).ema_latency_ms.round() as u64
    }

    pub fn get_circuit_state(&self) -> crate::types::CircuitState {
        self.circuit_breaker.get_state()
    }

    pub fn get_health(&self) -> EndpointHealth {
        poison_ok(self.inner.lock()).health_status
    }

    pub fn get_metrics(&self) -> EndpointMetrics {
        let inner = poison_ok(self.inner.lock());
        let (rpm, tpm, pages) = self.budget_limiter.get_usage();

        let p95 = if inner.recent_latencies.is_empty() {
            0
        } else {
            let mut sorted = inner.recent_latencies.clone();
            sorted.sort_unstable();
            let idx = ((sorted.len() as f64 * 0.95).floor() as usize).min(sorted.len() - 1);
            sorted[idx]
        };

        EndpointMetrics {
            id: self.config.id.clone(),
            url: self.config.url.clone(),
            health: inner.health_status,
            circuit_state: self.circuit_breaker.get_state(),
            active_leases: inner.active_leases,
            max_concurrency: self.config.max_concurrency,
            total_requests: inner.total_requests,
            successful_requests: inner.successful_requests,
            failed_requests: inner.failed_requests,
            avg_latency_ms: inner.ema_latency_ms.round() as u64,
            p95_latency_ms: p95,
            budget_usage: BudgetUsageMetrics {
                rpm,
                tpm,
                pages,
                effective_rpm: self.budget_limiter.effective_rpm(),
                effective_tpm: self.budget_limiter.effective_tpm(),
                signal_tightened: self.budget_limiter.is_signal_tightened(),
            },
        }
    }
}
