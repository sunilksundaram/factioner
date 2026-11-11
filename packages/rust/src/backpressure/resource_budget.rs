use crate::types::{BudgetDeduction, ResourceBudgetConfig};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const TPM_OVERSHOOT_RATIO: f64 = 1.25;
const MAX_TPM_ESTIMATE_BIAS: f64 = 2.0;
const TPM_BIAS_DEFAULT_MS: u64 = 30_000;

struct WindowState {
    consumed: f64,
    window_start: Instant,
}

struct LimiterInner {
    rpm_state: WindowState,
    tpm_state: WindowState,
    pages_state: WindowState,
    rps_state: WindowState,
    effective_rpm: Option<f64>,
    rpm_signal_until: Option<Instant>,
    effective_tpm: Option<f64>,
    tpm_signal_until: Option<Instant>,
    tpm_estimate_bias: f64,
    signal_tightened: bool,
}

fn poison_ok<T>(result: Result<T, std::sync::PoisonError<T>>) -> T {
    result.unwrap_or_else(|e| e.into_inner())
}

pub struct MultiResourceLimiter {
    rpm_limit: Option<f64>,
    tpm_limit: Option<f64>,
    pages_limit: Option<f64>,
    rps_limit: Option<f64>,
    inner: Mutex<LimiterInner>,
}

impl MultiResourceLimiter {
    pub fn new(config: ResourceBudgetConfig) -> Self {
        let now = Instant::now();
        Self {
            rpm_limit: config.rpm,
            tpm_limit: config.tpm,
            pages_limit: config.pages_per_min,
            rps_limit: config.rps,
            inner: Mutex::new(LimiterInner {
                rpm_state: WindowState {
                    consumed: 0.0,
                    window_start: now,
                },
                tpm_state: WindowState {
                    consumed: 0.0,
                    window_start: now,
                },
                pages_state: WindowState {
                    consumed: 0.0,
                    window_start: now,
                },
                rps_state: WindowState {
                    consumed: 0.0,
                    window_start: now,
                },
                effective_rpm: None,
                rpm_signal_until: None,
                effective_tpm: None,
                tpm_signal_until: None,
                tpm_estimate_bias: 1.0,
                signal_tightened: false,
            }),
        }
    }

    fn refresh_window(state: &mut WindowState, window_secs: f64, now: Instant) {
        let elapsed = now.duration_since(state.window_start).as_secs_f64();
        if elapsed >= window_secs {
            state.consumed = 0.0;
            state.window_start = now;
        }
    }

    fn restore_signal_if_expired(inner: &mut LimiterInner, now: Instant) {
        if let Some(until) = inner.rpm_signal_until {
            if now >= until {
                inner.effective_rpm = None;
                inner.rpm_signal_until = None;
            }
        }
        if let Some(until) = inner.tpm_signal_until {
            if now >= until {
                inner.effective_tpm = None;
                inner.tpm_signal_until = None;
                inner.tpm_estimate_bias = 1.0;
            }
        }
        inner.signal_tightened =
            inner.rpm_signal_until.is_some() || inner.tpm_signal_until.is_some();
    }

    fn current_rpm_limit(&self, inner: &LimiterInner) -> Option<f64> {
        match (self.rpm_limit, inner.effective_rpm) {
            (Some(base), Some(eff)) => Some(eff.min(base)),
            (Some(base), None) => Some(base),
            (None, Some(eff)) => Some(eff),
            (None, None) => None,
        }
    }

    fn current_tpm_limit(&self, inner: &LimiterInner) -> Option<f64> {
        match (self.tpm_limit, inner.effective_tpm) {
            (Some(base), Some(eff)) => Some(eff.min(base)),
            (Some(base), None) => Some(base),
            (None, Some(eff)) => Some(eff),
            (None, None) => None,
        }
    }

    fn tighten_tpm(inner: &mut LimiterInner, baseline: f64, now: Instant, retry_after_ms: u64, factor: f64) {
        let tightened = (baseline * factor).max(1.0).min(baseline);
        inner.effective_tpm = Some(tightened);
        let until = now + Duration::from_millis(retry_after_ms.max(1));
        inner.tpm_signal_until = Some(match inner.tpm_signal_until {
            Some(prev) if prev > until => prev,
            _ => until,
        });
        inner.signal_tightened = true;
    }

    /// Temporarily tighten effective RPM and TPM on OVERLOADED + Retry-After.
    /// Never raises above the declared baseline.
    pub fn apply_overload_signal(&self, retry_after_ms: u64) {
        let now = Instant::now();
        let mut inner = poison_ok(self.inner.lock());
        let until_ms = retry_after_ms.max(1);

        if let Some(baseline) = self.rpm_limit {
            let tightened = (baseline / 2.0).max(1.0).min(baseline);
            inner.effective_rpm = Some(tightened);
            inner.rpm_signal_until = Some(now + Duration::from_millis(until_ms));
            inner.signal_tightened = true;
        }

        if let Some(baseline) = self.tpm_limit {
            Self::tighten_tpm(&mut inner, baseline, now, until_ms, 0.5);
        }
    }

    pub fn effective_rpm(&self) -> Option<f64> {
        let now = Instant::now();
        let mut inner = poison_ok(self.inner.lock());
        Self::restore_signal_if_expired(&mut inner, now);
        self.current_rpm_limit(&inner)
    }

    pub fn effective_tpm(&self) -> Option<f64> {
        let now = Instant::now();
        let mut inner = poison_ok(self.inner.lock());
        Self::restore_signal_if_expired(&mut inner, now);
        self.current_tpm_limit(&inner)
    }

    pub fn tpm_estimate_bias(&self) -> f64 {
        let now = Instant::now();
        let mut inner = poison_ok(self.inner.lock());
        Self::restore_signal_if_expired(&mut inner, now);
        inner.tpm_estimate_bias
    }

    pub fn is_signal_tightened(&self) -> bool {
        let now = Instant::now();
        let mut inner = poison_ok(self.inner.lock());
        Self::restore_signal_if_expired(&mut inner, now);
        inner.signal_tightened
    }

    pub fn has_available(&self, estimate: &BudgetDeduction) -> bool {
        let now = Instant::now();
        let mut inner = poison_ok(self.inner.lock());
        Self::restore_signal_if_expired(&mut inner, now);

        if let Some(limit) = self.current_rpm_limit(&inner) {
            Self::refresh_window(&mut inner.rpm_state, 60.0, now);
            let needed = estimate.rpm.unwrap_or(1.0);
            if inner.rpm_state.consumed + needed > limit {
                return false;
            }
        }

        if let Some(limit) = self.current_tpm_limit(&inner) {
            if let Some(needed) = estimate.tpm {
                Self::refresh_window(&mut inner.tpm_state, 60.0, now);
                let biased = needed * inner.tpm_estimate_bias;
                if inner.tpm_state.consumed + biased > limit {
                    return false;
                }
            }
        }

        if let Some(limit) = self.pages_limit {
            if let Some(needed) = estimate.pages {
                Self::refresh_window(&mut inner.pages_state, 60.0, now);
                if inner.pages_state.consumed + needed > limit {
                    return false;
                }
            }
        }

        if let Some(limit) = self.rps_limit {
            Self::refresh_window(&mut inner.rps_state, 1.0, now);
            let needed = estimate.rps.unwrap_or(1.0);
            if inner.rps_state.consumed + needed > limit {
                return false;
            }
        }

        true
    }

    /// Atomic check and acquire across all configured resource dimensions under a single lock.
    pub fn try_acquire(&self, estimate: &BudgetDeduction) -> bool {
        let now = Instant::now();
        let mut inner = poison_ok(self.inner.lock());
        Self::restore_signal_if_expired(&mut inner, now);

        let rpm_needed = estimate.rpm.unwrap_or(1.0);
        let tpm_needed = estimate.tpm.unwrap_or(0.0) * inner.tpm_estimate_bias;
        let pages_needed = estimate.pages.unwrap_or(0.0);
        let rps_needed = estimate.rps.unwrap_or(1.0);

        if let Some(limit) = self.current_rpm_limit(&inner) {
            Self::refresh_window(&mut inner.rpm_state, 60.0, now);
            if inner.rpm_state.consumed + rpm_needed > limit {
                return false;
            }
        }

        if let Some(limit) = self.current_tpm_limit(&inner) {
            if estimate.tpm.is_some() {
                Self::refresh_window(&mut inner.tpm_state, 60.0, now);
                if inner.tpm_state.consumed + tpm_needed > limit {
                    return false;
                }
            }
        }

        if let Some(limit) = self.pages_limit {
            if estimate.pages.is_some() {
                Self::refresh_window(&mut inner.pages_state, 60.0, now);
                if inner.pages_state.consumed + pages_needed > limit {
                    return false;
                }
            }
        }

        if let Some(limit) = self.rps_limit {
            Self::refresh_window(&mut inner.rps_state, 1.0, now);
            if inner.rps_state.consumed + rps_needed > limit {
                return false;
            }
        }

        if self.current_rpm_limit(&inner).is_some() {
            inner.rpm_state.consumed += rpm_needed;
        }
        if self.tpm_limit.is_some() && estimate.tpm.is_some() {
            inner.tpm_state.consumed += tpm_needed;
        }
        if self.pages_limit.is_some() && estimate.pages.is_some() {
            inner.pages_state.consumed += pages_needed;
        }
        if self.rps_limit.is_some() {
            inner.rps_state.consumed += rps_needed;
        }

        true
    }

    pub fn reconcile(&self, estimated: &BudgetDeduction, actual: &BudgetDeduction) {
        let now = Instant::now();
        let mut inner = poison_ok(self.inner.lock());

        if self.tpm_limit.is_some() {
            let est_tpm = estimated.tpm.unwrap_or(0.0);
            let act_tpm = actual.tpm.unwrap_or(0.0);
            Self::refresh_window(&mut inner.tpm_state, 60.0, now);
            inner.tpm_state.consumed = (inner.tpm_state.consumed + (act_tpm - est_tpm)).max(0.0);

            if est_tpm > 0.0 && act_tpm > est_tpm * TPM_OVERSHOOT_RATIO {
                if let Some(baseline) = self.tpm_limit {
                    let ratio = act_tpm / est_tpm;
                    inner.tpm_estimate_bias = inner.tpm_estimate_bias.max(ratio).min(MAX_TPM_ESTIMATE_BIAS);
                    let factor = 1.0 / ratio.min(2.0);
                    Self::tighten_tpm(&mut inner, baseline, now, TPM_BIAS_DEFAULT_MS, factor);
                }
            }
        }

        if self.pages_limit.is_some() {
            let est_pages = estimated.pages.unwrap_or(0.0);
            let act_pages = actual.pages.unwrap_or(0.0);
            Self::refresh_window(&mut inner.pages_state, 60.0, now);
            inner.pages_state.consumed =
                (inner.pages_state.consumed + (act_pages - est_pages)).max(0.0);
        }
    }

    pub fn refund(&self, deduction: &BudgetDeduction) {
        let mut inner = poison_ok(self.inner.lock());
        if self.current_rpm_limit(&inner).is_some() || self.rpm_limit.is_some() {
            inner.rpm_state.consumed =
                (inner.rpm_state.consumed - deduction.rpm.unwrap_or(1.0)).max(0.0);
        }
        if self.tpm_limit.is_some() {
            if let Some(tpm) = deduction.tpm {
                let bias = inner.tpm_estimate_bias;
                inner.tpm_state.consumed = (inner.tpm_state.consumed - tpm * bias).max(0.0);
            }
        }
        if self.pages_limit.is_some() {
            if let Some(pages) = deduction.pages {
                inner.pages_state.consumed = (inner.pages_state.consumed - pages).max(0.0);
            }
        }
        if self.rps_limit.is_some() {
            inner.rps_state.consumed =
                (inner.rps_state.consumed - deduction.rps.unwrap_or(1.0)).max(0.0);
        }
    }

    pub fn get_usage(&self) -> (f64, f64, f64) {
        let now = Instant::now();
        let mut inner = poison_ok(self.inner.lock());
        Self::restore_signal_if_expired(&mut inner, now);
        Self::refresh_window(&mut inner.rpm_state, 60.0, now);
        Self::refresh_window(&mut inner.tpm_state, 60.0, now);
        Self::refresh_window(&mut inner.pages_state, 60.0, now);

        (
            inner.rpm_state.consumed,
            inner.tpm_state.consumed,
            inner.pages_state.consumed,
        )
    }
}
