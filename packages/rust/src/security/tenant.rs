use crate::types::{JobPriority, TenantConfig};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

struct TenantState {
    config: TenantConfig,
    tokens: f64,
    last_refill: Instant,
    active_concurrency: usize,
    daily_consumed: usize,
    last_daily_reset: Instant,
    tpm_tokens: f64,
    last_tpm_refill: Instant,
}

pub struct TenantGovernor {
    tenants: Mutex<HashMap<String, TenantState>>,
    enforce_isolation: bool,
    allowed_tenants: Option<Vec<String>>,
}

fn poison_ok<T>(result: Result<T, std::sync::PoisonError<T>>) -> T {
    result.unwrap_or_else(|e| e.into_inner())
}

impl TenantGovernor {
    pub fn new(enforce_isolation: bool) -> Self {
        Self {
            tenants: Mutex::new(HashMap::new()),
            enforce_isolation,
            allowed_tenants: None,
        }
    }

    pub fn with_security(enforce_isolation: bool, allowed_tenants: Option<Vec<String>>) -> Self {
        Self {
            tenants: Mutex::new(HashMap::new()),
            enforce_isolation,
            allowed_tenants,
        }
    }

    pub fn register_tenant(&self, config: TenantConfig) {
        let max_rps = config.max_rps.unwrap_or(50.0);
        let max_tpm = config.max_tpm.unwrap_or(f64::INFINITY);
        let mut map = poison_ok(self.tenants.lock());
        map.insert(
            config.id.clone(),
            TenantState {
                config,
                tokens: max_rps,
                last_refill: Instant::now(),
                active_concurrency: 0,
                daily_consumed: 0,
                last_daily_reset: Instant::now(),
                tpm_tokens: max_tpm,
                last_tpm_refill: Instant::now(),
            },
        );
    }

    pub fn check_eligibility(
        &self,
        tenant_id: Option<&str>,
        estimate_tpm: Option<f64>,
    ) -> Result<(), String> {
        let tid = match tenant_id {
            Some(id) => id,
            None => {
                if self.enforce_isolation {
                    return Err("Tenant ID is required under strict isolation policy".to_string());
                }
                return Ok(());
            }
        };

        if let Some(ref allowed) = self.allowed_tenants {
            if !allowed.iter().any(|a| a == tid) {
                return Err(format!("Tenant '{}' is not authorized", tid));
            }
        }

        let mut map = poison_ok(self.tenants.lock());
        if !map.contains_key(tid) {
            if self.enforce_isolation {
                return Err(format!(
                    "Tenant '{}' is not registered and auto-registration is disabled",
                    tid
                ));
            }
            let max_rps = 50.0;
            map.insert(
                tid.to_string(),
                TenantState {
                    config: TenantConfig {
                        id: tid.to_string(),
                        max_rps: Some(max_rps),
                        max_tpm: None,
                        max_concurrency: None,
                        daily_quota: None,
                        max_allowed_priority: None,
                        priority_bonus: None,
                    },
                    tokens: max_rps,
                    last_refill: Instant::now(),
                    active_concurrency: 0,
                    daily_consumed: 0,
                    last_daily_reset: Instant::now(),
                    tpm_tokens: f64::INFINITY,
                    last_tpm_refill: Instant::now(),
                },
            );
        }

        let state = map.get_mut(tid).unwrap();

        if state.last_daily_reset.elapsed().as_secs() > 86400 {
            state.daily_consumed = 0;
            state.last_daily_reset = Instant::now();
        }

        if let Some(max_conc) = state.config.max_concurrency {
            if state.active_concurrency >= max_conc {
                return Err(format!(
                    "Tenant '{}' reached concurrency limit ({})",
                    tid, max_conc
                ));
            }
        }

        if let Some(quota) = state.config.daily_quota {
            if state.daily_consumed >= quota {
                return Err(format!("Tenant '{}' reached daily quota ({})", tid, quota));
            }
        }

        let max_rps = state.config.max_rps.unwrap_or(50.0);
        let elapsed = state.last_refill.elapsed().as_secs_f64();
        let current_tokens = (state.tokens + elapsed * max_rps).min(max_rps);
        if current_tokens < 1.0 {
            return Err(format!(
                "Tenant '{}' rate-limited ({:.0} RPS limit)",
                tid, max_rps
            ));
        }

        if let (Some(max_tpm), Some(needed)) = (state.config.max_tpm, estimate_tpm) {
            let tpm_elapsed = state.last_tpm_refill.elapsed().as_secs_f64();
            let available = (state.tpm_tokens + tpm_elapsed * (max_tpm / 60.0)).min(max_tpm);
            if available < needed {
                return Err(format!("Tenant '{}' TPM budget exceeded", tid));
            }
        }

        Ok(())
    }

    pub fn deduct_on_admission(&self, tenant_id: Option<&str>, estimate_tpm: Option<f64>) {
        if let Some(tid) = tenant_id {
            let mut map = poison_ok(self.tenants.lock());
            if let Some(state) = map.get_mut(tid) {
                let max_rps = state.config.max_rps.unwrap_or(50.0);
                let elapsed = state.last_refill.elapsed().as_secs_f64();
                state.tokens = (state.tokens + elapsed * max_rps).min(max_rps);
                state.last_refill = Instant::now();
                state.tokens = (state.tokens - 1.0).max(0.0);
                state.active_concurrency += 1;
                state.daily_consumed += 1;

                if let (Some(max_tpm), Some(needed)) = (state.config.max_tpm, estimate_tpm) {
                    let tpm_elapsed = state.last_tpm_refill.elapsed().as_secs_f64();
                    state.tpm_tokens =
                        (state.tpm_tokens + tpm_elapsed * (max_tpm / 60.0)).min(max_tpm);
                    state.last_tpm_refill = Instant::now();
                    state.tpm_tokens = (state.tpm_tokens - needed).max(0.0);
                }
            }
        }
    }

    pub fn refund_on_drop(&self, tenant_id: Option<&str>, estimate_tpm: Option<f64>) {
        if let Some(tid) = tenant_id {
            let mut map = poison_ok(self.tenants.lock());
            if let Some(state) = map.get_mut(tid) {
                if state.active_concurrency > 0 {
                    state.active_concurrency -= 1;
                }
                if state.daily_consumed > 0 {
                    state.daily_consumed -= 1;
                }
                state.tokens += 1.0;
                if let Some(needed) = estimate_tpm {
                    state.tpm_tokens += needed;
                }
            }
        }
    }

    pub fn release_slot(&self, tenant_id: Option<&str>) {
        if let Some(tid) = tenant_id {
            let mut map = poison_ok(self.tenants.lock());
            if let Some(state) = map.get_mut(tid) {
                if state.active_concurrency > 0 {
                    state.active_concurrency -= 1;
                }
            }
        }
    }

    pub fn resolve_priority(&self, tenant_id: Option<&str>, requested: JobPriority) -> JobPriority {
        let tid = match tenant_id {
            Some(id) => id,
            None => {
                return if requested == JobPriority::Critical {
                    JobPriority::High
                } else {
                    requested
                };
            }
        };

        let map = poison_ok(self.tenants.lock());
        let state = match map.get(tid) {
            Some(s) => s,
            None => {
                return if requested == JobPriority::Critical {
                    JobPriority::High
                } else {
                    requested
                };
            }
        };

        let mut priority = requested;

        if let Some(max_allowed) = state.config.max_allowed_priority {
            if (priority as usize) < (max_allowed as usize) {
                priority = max_allowed;
            }
        }

        if let Some(bonus) = state.config.priority_bonus {
            let val = (priority as i32) + bonus;
            let clamped = val.clamp(0, 3);
            priority = match clamped {
                0 => JobPriority::Critical,
                1 => JobPriority::High,
                2 => JobPriority::Normal,
                _ => JobPriority::Batch,
            };
        }

        priority
    }
}
