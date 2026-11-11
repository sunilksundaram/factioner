use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum JobPriority {
    Critical = 0,
    High = 1,
    Normal = 2,
    Batch = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionPolicy {
    #[serde(alias = "reject")]
    Reject,
    #[serde(rename = "drop_oldest", alias = "DropOldest", alias = "dropOldest")]
    DropOldest,
    #[serde(rename = "drop_newest", alias = "DropNewest", alias = "dropNewest")]
    DropNewest,
    #[serde(alias = "timeout")]
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackoffStrategy {
    FullJitter,
    EqualJitter,
    Decorrelated,
    Exponential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingStrategy {
    #[serde(rename = "round_robin", alias = "RoundRobin", alias = "roundRobin")]
    RoundRobin,
    #[serde(rename = "least_loaded", alias = "LeastLoaded", alias = "leastLoaded")]
    LeastLoaded,
    #[serde(rename = "weighted_latency", alias = "WeightedLatency", alias = "weightedLatency")]
    WeightedLatency,
    #[serde(rename = "priority_failover", alias = "PriorityFailover", alias = "priorityFailover")]
    PriorityFailover,
    #[serde(rename = "cost_optimized", alias = "CostOptimized", alias = "costOptimized")]
    CostOptimized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndpointHealth {
    Healthy,
    Degraded,
    Unhealthy,
    CoolingOff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureClass {
    Transient,   // Network glitch, 503, retryable
    Overloaded,  // 429 rate limit or TPM/RPM exceeded, retryable with backoff
    Unavailable, // Circuit tripped or endpoint down, failover to secondary
    Fatal,       // 400 Bad Request, 401 Unauthorized, unrecoverable
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureClassification {
    pub class: FailureClass,
    pub retry_after_ms: Option<u64>,
    pub message: String,
}

#[derive(Debug, Clone, Default)]
pub struct SecurityConfig {
    pub enforce_isolation: bool,
    pub allowed_tenants: Option<Vec<String>>,
    pub enable_pii_redaction: bool,
    pub vault_encryption_key: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceBudgetConfig {
    pub rpm: Option<f64>,
    pub tpm: Option<f64>,
    pub pages_per_min: Option<f64>,
    pub rps: Option<f64>,
    pub concurrency: Option<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BudgetDeduction {
    pub rpm: Option<f64>,
    pub tpm: Option<f64>,
    pub pages: Option<f64>,
    pub rps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretCredential {
    /// Wire name `type` (TS) with alias `credType` for older Rust-shaped JSON.
    #[serde(rename = "type", alias = "credType", alias = "cred_type")]
    pub cred_type: String, // "api_key" | "bearer" | "custom"
    pub value: String,
    pub header_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointConfig {
    pub id: String,
    pub url: String,
    pub max_concurrency: usize,
    pub budget: Option<ResourceBudgetConfig>,
    pub weight: Option<f64>,
    pub priority_tier: Option<usize>, // 0 = primary, 1 = secondary/failover
    pub cost_per_request: Option<f64>,
    pub credential: Option<SecretCredential>,
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CircuitBreakerConfig {
    pub failure_threshold_percentage: f64,
    pub minimum_request_volume: usize,
    pub sampling_window_ms: u64,
    pub cooling_off_duration_ms: u64,
    pub half_open_trial_successes: usize,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold_percentage: 50.0,
            minimum_request_volume: 5,
            sampling_window_ms: 10_000,
            cooling_off_duration_ms: 15_000,
            half_open_trial_successes: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackoffConfig {
    pub strategy: BackoffStrategy,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub factor: f64,
    pub respect_retry_after: bool,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            strategy: BackoffStrategy::FullJitter,
            initial_delay_ms: 250,
            max_delay_ms: 10_000,
            factor: 2.0,
            respect_retry_after: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackpressureConfig {
    pub max_queue_size: usize,
    pub admission_policy: AdmissionPolicy,
    pub queue_timeout_ms: u64,
    pub execution_timeout_ms: u64,
    pub admission_wait_timeout_ms: u64,
    pub shedding_threshold_percentage: f64,
    pub global_retry_budget_percentage: f64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 500,
            admission_policy: AdmissionPolicy::Reject,
            queue_timeout_ms: 30_000,
            execution_timeout_ms: 60_000,
            admission_wait_timeout_ms: 3_000,
            shedding_threshold_percentage: 85.0,
            global_retry_budget_percentage: 20.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantConfig {
    pub id: String,
    pub max_rps: Option<f64>,
    pub max_tpm: Option<f64>,
    pub max_concurrency: Option<usize>,
    pub daily_quota: Option<usize>,
    pub max_allowed_priority: Option<JobPriority>,
    pub priority_bonus: Option<i32>,
}

#[derive(Debug, Clone, Default)]
pub struct SubmitOptions {
    pub priority: Option<JobPriority>,
    pub tenant_id: Option<String>,
    pub queue_timeout_ms: Option<u64>,
    pub execution_timeout_ms: Option<u64>,
    pub max_retries: Option<usize>,
    pub idempotency_key: Option<String>,
    pub estimated_budget: Option<BudgetDeduction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
    pub endpoint_id: Option<String>,
    pub attempts: usize,
    pub duration_ms: u64,
    pub queue_wait_ms: u64,
    pub budget_deducted: Option<BudgetDeduction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BudgetUsageMetrics {
    pub rpm: f64,
    pub tpm: f64,
    pub pages: f64,
    pub effective_rpm: Option<f64>,
    pub effective_tpm: Option<f64>,
    /// Estimate inflation from usage overshoot (1 = none).
    pub tpm_estimate_bias: f64,
    pub signal_tightened: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointMetrics {
    pub id: String,
    pub url: String,
    pub health: EndpointHealth,
    pub circuit_state: CircuitState,
    pub active_leases: usize,
    pub max_concurrency: usize,
    pub total_requests: usize,
    pub successful_requests: usize,
    pub failed_requests: usize,
    pub avg_latency_ms: u64,
    pub p95_latency_ms: u64,
    pub budget_usage: BudgetUsageMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolMetrics {
    pub pool_name: String,
    pub service_type: String,
    pub active_jobs: usize,
    pub queued_jobs: usize,
    pub queue_capacity: usize,
    pub queue_saturation_pct: u8,
    pub total_completed: usize,
    pub total_failed: usize,
    pub total_rejected: usize,
    pub total_retries: usize,
    pub average_latency_ms: u64,
    pub p95_latency_ms: u64,
    pub throughput_rps: f64,
    pub endpoints: Vec<EndpointMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationPollSnapshot<T> {
    pub done: bool,
    pub status: String,
    pub data: Option<T>,
}
