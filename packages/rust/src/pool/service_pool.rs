use crate::adapters::ServiceAdapter;
use crate::backpressure::admission::AdmissionCode;
use crate::backpressure::priority_queue::{PriorityQueue, QueuedItem};
use crate::backpressure::AdmissionController;
use crate::pool::EndpointInstance;
use crate::resilience::BackoffCalculator;
use crate::security::{CredentialVault, PIIGuard, TenantGovernor};
use crate::types::{
    BackoffConfig, BackpressureConfig, BudgetDeduction, CircuitBreakerConfig, EndpointConfig,
    FailureClass, JobPriority, JobResult, OperationPollSnapshot, PoolMetrics, RoutingStrategy,
    SubmitOptions,
};
use futures::stream::Stream;
use futures::StreamExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Mutex, Notify};
use uuid::Uuid;

struct OpHandle {
    endpoint_id: String,
    operation_id: String,
}

pub struct ServicePool<TReq, TRes> {
    pub name: String,
    pub service_type: String,
    endpoints: Mutex<Vec<Arc<EndpointInstance>>>,
    adapter: Arc<dyn ServiceAdapter<TReq, TRes>>,
    admission_controller: Arc<AdmissionController>,
    backoff_calculator: BackoffCalculator,
    credential_vault: Arc<CredentialVault>,
    tenant_governor: Arc<TenantGovernor>,
    routing_strategy: RoutingStrategy,
    max_retries: usize,
    enable_pii_redaction: bool,
    queue: Arc<Mutex<PriorityQueue<(TReq, oneshot::Sender<Result<JobResult<TRes>, String>>)>>>,
    dispatcher_notify: Arc<Notify>,
    rr_weights: Arc<Mutex<HashMap<String, i64>>>,
    active_jobs: AtomicUsize,
    total_completed: AtomicUsize,
    total_failed: AtomicUsize,
    total_rejected: AtomicUsize,
    total_retries: AtomicUsize,
    recent_outcomes: Arc<Mutex<Vec<bool>>>,
    recent_latencies: Arc<Mutex<Vec<u64>>>,
    recent_completions: Arc<Mutex<Vec<Instant>>>,
    draining: AtomicUsize, // 0 = open, 1 = draining
    idempotency: Arc<Mutex<HashMap<String, IdempotencyEntry<TRes>>>>,
    operations: Arc<Mutex<HashMap<String, OpHandle>>>,
}

#[derive(Clone)]
enum IdempotencyEntry<TRes> {
    InFlight(tokio::sync::watch::Receiver<Option<Result<JobResult<TRes>, String>>>),
    Completed {
        result: JobResult<TRes>,
        expires_at: Instant,
    },
}

fn maybe_redact_payload<T: serde::Serialize + serde::de::DeserializeOwned>(
    enable: bool,
    payload: T,
) -> T {
    if !enable {
        return payload;
    }
    match serde_json::to_value(&payload) {
        Ok(v) => {
            let sanitized = PIIGuard::sanitize_json(v);
            serde_json::from_value(sanitized).unwrap_or(payload)
        }
        Err(_) => payload,
    }
}

struct StreamLeaseGuard {
    endpoint: Arc<EndpointInstance>,
    estimate: BudgetDeduction,
    tenant_id: Option<String>,
    tenant_governor: Arc<TenantGovernor>,
    dispatcher_notify: Arc<Notify>,
    start: Instant,
    released: AtomicBool,
    success: AtomicBool,
}

impl StreamLeaseGuard {
    fn mark_success(&self) {
        self.success.store(true, Ordering::SeqCst);
    }

    fn release(&self) {
        if self
            .released
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let ok = self.success.load(Ordering::SeqCst);
        let duration_ms = self.start.elapsed().as_millis() as u64;
        if ok {
            self.endpoint.release_slot(
                true,
                duration_ms,
                Some(&self.estimate),
                Some(&self.estimate),
                None,
            );
        } else {
            self.endpoint.release_slot(
                false,
                duration_ms,
                Some(&self.estimate),
                None,
                Some(FailureClass::Transient),
            );
        }
        self.tenant_governor
            .release_slot(self.tenant_id.as_deref());
        self.dispatcher_notify.notify_waiters();
    }
}

impl Drop for StreamLeaseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

struct GuardedTokenStream {
    inner: Pin<Box<dyn Stream<Item = Result<String, crate::adapters::AdapterError>> + Send>>,
    guard: Arc<StreamLeaseGuard>,
    finished_ok: bool,
}

impl Stream for GuardedTokenStream {
    type Item = Result<String, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(tok))) => Poll::Ready(Some(Ok(tok))),
            Poll::Ready(Some(Err(e))) => {
                self.finished_ok = false;
                self.guard.release();
                Poll::Ready(Some(Err(e.classification.message)))
            }
            Poll::Ready(None) => {
                if !self.finished_ok {
                    self.finished_ok = true;
                    self.guard.mark_success();
                    self.guard.release();
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<TReq: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static, TRes: Clone + Send + Sync + 'static>
    ServicePool<TReq, TRes>
{
    pub fn new(
        name: String,
        service_type: String,
        endpoints: Vec<EndpointConfig>,
        adapter: Arc<dyn ServiceAdapter<TReq, TRes>>,
        routing_strategy: RoutingStrategy,
        backpressure_config: Option<BackpressureConfig>,
        circuit_breaker_config: Option<CircuitBreakerConfig>,
        backoff_config: Option<BackoffConfig>,
        max_retries: usize,
    ) -> Arc<Self> {
        Self::with_security(
            name,
            service_type,
            endpoints,
            adapter,
            routing_strategy,
            backpressure_config,
            circuit_breaker_config,
            backoff_config,
            max_retries,
            false,
            None,
            None,
            false,
        )
    }

    pub fn with_security(
        name: String,
        service_type: String,
        endpoints: Vec<EndpointConfig>,
        adapter: Arc<dyn ServiceAdapter<TReq, TRes>>,
        routing_strategy: RoutingStrategy,
        backpressure_config: Option<BackpressureConfig>,
        circuit_breaker_config: Option<CircuitBreakerConfig>,
        backoff_config: Option<BackoffConfig>,
        max_retries: usize,
        enforce_isolation: bool,
        allowed_tenants: Option<Vec<String>>,
        vault_key: Option<&str>,
        enable_pii_redaction: bool,
    ) -> Arc<Self> {
        let vault = Arc::new(CredentialVault::with_key(vault_key));
        let mut endpoint_instances = Vec::new();

        for ep in endpoints {
            if let Some(ref cred) = ep.credential {
                vault.register(&ep.id, cred.clone());
            }
            endpoint_instances.push(Arc::new(EndpointInstance::new(
                ep,
                circuit_breaker_config.clone(),
            )));
        }

        let dispatcher_notify = Arc::new(Notify::new());

        let pool = Arc::new(Self {
            name,
            service_type,
            endpoints: Mutex::new(endpoint_instances),
            adapter,
            admission_controller: Arc::new(AdmissionController::new(
                backpressure_config.unwrap_or_default(),
            )),
            backoff_calculator: BackoffCalculator::new(backoff_config.unwrap_or_default()),
            credential_vault: vault,
            tenant_governor: Arc::new(TenantGovernor::with_security(
                enforce_isolation,
                allowed_tenants,
            )),
            routing_strategy,
            max_retries,
            enable_pii_redaction,
            queue: Arc::new(Mutex::new(PriorityQueue::new())),
            dispatcher_notify: Arc::clone(&dispatcher_notify),
            rr_weights: Arc::new(Mutex::new(HashMap::new())),
            active_jobs: AtomicUsize::new(0),
            total_completed: AtomicUsize::new(0),
            total_failed: AtomicUsize::new(0),
            total_rejected: AtomicUsize::new(0),
            total_retries: AtomicUsize::new(0),
            recent_outcomes: Arc::new(Mutex::new(Vec::new())),
            recent_latencies: Arc::new(Mutex::new(Vec::new())),
            recent_completions: Arc::new(Mutex::new(Vec::new())),
            draining: AtomicUsize::new(0),
            idempotency: Arc::new(Mutex::new(HashMap::new())),
            operations: Arc::new(Mutex::new(HashMap::new())),
        });

        let dispatcher = Arc::clone(&pool);
        tokio::spawn(async move {
            loop {
                if dispatcher.draining.load(Ordering::Relaxed) == 1
                    && dispatcher.queue.lock().await.is_empty()
                    && dispatcher.active_jobs.load(Ordering::Relaxed) == 0
                {
                    break;
                }
                dispatcher.dispatch_ready_jobs().await;
                tokio::select! {
                    _ = dispatcher.dispatcher_notify.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
            }
        });

        pool
    }

    pub fn register_tenant(&self, config: crate::types::TenantConfig) {
        self.tenant_governor.register_tenant(config);
    }

    async fn has_retry_budget(&self) -> bool {
        let pct = self
            .admission_controller
            .get_config()
            .global_retry_budget_percentage;
        let outcomes = self.recent_outcomes.lock().await;
        if outcomes.is_empty() {
            return true;
        }
        let window = if outcomes.len() > 100 {
            &outcomes[outcomes.len() - 100..]
        } else {
            &outcomes[..]
        };
        let retries = window.iter().filter(|r| **r).count();
        (retries as f64 / window.len() as f64) * 100.0 <= pct
    }

    fn format_admission_err(decision: &crate::backpressure::admission::AdmissionDecision) -> String {
        let reason = decision
            .reason
            .clone()
            .unwrap_or_else(|| "Queue full".to_string());
        match decision.code {
            Some(AdmissionCode::Saturated) => format!("SATURATED: {}", reason),
            Some(AdmissionCode::QueueFull) => format!("QUEUE_FULL: {}", reason),
            None => reason,
        }
    }

    pub async fn submit(
        &self,
        payload: TReq,
        options: SubmitOptions,
    ) -> Result<JobResult<TRes>, String> {
        if self.draining.load(Ordering::Relaxed) == 1 {
            return Err("Pool is draining".to_string());
        }

        let mut in_flight_tx = None;
        if let Some(ref key) = options.idempotency_key {
            let mut map = self.idempotency.lock().await;
            if let Some(entry) = map.get(key) {
                match entry {
                    IdempotencyEntry::Completed { result, expires_at } => {
                        if Instant::now() < *expires_at {
                            return Ok(result.clone());
                        }
                    }
                    IdempotencyEntry::InFlight(rx) => {
                        let mut rx = rx.clone();
                        drop(map);
                        while rx.borrow().is_none() {
                            if rx.changed().await.is_err() {
                                break;
                            }
                        }
                        if let Some(res) = &*rx.borrow() {
                            return res.clone();
                        }
                        let mut map = self.idempotency.lock().await;
                        map.remove(key);
                        drop(map);
                        return Err("Idempotent in-flight operation dropped".to_string());
                    }
                }
            }
            let (tx, rx) = tokio::sync::watch::channel(None);
            map.insert(key.clone(), IdempotencyEntry::InFlight(rx));
            in_flight_tx = Some(tx);
        }

        let payload = maybe_redact_payload(self.enable_pii_redaction, payload);

        let tenant_ref = options.tenant_id.as_deref();
        let estimate = options.estimated_budget.clone().unwrap_or_else(|| {
            self.adapter.estimate_budget(&payload)
        });

        if let Err(reason) = self
            .tenant_governor
            .check_eligibility(tenant_ref, estimate.tpm)
        {
            if let Some(ref key) = options.idempotency_key {
                let mut map = self.idempotency.lock().await;
                map.remove(key);
                drop(map);
                if let Some(tx) = in_flight_tx {
                    let _ = tx.send(Some(Err(format!("TENANT_DENIED: {}", reason))));
                }
            }
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(format!("TENANT_DENIED: Tenant admission denied: {}", reason));
        }

        let resolved_priority = self.tenant_governor.resolve_priority(
            tenant_ref,
            options.priority.unwrap_or(JobPriority::Normal),
        );

        self.tenant_governor
            .deduct_on_admission(tenant_ref, estimate.tpm);

        let bp = self.admission_controller.get_config();
        let (tx, rx) = oneshot::channel();
        let item = QueuedItem {
            id: options
                .idempotency_key
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
            priority: resolved_priority,
            payload: (payload, tx),
            tenant_id: options.tenant_id.clone(),
            created_at: Instant::now(),
            retries_remaining: options.max_retries.unwrap_or(self.max_retries),
            attempts: 0,
            previous_decorrelated_delay: None,
            estimated_budget: estimate.clone(),
            avoid_endpoint_ids: Vec::new(),
            tenant_charged: true,
            queue_timeout_ms: options
                .queue_timeout_ms
                .unwrap_or(bp.queue_timeout_ms),
            execution_timeout_ms: options
                .execution_timeout_ms
                .unwrap_or(bp.execution_timeout_ms),
        };

        let (decision, dropped, rejected) = self
            .admission_controller
            .admit_and_enqueue(&self.queue, item)
            .await;

        if let Some(dropped_item) = dropped {
            if dropped_item.tenant_charged {
                self.tenant_governor.refund_on_drop(
                    dropped_item.tenant_id.as_deref(),
                    dropped_item.estimated_budget.tpm,
                );
            }
            let (_payload, dtx) = dropped_item.payload;
            let _ = dtx.send(Err(
                "Dropped from queue to make room for higher-priority work".to_string(),
            ));
        }

        if !decision.accepted {
            let reason = Self::format_admission_err(&decision);
            if let Some(rej) = rejected {
                if rej.tenant_charged {
                    self.tenant_governor
                        .refund_on_drop(rej.tenant_id.as_deref(), rej.estimated_budget.tpm);
                }
            } else {
                self.tenant_governor
                    .refund_on_drop(tenant_ref, estimate.tpm);
            }
            if let Some(ref key) = options.idempotency_key {
                let mut map = self.idempotency.lock().await;
                map.remove(key);
                drop(map);
                if let Some(tx) = in_flight_tx {
                    let _ = tx.send(Some(Err(reason.clone())));
                }
            }
            self.total_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(reason);
        }

        // Net depth increased — wake dispatcher (not Timeout waiters via slot notify).
        self.dispatcher_notify.notify_one();

        let final_res = match rx.await {
            Ok(res) => res,
            Err(_) => Err("Job channel closed".to_string()),
        };

        if let Some(ref key) = options.idempotency_key {
            let mut map = self.idempotency.lock().await;
            match &final_res {
                Ok(result) => {
                    map.insert(
                        key.clone(),
                        IdempotencyEntry::Completed {
                            result: result.clone(),
                            expires_at: Instant::now() + Duration::from_secs(60),
                        },
                    );
                }
                Err(_) => {
                    map.remove(key);
                }
            }
            drop(map);
            if let Some(tx) = in_flight_tx {
                let _ = tx.send(Some(final_res.clone()));
            }
        }

        final_res
    }

    /// Streaming submit: returns a token stream that releases the lease on abort/drop.
    pub async fn submit_stream(
        &self,
        payload: TReq,
        options: SubmitOptions,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<String, String>> + Send>>, String> {
        if self.draining.load(Ordering::Relaxed) == 1 {
            return Err("Pool is draining".to_string());
        }

        let payload = maybe_redact_payload(self.enable_pii_redaction, payload);
        let tenant_ref = options.tenant_id.as_deref();
        let estimate = options
            .estimated_budget
            .clone()
            .unwrap_or_else(|| self.adapter.estimate_budget(&payload));
        self.tenant_governor
            .check_eligibility(tenant_ref, estimate.tpm)
            .map_err(|e| format!("TENANT_DENIED: Tenant admission denied: {}", e))?;
        self.tenant_governor
            .deduct_on_admission(tenant_ref, estimate.tpm);

        let ep = self
            .select_endpoint(&estimate, &[])
            .await
            .ok_or_else(|| "No capacity for stream".to_string())?;
        if !ep.acquire_slot(&estimate) {
            self.tenant_governor
                .refund_on_drop(tenant_ref, estimate.tpm);
            return Err("No capacity for stream".to_string());
        }

        let auth = self.credential_vault.get_auth_header(&ep.config.id);
        let auth_ref = auth.as_ref().map(|(k, v)| (k.as_str(), v.as_str()));

        match self.adapter.execute_stream(&ep.config, &payload, auth_ref).await {
            Ok((stream, _actual)) => {
                let guard = Arc::new(StreamLeaseGuard {
                    endpoint: Arc::clone(&ep),
                    estimate,
                    tenant_id: options.tenant_id.clone(),
                    tenant_governor: Arc::clone(&self.tenant_governor),
                    dispatcher_notify: Arc::clone(&self.dispatcher_notify),
                    start: Instant::now(),
                    released: AtomicBool::new(false),
                    success: AtomicBool::new(false),
                });
                let guarded = GuardedTokenStream {
                    inner: stream,
                    guard,
                    finished_ok: false,
                };
                Ok(Box::pin(guarded))
            }
            Err(e) => {
                ep.release_slot(false, 0, Some(&estimate), None, Some(e.classification.class));
                self.tenant_governor
                    .refund_on_drop(tenant_ref, estimate.tpm);
                self.dispatcher_notify.notify_waiters();
                Err(e.classification.message)
            }
        }
    }

    /// Collect stream tokens (test/demo helper).
    pub async fn submit_stream_collect(
        &self,
        payload: TReq,
        options: SubmitOptions,
    ) -> Result<Vec<String>, String> {
        let mut stream = self.submit_stream(payload, options).await?;
        let mut tokens = Vec::new();
        while let Some(chunk) = stream.next().await {
            tokens.push(chunk?);
        }
        Ok(tokens)
    }

    /// Submit long-running op; returns opaque handle id (not upstream operation id).
    pub async fn submit_operation(
        &self,
        payload: TReq,
        options: SubmitOptions,
    ) -> Result<String, String> {
        if self.draining.load(Ordering::Relaxed) == 1 {
            return Err("Pool is draining".to_string());
        }

        let payload = maybe_redact_payload(self.enable_pii_redaction, payload);
        let tenant_ref = options.tenant_id.as_deref();
        let estimate = options
            .estimated_budget
            .clone()
            .unwrap_or_else(|| self.adapter.estimate_budget(&payload));
        self.tenant_governor
            .check_eligibility(tenant_ref, estimate.tpm)
            .map_err(|e| format!("TENANT_DENIED: Tenant admission denied: {}", e))?;
        self.tenant_governor
            .deduct_on_admission(tenant_ref, estimate.tpm);

        let submit_budget = BudgetDeduction {
            rpm: Some(1.0),
            ..Default::default()
        };
        let ep = self
            .select_endpoint(&submit_budget, &[])
            .await
            .ok_or_else(|| "No capacity for operation".to_string())?;
        if !ep.acquire_slot(&submit_budget) {
            self.tenant_governor
                .refund_on_drop(tenant_ref, estimate.tpm);
            return Err("No capacity for operation".to_string());
        }

        let auth = self.credential_vault.get_auth_header(&ep.config.id);
        let auth_ref = auth.as_ref().map(|(k, v)| (k.as_str(), v.as_str()));

        match self
            .adapter
            .submit_operation(&ep.config, &payload, auth_ref)
            .await
        {
            Ok(sub) => {
                ep.release_slot(
                    true,
                    50,
                    Some(&submit_budget),
                    Some(&sub.initial_budget),
                    None,
                );
                self.tenant_governor.release_slot(tenant_ref);
                self.dispatcher_notify.notify_waiters();

                let handle_id = Uuid::new_v4().to_string();
                self.operations.lock().await.insert(
                    handle_id.clone(),
                    OpHandle {
                        endpoint_id: ep.config.id.clone(),
                        operation_id: sub.operation_id,
                    },
                );
                Ok(handle_id)
            }
            Err(e) => {
                ep.release_slot(
                    false,
                    0,
                    Some(&submit_budget),
                    None,
                    Some(e.classification.class),
                );
                self.tenant_governor
                    .refund_on_drop(tenant_ref, estimate.tpm);
                self.dispatcher_notify.notify_waiters();
                Err(e.classification.message)
            }
        }
    }

    /// Poll by opaque handle. `done: false` is a successful Ok snapshot (not an Err).
    pub async fn poll_operation(
        &self,
        handle_id: &str,
    ) -> Result<OperationPollSnapshot<TRes>, String> {
        let entry = {
            let map = self.operations.lock().await;
            map.get(handle_id)
                .map(|e| (e.endpoint_id.clone(), e.operation_id.clone()))
        };
        let (endpoint_id, operation_id) =
            entry.ok_or_else(|| format!("Operation '{}' not found", handle_id))?;

        let ep = {
            let eps = self.endpoints.lock().await;
            eps.iter()
                .find(|e| e.config.id == endpoint_id)
                .cloned()
                .ok_or_else(|| format!("Endpoint '{}' not found for operation", endpoint_id))?
        };

        let poll_budget = BudgetDeduction {
            rpm: Some(1.0),
            ..Default::default()
        };
        if !ep.acquire_slot(&poll_budget) {
            return Err("No capacity for poll".to_string());
        }

        let released = AtomicBool::new(false);
        let release_once = |success: bool, actual: Option<&BudgetDeduction>| {
            if released
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                return;
            }
            if success {
                ep.release_slot(true, 10, Some(&poll_budget), actual.or(Some(&poll_budget)), None);
            } else {
                ep.release_slot(
                    false,
                    10,
                    Some(&poll_budget),
                    None,
                    Some(FailureClass::Transient),
                );
            }
            self.dispatcher_notify.notify_waiters();
        };

        let auth = self.credential_vault.get_auth_header(&ep.config.id);
        let auth_ref = auth.as_ref().map(|(k, v)| (k.as_str(), v.as_str()));
        match self
            .adapter
            .poll_operation(&ep.config, &operation_id, auth_ref)
            .await
        {
            Ok(res) => {
                release_once(true, res.budget_deduction.as_ref());
                if res.done {
                    self.operations.lock().await.remove(handle_id);
                }
                Ok(OperationPollSnapshot {
                    done: res.done,
                    status: res.status,
                    data: res.data,
                })
            }
            Err(e) => {
                release_once(false, None);
                Err(e.classification.message)
            }
        }
    }

    pub async fn cancel_operation(&self, handle_id: &str) -> Result<(bool,), String> {
        let entry = {
            let mut map = self.operations.lock().await;
            map.remove(handle_id)
        };
        let Some(entry) = entry else {
            return Ok((false,));
        };

        let ep = {
            let eps = self.endpoints.lock().await;
            eps.iter()
                .find(|e| e.config.id == entry.endpoint_id)
                .cloned()
        };

        let mut upstream = false;
        if let Some(ep) = ep {
            let auth = self.credential_vault.get_auth_header(&ep.config.id);
            let auth_ref = auth.as_ref().map(|(k, v)| (k.as_str(), v.as_str()));
            match self
                .adapter
                .cancel_operation(&ep.config, &entry.operation_id, auth_ref)
                .await
            {
                Ok(()) => upstream = true,
                Err(_) => upstream = false,
            }
        }
        Ok((upstream,))
    }

    async fn schedule_cool_off_wake(self: &Arc<Self>) {
        let eps = self.endpoints.lock().await;
        let mut min_remaining = u64::MAX;
        for ep in eps.iter() {
            let rem = ep.cool_off_remaining_ms();
            if rem > 0 && rem < min_remaining {
                min_remaining = rem;
            }
        }
        drop(eps);

        let delay = if min_remaining == u64::MAX || min_remaining == 0 {
            50
        } else {
            min_remaining.min(1000)
        };
        let pool = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            pool.dispatcher_notify.notify_waiters();
        });
    }

    async fn dispatch_ready_jobs(self: &Arc<Self>) {
        loop {
            let mut q = self.queue.lock().await;
            if q.is_empty() {
                break;
            }

            let item = match q.dequeue() {
                Some(i) => i,
                None => break,
            };
            // Net depth decreased — wake Timeout admission waiters.
            self.admission_controller.notify_slot_available();
            drop(q);

            if item.created_at.elapsed().as_millis() as u64 >= item.queue_timeout_ms {
                if item.tenant_charged {
                    self.tenant_governor.refund_on_drop(
                        item.tenant_id.as_deref(),
                        item.estimated_budget.tpm,
                    );
                }
                let (_payload, tx) = item.payload;
                let _ = tx.send(Err(format!(
                    "Job timed out waiting {}ms in queue",
                    item.queue_timeout_ms
                )));
                continue;
            }

            let estimate = item.estimated_budget.clone();
            let ep = match self.select_endpoint(&estimate, &item.avoid_endpoint_ids).await {
                Some(e) => e,
                None => {
                    self.queue.lock().await.enqueue(item, true);
                    self.schedule_cool_off_wake().await;
                    break;
                }
            };

            if !ep.acquire_slot(&estimate) {
                self.queue.lock().await.enqueue(item, true);
                self.schedule_cool_off_wake().await;
                break;
            }

            self.active_jobs.fetch_add(1, Ordering::SeqCst);

            let pool_task = Arc::clone(self);
            let ep_task = Arc::clone(&ep);

            tokio::spawn(async move {
                pool_task.execute_job(item, ep_task).await;
            });
        }
    }

    async fn execute_job(
        self: Arc<Self>,
        mut item: QueuedItem<(TReq, oneshot::Sender<Result<JobResult<TRes>, String>>)>,
        ep: Arc<EndpointInstance>,
    ) {
        let (payload, tx) = item.payload;
        let queue_wait_ms = item.created_at.elapsed().as_millis() as u64;
        let start = Instant::now();
        item.attempts += 1;

        let auth = self.credential_vault.get_auth_header(&ep.config.id);
        let auth_ref = auth.as_ref().map(|(k, v)| (k.as_str(), v.as_str()));

        let exec_fut = self.adapter.execute(&ep.config, &payload, auth_ref);
        let timed = tokio::time::timeout(
            Duration::from_millis(item.execution_timeout_ms),
            exec_fut,
        )
        .await;

        let duration_ms = start.elapsed().as_millis() as u64;

        let exec_result: Result<(TRes, BudgetDeduction), crate::types::FailureClassification> =
            match timed {
                Ok(Ok((data, budget))) => Ok((data, budget)),
                Ok(Err(e)) => Err(e.classification),
                Err(_) => Err(crate::types::FailureClassification {
                    class: FailureClass::Transient,
                    retry_after_ms: None,
                    message: "execution timeout".to_string(),
                }),
            };

        match exec_result {
            Ok((data, actual_budget)) => {
                ep.release_slot(
                    true,
                    duration_ms,
                    Some(&item.estimated_budget),
                    Some(&actual_budget),
                    None,
                );
                self.active_jobs.fetch_sub(1, Ordering::SeqCst);
                self.tenant_governor.release_slot(item.tenant_id.as_deref());
                self.total_completed.fetch_add(1, Ordering::Relaxed);
                {
                    let mut outcomes = self.recent_outcomes.lock().await;
                    outcomes.push(false);
                    if outcomes.len() > 100 {
                        outcomes.remove(0);
                    }
                }
                {
                    let mut lats = self.recent_latencies.lock().await;
                    lats.push(duration_ms);
                    if lats.len() > 100 {
                        lats.remove(0);
                    }
                }
                self.recent_completions.lock().await.push(Instant::now());
                self.dispatcher_notify.notify_waiters();

                let _ = tx.send(Ok(JobResult {
                    success: true,
                    data: Some(data),
                    error: None,
                    endpoint_id: Some(ep.config.id.clone()),
                    attempts: item.attempts,
                    duration_ms,
                    queue_wait_ms,
                    budget_deducted: Some(actual_budget),
                }));
            }
            Err(classification) => {
                if classification.class == FailureClass::Overloaded {
                    let ra = classification.retry_after_ms.unwrap_or(1000);
                    ep.budget_limiter.apply_overload_signal(ra);
                }

                ep.release_slot(
                    false,
                    duration_ms,
                    Some(&item.estimated_budget),
                    None,
                    Some(classification.class),
                );
                self.active_jobs.fetch_sub(1, Ordering::SeqCst);
                self.dispatcher_notify.notify_waiters();

                if classification.class == FailureClass::Unavailable {
                    item.avoid_endpoint_ids.push(ep.config.id.clone());
                }

                let can_retry = matches!(
                    classification.class,
                    FailureClass::Transient | FailureClass::Overloaded | FailureClass::Unavailable
                ) && item.retries_remaining > 0
                    && self.has_retry_budget().await;

                if can_retry {
                    item.retries_remaining -= 1;
                    self.total_retries.fetch_add(1, Ordering::Relaxed);
                    {
                        let mut outcomes = self.recent_outcomes.lock().await;
                        outcomes.push(true);
                        if outcomes.len() > 100 {
                            outcomes.remove(0);
                        }
                    }

                    let (delay_ms, next) = self.backoff_calculator.calculate_delay(
                        item.attempts,
                        classification.retry_after_ms,
                        item.previous_decorrelated_delay,
                    );
                    item.previous_decorrelated_delay = Some(next);

                    let deadline = item.created_at
                        + Duration::from_millis(item.queue_timeout_ms + item.execution_timeout_ms);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let sleep_ms = delay_ms.min(remaining.as_millis() as u64);

                    item.payload = (payload, tx);

                    let pool_retry = Arc::clone(&self);
                    let fail_msg = classification.message.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

                        let (decision, dropped, rejected) = pool_retry
                            .admission_controller
                            .admit_and_enqueue(&pool_retry.queue, item)
                            .await;
                        if let Some(d) = dropped {
                            if d.tenant_charged {
                                pool_retry
                                    .tenant_governor
                                    .refund_on_drop(d.tenant_id.as_deref(), d.estimated_budget.tpm);
                            }
                            let (_p, dtx) = d.payload;
                            let _ = dtx.send(Err(
                                "Dropped from queue to make room for higher-priority work"
                                    .to_string(),
                            ));
                        }
                        if decision.accepted {
                            pool_retry.dispatcher_notify.notify_waiters();
                        } else if let Some(rej) = rejected {
                            pool_retry
                                .tenant_governor
                                .release_slot(rej.tenant_id.as_deref());
                            pool_retry.total_failed.fetch_add(1, Ordering::Relaxed);
                            let sanitized = pool_retry.credential_vault.redact_secrets(&fail_msg);
                            let (_p, fail_tx) = rej.payload;
                            let _ = fail_tx.send(Err(sanitized));
                        }
                    });
                } else {
                    self.tenant_governor
                        .release_slot(item.tenant_id.as_deref());
                    self.total_failed.fetch_add(1, Ordering::Relaxed);
                    let sanitized = self
                        .credential_vault
                        .redact_secrets(&classification.message);
                    let _ = tx.send(Err(sanitized));
                }
            }
        }
    }

    async fn select_endpoint(
        &self,
        estimate: &BudgetDeduction,
        avoid: &[String],
    ) -> Option<Arc<EndpointInstance>> {
        let endpoints = self.endpoints.lock().await;
        let available: Vec<Arc<EndpointInstance>> = endpoints
            .iter()
            .filter(|e| !avoid.contains(&e.config.id) && e.is_available(estimate))
            .cloned()
            .collect();
        drop(endpoints);

        if available.is_empty() {
            return None;
        }

        match self.routing_strategy {
            RoutingStrategy::LeastLoaded => available.into_iter().min_by_key(|e| {
                let weight = e.config.weight.unwrap_or(1.0).max(0.1);
                ((e.get_active_leases() as f64 / e.config.max_concurrency as f64) / weight * 1000.0)
                    as usize
            }),
            RoutingStrategy::PriorityFailover => {
                let mut sorted = available;
                sorted.sort_by_key(|e| e.config.priority_tier.unwrap_or(0));
                let best_tier = sorted[0].config.priority_tier.unwrap_or(0);
                sorted
                    .into_iter()
                    .filter(|e| e.config.priority_tier.unwrap_or(0) == best_tier)
                    .min_by_key(|e| {
                        let weight = e.config.weight.unwrap_or(1.0).max(0.1);
                        ((e.get_active_leases() as f64 / e.config.max_concurrency as f64) / weight
                            * 1000.0) as usize
                    })
            }
            RoutingStrategy::WeightedLatency => {
                if available.len() == 1 {
                    return Some(available[0].clone());
                }
                let scores: Vec<f64> = available
                    .iter()
                    .map(|ep| {
                        let lat = ep.get_latency_ema().max(1) as f64;
                        let weight = ep.config.weight.unwrap_or(1.0);
                        (1000.0 / lat) * weight
                    })
                    .collect();
                let total: f64 = scores.iter().sum();
                let mut rng = rand::thread_rng();
                use rand::Rng;
                let target = rng.gen_range(0.0..total.max(1.0));
                let mut cumulative = 0.0;
                for (i, ep) in available.iter().enumerate() {
                    cumulative += scores[i];
                    if target <= cumulative {
                        return Some(ep.clone());
                    }
                }
                Some(available[0].clone())
            }
            RoutingStrategy::CostOptimized => available.into_iter().min_by(|a, b| {
                let cost_a = a.config.cost_per_request.unwrap_or(1.0);
                let cost_b = b.config.cost_per_request.unwrap_or(1.0);
                match (cost_a.is_nan(), cost_b.is_nan()) {
                    (true, true) => std::cmp::Ordering::Equal,
                    (true, false) => std::cmp::Ordering::Greater,
                    (false, true) => std::cmp::Ordering::Less,
                    _ => cost_a
                        .partial_cmp(&cost_b)
                        .unwrap_or(std::cmp::Ordering::Equal),
                }
            }),
            RoutingStrategy::RoundRobin => {
                if available.len() == 1 {
                    return Some(available[0].clone());
                }
                let mut weights = self.rr_weights.lock().await;
                let mut total_weight: i64 = 0;
                let mut best_ep: Option<Arc<EndpointInstance>> = None;
                let mut max_weight = i64::MIN;

                for ep in &available {
                    let w = ep.config.weight.unwrap_or(1.0).max(1.0).round() as i64;
                    total_weight += w;
                    let current = weights.entry(ep.config.id.clone()).or_insert(0);
                    *current += w;
                    if *current > max_weight {
                        max_weight = *current;
                        best_ep = Some(ep.clone());
                    }
                }

                if let Some(ref ep) = best_ep {
                    if let Some(w) = weights.get_mut(&ep.config.id) {
                        *w -= total_weight;
                    }
                }

                best_ep.or_else(|| Some(available[0].clone()))
            }
        }
    }

    pub async fn run_health_checks(&self) {
        let endpoints = self.endpoints.lock().await.clone();
        let mut any_healthy = false;
        for ep in endpoints {
            let auth = self.credential_vault.get_auth_header(&ep.config.id);
            let auth_ref = auth.as_ref().map(|(k, v)| (k.as_str(), v.as_str()));
            let is_up = self.adapter.health_check(&ep.config, auth_ref).await;
            if !is_up {
                ep.set_health(crate::types::EndpointHealth::Unhealthy);
            } else {
                ep.set_health(crate::types::EndpointHealth::Healthy);
                any_healthy = true;
            }
        }
        if any_healthy {
            self.dispatcher_notify.notify_waiters();
        }
    }

    pub async fn get_metrics(&self) -> PoolMetrics {
        let active = self.active_jobs.load(Ordering::Relaxed);
        let queued = self.queue.lock().await.len();
        let capacity = self.admission_controller.get_config().max_queue_size;
        let ep_metrics: Vec<_> = self
            .endpoints
            .lock()
            .await
            .iter()
            .map(|e| e.get_metrics())
            .collect();

        let lats = self.recent_latencies.lock().await;
        let avg_lat = if lats.is_empty() {
            0
        } else {
            lats.iter().sum::<u64>() / lats.len() as u64
        };
        let p95_lat = if lats.is_empty() {
            0
        } else {
            let mut sorted = lats.clone();
            sorted.sort_unstable();
            let idx = ((sorted.len() as f64 * 0.95).floor() as usize).min(sorted.len() - 1);
            sorted[idx]
        };
        let mut completions = self.recent_completions.lock().await;
        completions.retain(|t| t.elapsed() < Duration::from_secs(10));
        let throughput = completions.len() as f64 / 10.0;

        PoolMetrics {
            pool_name: self.name.clone(),
            service_type: self.service_type.clone(),
            active_jobs: active,
            queued_jobs: queued,
            queue_capacity: capacity,
            queue_saturation_pct: ((queued as f64 / capacity.max(1) as f64) * 100.0).min(100.0)
                as u8,
            total_completed: self.total_completed.load(Ordering::Relaxed),
            total_failed: self.total_failed.load(Ordering::Relaxed),
            total_rejected: self.total_rejected.load(Ordering::Relaxed),
            total_retries: self.total_retries.load(Ordering::Relaxed),
            average_latency_ms: avg_lat,
            p95_latency_ms: p95_lat,
            throughput_rps: (throughput * 10.0).round() / 10.0,
            endpoints: ep_metrics,
        }
    }

    pub async fn drain(&self, timeout_ms: u64) {
        self.draining.store(1, Ordering::SeqCst);
        let start = Instant::now();
        while self.active_jobs.load(Ordering::Relaxed) > 0
            || !self.queue.lock().await.is_empty()
        {
            if start.elapsed().as_millis() as u64 >= timeout_ms {
                let remaining = self.queue.lock().await.drain_all();
                for item in remaining {
                    if item.tenant_charged {
                        self.tenant_governor.refund_on_drop(
                            item.tenant_id.as_deref(),
                            item.estimated_budget.tpm,
                        );
                    }
                    let (_p, tx) = item.payload;
                    let _ = tx.send(Err(
                        "Pool drained before queued job could finish".to_string(),
                    ));
                }
                self.admission_controller.notify_slot_available();
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn remove_endpoint(&self, endpoint_id: &str, wait_ms: u64) -> bool {
        let ep = {
            let eps = self.endpoints.lock().await;
            eps.iter().find(|e| e.config.id == endpoint_id).cloned()
        };
        let Some(ep) = ep else {
            return false;
        };

        ep.set_health(crate::types::EndpointHealth::Unhealthy);
        let start = Instant::now();
        while ep.get_active_leases() > 0 && (start.elapsed().as_millis() as u64) < wait_ms {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        {
            let mut eps = self.endpoints.lock().await;
            eps.retain(|e| e.config.id != endpoint_id);
        }
        self.credential_vault.remove(endpoint_id);
        true
    }
}
