use super::{AdapterError, OperationPollResult, OperationSubmission, ServiceAdapter};
use crate::resilience::ErrorClassifier;
use crate::types::{BudgetDeduction, EndpointConfig, FailureClassification};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use rand::Rng;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub struct MockAdapter {
    pub base_latency_ms: u64,
    pub jitter_ms: u64,
    pub failure_rate_pct: f64,
    pub total_calls: AtomicUsize,
}

impl MockAdapter {
    pub fn new(base_latency_ms: u64, failure_rate_pct: f64) -> Self {
        Self {
            base_latency_ms,
            jitter_ms: 10,
            failure_rate_pct,
            total_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl<TReq: Send + Sync + 'static> ServiceAdapter<TReq, String> for MockAdapter {
    fn estimate_budget(&self, _payload: &TReq) -> BudgetDeduction {
        BudgetDeduction {
            rpm: Some(1.0),
            tpm: Some(120.0),
            ..Default::default()
        }
    }

    async fn execute(
        &self,
        endpoint: &EndpointConfig,
        _payload: &TReq,
        _auth_header: Option<(&str, &str)>,
    ) -> Result<(String, BudgetDeduction), AdapterError> {
        self.total_calls.fetch_add(1, Ordering::Relaxed);

        let jitter = if self.jitter_ms > 0 {
            rand::thread_rng().gen_range(0..=self.jitter_ms)
        } else {
            0
        };
        tokio::time::sleep(Duration::from_millis(self.base_latency_ms + jitter)).await;

        if self.failure_rate_pct > 0.0 {
            let roll = rand::thread_rng().gen_range(0.0..100.0);
            if roll < self.failure_rate_pct {
                return Err(AdapterError {
                    classification: ErrorClassifier::classify_with_status(
                        &format!(
                            "Endpoint '{}' failed with HTTP 429 Too Many Requests. Retry-After: 1",
                            endpoint.id
                        ),
                        Some(429),
                        Some(1000),
                    ),
                });
            }
        }

        Ok((
            format!("Success from endpoint {}", endpoint.id),
            BudgetDeduction {
                rpm: Some(1.0),
                tpm: Some(120.0),
                pages: None,
                rps: None,
            },
        ))
    }

    async fn execute_stream(
        &self,
        _endpoint: &EndpointConfig,
        _payload: &TReq,
        _auth_header: Option<(&str, &str)>,
    ) -> Result<(BoxStream<'static, Result<String, AdapterError>>, BudgetDeduction), AdapterError>
    {
        self.total_calls.fetch_add(1, Ordering::Relaxed);
        let tokens = vec![
            "Hello".to_string(),
            " from".to_string(),
            " Factioner".to_string(),
            " streaming".to_string(),
            " pool!".to_string(),
        ];
        let s = stream::iter(tokens.into_iter().map(Ok));
        Ok((
            Box::pin(s),
            BudgetDeduction {
                rpm: Some(1.0),
                tpm: Some(50.0),
                ..Default::default()
            },
        ))
    }

    async fn submit_operation(
        &self,
        _endpoint: &EndpointConfig,
        _payload: &TReq,
        _auth_header: Option<(&str, &str)>,
    ) -> Result<OperationSubmission, AdapterError> {
        self.total_calls.fetch_add(1, Ordering::Relaxed);
        Ok(OperationSubmission {
            operation_id: "mock-op-12345".to_string(),
            initial_budget: BudgetDeduction {
                rpm: Some(1.0),
                pages: Some(1.0),
                ..Default::default()
            },
        })
    }

    async fn poll_operation(
        &self,
        _endpoint: &EndpointConfig,
        _operation_id: &str,
        _auth_header: Option<(&str, &str)>,
    ) -> Result<OperationPollResult<String>, AdapterError> {
        Ok(OperationPollResult {
            done: true,
            data: Some("operation_complete".to_string()),
            status: "succeeded".to_string(),
            budget_deduction: Some(BudgetDeduction {
                pages: Some(2.0),
                rpm: Some(1.0),
                ..Default::default()
            }),
        })
    }

    fn classify_error(&self, error_msg: &str) -> FailureClassification {
        ErrorClassifier::classify(error_msg)
    }

    async fn health_check(
        &self,
        _endpoint: &EndpointConfig,
        _auth_header: Option<(&str, &str)>,
    ) -> bool {
        true
    }
}
