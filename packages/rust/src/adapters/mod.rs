use crate::types::{BudgetDeduction, EndpointConfig, FailureClassification};
use async_trait::async_trait;
use futures::stream::BoxStream;

pub mod mock;
pub mod generic_http;
pub mod llm;

pub use mock::MockAdapter;
pub use generic_http::{GenericHttpAdapter, GenericHttpRequest};
pub use llm::{LLMAdapter, LLMMessage, LLMRequest, LLMResponse};

#[derive(Debug)]
pub struct AdapterError {
    pub classification: FailureClassification,
}

impl AdapterError {
    pub fn msg(message: impl Into<String>, class: crate::types::FailureClass) -> Self {
        Self {
            classification: FailureClassification {
                class,
                retry_after_ms: None,
                message: message.into(),
            },
        }
    }
}

pub struct OperationSubmission {
    pub operation_id: String,
    pub initial_budget: BudgetDeduction,
}

pub struct OperationPollResult<TRes> {
    pub done: bool,
    pub data: Option<TRes>,
    pub status: String,
    pub budget_deduction: Option<BudgetDeduction>,
}

#[async_trait]
pub trait ServiceAdapter<TReq: Send + Sync + 'static, TRes: Send + Sync + 'static>:
    Send + Sync
{
    fn estimate_budget(&self, _payload: &TReq) -> BudgetDeduction {
        BudgetDeduction {
            rpm: Some(1.0),
            ..Default::default()
        }
    }

    async fn execute(
        &self,
        endpoint: &EndpointConfig,
        payload: &TReq,
        auth_header: Option<(&str, &str)>,
    ) -> Result<(TRes, BudgetDeduction), AdapterError>;

    async fn execute_stream(
        &self,
        _endpoint: &EndpointConfig,
        _payload: &TReq,
        _auth_header: Option<(&str, &str)>,
    ) -> Result<(BoxStream<'static, Result<String, AdapterError>>, BudgetDeduction), AdapterError>
    {
        Err(AdapterError::msg(
            "streaming not supported",
            crate::types::FailureClass::Fatal,
        ))
    }

    async fn submit_operation(
        &self,
        _endpoint: &EndpointConfig,
        _payload: &TReq,
        _auth_header: Option<(&str, &str)>,
    ) -> Result<OperationSubmission, AdapterError> {
        Err(AdapterError::msg(
            "operations not supported",
            crate::types::FailureClass::Fatal,
        ))
    }

    async fn poll_operation(
        &self,
        _endpoint: &EndpointConfig,
        _operation_id: &str,
        _auth_header: Option<(&str, &str)>,
    ) -> Result<OperationPollResult<TRes>, AdapterError> {
        Err(AdapterError::msg(
            "operations not supported",
            crate::types::FailureClass::Fatal,
        ))
    }

    async fn cancel_operation(
        &self,
        _endpoint: &EndpointConfig,
        _operation_id: &str,
        _auth_header: Option<(&str, &str)>,
    ) -> Result<(), AdapterError> {
        Err(AdapterError::msg(
            "cancel_operation not supported",
            crate::types::FailureClass::Fatal,
        ))
    }

    fn classify_error(&self, error_msg: &str) -> FailureClassification {
        crate::resilience::ErrorClassifier::classify(error_msg)
    }

    async fn health_check(
        &self,
        _endpoint: &EndpointConfig,
        _auth_header: Option<(&str, &str)>,
    ) -> bool {
        true
    }
}
