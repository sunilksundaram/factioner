use crate::adapters::{AdapterError, ServiceAdapter};
use crate::resilience::ErrorClassifier;
use crate::types::{BudgetDeduction, EndpointConfig, FailureClass};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericHttpRequest {
    pub path: Option<String>,
    pub method: Option<String>,
    pub headers: Option<HashMap<String, String>>,
    pub body: Option<serde_json::Value>,
    pub query_params: Option<HashMap<String, String>>,
}

pub struct GenericHttpAdapter {
    client: reqwest::Client,
}

impl GenericHttpAdapter {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
        }
    }

    pub fn validate_path(path: &str) -> Result<(), AdapterError> {
        if path.contains("://")
            || path.starts_with("//")
            || path.contains("..")
            || !path
                .chars()
                .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'))
        {
            return Err(AdapterError::msg(
                format!("SSRF validation failed: Invalid path '{}'", path),
                FailureClass::Fatal,
            ));
        }
        Ok(())
    }
}

impl Default for GenericHttpAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ServiceAdapter<GenericHttpRequest, serde_json::Value> for GenericHttpAdapter {
    fn estimate_budget(&self, _payload: &GenericHttpRequest) -> BudgetDeduction {
        BudgetDeduction {
            rps: Some(1.0),
            rpm: Some(1.0),
            ..Default::default()
        }
    }

    async fn execute(
        &self,
        endpoint: &EndpointConfig,
        payload: &GenericHttpRequest,
        auth_header: Option<(&str, &str)>,
    ) -> Result<(serde_json::Value, BudgetDeduction), AdapterError> {
        if let Some(ref p) = payload.path {
            Self::validate_path(p)?;
        }

        let base = endpoint.url.trim_end_matches('/');
        let path = payload
            .path
            .as_deref()
            .unwrap_or("")
            .trim_start_matches('/');
        let url = if path.is_empty() {
            base.to_string()
        } else {
            format!("{}/{}", base, path)
        };

        let method = payload.method.as_deref().unwrap_or("POST");
        let req_method = match method.to_uppercase().as_str() {
            "GET" => reqwest::Method::GET,
            "PUT" => reqwest::Method::PUT,
            "DELETE" => reqwest::Method::DELETE,
            "PATCH" => reqwest::Method::PATCH,
            "HEAD" => reqwest::Method::HEAD,
            _ => reqwest::Method::POST,
        };

        let mut req = self.client.request(req_method, &url);

        if let Some(ref qp) = payload.query_params {
            req = req.query(qp);
        }

        req = req.header("Content-Type", "application/json");
        req = req.header("User-Agent", "Factioner-HttpGateway/1.0");

        if let Some(ref hdrs) = payload.headers {
            for (k, v) in hdrs {
                req = req.header(k.as_str(), v.as_str());
            }
        }

        if let Some((k, v)) = auth_header {
            req = req.header(k, v);
        }

        if let Some(ref body) = payload.body {
            req = req.json(body);
        }

        let resp = req.send().await.map_err(|e| {
            let msg = format!("Transport error: {}", e);
            let class = ErrorClassifier::classify(&msg).class;
            AdapterError::msg(msg, class)
        })?;

        let status = resp.status();
        if !status.is_success() {
            let status_code = status.as_u16();
            let retry_after_ms = ErrorClassifier::retry_after_from_headers(resp.headers());
            let body_text = resp.text().await.unwrap_or_default();
            let classification = ErrorClassifier::classify_with_status(
                &format!("HTTP error {}: {}", status_code, body_text),
                Some(status_code),
                retry_after_ms,
            );
            return Err(AdapterError {
                classification,
            });
        }

        let data = resp
            .json::<serde_json::Value>()
            .await
            .unwrap_or(serde_json::json!({ "status": "ok" }));
        Ok((
            data,
            BudgetDeduction {
                rps: Some(1.0),
                ..Default::default()
            },
        ))
    }
}
