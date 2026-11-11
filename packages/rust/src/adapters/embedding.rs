use crate::adapters::{AdapterError, ServiceAdapter};
use crate::resilience::ErrorClassifier;
use crate::tokenizer::estimate_tokens;
use crate::types::{BudgetDeduction, EndpointConfig, FailureClass};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbeddingRequest {
    pub model: Option<String>,
    pub input: EmbeddingInput,
    pub dimensions: Option<usize>,
    pub encoding_format: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EmbeddingInput {
    Text(String),
    Many(Vec<String>),
}

impl EmbeddingInput {
    pub fn total_chars(&self) -> usize {
        match self {
            EmbeddingInput::Text(s) => s.len(),
            EmbeddingInput::Many(v) => v.iter().map(|s| s.len()).sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            EmbeddingInput::Text(s) => s.is_empty(),
            EmbeddingInput::Many(v) => v.is_empty(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbeddingItem {
    pub index: usize,
    pub embedding: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbeddingUsage {
    pub prompt_tokens: usize,
    pub total_tokens: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbeddingResponse {
    pub model: String,
    pub data: Vec<EmbeddingItem>,
    pub usage: Option<EmbeddingUsage>,
    pub raw: Option<Value>,
}

pub struct EmbeddingAdapter {
    client: reqwest::Client,
}

impl EmbeddingAdapter {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .unwrap_or_default(),
        }
    }
}

impl Default for EmbeddingAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ServiceAdapter<EmbeddingRequest, EmbeddingResponse> for EmbeddingAdapter {
    fn estimate_budget(&self, payload: &EmbeddingRequest) -> BudgetDeduction {
        let text = match &payload.input {
            EmbeddingInput::Text(s) => s.as_str(),
            EmbeddingInput::Many(v) => {
                // Join for estimation without allocating huge strings when possible
                return BudgetDeduction {
                    rpm: Some(1.0),
                    tpm: Some(
                        v.iter()
                            .map(|s| estimate_tokens(s) as f64)
                            .sum::<f64>()
                            .max(1.0),
                    ),
                    ..Default::default()
                };
            }
        };
        BudgetDeduction {
            rpm: Some(1.0),
            tpm: Some(estimate_tokens(text).max(1) as f64),
            ..Default::default()
        }
    }

    async fn execute(
        &self,
        endpoint: &EndpointConfig,
        payload: &EmbeddingRequest,
        auth_header: Option<(&str, &str)>,
    ) -> Result<(EmbeddingResponse, BudgetDeduction), AdapterError> {
        if payload.input.is_empty() {
            return Err(AdapterError::msg(
                "Embedding input is required (string or array of strings)",
                FailureClass::Fatal,
            ));
        }

        let default_model = payload
            .model
            .clone()
            .or_else(|| endpoint.metadata.get("model").cloned())
            .unwrap_or_else(|| "text-embedding-3-small".into());

        let input_json = match &payload.input {
            EmbeddingInput::Text(s) => {
                let resolved = crate::spill::resolve_spilled_string(s).unwrap_or_else(|| s.clone());
                serde_json::json!(resolved)
            }
            EmbeddingInput::Many(v) => {
                let resolved: Vec<String> = v
                    .iter()
                    .map(|s| crate::spill::resolve_spilled_string(s).unwrap_or_else(|| s.clone()))
                    .collect();
                serde_json::json!(resolved)
            }
        };

        let body = serde_json::json!({
            "model": default_model,
            "input": input_json,
            "dimensions": payload.dimensions,
            "encoding_format": payload.encoding_format.as_deref().unwrap_or("float"),
        });

        let mut req = self
            .client
            .post(&endpoint.url)
            .header("Content-Type", "application/json")
            .header("User-Agent", "Factioner-Embedding/1.0")
            .json(&body);
        if let Some((k, v)) = auth_header {
            req = req.header(k, v);
        }

        let res = req.send().await.map_err(|e| AdapterError {
            classification: ErrorClassifier::classify(&e.to_string()),
        })?;

        if !res.status().is_success() {
            let status = res.status().as_u16();
            let retry_after = ErrorClassifier::retry_after_from_headers(res.headers());
            let err_text = res.text().await.unwrap_or_default();
            let mut class =
                ErrorClassifier::classify_with_status(&err_text, Some(status), retry_after);
            if class.retry_after_ms.is_none() {
                class.retry_after_ms = retry_after;
            }
            return Err(AdapterError {
                classification: class,
            });
        }

        let data: Value = res.json().await.map_err(|e| {
            AdapterError::msg(format!("Invalid embedding JSON: {}", e), FailureClass::Transient)
        })?;

        let fallback_tokens = match &payload.input {
            EmbeddingInput::Text(s) => estimate_tokens(s).max(1),
            EmbeddingInput::Many(v) => v.iter().map(|s| estimate_tokens(s)).sum::<usize>().max(1),
        };
        let total_tokens = data
            .pointer("/usage/total_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(fallback_tokens as u64) as usize;
        let prompt_tokens = data
            .pointer("/usage/prompt_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;

        let items = data
            .get("data")
            .and_then(|d| d.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|d| EmbeddingItem {
                        index: d.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
                        embedding: d
                            .get("embedding")
                            .and_then(|e| e.as_array())
                            .map(|a| {
                                a.iter()
                                    .filter_map(|n| n.as_f64())
                                    .collect()
                            })
                            .unwrap_or_default(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok((
            EmbeddingResponse {
                model: data
                    .get("model")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&default_model)
                    .to_string(),
                data: items,
                usage: Some(EmbeddingUsage {
                    prompt_tokens,
                    total_tokens,
                }),
                raw: Some(data),
            },
            BudgetDeduction {
                rpm: Some(1.0),
                tpm: Some(total_tokens as f64),
                ..Default::default()
            },
        ))
    }

    async fn health_check(
        &self,
        endpoint: &EndpointConfig,
        auth_header: Option<(&str, &str)>,
    ) -> bool {
        let mut req = self.client.head(&endpoint.url);
        if let Some((k, v)) = auth_header {
            req = req.header(k, v);
        }
        match req.send().await {
            Ok(res) => res.status().as_u16() < 500,
            Err(_) => false,
        }
    }
}
