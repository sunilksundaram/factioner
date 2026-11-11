use crate::adapters::{AdapterError, OperationPollResult, OperationSubmission, ServiceAdapter};
use crate::resilience::ErrorClassifier;
use crate::types::{BudgetDeduction, EndpointConfig, FailureClass};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DocumentAnalysisRequest {
    pub model_id: Option<String>,
    pub url_source: Option<String>,
    pub base64_source: Option<String>,
    pub pages: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentPage {
    pub page_number: u32,
    pub lines: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentTable {
    pub row_count: u32,
    pub column_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentAnalysisResponse {
    pub model_id: String,
    pub status: String,
    pub content: Option<String>,
    pub pages: Option<Vec<DocumentPage>>,
    pub tables: Option<Vec<DocumentTable>>,
    pub raw: Option<Value>,
}

pub struct DocumentIntelligenceAdapter {
    client: reqwest::Client,
}

impl DocumentIntelligenceAdapter {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(120))
                .build()
                .unwrap_or_default(),
        }
    }

    fn assert_poll_url_allowed(endpoint: &EndpointConfig, operation_id: &str) -> Result<(), AdapterError> {
        if operation_id == "direct" {
            return Ok(());
        }
        let poll_url = reqwest::Url::parse(operation_id).map_err(|_| {
            AdapterError::msg(
                format!("Invalid operation-location URL: {}", operation_id),
                FailureClass::Fatal,
            )
        })?;
        let endpoint_url = reqwest::Url::parse(&endpoint.url).map_err(|_| {
            AdapterError::msg("Invalid endpoint URL", FailureClass::Fatal)
        })?;
        let allowlist: Vec<String> = endpoint
            .metadata
            .get("pollOriginAllowlist")
            .map(|v| {
                serde_json::from_str::<Vec<String>>(v).unwrap_or_else(|_| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
            })
            .filter(|v: &Vec<String>| !v.is_empty())
            .unwrap_or_else(|| vec![endpoint_url.origin().ascii_serialization()]);

        let origin = poll_url.origin().ascii_serialization();
        if !allowlist.iter().any(|o| o == &origin) {
            return Err(AdapterError::msg(
                format!("SSRF blocked: operation-location origin '{}' is not allowed", origin),
                FailureClass::Fatal,
            ));
        }
        Ok(())
    }

    fn analyze_url(endpoint: &EndpointConfig, model_id: &str) -> String {
        if endpoint.url.contains("{modelId}") {
            endpoint.url.replace("{modelId}", &urlencoding_simple(model_id))
        } else {
            format!(
                "{}/documentModels/{}:analyze?api-version=2024-02-29-preview",
                endpoint.url.trim_end_matches('/'),
                urlencoding_simple(model_id)
            )
        }
    }
}

impl Default for DocumentIntelligenceAdapter {
    fn default() -> Self {
        Self::new()
    }
}

fn urlencoding_simple(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

#[async_trait]
impl ServiceAdapter<DocumentAnalysisRequest, DocumentAnalysisResponse> for DocumentIntelligenceAdapter {
    fn estimate_budget(&self, _payload: &DocumentAnalysisRequest) -> BudgetDeduction {
        BudgetDeduction {
            rpm: Some(1.0),
            pages: Some(1.0),
            ..Default::default()
        }
    }

    async fn execute(
        &self,
        endpoint: &EndpointConfig,
        payload: &DocumentAnalysisRequest,
        auth_header: Option<(&str, &str)>,
    ) -> Result<(DocumentAnalysisResponse, BudgetDeduction), AdapterError> {
        if payload.url_source.is_none() && payload.base64_source.is_none() {
            return Err(AdapterError::msg(
                "Document analysis requires either urlSource or base64Source",
                FailureClass::Fatal,
            ));
        }

        let sub = self.submit_operation(endpoint, payload, auth_header).await?;
        if sub.operation_id == "direct" {
            return Ok((
                DocumentAnalysisResponse {
                    model_id: payload.model_id.clone().unwrap_or_else(|| "prebuilt".into()),
                    status: "succeeded".into(),
                    content: None,
                    pages: None,
                    tables: None,
                    raw: None,
                },
                sub.initial_budget,
            ));
        }

        for _ in 0..120 {
            let poll = self
                .poll_operation(endpoint, &sub.operation_id, auth_header)
                .await?;
            if poll.done {
                if poll.status == "succeeded" {
                    if let Some(data) = poll.data {
                        return Ok((
                            data,
                            poll.budget_deduction.unwrap_or(BudgetDeduction {
                                rpm: Some(1.0),
                                pages: Some(1.0),
                                ..Default::default()
                            }),
                        ));
                    }
                }
                return Err(AdapterError::msg(
                    format!("Document analysis failed with status {}", poll.status),
                    FailureClass::Fatal,
                ));
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        Err(AdapterError::msg(
            "Document analysis timed out while polling",
            FailureClass::Transient,
        ))
    }

    async fn submit_operation(
        &self,
        endpoint: &EndpointConfig,
        payload: &DocumentAnalysisRequest,
        auth_header: Option<(&str, &str)>,
    ) -> Result<OperationSubmission, AdapterError> {
        let model_id = payload
            .model_id
            .clone()
            .or_else(|| endpoint.metadata.get("defaultModel").cloned())
            .unwrap_or_else(|| "prebuilt-layout".into());
        let target = Self::analyze_url(endpoint, &model_id);

        let resolved_b64 = payload.base64_source.as_ref().map(|s| {
            crate::spill::resolve_spilled_string(s).unwrap_or_else(|| s.clone())
        });
        let body = if let Some(ref url) = payload.url_source {
            serde_json::json!({ "urlSource": url })
        } else {
            serde_json::json!({ "base64Source": resolved_b64 })
        };

        let mut req = self
            .client
            .post(&target)
            .header("Content-Type", "application/json")
            .header("User-Agent", "Factioner-DocIntel/1.0")
            .json(&body);
        if let Some((k, v)) = auth_header {
            req = req.header(k, v);
        }

        let res = req.send().await.map_err(|e| {
            let class = ErrorClassifier::classify(&e.to_string());
            AdapterError { classification: class }
        })?;

        let status = res.status();
        if status.as_u16() == 202 {
            let op = res
                .headers()
                .get("operation-location")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    AdapterError::msg(
                        "Document Intelligence returned 202 without operation-location header",
                        FailureClass::Fatal,
                    )
                })?;
            Self::assert_poll_url_allowed(endpoint, &op)?;
            return Ok(OperationSubmission {
                operation_id: op,
                initial_budget: BudgetDeduction {
                    rpm: Some(1.0),
                    pages: Some(1.0),
                    ..Default::default()
                },
            });
        }

        if !status.is_success() {
            let retry_after = ErrorClassifier::retry_after_from_headers(res.headers());
            let err_text = res.text().await.unwrap_or_default();
            let mut class =
                ErrorClassifier::classify_with_status(&err_text, Some(status.as_u16()), retry_after);
            if class.retry_after_ms.is_none() {
                class.retry_after_ms = retry_after;
            }
            return Err(AdapterError {
                classification: class,
            });
        }

        Ok(OperationSubmission {
            operation_id: "direct".into(),
            initial_budget: BudgetDeduction {
                rpm: Some(1.0),
                pages: Some(1.0),
                ..Default::default()
            },
        })
    }

    async fn poll_operation(
        &self,
        endpoint: &EndpointConfig,
        operation_id: &str,
        auth_header: Option<(&str, &str)>,
    ) -> Result<OperationPollResult<DocumentAnalysisResponse>, AdapterError> {
        if operation_id == "direct" {
            return Ok(OperationPollResult {
                done: true,
                status: "succeeded".into(),
                data: Some(DocumentAnalysisResponse {
                    model_id: "prebuilt".into(),
                    status: "succeeded".into(),
                    content: None,
                    pages: None,
                    tables: None,
                    raw: None,
                }),
                budget_deduction: Some(BudgetDeduction {
                    rpm: Some(1.0),
                    pages: Some(1.0),
                    ..Default::default()
                }),
            });
        }

        Self::assert_poll_url_allowed(endpoint, operation_id)?;

        let mut req = self.client.get(operation_id);
        if let Some((k, v)) = auth_header {
            req = req.header(k, v);
        }

        let res = req.send().await.map_err(|e| AdapterError {
            classification: ErrorClassifier::classify(&e.to_string()),
        })?;

        if !res.status().is_success() {
            let status = res.status().as_u16();
            let err_text = res.text().await.unwrap_or_default();
            return Err(AdapterError {
                classification: ErrorClassifier::classify_with_status(&err_text, Some(status), None),
            });
        }

        let data: Value = res.json().await.map_err(|e| {
            AdapterError::msg(format!("Invalid poll JSON: {}", e), FailureClass::Transient)
        })?;
        let status = data
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("running");

        if status == "succeeded" {
            let analyze = data.get("analyzeResult").unwrap_or(&data);
            let page_count = analyze
                .get("pages")
                .and_then(|p| p.as_array())
                .map(|a| a.len())
                .unwrap_or(1) as f64;
            let pages = analyze.get("pages").and_then(|p| p.as_array()).map(|arr| {
                arr.iter()
                    .map(|p| DocumentPage {
                        page_number: p.get("pageNumber").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                        lines: p.get("lines").and_then(|l| l.as_array()).map(|lines| {
                            lines
                                .iter()
                                .filter_map(|line| {
                                    line.get("content")
                                        .and_then(|c| c.as_str())
                                        .map(|s| s.to_string())
                                })
                                .collect()
                        }),
                    })
                    .collect()
            });
            let tables = analyze.get("tables").and_then(|t| t.as_array()).map(|arr| {
                arr.iter()
                    .map(|t| DocumentTable {
                        row_count: t.get("rowCount").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                        column_count: t.get("columnCount").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                    })
                    .collect()
            });

            return Ok(OperationPollResult {
                done: true,
                status: "succeeded".into(),
                data: Some(DocumentAnalysisResponse {
                    model_id: data
                        .get("modelId")
                        .and_then(|v| v.as_str())
                        .unwrap_or("prebuilt")
                        .to_string(),
                    status: "succeeded".into(),
                    content: analyze
                        .get("content")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    pages,
                    tables,
                    raw: Some(data),
                }),
                budget_deduction: Some(BudgetDeduction {
                    rpm: Some(1.0),
                    pages: Some(page_count),
                    ..Default::default()
                }),
            });
        }

        if status == "failed" {
            return Ok(OperationPollResult {
                done: true,
                status: "failed".into(),
                data: Some(DocumentAnalysisResponse {
                    model_id: "prebuilt".into(),
                    status: "failed".into(),
                    content: None,
                    pages: None,
                    tables: None,
                    raw: data.get("error").cloned(),
                }),
                budget_deduction: None,
            });
        }

        Ok(OperationPollResult {
            done: false,
            status: "running".into(),
            data: None,
            budget_deduction: None,
        })
    }

    async fn health_check(
        &self,
        endpoint: &EndpointConfig,
        auth_header: Option<(&str, &str)>,
    ) -> bool {
        let url = format!(
            "{}/info?api-version=2024-02-29-preview",
            endpoint.url.trim_end_matches('/')
        );
        let mut req = self.client.get(&url);
        if let Some((k, v)) = auth_header {
            req = req.header(k, v);
        }
        match req.send().await {
            Ok(res) => res.status().as_u16() < 500,
            Err(_) => false,
        }
    }
}
