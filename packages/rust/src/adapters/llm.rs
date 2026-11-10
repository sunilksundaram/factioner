use crate::adapters::{AdapterError, ServiceAdapter};
use crate::resilience::ErrorClassifier;
use crate::types::{BudgetDeduction, EndpointConfig, FailureClass};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LLMMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LLMRequest {
    pub model: Option<String>,
    pub messages: Vec<LLMMessage>,
    pub max_tokens: Option<usize>,
    pub temperature: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LLMResponse {
    pub content: String,
    pub model: Option<String>,
}

pub struct LLMAdapter {
    client: reqwest::Client,
}

impl LLMAdapter {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .unwrap_or_default(),
        }
    }

    fn default_model(endpoint: &EndpointConfig, payload: &LLMRequest) -> String {
        payload
            .model
            .clone()
            .or_else(|| endpoint.metadata.get("model").cloned())
            .unwrap_or_else(|| "gpt-4o".to_string())
    }
}

impl Default for LLMAdapter {
    fn default() -> Self {
        Self::new()
    }
}

struct SseTokenStream {
    bytes: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    buffer: String,
    done: bool,
}

impl Stream for SseTokenStream {
    type Item = Result<String, AdapterError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            // Emit any complete lines already buffered
            if let Some(nl) = self.buffer.find('\n') {
                let line = self.buffer[..nl].trim_end_matches('\r').to_string();
                self.buffer = self.buffer[nl + 1..].to_string();
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with(':') {
                    continue;
                }
                if trimmed == "data: [DONE]" {
                    self.done = true;
                    return Poll::Ready(None);
                }
                if let Some(data) = trimmed.strip_prefix("data: ") {
                    match serde_json::from_str::<serde_json::Value>(data) {
                        Ok(parsed) => {
                            let delta = parsed["choices"][0]["delta"]["content"]
                                .as_str()
                                .or_else(|| {
                                    parsed["candidates"][0]["content"]["parts"][0]["text"].as_str()
                                })
                                .or_else(|| parsed["delta"]["text"].as_str())
                                .unwrap_or("");
                            if !delta.is_empty() {
                                return Poll::Ready(Some(Ok(delta.to_string())));
                            }
                            continue;
                        }
                        Err(_) => continue,
                    }
                }
                continue;
            }

            match self.bytes.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    self.buffer
                        .push_str(&String::from_utf8_lossy(&chunk));
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(AdapterError::msg(
                        format!("Stream transport error: {}", e),
                        FailureClass::Transient,
                    ))));
                }
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[async_trait]
impl ServiceAdapter<LLMRequest, LLMResponse> for LLMAdapter {
    fn estimate_budget(&self, payload: &LLMRequest) -> BudgetDeduction {
        let chars: usize = payload.messages.iter().map(|m| m.content.len()).sum();
        let est_tpm = (chars as f64 / 4.0).ceil() + payload.max_tokens.unwrap_or(1024) as f64;
        BudgetDeduction {
            rpm: Some(1.0),
            tpm: Some(est_tpm),
            ..Default::default()
        }
    }

    async fn execute(
        &self,
        endpoint: &EndpointConfig,
        payload: &LLMRequest,
        auth_header: Option<(&str, &str)>,
    ) -> Result<(LLMResponse, BudgetDeduction), AdapterError> {
        let is_anthropic = endpoint.url.contains("anthropic.com");
        let is_gemini = endpoint.url.contains("googleapis.com");
        let default_model = Self::default_model(endpoint, payload);

        let mut req = self.client.post(&endpoint.url);
        req = req.header("Content-Type", "application/json");

        let body_len_estimate: usize;

        if is_anthropic {
            req = req.header("anthropic-version", "2023-06-01");
            if let Some((_, val)) = auth_header {
                req = req.header("x-api-key", val);
            }
            let system = payload
                .messages
                .iter()
                .find(|m| m.role == "system")
                .map(|m| m.content.clone());
            let messages: Vec<_> = payload
                .messages
                .iter()
                .filter(|m| m.role != "system")
                .collect();
            let body = serde_json::json!({
                "model": default_model,
                "messages": messages,
                "system": system,
                "max_tokens": payload.max_tokens.unwrap_or(1024),
                "temperature": payload.temperature.unwrap_or(0.7),
            });
            body_len_estimate = body.to_string().len();
            req = req.json(&body);
        } else if is_gemini {
            if let Some((_, val)) = auth_header {
                req = req.header("x-goog-api-key", val);
            }
            let contents: Vec<_> = payload
                .messages
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "role": if m.role == "assistant" { "model" } else { "user" },
                        "parts": [{ "text": m.content }]
                    })
                })
                .collect();
            let body = serde_json::json!({ "contents": contents });
            body_len_estimate = body.to_string().len();
            req = req.json(&body);
        } else {
            if let Some((k, v)) = auth_header {
                req = req.header(k, v);
            }
            let body = serde_json::json!({
                "model": default_model,
                "messages": payload.messages,
                "max_tokens": payload.max_tokens.unwrap_or(1024),
                "temperature": payload.temperature.unwrap_or(0.7),
                "stream": false,
            });
            body_len_estimate = body.to_string().len();
            req = req.json(&body);
        }

        let resp = req.send().await.map_err(|e| {
            let msg = format!("Transport failure: {}", e);
            AdapterError::msg(msg.clone(), ErrorClassifier::classify(&msg).class)
        })?;

        let status = resp.status();
        if !status.is_success() {
            let code = status.as_u16();
            let retry_after_ms = ErrorClassifier::retry_after_from_headers(resp.headers());
            let txt = resp.text().await.unwrap_or_default();
            let classification = ErrorClassifier::classify_with_status(
                &format!("LLM error {}: {}", code, txt),
                Some(code),
                retry_after_ms,
            );
            return Err(AdapterError { classification });
        }

        let json_val = resp.json::<serde_json::Value>().await.map_err(|e| {
            AdapterError::msg(
                format!("Failed to parse response JSON: {}", e),
                FailureClass::Fatal,
            )
        })?;

        let content = if is_anthropic {
            json_val["content"][0]["text"]
                .as_str()
                .unwrap_or("")
                .to_string()
        } else if is_gemini {
            json_val["candidates"][0]["content"]["parts"][0]["text"]
                .as_str()
                .unwrap_or("")
                .to_string()
        } else {
            json_val["choices"][0]["message"]["content"]
                .as_str()
                .unwrap_or("")
                .to_string()
        };

        let prompt_tokens = json_val["usage"]["prompt_tokens"]
            .as_f64()
            .or_else(|| json_val["usage"]["input_tokens"].as_f64())
            .or_else(|| json_val["usageMetadata"]["promptTokenCount"].as_f64())
            .unwrap_or(0.0);
        let completion_tokens = json_val["usage"]["completion_tokens"]
            .as_f64()
            .or_else(|| json_val["usage"]["output_tokens"].as_f64())
            .or_else(|| json_val["usageMetadata"]["candidatesTokenCount"].as_f64())
            .unwrap_or(0.0);
        let total_tokens = if prompt_tokens + completion_tokens > 0.0 {
            prompt_tokens + completion_tokens
        } else {
            ((body_len_estimate + content.len()) as f64 / 4.0).ceil()
        };

        Ok((
            LLMResponse {
                content,
                model: Some(default_model),
            },
            BudgetDeduction {
                rpm: Some(1.0),
                tpm: Some(total_tokens),
                ..Default::default()
            },
        ))
    }

    async fn execute_stream(
        &self,
        endpoint: &EndpointConfig,
        payload: &LLMRequest,
        auth_header: Option<(&str, &str)>,
    ) -> Result<(BoxStream<'static, Result<String, AdapterError>>, BudgetDeduction), AdapterError>
    {
        let is_anthropic = endpoint.url.contains("anthropic.com");
        let is_gemini = endpoint.url.contains("googleapis.com");
        let default_model = Self::default_model(endpoint, payload);

        let mut req = self.client.post(&endpoint.url);
        req = req.header("Content-Type", "application/json");
        req = req.header("User-Agent", "Factioner-LLM-Stream/1.0");

        if is_anthropic {
            req = req.header("anthropic-version", "2023-06-01");
            if let Some((_, val)) = auth_header {
                req = req.header("x-api-key", val);
            }
            let system = payload
                .messages
                .iter()
                .find(|m| m.role == "system")
                .map(|m| m.content.clone());
            let messages: Vec<_> = payload
                .messages
                .iter()
                .filter(|m| m.role != "system")
                .collect();
            let body = serde_json::json!({
                "model": default_model,
                "messages": messages,
                "system": system,
                "max_tokens": payload.max_tokens.unwrap_or(1024),
                "temperature": payload.temperature.unwrap_or(0.7),
                "stream": true,
            });
            req = req.json(&body);
        } else if is_gemini {
            if let Some((_, val)) = auth_header {
                req = req.header("x-goog-api-key", val);
            }
            let contents: Vec<_> = payload
                .messages
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "role": if m.role == "assistant" { "model" } else { "user" },
                        "parts": [{ "text": m.content }]
                    })
                })
                .collect();
            let body = serde_json::json!({ "contents": contents });
            req = req.json(&body);
        } else {
            // OpenAI-style SSE
            if let Some((k, v)) = auth_header {
                req = req.header(k, v);
            }
            let body = serde_json::json!({
                "model": default_model,
                "messages": payload.messages,
                "max_tokens": payload.max_tokens.unwrap_or(1024),
                "temperature": payload.temperature.unwrap_or(0.7),
                "stream": true,
            });
            req = req.json(&body);
        }

        let resp = req.send().await.map_err(|e| {
            let msg = format!("LLM streaming transport failure: {}", e);
            AdapterError::msg(msg.clone(), ErrorClassifier::classify(&msg).class)
        })?;

        let status = resp.status();
        if !status.is_success() {
            let code = status.as_u16();
            let retry_after_ms = ErrorClassifier::retry_after_from_headers(resp.headers());
            let txt = resp.text().await.unwrap_or_default();
            return Err(AdapterError {
                classification: ErrorClassifier::classify_with_status(
                    &format!("LLM streaming failed {}: {}", code, txt),
                    Some(code),
                    retry_after_ms,
                ),
            });
        }

        let byte_stream = resp.bytes_stream();
        let token_stream = SseTokenStream {
            bytes: Box::pin(byte_stream),
            buffer: String::new(),
            done: false,
        };

        let est = self.estimate_budget(payload);
        Ok((Box::pin(token_stream), est))
    }
}
