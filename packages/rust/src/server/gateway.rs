use crate::pool::PoolRegistry;
use crate::spill::{configure_global_spill, BlobSpillStore};
use crate::types::{JobPriority, SubmitOptions};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::StreamExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tower_http::cors::{AllowOrigin, CorsLayer};

#[derive(Clone)]
pub struct GatewayState {
    pub registry: Arc<PoolRegistry>,
    pub api_key: Option<String>,
    pub allow_unauthenticated: bool,
    pub started_at: Instant,
    pub spill: Option<Arc<BlobSpillStore>>,
    /// Spilled payloads retained for in-flight LROs until done/cancel.
    pub operation_spills: Arc<Mutex<HashMap<String, Value>>>,
}

pub struct GatewayServer {
    state: GatewayState,
    host: String,
    port: u16,
    cors_origins: Vec<String>,
    max_body_bytes: usize,
}

impl GatewayServer {
    pub fn new(
        registry: Arc<PoolRegistry>,
        api_key: Option<String>,
        host: &str,
        port: u16,
    ) -> Self {
        Self {
            state: GatewayState {
                registry,
                api_key,
                allow_unauthenticated: false,
                started_at: Instant::now(),
                spill: None,
                operation_spills: Arc::new(Mutex::new(HashMap::new())),
            },
            host: host.to_string(),
            port,
            cors_origins: Vec::new(),
            max_body_bytes: 10 * 1024 * 1024,
        }
    }

    pub fn with_cors(mut self, origins: Vec<String>) -> Self {
        self.cors_origins = origins;
        self
    }

    pub fn with_max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        self.max_body_bytes = max_body_bytes;
        self
    }

    pub fn with_spill(mut self, store: Arc<BlobSpillStore>) -> Self {
        configure_global_spill(store.clone());
        self.state.spill = Some(store);
        self
    }

    pub fn allow_unauthenticated(mut self) -> Self {
        self.state.allow_unauthenticated = true;
        self
    }

    pub async fn serve(self) -> Result<(), std::io::Error> {
        let cors = if self.cors_origins.is_empty() {
            CorsLayer::new()
        } else {
            CorsLayer::new().allow_origin(AllowOrigin::list(
                self.cors_origins
                    .iter()
                    .filter_map(|o| o.parse().ok())
                    .collect::<Vec<_>>(),
            ))
        };

        let app = Router::new()
            .route("/v1/health", get(health))
            .route("/health", get(health))
            .route("/v1/metrics", get(metrics))
            .route("/v1/pools/:name/metrics", get(pool_metrics))
            .route("/v1/pools/:name/execute", post(execute_handler))
            .route("/v1/pools/:name/stream", post(stream_handler))
            .route("/v1/pools/:name/operations", post(submit_operation_handler))
            .route(
                "/v1/pools/:name/operations/:id",
                get(poll_operation_handler).delete(cancel_operation_handler),
            )
            .layer(DefaultBodyLimit::max(self.max_body_bytes))
            .layer(cors)
            .with_state(self.state);

        let addr: SocketAddr = format!("{}:{}", self.host, self.port)
            .parse()
            .expect("valid bind address");
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await
    }
}

fn authorize(headers: &HeaderMap, state: &GatewayState) -> bool {
    if state.allow_unauthenticated && state.api_key.is_none() {
        return true;
    }
    let Some(ref key) = state.api_key else {
        return false;
    };
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        // Case-insensitive "Bearer "
        let auth_trim = auth.trim();
        if auth_trim.len() > 6 && auth_trim[..6].eq_ignore_ascii_case("bearer") {
            let token = auth_trim[6..].trim();
            if token == key {
                return true;
            }
        }
    }
    if let Some(k) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return k == key;
    }
    false
}

fn extract_submit_options(headers: &HeaderMap) -> SubmitOptions {
    let tenant_id = headers
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let priority = headers
        .get("x-priority")
        .and_then(|v| v.to_str().ok())
        .map(|p| match p.to_lowercase().as_str() {
            "critical" | "0" => JobPriority::Critical,
            "high" | "1" => JobPriority::High,
            "batch" | "3" => JobPriority::Batch,
            _ => JobPriority::Normal,
        });

    let queue_timeout_ms = headers
        .get("x-timeout-ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let idempotency_key = headers
        .get("x-idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    SubmitOptions {
        priority,
        tenant_id,
        queue_timeout_ms,
        execution_timeout_ms: None,
        max_retries: None,
        idempotency_key,
        estimated_budget: None,
    }
}

fn classify_gateway_error(err: &str) -> (StatusCode, Option<&'static str>, bool) {
    let lower = err.to_lowercase();
    if err.starts_with("TENANT_DENIED")
        || lower.contains("tenant admission denied")
        || lower.contains("tenant rejected")
    {
        return (StatusCode::TOO_MANY_REQUESTS, Some("TENANT_DENIED"), true);
    }
    if err.starts_with("SATURATED") || lower.contains("saturated") {
        return (StatusCode::TOO_MANY_REQUESTS, Some("SATURATED"), true);
    }
    if err.starts_with("QUEUE_FULL")
        || lower.contains("backpressure")
        || lower.contains("queue capacity")
        || lower.contains("queue full")
        || lower.contains("no capacity")
    {
        return (StatusCode::TOO_MANY_REQUESTS, Some("QUEUE_FULL"), true);
    }
    (StatusCode::BAD_GATEWAY, None, false)
}

fn error_response(err: String) -> axum::response::Response {
    let (status, code, backpressure) = classify_gateway_error(&err);
    let mut body = json!({ "error": err });
    if let Some(c) = code {
        body["code"] = json!(c);
    }
    if backpressure {
        body["backpressure"] = json!(true);
    }
    let mut res = (status, Json(body)).into_response();
    if backpressure {
        res.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("2"),
        );
    }
    res
}

async fn health(State(state): State<GatewayState>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "uptime": state.started_at.elapsed().as_secs_f64()
    }))
}

async fn metrics(State(state): State<GatewayState>, headers: HeaderMap) -> impl IntoResponse {
    if !authorize(&headers, &state) {
        return (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Unauthorized" }))).into_response();
    }
    let all = state.registry.get_all_metrics().await;
    (StatusCode::OK, Json(json!(all))).into_response()
}

async fn pool_metrics(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if !authorize(&headers, &state) {
        return (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Unauthorized" }))).into_response();
    }
    let all = state.registry.get_all_metrics().await;
    match all.get(&name) {
        Some(m) => (StatusCode::OK, Json(json!(m))).into_response(),
        None => (StatusCode::NOT_FOUND, Json(json!({ "error": "Pool not found" }))).into_response(),
    }
}

async fn execute_handler(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    if !authorize(&headers, &state) {
        return (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Unauthorized" }))).into_response();
    }
    let pool = match state.registry.get(&name).await {
        Some(p) => p,
        None => {
            return (StatusCode::NOT_FOUND, Json(json!({ "error": "Pool not found" }))).into_response()
        }
    };

    let options = extract_submit_options(&headers);
    let payload = prepare_payload(&state, payload);
    match pool.execute_json(payload.clone(), options).await {
        Ok(res) => {
            cleanup_payload_spills(&state, &payload);
            (StatusCode::OK, Json(res)).into_response()
        }
        Err(err) => {
            cleanup_payload_spills(&state, &payload);
            error_response(err)
        }
    }
}

fn prepare_payload(state: &GatewayState, payload: serde_json::Value) -> serde_json::Value {
    match &state.spill {
        Some(store) => store.spill_value(payload),
        None => payload,
    }
}

fn cleanup_payload_spills(state: &GatewayState, payload: &serde_json::Value) {
    if let Some(store) = &state.spill {
        store.delete_payload_spills(payload);
    }
}

fn operation_spill_key(pool_name: &str, operation_id: &str) -> String {
    format!("{}::{}", pool_name, operation_id)
}

fn retain_operation_spill(state: &GatewayState, pool_name: &str, operation_id: &str, payload: Value) {
    if state.spill.is_none() {
        return;
    }
    if let Ok(mut map) = state.operation_spills.lock() {
        map.insert(operation_spill_key(pool_name, operation_id), payload);
    }
}

fn release_operation_spill(state: &GatewayState, pool_name: &str, operation_id: &str) {
    let payload = state
        .operation_spills
        .lock()
        .ok()
        .and_then(|mut map| map.remove(&operation_spill_key(pool_name, operation_id)));
    if let Some(payload) = payload {
        cleanup_payload_spills(state, &payload);
    }
}

async fn stream_handler(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    if !authorize(&headers, &state) {
        return (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Unauthorized" }))).into_response();
    }
    let pool = match state.registry.get(&name).await {
        Some(p) => p,
        None => {
            return (StatusCode::NOT_FOUND, Json(json!({ "error": "Pool not found" }))).into_response()
        }
    };

    let options = extract_submit_options(&headers);
    let payload = prepare_payload(&state, payload);
    match pool.stream_sse(payload.clone(), options).await {
        Ok(token_stream) => {
            let spill = state.spill.clone();
            let spilled = payload.clone();
            let sse_stream = token_stream
                .map(|item| {
                    let event = match item {
                        Ok(token) => Event::default().data(json!({ "token": token }).to_string()),
                        Err(err) => Event::default().data(json!({ "error": err }).to_string()),
                    };
                    Ok::<Event, Infallible>(event)
                })
                .chain(futures::stream::once(async move {
                    if let Some(store) = spill {
                        store.delete_payload_spills(&spilled);
                    }
                    Ok::<Event, Infallible>(Event::default().data("[DONE]"))
                }));
            // Client disconnect drops the stream → GuardedTokenStream Drop releases lease.
            Sse::new(sse_stream)
                .keep_alive(KeepAlive::default())
                .into_response()
        }
        Err(err) => {
            cleanup_payload_spills(&state, &payload);
            error_response(err)
        }
    }
}

async fn submit_operation_handler(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    if !authorize(&headers, &state) {
        return (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Unauthorized" }))).into_response();
    }
    let pool = match state.registry.get(&name).await {
        Some(p) => p,
        None => {
            return (StatusCode::NOT_FOUND, Json(json!({ "error": "Pool not found" }))).into_response()
        }
    };

    let options = extract_submit_options(&headers);
    let payload = prepare_payload(&state, payload);
    match pool.submit_operation_json(payload.clone(), options).await {
        Ok(operation_id) => {
            retain_operation_spill(&state, &name, &operation_id, payload);
            (
                StatusCode::ACCEPTED,
                Json(json!({
                    "operationId": operation_id,
                    "status": "running"
                })),
            )
                .into_response()
        }
        Err(err) => {
            cleanup_payload_spills(&state, &payload);
            error_response(err)
        }
    }
}

async fn poll_operation_handler(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Path((name, id)): Path<(String, String)>,
) -> impl IntoResponse {
    if !authorize(&headers, &state) {
        return (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Unauthorized" }))).into_response();
    }
    let pool = match state.registry.get(&name).await {
        Some(p) => p,
        None => {
            return (StatusCode::NOT_FOUND, Json(json!({ "error": "Pool not found" }))).into_response()
        }
    };

    // Opaque handle — no x-endpoint-id required.
    match pool.poll_operation_json(&id).await {
        Ok(snap) => {
            if snap.done {
                release_operation_spill(&state, &name, &id);
            }
            (
                StatusCode::OK,
                Json(json!({
                    "operationId": id,
                    "status": snap.status,
                    "done": snap.done,
                    "data": snap.data,
                })),
            )
                .into_response()
        }
        Err(err) => {
            let status = if err.contains("not found") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::BAD_GATEWAY
            };
            (status, Json(json!({ "operationId": id, "error": err }))).into_response()
        }
    }
}

async fn cancel_operation_handler(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Path((name, id)): Path<(String, String)>,
) -> impl IntoResponse {
    if !authorize(&headers, &state) {
        return (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Unauthorized" }))).into_response();
    }
    let pool = match state.registry.get(&name).await {
        Some(p) => p,
        None => {
            return (StatusCode::NOT_FOUND, Json(json!({ "error": "Pool not found" }))).into_response()
        }
    };

    match pool.cancel_operation_json(&id).await {
        Ok(upstream_cancelled) => {
            release_operation_spill(&state, &name, &id);
            (
                StatusCode::OK,
                Json(json!({
                    "operationId": id,
                    "status": "cancelled",
                    "upstreamCancelled": upstream_cancelled
                })),
            )
                .into_response()
        }
        Err(err) => {
            (StatusCode::BAD_GATEWAY, Json(json!({ "operationId": id, "error": err }))).into_response()
        }
    }
}
