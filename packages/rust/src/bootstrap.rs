use crate::adapters::{
    DocumentIntelligenceAdapter, EmbeddingAdapter, GenericHttpAdapter, LLMAdapter, MockAdapter,
};
use crate::config::{load_faction_config, FactionFileConfig, LoadedConfig, PoolFileConfig};
use crate::pool::{PoolRegistry, ServicePool};
use crate::server::GatewayServer;
use crate::spill::BlobSpillStore;
use crate::types::{
    AdmissionPolicy, BackpressureConfig, CircuitBreakerConfig, EndpointConfig, JobPriority,
    RoutingStrategy, SecretCredential, TenantConfig,
};
use std::collections::HashMap;
use std::sync::Arc;

pub struct BootstrapOptions {
    pub use_mock: bool,
    pub loaded: Option<LoadedConfig>,
}

impl Default for BootstrapOptions {
    fn default() -> Self {
        Self {
            use_mock: std::env::var("FACTIONER_USE_MOCK")
                .or_else(|_| std::env::var("FACTION_USE_MOCK"))
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            loaded: None,
        }
    }
}

pub struct BootstrapResult {
    pub registry: Arc<PoolRegistry>,
    pub gateway: GatewayServer,
    pub source_path: Option<String>,
    pub use_mock: bool,
    pub pool_names: Vec<String>,
}

pub fn normalize_service_type(service_type: &str) -> String {
    let key = service_type.trim().to_ascii_lowercase().replace('-', "_");
    match key.as_str() {
        "doc_intel" | "docintel" | "form_recognizer" | "ocr" => "document_intelligence".into(),
        "embeddings" | "vector" => "embedding".into(),
        "http" | "rest" => "custom".into(),
        other => other.to_string(),
    }
}

fn demo_pool_config() -> PoolFileConfig {
    PoolFileConfig {
        name: "llm-prod-pool".into(),
        service_type: "llm".into(),
        endpoints: demo_endpoints(),
        max_retries: Some(3),
        backpressure: Some(BackpressureConfig {
            max_queue_size: 100,
            admission_policy: AdmissionPolicy::DropOldest,
            queue_timeout_ms: 10_000,
            execution_timeout_ms: 30_000,
            admission_wait_timeout_ms: 3_000,
            shedding_threshold_percentage: 85.0,
            global_retry_budget_percentage: 20.0,
        }),
        circuit_breaker: Some(CircuitBreakerConfig {
            failure_threshold_percentage: 50.0,
            minimum_request_volume: 5,
            sampling_window_ms: 10_000,
            cooling_off_duration_ms: 3_000,
            half_open_trial_successes: 2,
        }),
        enable_pii_redaction: Some(false),
    }
}

fn demo_endpoints() -> Vec<EndpointConfig> {
    vec![
        EndpointConfig {
            id: "azure-openai-eastus".into(),
            url: "https://eastus.api.cognitive.microsoft.com/openai/deployments/gpt-4o".into(),
            max_concurrency: 4,
            budget: Some(crate::types::ResourceBudgetConfig {
                rpm: Some(600.0),
                tpm: Some(30000.0),
                ..Default::default()
            }),
            weight: Some(1.0),
            priority_tier: Some(0),
            cost_per_request: Some(1.0),
            credential: Some(SecretCredential {
                cred_type: "api_key".into(),
                value: "sk-azure-prod-secret-9999".into(),
                header_name: None,
            }),
            timeout_ms: Some(5000),
            metadata: HashMap::new(),
        },
        EndpointConfig {
            id: "openai-direct-primary".into(),
            url: "https://api.openai.com/v1/chat/completions".into(),
            max_concurrency: 4,
            budget: Some(crate::types::ResourceBudgetConfig {
                rpm: Some(600.0),
                tpm: Some(30000.0),
                ..Default::default()
            }),
            weight: Some(1.0),
            priority_tier: Some(0),
            cost_per_request: Some(1.0),
            credential: Some(SecretCredential {
                cred_type: "bearer".into(),
                value: "sk-live-openai-token-12345".into(),
                header_name: None,
            }),
            timeout_ms: Some(5000),
            metadata: HashMap::new(),
        },
    ]
}

fn default_tenants() -> Vec<TenantConfig> {
    vec![
        TenantConfig {
            id: "tenant-enterprise-gold".into(),
            max_rps: Some(30.0),
            max_tpm: Some(20000.0),
            max_concurrency: Some(10),
            daily_quota: None,
            max_allowed_priority: None,
            priority_bonus: None,
        },
        TenantConfig {
            id: "tenant-free-tier".into(),
            max_rps: Some(3.0),
            max_tpm: Some(1000.0),
            max_concurrency: Some(1),
            daily_quota: None,
            max_allowed_priority: Some(JobPriority::High),
            priority_bonus: None,
        },
    ]
}

fn register_tenants_on_pool<TReq, TRes>(
    pool: &ServicePool<TReq, TRes>,
    tenants: &[TenantConfig],
) where
    TReq: Clone + Send + Sync + serde::Serialize + serde::de::DeserializeOwned + 'static,
    TRes: Clone + Send + Sync + serde::Serialize + 'static,
{
    for t in tenants {
        pool.register_tenant(t.clone());
    }
}

async fn register_pool_for_type(
    registry: &PoolRegistry,
    pool_cfg: &PoolFileConfig,
    tenants: &[TenantConfig],
    use_mock: bool,
) -> String {
    let mut endpoints = pool_cfg.endpoints.clone();
    crate::config::apply_endpoint_env_overlays(&mut endpoints);
    let name = pool_cfg.name.clone();
    let service_type = pool_cfg.service_type.clone();
    let max_retries = pool_cfg.max_retries.unwrap_or(3);
    let enable_pii = pool_cfg.enable_pii_redaction.unwrap_or(false);
    let backpressure = pool_cfg.backpressure.clone();
    let circuit = pool_cfg.circuit_breaker.clone();

    if use_mock {
        let pool = ServicePool::<String, String>::with_security(
            name.clone(),
            service_type,
            endpoints,
            Arc::new(MockAdapter::new(25, 10.0)),
            RoutingStrategy::LeastLoaded,
            backpressure,
            circuit,
            None,
            max_retries,
            false,
            None,
            None,
            enable_pii,
        );
        register_tenants_on_pool(&pool, tenants);
        registry.register(pool).await;
        return name;
    }

    match normalize_service_type(&service_type).as_str() {
        "document_intelligence" => {
            let pool = ServicePool::with_security(
                name.clone(),
                service_type,
                endpoints,
                Arc::new(DocumentIntelligenceAdapter::new()),
                RoutingStrategy::LeastLoaded,
                backpressure,
                circuit,
                None,
                max_retries,
                false,
                None,
                None,
                enable_pii,
            );
            register_tenants_on_pool(&pool, tenants);
            registry.register(pool).await;
        }
        "embedding" => {
            let pool = ServicePool::with_security(
                name.clone(),
                service_type,
                endpoints,
                Arc::new(EmbeddingAdapter::new()),
                RoutingStrategy::LeastLoaded,
                backpressure,
                circuit,
                None,
                max_retries,
                false,
                None,
                None,
                enable_pii,
            );
            register_tenants_on_pool(&pool, tenants);
            registry.register(pool).await;
        }
        "custom" => {
            let pool = ServicePool::with_security(
                name.clone(),
                service_type,
                endpoints,
                Arc::new(GenericHttpAdapter::new()),
                RoutingStrategy::LeastLoaded,
                backpressure,
                circuit,
                None,
                max_retries,
                false,
                None,
                None,
                enable_pii,
            );
            register_tenants_on_pool(&pool, tenants);
            registry.register(pool).await;
        }
        _ => {
            // llm (default)
            let pool = ServicePool::with_security(
                name.clone(),
                service_type,
                endpoints,
                Arc::new(LLMAdapter::new()),
                RoutingStrategy::LeastLoaded,
                backpressure,
                circuit,
                None,
                max_retries,
                false,
                None,
                None,
                enable_pii,
            );
            register_tenants_on_pool(&pool, tenants);
            registry.register(pool).await;
        }
    }
    name
}

fn build_gateway(registry: Arc<PoolRegistry>, file: &FactionFileConfig) -> GatewayServer {
    let gw = file.gateway.clone().unwrap_or_default();
    let host = gw.host.unwrap_or_else(|| "127.0.0.1".into());
    let port = gw.port.unwrap_or(8080);
    let max_body = gw.max_body_bytes.unwrap_or(10 * 1024 * 1024);
    // Null/empty apiKey in example config → demo default key (matches TS CLI).
    let api_key = gw
        .api_key
        .filter(|k| !k.is_empty())
        .or_else(|| Some("factioner-secret-key-prod".into()));

    let mut server = GatewayServer::new(registry, api_key, &host, port).with_max_body_bytes(max_body);
    if let Some(dir) = gw.spill_dir {
        if let Ok(store) = BlobSpillStore::new(&dir, gw.spill_threshold_bytes, None) {
            server = server.with_spill(Arc::new(store));
        }
    }
    server
}

/// Load config, create adapters by serviceType, register pools/tenants, build gateway.
pub async fn bootstrap_from_config(options: BootstrapOptions) -> Result<BootstrapResult, String> {
    let loaded = match options.loaded {
        Some(l) => l,
        None => load_faction_config(None)?,
    };
    let registry = Arc::new(PoolRegistry::new());
    let tenants = loaded
        .file
        .tenants
        .clone()
        .filter(|t| !t.is_empty())
        .unwrap_or_else(default_tenants);

    let pools: Vec<PoolFileConfig> = match &loaded.file.pools {
        Some(p) if !p.is_empty() => p.clone(),
        _ => vec![demo_pool_config()],
    };

    let mut pool_names = Vec::new();
    for pool_cfg in &pools {
        let name =
            register_pool_for_type(&registry, pool_cfg, &tenants, options.use_mock).await;
        pool_names.push(name);
    }

    let gateway = build_gateway(registry.clone(), &loaded.file);
    Ok(BootstrapResult {
        registry,
        gateway,
        source_path: loaded.source_path,
        use_mock: options.use_mock,
        pool_names,
    })
}
