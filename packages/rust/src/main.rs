use factioner::*;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    println!("{}", "=".repeat(70));
    println!("   FACTIONER (Rust): Ultra-High-Performance Service Pool Engine");
    println!("{}", "=".repeat(70));

    let loaded = load_faction_config(None).ok();
    if let Some(ref cfg) = loaded {
        if let Some(ref path) = cfg.source_path {
            println!("\n[config] Loaded FACTIONER_CONFIG from {}", path);
        }
    }

    println!("\n[1] Initializing LLM Service Pool with Priority Failover & TPM Budgets...");
    let llm_adapter = Arc::new(MockAdapter::new(25, 15.0));

    let (endpoints, backpressure, circuit, max_retries, enable_pii, tenants) =
        if let Some(ref cfg) = loaded {
            if let Some(pools) = cfg.file.pools.as_ref() {
                if let Some(pool_cfg) = pools.first() {
                    let mut eps = pool_cfg.endpoints.clone();
                    factioner::config::apply_endpoint_env_overlays(&mut eps);
                    (
                        eps,
                        pool_cfg.backpressure.clone(),
                        pool_cfg.circuit_breaker.clone(),
                        pool_cfg.max_retries.unwrap_or(3),
                        pool_cfg.enable_pii_redaction.unwrap_or(false),
                        cfg.file.tenants.clone().unwrap_or_default(),
                    )
                } else {
                    demo_defaults()
                }
            } else {
                demo_defaults()
            }
        } else {
            demo_defaults()
        };

    let pool = ServicePool::with_security(
        "llm-prod-pool".to_string(),
        "llm".to_string(),
        endpoints,
        llm_adapter,
        RoutingStrategy::PriorityFailover,
        backpressure,
        circuit,
        None,
        max_retries,
        false,
        None,
        None,
        enable_pii,
    );

    for t in tenants {
        pool.register_tenant(t);
    }

    if loaded
        .as_ref()
        .and_then(|c| c.file.tenants.as_ref())
        .map(|t| t.is_empty())
        .unwrap_or(true)
    {
        pool.register_tenant(TenantConfig {
            id: "tenant-enterprise-gold".to_string(),
            max_rps: Some(30.0),
            max_tpm: Some(20000.0),
            max_concurrency: Some(10),
            daily_quota: None,
            max_allowed_priority: None,
            priority_bonus: None,
        });
        pool.register_tenant(TenantConfig {
            id: "tenant-free-tier".to_string(),
            max_rps: Some(3.0),
            max_tpm: Some(1000.0),
            max_concurrency: Some(1),
            daily_quota: None,
            max_allowed_priority: Some(JobPriority::High),
            priority_bonus: None,
        });
    }

    println!("\n[2] Simulating high-concurrency burst traffic...");
    let mut handles = Vec::new();
    for i in 0..40 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            let priority = if i % 10 == 0 {
                JobPriority::Critical
            } else if i % 3 == 0 {
                JobPriority::High
            } else {
                JobPriority::Normal
            };
            let tenant = if i % 2 == 0 {
                "tenant-enterprise-gold"
            } else {
                "tenant-free-tier"
            };
            pool.submit(
                format!("request-{}", i),
                SubmitOptions {
                    priority: Some(priority),
                    tenant_id: Some(tenant.to_string()),
                    ..Default::default()
                },
            )
            .await
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    println!("\n[3] Metrics snapshot:");
    let metrics = pool.get_metrics().await;
    println!("{:#?}", metrics);

    let stream_tokens = pool
        .submit_stream_collect("stream".to_string(), SubmitOptions::default())
        .await
        .unwrap_or_default();
    println!("\n[4] Stream tokens: {:?}", stream_tokens);

    let handle = pool
        .submit_operation("doc".to_string(), SubmitOptions::default())
        .await
        .expect("operation submit");
    let snap = pool.poll_operation(&handle).await.expect("poll");
    println!(
        "\n[5] Operation result: done={} status={} data={:?}",
        snap.done, snap.status, snap.data
    );

    pool.drain(5_000).await;
    println!("\n✓ Demo complete.");
}

fn demo_defaults() -> (
    Vec<EndpointConfig>,
    Option<BackpressureConfig>,
    Option<CircuitBreakerConfig>,
    usize,
    bool,
    Vec<TenantConfig>,
) {
    let mut endpoints = vec![
        EndpointConfig {
            id: "azure-openai-eastus".to_string(),
            url: "https://eastus.api.cognitive.microsoft.com/openai/deployments/gpt-4o".to_string(),
            max_concurrency: 4,
            budget: Some(ResourceBudgetConfig {
                rpm: Some(600.0),
                tpm: Some(30000.0),
                ..Default::default()
            }),
            weight: Some(1.0),
            priority_tier: Some(0),
            cost_per_request: Some(1.0),
            credential: Some(SecretCredential {
                cred_type: "api_key".to_string(),
                value: "sk-azure-prod-secret-9999".to_string(),
                header_name: None,
            }),
            timeout_ms: Some(5000),
            metadata: HashMap::new(),
        },
        EndpointConfig {
            id: "openai-direct-primary".to_string(),
            url: "https://api.openai.com/v1/chat/completions".to_string(),
            max_concurrency: 4,
            budget: Some(ResourceBudgetConfig {
                rpm: Some(600.0),
                tpm: Some(30000.0),
                ..Default::default()
            }),
            weight: Some(1.0),
            priority_tier: Some(0),
            cost_per_request: Some(1.0),
            credential: Some(SecretCredential {
                cred_type: "bearer".to_string(),
                value: "sk-live-openai-token-12345".to_string(),
                header_name: None,
            }),
            timeout_ms: Some(5000),
            metadata: HashMap::new(),
        },
        EndpointConfig {
            id: "gemini-fallback".to_string(),
            url: "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro".to_string(),
            max_concurrency: 6,
            budget: Some(ResourceBudgetConfig {
                rpm: Some(1200.0),
                tpm: Some(60000.0),
                ..Default::default()
            }),
            weight: Some(1.0),
            priority_tier: Some(1),
            cost_per_request: Some(0.5),
            credential: Some(SecretCredential {
                cred_type: "api_key".to_string(),
                value: "AIzaSy-gemini-secret-5555".to_string(),
                header_name: None,
            }),
            timeout_ms: Some(5000),
            metadata: HashMap::new(),
        },
    ];
    factioner::config::apply_endpoint_env_overlays(&mut endpoints);

    (
        endpoints,
        Some(BackpressureConfig {
            max_queue_size: 100,
            admission_policy: AdmissionPolicy::DropOldest,
            queue_timeout_ms: 10_000,
            execution_timeout_ms: 30_000,
            admission_wait_timeout_ms: 3_000,
            shedding_threshold_percentage: 85.0,
            global_retry_budget_percentage: 20.0,
        }),
        Some(CircuitBreakerConfig {
            failure_threshold_percentage: 50.0,
            minimum_request_volume: 5,
            sampling_window_ms: 10_000,
            cooling_off_duration_ms: 3_000,
            half_open_trial_successes: 2,
        }),
        3,
        false,
        Vec::new(),
    )
}
