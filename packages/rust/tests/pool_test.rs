use factioner::*;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_priority_queue_fair_round_robin() {
    let mut pq = PriorityQueue::new();

    pq.enqueue(
        factioner::backpressure::priority_queue::QueuedItem {
            id: "A1".to_string(),
            priority: JobPriority::High,
            payload: "data",
            tenant_id: Some("tenant-A".to_string()),
            created_at: std::time::Instant::now(),
            retries_remaining: 1,
            attempts: 0,
            previous_decorrelated_delay: None,
            estimated_budget: BudgetDeduction::default(),
            avoid_endpoint_ids: vec![],
            tenant_charged: false,
            queue_timeout_ms: 5000,
            execution_timeout_ms: 5000,
        },
        false,
    );
    pq.enqueue(
        factioner::backpressure::priority_queue::QueuedItem {
            id: "A2".to_string(),
            priority: JobPriority::High,
            payload: "data",
            tenant_id: Some("tenant-A".to_string()),
            created_at: std::time::Instant::now(),
            retries_remaining: 1,
            attempts: 0,
            previous_decorrelated_delay: None,
            estimated_budget: BudgetDeduction::default(),
            avoid_endpoint_ids: vec![],
            tenant_charged: false,
            queue_timeout_ms: 5000,
            execution_timeout_ms: 5000,
        },
        false,
    );
    pq.enqueue(
        factioner::backpressure::priority_queue::QueuedItem {
            id: "B1".to_string(),
            priority: JobPriority::High,
            payload: "data",
            tenant_id: Some("tenant-B".to_string()),
            created_at: std::time::Instant::now(),
            retries_remaining: 1,
            attempts: 0,
            previous_decorrelated_delay: None,
            estimated_budget: BudgetDeduction::default(),
            avoid_endpoint_ids: vec![],
            tenant_charged: false,
            queue_timeout_ms: 5000,
            execution_timeout_ms: 5000,
        },
        false,
    );

    assert_eq!(pq.len(), 3);
    let first = pq.dequeue().unwrap().id;
    let second = pq.dequeue().unwrap().id;
    let third = pq.dequeue().unwrap().id;

    assert_eq!(first, "A1");
    assert_eq!(second, "B1");
    assert_eq!(third, "A2");
    assert_eq!(pq.len(), 0);
}

#[tokio::test]
async fn test_circuit_breaker_single_probe_half_open() {
    let cb = CircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold_percentage: 50.0,
        minimum_request_volume: 2,
        sampling_window_ms: 10_000,
        cooling_off_duration_ms: 40,
        half_open_trial_successes: 2,
    });

    assert_eq!(cb.get_state(), CircuitState::Closed);
    assert!(cb.can_execute());

    cb.record_failure();
    cb.record_failure();

    assert_eq!(cb.get_state(), CircuitState::Open);
    assert!(!cb.can_execute());

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert!(cb.can_execute());
    assert!(cb.try_claim_probe());
    assert_eq!(cb.get_state(), CircuitState::HalfOpen);

    assert!(!cb.can_execute());
    assert!(!cb.try_claim_probe());

    cb.record_success();

    assert!(cb.try_claim_probe());
    cb.record_success();

    assert_eq!(cb.get_state(), CircuitState::Closed);
}

#[tokio::test]
async fn test_credential_vault_secret_redaction() {
    let vault = CredentialVault::new();
    vault.register(
        "ep-test",
        SecretCredential {
            cred_type: "bearer".to_string(),
            value: "sk-super-secret-key-123456789".to_string(),
            header_name: None,
        },
    );

    let masked = CredentialVault::mask_credential("sk-super-secret-key-123456789");
    assert_eq!(masked, "sk...89");

    let redacted1 = vault.redact_secrets("Error with key sk-super-secret-key-123456789 in endpoint");
    assert_eq!(redacted1, "Error with key [REDACTED_SECRET] in endpoint");

    let redacted2 =
        vault.redact_secrets("Error: api_key=unregistered_live_key_9999 and token: Bearer abcdef123456");
    assert!(redacted2.contains("api_key=[REDACTED]"));
    assert!(redacted2.contains("Bearer [REDACTED_TOKEN]"));
}

#[tokio::test]
async fn test_tenant_governor_eligibility_and_priority() {
    let gov = TenantGovernor::new(true);
    gov.register_tenant(TenantConfig {
        id: "tenant-1".to_string(),
        max_rps: Some(1.0),
        max_tpm: None,
        max_concurrency: Some(1),
        daily_quota: None,
        max_allowed_priority: Some(JobPriority::High),
        priority_bonus: None,
    });

    assert!(gov.check_eligibility(Some("tenant-1"), None).is_ok());
    assert!(gov.check_eligibility(Some("tenant-1"), None).is_ok());

    let prio = gov.resolve_priority(Some("tenant-1"), JobPriority::Critical);
    assert_eq!(prio, JobPriority::High);

    gov.deduct_on_admission(Some("tenant-1"), None);
    assert!(gov.check_eligibility(Some("tenant-1"), None).is_err());

    gov.refund_on_drop(Some("tenant-1"), None);
    assert!(gov.check_eligibility(Some("tenant-1"), None).is_ok());
}

#[tokio::test]
async fn test_multi_resource_limiter_rpm_and_tpm() {
    let limiter = MultiResourceLimiter::new(ResourceBudgetConfig {
        rpm: Some(2.0),
        tpm: Some(1000.0),
        ..Default::default()
    });

    let est = BudgetDeduction {
        rpm: Some(1.0),
        tpm: Some(400.0),
        ..Default::default()
    };
    assert!(limiter.has_available(&est));
    assert!(limiter.try_acquire(&est));

    let big_est = BudgetDeduction {
        rpm: Some(1.0),
        tpm: Some(700.0),
        ..Default::default()
    };
    assert!(!limiter.has_available(&big_est));
}

#[tokio::test]
async fn test_service_pool_execution_and_metrics() {
    let adapter = Arc::new(MockAdapter::new(10, 0.0));
    let endpoints = vec![EndpointConfig {
        id: "ep-1".to_string(),
        url: "http://localhost:8000".to_string(),
        max_concurrency: 2,
        budget: Some(ResourceBudgetConfig {
            rpm: Some(60.0),
            tpm: Some(10000.0),
            ..Default::default()
        }),
        weight: Some(1.0),
        priority_tier: Some(0),
        cost_per_request: Some(1.0),
        credential: None,
        timeout_ms: Some(1000),
        metadata: HashMap::new(),
    }];

    let pool = ServicePool::new(
        "test-pool".to_string(),
        "mock".to_string(),
        endpoints,
        adapter,
        RoutingStrategy::LeastLoaded,
        None,
        None,
        None,
        2,
    );

    let res = pool
        .submit("test payload".to_string(), SubmitOptions::default())
        .await
        .unwrap();

    assert!(res.success);
    assert_eq!(res.endpoint_id.unwrap(), "ep-1");
    assert!(res.budget_deducted.is_some());

    let metrics = pool.get_metrics().await;
    assert_eq!(metrics.total_completed, 1);
    assert_eq!(metrics.endpoints.len(), 1);

    let tokens = pool
        .submit_stream_collect("s".to_string(), SubmitOptions::default())
        .await
        .unwrap();
    assert!(!tokens.is_empty());

    let handle = pool
        .submit_operation("o".to_string(), SubmitOptions::default())
        .await
        .unwrap();
    let snap = pool.poll_operation(&handle).await.unwrap();
    assert!(snap.done);
    assert_eq!(snap.data.as_deref(), Some("operation_complete"));

    pool.drain(2000).await;
}

#[tokio::test]
async fn test_drop_oldest_never_drops_critical() {
    let mut pq = PriorityQueue::new();
    pq.enqueue(
        factioner::backpressure::priority_queue::QueuedItem {
            id: "C1".to_string(),
            priority: JobPriority::Critical,
            payload: "critical-data",
            tenant_id: Some("vip".to_string()),
            created_at: std::time::Instant::now(),
            retries_remaining: 0,
            attempts: 0,
            previous_decorrelated_delay: None,
            estimated_budget: BudgetDeduction::default(),
            avoid_endpoint_ids: vec![],
            tenant_charged: false,
            queue_timeout_ms: 5000,
            execution_timeout_ms: 5000,
        },
        false,
    );
    pq.enqueue(
        factioner::backpressure::priority_queue::QueuedItem {
            id: "B1".to_string(),
            priority: JobPriority::Batch,
            payload: "batch-data",
            tenant_id: Some("batch".to_string()),
            created_at: std::time::Instant::now(),
            retries_remaining: 0,
            attempts: 0,
            previous_decorrelated_delay: None,
            estimated_budget: BudgetDeduction::default(),
            avoid_endpoint_ids: vec![],
            tenant_charged: false,
            queue_timeout_ms: 5000,
            execution_timeout_ms: 5000,
        },
        false,
    );

    let dropped1 = pq.drop_oldest_low_priority();
    assert_eq!(dropped1.map(|i| i.id), Some("B1".to_string()));

    let dropped2 = pq.drop_oldest_low_priority();
    assert!(dropped2.is_none(), "Must never drop Critical items");
    assert_eq!(pq.len(), 1);
    assert_eq!(pq.dequeue().unwrap().id, "C1");
}

#[tokio::test]
async fn test_tenant_isolation_strict() {
    let gov = TenantGovernor::with_security(true, Some(vec!["allowed-1".to_string()]));

    assert!(gov.check_eligibility(None, None).is_err());
    assert!(gov.check_eligibility(Some("unauthorized"), None).is_err());

    gov.register_tenant(TenantConfig {
        id: "allowed-1".to_string(),
        max_rps: Some(10.0),
        max_tpm: None,
        max_concurrency: None,
        daily_quota: None,
        max_allowed_priority: None,
        priority_bonus: None,
    });
    assert!(gov.check_eligibility(Some("allowed-1"), None).is_ok());
}

#[test]
fn test_pii_guard_sanitization() {
    let input = "Contact john.doe@example.com or call 555-123-4567, SSN: 123-45-6789";
    let (sanitized, count) = PIIGuard::sanitize_text(input);
    assert!(count >= 2);
    assert!(!sanitized.contains("123-45-6789"));
    assert!(!sanitized.contains("john.doe@example.com"));
    assert!(sanitized.contains("[REDACTED_SSN]"));
    assert!(sanitized.contains("[REDACTED_EMAIL]"));
}

#[test]
fn test_generic_http_adapter_ssrf_validation() {
    assert!(factioner::adapters::generic_http::GenericHttpAdapter::validate_path("../../etc/passwd").is_err());
    assert!(factioner::adapters::generic_http::GenericHttpAdapter::validate_path("https://evil.com").is_err());
    assert!(factioner::adapters::generic_http::GenericHttpAdapter::validate_path("api/v1/resource").is_ok());
}

#[tokio::test]
async fn test_multi_resource_limiter_fixed_window_idle_reset() {
    let limiter = MultiResourceLimiter::new(ResourceBudgetConfig {
        rps: Some(2.0),
        rpm: Some(10.0),
        ..Default::default()
    });

    let est = BudgetDeduction {
        rps: Some(2.0),
        rpm: Some(2.0),
        ..Default::default()
    };

    assert!(limiter.try_acquire(&est));
    assert!(!limiter.has_available(&BudgetDeduction {
        rps: Some(1.0),
        ..Default::default()
    }));

    tokio::time::sleep(std::time::Duration::from_millis(1050)).await;

    assert!(limiter.has_available(&BudgetDeduction {
        rps: Some(2.0),
        ..Default::default()
    }));
    assert!(limiter.try_acquire(&BudgetDeduction {
        rps: Some(2.0),
        ..Default::default()
    }));
}

#[tokio::test]
async fn test_service_pool_idempotency_in_flight_coalescing() {
    let adapter = Arc::new(MockAdapter::new(30, 0.0));
    let endpoints = vec![EndpointConfig {
        id: "ep-idem".to_string(),
        url: "http://localhost:8000".to_string(),
        max_concurrency: 5,
        budget: Some(ResourceBudgetConfig {
            rpm: Some(60.0),
            tpm: Some(10000.0),
            ..Default::default()
        }),
        weight: Some(1.0),
        priority_tier: Some(0),
        cost_per_request: Some(1.0),
        credential: None,
        timeout_ms: Some(1000),
        metadata: HashMap::new(),
    }];

    let pool = ServicePool::new(
        "idempotent-pool".to_string(),
        "mock".to_string(),
        endpoints,
        adapter,
        RoutingStrategy::LeastLoaded,
        None,
        None,
        None,
        2,
    );

    let opts1 = SubmitOptions {
        idempotency_key: Some("idem-key-1".to_string()),
        ..Default::default()
    };
    let opts2 = SubmitOptions {
        idempotency_key: Some("idem-key-1".to_string()),
        ..Default::default()
    };

    let fut1 = pool.submit("same payload".to_string(), opts1);
    let fut2 = pool.submit("same payload".to_string(), opts2);

    let (res1, res2) = tokio::join!(fut1, fut2);
    let r1 = res1.expect("First request succeeded");
    let r2 = res2.expect("Second request coalesced and succeeded");

    assert!(r1.success);
    assert!(r2.success);

    let opts3 = SubmitOptions {
        idempotency_key: Some("idem-key-1".to_string()),
        ..Default::default()
    };
    let r3 = pool.submit("same payload".to_string(), opts3).await.unwrap();
    assert!(r3.success);

    let metrics = pool.get_metrics().await;
    assert_eq!(metrics.total_completed, 1);

    pool.drain(2000).await;
}

#[test]
fn test_overload_signal_tightens_rpm_and_tpm() {
    let limiter = MultiResourceLimiter::new(ResourceBudgetConfig {
        rpm: Some(100.0),
        tpm: Some(1000.0),
        ..Default::default()
    });
    assert_eq!(limiter.effective_rpm(), Some(100.0));
    assert_eq!(limiter.effective_tpm(), Some(1000.0));
    limiter.apply_overload_signal(40);
    assert!(limiter.is_signal_tightened());
    assert_eq!(limiter.effective_rpm(), Some(50.0));
    assert_eq!(limiter.effective_tpm(), Some(500.0));
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(!limiter.is_signal_tightened());
    assert_eq!(limiter.effective_rpm(), Some(100.0));
    assert_eq!(limiter.effective_tpm(), Some(1000.0));
}

#[test]
fn test_tpm_reconcile_overshoot_bias() {
    let limiter = MultiResourceLimiter::new(ResourceBudgetConfig {
        tpm: Some(10_000.0),
        ..Default::default()
    });
    assert_eq!(limiter.tpm_estimate_bias(), 1.0);
    assert!(limiter.try_acquire(&BudgetDeduction {
        tpm: Some(100.0),
        ..Default::default()
    }));
    limiter.reconcile(
        &BudgetDeduction {
            tpm: Some(100.0),
            ..Default::default()
        },
        &BudgetDeduction {
            tpm: Some(250.0),
            ..Default::default()
        },
    );
    assert!(limiter.tpm_estimate_bias() > 1.0);
    assert!(limiter.is_signal_tightened());
    assert!(limiter.effective_tpm().unwrap() < 10_000.0);
}

#[test]
fn test_tokenizer_and_blob_spill() {
    assert!(estimate_tokens("hello world") >= 2);
    let dir = std::env::temp_dir().join(format!(
        "factioner-spill-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let store = BlobSpillStore::new(&dir, Some(32), None).expect("spill store");
    let big = "x".repeat(64);
    let spilled = store.spill_value(serde_json::json!({
        "base64Source": big,
        "modelId": "prebuilt"
    }));
    let uri = spilled["base64Source"].as_str().unwrap();
    assert!(uri.starts_with("spill://"));
    let resolved = store.resolve_value(spilled.clone());
    assert_eq!(resolved["base64Source"].as_str().unwrap(), big);
    store.delete_payload_spills(&spilled);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn test_bootstrap_from_config_registers_pools() {
    assert_eq!(
        normalize_service_type("doc-intel"),
        "document_intelligence"
    );
    let boot = bootstrap_from_config(BootstrapOptions {
        use_mock: true,
        loaded: Some(LoadedConfig {
            source_path: None,
            file: FactionFileConfig {
                gateway: Some(GatewayFileConfig {
                    host: Some("127.0.0.1".into()),
                    port: Some(18080),
                    api_key: Some("test".into()),
                    max_body_bytes: Some(1024),
                    spill_dir: None,
                    spill_threshold_bytes: None,
                }),
                pools: Some(vec![PoolFileConfig {
                    name: "embed-pool".into(),
                    service_type: "embedding".into(),
                    endpoints: vec![EndpointConfig {
                        id: "e1".into(),
                        url: "https://api.openai.com/v1/embeddings".into(),
                        max_concurrency: 2,
                        budget: Some(ResourceBudgetConfig {
                            rpm: Some(60.0),
                            tpm: Some(10_000.0),
                            ..Default::default()
                        }),
                        weight: Some(1.0),
                        priority_tier: Some(0),
                        cost_per_request: None,
                        credential: None,
                        timeout_ms: None,
                        metadata: Default::default(),
                    }],
                    max_retries: Some(1),
                    backpressure: None,
                    circuit_breaker: None,
                    enable_pii_redaction: Some(false),
                }]),
                tenants: Some(vec![TenantConfig {
                    id: "t1".into(),
                    max_rps: Some(5.0),
                    max_tpm: None,
                    max_concurrency: None,
                    daily_quota: None,
                    max_allowed_priority: None,
                    priority_bonus: None,
                }]),
            },
        }),
    })
    .await
    .expect("bootstrap");
    assert_eq!(boot.pool_names, vec!["embed-pool".to_string()]);
    assert!(boot.registry.get("embed-pool").await.is_some());
}

#[test]
fn test_doc_intel_and_embedding_budget_estimates() {
    let doc = DocumentIntelligenceAdapter::new();
    let doc_est = doc.estimate_budget(&DocumentAnalysisRequest {
        url_source: Some("https://example.com/a.pdf".into()),
        ..Default::default()
    });
    assert_eq!(doc_est.pages, Some(1.0));

    let emb = EmbeddingAdapter::new();
    let emb_est = emb.estimate_budget(&EmbeddingRequest {
        model: None,
        input: EmbeddingInput::Text("hello embeddings".into()),
        dimensions: None,
        encoding_format: None,
    });
    assert!(emb_est.tpm.unwrap() >= 1.0);
}
