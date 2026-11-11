use factioner::*;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("{}", "=".repeat(70));
    println!("   FACTIONER (Rust): Ultra-High-Performance Service Pool Engine");
    println!("{}", "=".repeat(70));

    // Demo uses mocks so burst traffic does not call real providers.
    // Production: BootstrapOptions { use_mock: false, ..Default::default() }
    let boot = bootstrap_from_config(BootstrapOptions {
        use_mock: true,
        loaded: None,
    })
    .await
    .expect("bootstrap");

    if let Some(ref path) = boot.source_path {
        println!("\n[config] Loaded FACTIONER_CONFIG from {}", path);
    }
    println!(
        "\n[1] Bootstrapped {} pool(s) via serviceType adapter factory (use_mock={})...",
        boot.pool_names.len(),
        boot.use_mock
    );

    let primary = boot
        .pool_names
        .first()
        .cloned()
        .unwrap_or_else(|| "llm-prod-pool".into());

    println!("\n[2] Simulating high-concurrency burst traffic...");
    let mut handles = Vec::new();
    for i in 0..40 {
        let registry = Arc::clone(&boot.registry);
        let pool_name = primary.clone();
        handles.push(tokio::spawn(async move {
            let pool = registry.get(&pool_name).await.expect("pool");
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
            // Mock pools accept String payloads via erased JSON path.
            let _ = pool
                .execute_json(
                    serde_json::json!(format!("request-{}", i)),
                    SubmitOptions {
                        priority: Some(priority),
                        tenant_id: Some(tenant.to_string()),
                        ..Default::default()
                    },
                )
                .await;
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    println!("\n[3] Metrics snapshot:");
    let metrics = boot.registry.get_all_metrics().await;
    println!("{:#?}", metrics);

    println!("\n[4] Starting Factioner Gateway briefly...");
    let gateway = boot.gateway;
    let registry = Arc::clone(&boot.registry);
    let serve = tokio::spawn(async move {
        if let Err(e) = gateway.serve().await {
            eprintln!("gateway error: {}", e);
        }
    });

    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("\n[5] Draining pools...");
    registry.drain_all(5_000).await;
    serve.abort();
    println!("✓ Demo complete.");
}
