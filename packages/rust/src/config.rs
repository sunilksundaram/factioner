use crate::types::{
    BackpressureConfig, CircuitBreakerConfig, EndpointConfig, ResourceBudgetConfig, SecretCredential,
    TenantConfig,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FactionFileConfig {
    pub gateway: Option<GatewayFileConfig>,
    pub pools: Option<Vec<PoolFileConfig>>,
    pub tenants: Option<Vec<TenantConfig>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayFileConfig {
    pub api_key: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub max_body_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolFileConfig {
    pub name: String,
    pub service_type: String,
    pub endpoints: Vec<EndpointConfig>,
    pub max_retries: Option<usize>,
    pub backpressure: Option<BackpressureConfig>,
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    pub enable_pii_redaction: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub file: FactionFileConfig,
    pub source_path: Option<String>,
}

/// Load JSON config from `FACTIONER_CONFIG` (legacy `FACTION_CONFIG`) or the given path,
/// then apply `FACTIONER_ENDPOINT_<ID>_*` overlays (legacy `FACTION_ENDPOINT_*` accepted).
pub fn load_faction_config(explicit_path: Option<&str>) -> Result<LoadedConfig, String> {
    let path = explicit_path
        .map(|s| s.to_string())
        .or_else(|| std::env::var("FACTIONER_CONFIG").ok())
        .or_else(|| std::env::var("FACTION_CONFIG").ok());

    let mut file = if let Some(ref p) = path {
        let text = std::fs::read_to_string(Path::new(p))
            .map_err(|e| format!("Failed to read FACTIONER_CONFIG '{}': {}", p, e))?;
        serde_json::from_str::<FactionFileConfig>(&text)
            .map_err(|e| format!("Failed to parse FACTIONER_CONFIG JSON: {}", e))?
    } else {
        FactionFileConfig::default()
    };

    apply_env_overlays(&mut file);
    Ok(LoadedConfig {
        file,
        source_path: path,
    })
}

fn env_first(keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Ok(v) = std::env::var(k) {
            return Some(v);
        }
    }
    None
}

fn apply_env_overlays(file: &mut FactionFileConfig) {
    if let Some(pools) = file.pools.as_mut() {
        for pool in pools.iter_mut() {
            for ep in pool.endpoints.iter_mut() {
                overlay_endpoint(ep);
            }
        }
    }

    if let Some(key) = env_first(&["FACTIONER_GATEWAY_API_KEY", "FACTION_GATEWAY_API_KEY"]) {
        let gw = file.gateway.get_or_insert_with(Default::default);
        gw.api_key = Some(key);
    }
    if let Some(host) = env_first(&["FACTIONER_GATEWAY_HOST", "FACTION_GATEWAY_HOST"]) {
        let gw = file.gateway.get_or_insert_with(Default::default);
        gw.host = Some(host);
    }
    if let Some(port) = env_first(&["FACTIONER_GATEWAY_PORT", "FACTION_GATEWAY_PORT"]) {
        if let Ok(p) = port.parse::<u16>() {
            let gw = file.gateway.get_or_insert_with(Default::default);
            gw.port = Some(p);
        }
    }
}

fn overlay_endpoint(ep: &mut EndpointConfig) {
    let id_key = ep
        .id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>();

    let mut budget = ep.budget.clone().unwrap_or_default();
    let mut changed = false;

    let tpm = env_first(&[
        &format!("FACTIONER_ENDPOINT_{}_TPM", id_key),
        &format!("FACTION_ENDPOINT_{}_TPM", id_key),
    ]);
    if let Some(v) = tpm {
        if let Ok(n) = v.parse::<f64>() {
            budget.tpm = Some(n);
            changed = true;
        }
    }
    let rpm = env_first(&[
        &format!("FACTIONER_ENDPOINT_{}_RPM", id_key),
        &format!("FACTION_ENDPOINT_{}_RPM", id_key),
    ]);
    if let Some(v) = rpm {
        if let Ok(n) = v.parse::<f64>() {
            budget.rpm = Some(n);
            changed = true;
        }
    }
    if changed {
        ep.budget = Some(budget);
    }

    if let Some(v) = env_first(&[
        &format!("FACTIONER_ENDPOINT_{}_MAX_CONCURRENCY", id_key),
        &format!("FACTION_ENDPOINT_{}_MAX_CONCURRENCY", id_key),
    ]) {
        if let Ok(n) = v.parse::<usize>() {
            ep.max_concurrency = n;
        }
    }

    if let Some(secret) = env_first(&[
        &format!("FACTIONER_ENDPOINT_{}_API_KEY", id_key),
        &format!("FACTION_ENDPOINT_{}_API_KEY", id_key),
    ]) {
        let cred_type = ep
            .credential
            .as_ref()
            .map(|c| c.cred_type.clone())
            .unwrap_or_else(|| "api_key".to_string());
        ep.credential = Some(SecretCredential {
            cred_type,
            value: secret,
            header_name: ep.credential.as_ref().and_then(|c| c.header_name.clone()),
        });
    }
}

/// Helper: build endpoint list from loaded config pool, or return defaults.
pub fn endpoints_from_pool(pool: &PoolFileConfig) -> Vec<EndpointConfig> {
    pool.endpoints.clone()
}

/// Flatten all endpoints in config for overlay inspection / tests.
pub fn all_endpoint_ids(file: &FactionFileConfig) -> Vec<String> {
    let mut ids = Vec::new();
    if let Some(pools) = &file.pools {
        for p in pools {
            for ep in &p.endpoints {
                ids.push(ep.id.clone());
            }
        }
    }
    ids
}

/// Apply overlays onto a standalone endpoint list (demo path without full file).
pub fn apply_endpoint_env_overlays(endpoints: &mut [EndpointConfig]) {
    for ep in endpoints.iter_mut() {
        overlay_endpoint(ep);
    }
}

pub fn demo_resource_budget(rpm: f64, tpm: f64) -> ResourceBudgetConfig {
    ResourceBudgetConfig {
        rpm: Some(rpm),
        tpm: Some(tpm),
        ..Default::default()
    }
}

pub fn env_map_snapshot() -> HashMap<String, String> {
    std::env::vars()
        .filter(|(k, _)| k.starts_with("FACTION_"))
        .collect()
}
