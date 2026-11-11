import { readFileSync, existsSync } from 'node:fs';
import { resolve } from 'node:path';
import {
  AdmissionPolicy,
  EndpointConfig,
  RoutingStrategy,
  SecretCredential,
  ServicePoolOptions,
  TenantConfig,
  JobPriority
} from '../types/index.js';

export interface GatewayFileConfig {
  apiKey?: string | null;
  host?: string;
  port?: number;
  maxBodyBytes?: number;
  spillDir?: string;
  spillThresholdBytes?: number;
}

export interface PoolFileConfig {
  name: string;
  serviceType: string;
  endpoints: EndpointConfig[];
  maxRetries?: number;
  backpressure?: ServicePoolOptions['backpressure'];
  circuitBreaker?: ServicePoolOptions['circuitBreaker'];
  enablePiiRedaction?: boolean;
  routingStrategy?: ServicePoolOptions['routingStrategy'] | string;
  healthCheckIntervalMs?: number;
}

export interface FactionFileConfig {
  gateway?: GatewayFileConfig;
  pools?: PoolFileConfig[];
  tenants?: Array<TenantConfig & { maxAllowedPriority?: JobPriority | string }>;
}

export interface LoadedConfig {
  file: FactionFileConfig;
  sourcePath?: string;
}

/** Accept Rust PascalCase or TS snake_case admission policy strings. */
export function normalizeAdmissionPolicy(raw: unknown): AdmissionPolicy | undefined {
  if (typeof raw !== 'string') return undefined;
  const key = raw.trim().toLowerCase().replace(/-/g, '_');
  const map: Record<string, AdmissionPolicy> = {
    reject: 'reject',
    drop_oldest: 'drop_oldest',
    dropoldest: 'drop_oldest',
    drop_newest: 'drop_newest',
    dropnewest: 'drop_newest',
    timeout: 'timeout'
  };
  return map[key];
}

export function normalizeRoutingStrategy(raw: unknown): RoutingStrategy | undefined {
  if (typeof raw !== 'string') return undefined;
  const key = raw.trim().toLowerCase().replace(/-/g, '_');
  const map: Record<string, RoutingStrategy> = {
    round_robin: 'round_robin',
    roundrobin: 'round_robin',
    least_loaded: 'least_loaded',
    leastloaded: 'least_loaded',
    weighted_latency: 'weighted_latency',
    weightedlatency: 'weighted_latency',
    priority_failover: 'priority_failover',
    priorityfailover: 'priority_failover',
    cost_optimized: 'cost_optimized',
    costoptimized: 'cost_optimized'
  };
  return map[key];
}

/** Accept TS `type` or Rust JSON `credType`. */
export function normalizeCredential(raw: unknown): SecretCredential | undefined {
  if (!raw || typeof raw !== 'object') return undefined;
  const obj = raw as Record<string, unknown>;
  const typeRaw = (obj.type ?? obj.credType ?? obj.cred_type) as string | undefined;
  const value = obj.value as string | undefined;
  if (!typeRaw || !value) return undefined;
  const type = typeRaw as SecretCredential['type'];
  return {
    type: ['api_key', 'bearer', 'oauth2', 'custom'].includes(type) ? type : 'api_key',
    value,
    headerName: (obj.headerName ?? obj.header_name) as string | undefined,
    maskedValue: (obj.maskedValue ?? obj.masked_value) as string | undefined
  };
}

function envFirst(...keys: string[]): string | undefined {
  for (const k of keys) {
    const v = process.env[k];
    if (v !== undefined && v !== '') return v;
  }
  return undefined;
}

function overlayEndpoint(ep: EndpointConfig): void {
  const id = ep.id.toUpperCase().replace(/[^A-Z0-9]/g, '_');
  const tpm = envFirst(`FACTIONER_ENDPOINT_${id}_TPM`, `FACTION_ENDPOINT_${id}_TPM`);
  const rpm = envFirst(`FACTIONER_ENDPOINT_${id}_RPM`, `FACTION_ENDPOINT_${id}_RPM`);
  const maxConc = envFirst(
    `FACTIONER_ENDPOINT_${id}_MAX_CONCURRENCY`,
    `FACTION_ENDPOINT_${id}_MAX_CONCURRENCY`
  );
  const apiKey = envFirst(`FACTIONER_ENDPOINT_${id}_API_KEY`, `FACTION_ENDPOINT_${id}_API_KEY`);
  const url = envFirst(`FACTIONER_ENDPOINT_${id}_URL`, `FACTION_ENDPOINT_${id}_URL`);

  if (!ep.budget) ep.budget = {};
  if (tpm) ep.budget.tpm = Number(tpm);
  if (rpm) ep.budget.rpm = Number(rpm);
  if (maxConc) ep.maxConcurrency = Number(maxConc);
  if (url) ep.url = url;
  if (apiKey) {
    ep.credential = {
      type: 'api_key',
      value: apiKey,
      headerName: 'api-key'
    };
  }
}

function normalizeEndpoint(ep: EndpointConfig & { credential?: unknown }): EndpointConfig {
  const credential = normalizeCredential(ep.credential) ?? ep.credential;
  return {
    ...ep,
    credential: credential as SecretCredential | undefined
  };
}

function normalizeTenant(t: TenantConfig & { maxAllowedPriority?: JobPriority | string }): TenantConfig {
  const out: TenantConfig = { ...t };
  if (typeof t.maxAllowedPriority === 'string') {
    const map: Record<string, JobPriority> = {
      Critical: JobPriority.CRITICAL,
      CRITICAL: JobPriority.CRITICAL,
      High: JobPriority.HIGH,
      HIGH: JobPriority.HIGH,
      Normal: JobPriority.NORMAL,
      NORMAL: JobPriority.NORMAL,
      Batch: JobPriority.BATCH,
      BATCH: JobPriority.BATCH
    };
    out.maxAllowedPriority = map[t.maxAllowedPriority] ?? JobPriority.NORMAL;
  }
  return out;
}

function normalizePool(pool: PoolFileConfig): PoolFileConfig {
  const bp = pool.backpressure ? { ...pool.backpressure } : undefined;
  if (bp && (bp as { admissionPolicy?: unknown }).admissionPolicy !== undefined) {
    const normalized = normalizeAdmissionPolicy((bp as { admissionPolicy?: unknown }).admissionPolicy);
    if (normalized) {
      (bp as { admissionPolicy: AdmissionPolicy }).admissionPolicy = normalized;
    }
  }
  const routing = normalizeRoutingStrategy(pool.routingStrategy) ?? pool.routingStrategy;
  return {
    ...pool,
    backpressure: bp,
    routingStrategy: routing as PoolFileConfig['routingStrategy'],
    endpoints: pool.endpoints.map(ep => normalizeEndpoint(ep))
  };
}

/**
 * Load JSON config from FACTIONER_CONFIG (legacy FACTION_CONFIG) or explicit path,
 * then apply FACTIONER_ENDPOINT_<ID>_* overlays (legacy FACTION_ENDPOINT_* accepted).
 */
export function loadFactionConfig(explicitPath?: string): LoadedConfig {
  const path =
    explicitPath ??
    process.env.FACTIONER_CONFIG ??
    process.env.FACTION_CONFIG ??
    (existsSync(resolve(process.cwd(), 'factioner.config.json'))
      ? resolve(process.cwd(), 'factioner.config.json')
      : existsSync(resolve(process.cwd(), 'faction.config.json'))
        ? resolve(process.cwd(), 'faction.config.json')
        : undefined);

  let file: FactionFileConfig = {};
  if (path && existsSync(path)) {
    const text = readFileSync(path, 'utf8');
    file = JSON.parse(text) as FactionFileConfig;
  }

  if (file.pools) {
    file.pools = file.pools.map(normalizePool);
    for (const pool of file.pools) {
      for (const ep of pool.endpoints) {
        overlayEndpoint(ep);
      }
    }
  }

  const gwKey = envFirst('FACTIONER_GATEWAY_API_KEY', 'FACTION_GATEWAY_API_KEY');
  const gwHost = envFirst('FACTIONER_GATEWAY_HOST', 'FACTION_GATEWAY_HOST');
  const gwPort = envFirst('FACTIONER_GATEWAY_PORT', 'FACTION_GATEWAY_PORT');
  const gwBody = envFirst('FACTIONER_GATEWAY_MAX_BODY_BYTES', 'FACTION_GATEWAY_MAX_BODY_BYTES');
  const gwSpill = envFirst('FACTIONER_GATEWAY_SPILL_DIR', 'FACTION_GATEWAY_SPILL_DIR');
  const gwSpillThreshold = envFirst(
    'FACTIONER_GATEWAY_SPILL_THRESHOLD_BYTES',
    'FACTION_GATEWAY_SPILL_THRESHOLD_BYTES'
  );
  if (gwKey || gwHost || gwPort || gwBody || gwSpill || gwSpillThreshold) {
    file.gateway = file.gateway ?? {};
    if (gwKey) file.gateway.apiKey = gwKey;
    if (gwHost) file.gateway.host = gwHost;
    if (gwPort) file.gateway.port = Number(gwPort);
    if (gwBody) file.gateway.maxBodyBytes = Number(gwBody);
    if (gwSpill) file.gateway.spillDir = gwSpill;
    if (gwSpillThreshold) file.gateway.spillThresholdBytes = Number(gwSpillThreshold);
  }

  if (file.tenants) {
    file.tenants = file.tenants.map(normalizeTenant);
  }

  return { file, sourcePath: path };
}

export function poolOptionsFromFile(
  pool: PoolFileConfig,
  overrides: Partial<ServicePoolOptions> = {}
): ServicePoolOptions {
  const normalized = normalizePool(pool);
  return {
    name: normalized.name,
    serviceType: normalized.serviceType,
    endpoints: normalized.endpoints,
    maxRetries: normalized.maxRetries,
    backpressure: normalized.backpressure,
    circuitBreaker: normalized.circuitBreaker,
    routingStrategy: normalized.routingStrategy as ServicePoolOptions['routingStrategy'],
    healthCheckIntervalMs: normalized.healthCheckIntervalMs,
    security: {
      enablePIIRedaction: normalized.enablePiiRedaction ?? false,
      redactSecretsInLogs: true,
      enforceTenantIsolation: false
    },
    ...overrides
  };
}
