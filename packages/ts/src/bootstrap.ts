import { PoolRegistry } from './pool/pool-registry.js';
import { ServicePool } from './pool/service-pool.js';
import { ServiceAdapter } from './adapters/base-adapter.js';
import { LLMAdapter } from './adapters/llm-adapter.js';
import { DocumentIntelligenceAdapter } from './adapters/doc-intelligence-adapter.js';
import { EmbeddingAdapter } from './adapters/embedding-adapter.js';
import { GenericHttpAdapter } from './adapters/generic-http-adapter.js';
import { MockServiceAdapter } from './adapters/mock-adapter.js';
import {
  GatewayFileConfig,
  LoadedConfig,
  loadFactionConfig,
  poolOptionsFromFile
} from './config/load-config.js';
import { GatewayServerOptions } from './server/gateway-server.js';
import { JobPriority, ServicePoolOptions } from './types/index.js';

export interface BootstrapOptions {
  /** Prefer MockServiceAdapter for all pools (demos / offline CI). Default false. */
  useMock?: boolean;
  /** Existing registry to register into (default: singleton). */
  registry?: PoolRegistry;
  /** Explicit config; when omitted, loads via loadFactionConfig(). */
  loaded?: LoadedConfig;
}

export interface BootstrapResult {
  registry: PoolRegistry;
  pools: ServicePool<unknown, unknown>[];
  gatewayOptions: GatewayServerOptions;
  sourcePath?: string;
  useMock: boolean;
}

/** Normalize serviceType aliases to a canonical key. */
export function normalizeServiceType(serviceType: string): string {
  const key = serviceType.trim().toLowerCase().replace(/-/g, '_');
  if (key === 'doc_intel' || key === 'docintel' || key === 'form_recognizer' || key === 'ocr') {
    return 'document_intelligence';
  }
  if (key === 'embeddings' || key === 'vector') {
    return 'embedding';
  }
  if (key === 'http' || key === 'rest') {
    return 'custom';
  }
  return key;
}

/**
 * Select a concrete adapter for a pool's serviceType.
 * When useMock is true, returns MockServiceAdapter (demo / load simulation).
 */
export function createAdapterForServiceType(
  serviceType: string,
  useMock = false
): ServiceAdapter<unknown, unknown> {
  if (useMock) {
    return new MockServiceAdapter({
      baseLatencyMs: 25,
      jitterMs: 10,
      failureRatePercentage: 10
    }) as ServiceAdapter<unknown, unknown>;
  }

  switch (normalizeServiceType(serviceType)) {
    case 'llm':
      return new LLMAdapter() as ServiceAdapter<unknown, unknown>;
    case 'document_intelligence':
      return new DocumentIntelligenceAdapter() as ServiceAdapter<unknown, unknown>;
    case 'embedding':
      return new EmbeddingAdapter() as ServiceAdapter<unknown, unknown>;
    case 'custom':
      return new GenericHttpAdapter() as ServiceAdapter<unknown, unknown>;
    default:
      return new GenericHttpAdapter() as ServiceAdapter<unknown, unknown>;
  }
}

export function gatewayOptionsFromFile(
  gateway?: GatewayFileConfig | null,
  defaults: Partial<GatewayServerOptions> = {}
): GatewayServerOptions {
  return {
    host: gateway?.host ?? defaults.host ?? '127.0.0.1',
    port: gateway?.port ?? defaults.port ?? 8080,
    apiKey:
      gateway?.apiKey === null
        ? undefined
        : (gateway?.apiKey ?? defaults.apiKey ?? 'factioner-secret-key-prod'),
    allowUnauthenticated: defaults.allowUnauthenticated,
    corsOrigins: defaults.corsOrigins,
    maxBodyBytes: gateway?.maxBodyBytes ?? defaults.maxBodyBytes ?? 10 * 1024 * 1024,
    spillDir: gateway?.spillDir ?? defaults.spillDir,
    spillThresholdBytes: gateway?.spillThresholdBytes ?? defaults.spillThresholdBytes
  };
}

function defaultDemoPoolOptions(): ServicePoolOptions {
  return {
    name: 'llm-prod-pool',
    serviceType: 'llm',
    routingStrategy: 'least_loaded',
    backpressure: {
      maxQueueSize: 50,
      admissionPolicy: 'drop_oldest'
    },
    circuitBreaker: {
      failureThresholdPercentage: 50,
      coolingOffDurationMs: 3000,
      halfOpenTrialSuccesses: 2
    },
    backoff: {
      strategy: 'full_jitter',
      initialDelayMs: 100,
      maxDelayMs: 2000
    },
    endpoints: [
      {
        id: 'azure-openai-eastus',
        url: 'https://eastus.api.cognitive.microsoft.com/openai/deployments/gpt-4o',
        maxConcurrency: 3,
        budget: { rpm: 600, tpm: 20000 },
        priorityTier: 0,
        region: 'eastus',
        credential: { type: 'api_key', value: 'sk-azure-secret-key-12345' }
      },
      {
        id: 'openai-direct-primary',
        url: 'https://api.openai.com/v1/chat/completions',
        maxConcurrency: 3,
        budget: { rpm: 600, tpm: 20000 },
        priorityTier: 0,
        credential: { type: 'bearer', value: 'sk-proj-live-token-secret-99999' }
      },
      {
        id: 'gemini-fallback',
        url: 'https://generativelanguage.googleapis.com/v1beta/models/gemini-pro',
        maxConcurrency: 5,
        budget: { rpm: 1200, tpm: 40000 },
        priorityTier: 1,
        region: 'global',
        credential: { type: 'api_key', value: 'AIzaSyA-secret-gemini-key' }
      }
    ]
  };
}

/**
 * Load config (or use provided), create adapters by serviceType, register all pools
 * and tenants, and return gateway options derived from the file.
 */
export function bootstrapFromConfig(options: BootstrapOptions = {}): BootstrapResult {
  const useMock =
    options.useMock ??
    (process.env.FACTIONER_USE_MOCK === '1' || process.env.FACTION_USE_MOCK === '1');
  const loaded = options.loaded ?? loadFactionConfig();
  const registry = options.registry ?? PoolRegistry.getInstance();
  const pools: ServicePool<unknown, unknown>[] = [];

  const filePools = loaded.file.pools?.length
    ? loaded.file.pools
    : [undefined];

  for (const filePool of filePools) {
    const poolOpts = filePool
      ? poolOptionsFromFile(filePool, {
          routingStrategy: 'least_loaded',
          backoff: { strategy: 'full_jitter', initialDelayMs: 100, maxDelayMs: 2000 }
        })
      : defaultDemoPoolOptions();

    const adapter = createAdapterForServiceType(poolOpts.serviceType, useMock);
    const pool = new ServicePool(poolOpts, adapter);
    if (!registry.get(pool.name)) {
      registry.register(pool);
    }
    pools.push(pool);
  }

  const tenants = loaded.file.tenants ?? [
    { id: 'tenant-enterprise-gold', maxRps: 30, maxConcurrency: 10 },
    { id: 'tenant-free-tier', maxRps: 3, maxConcurrency: 1, maxAllowedPriority: JobPriority.HIGH }
  ];
  for (const pool of pools) {
    for (const t of tenants) {
      pool.registerTenant(t);
    }
  }

  return {
    registry,
    pools,
    gatewayOptions: gatewayOptionsFromFile(loaded.file.gateway),
    sourcePath: loaded.sourcePath,
    useMock
  };
}
