/**
 * Factioner: Universal Adaptive Service Resource Pooling, Backpressure & Resilience Engine
 */

export * from './types/index.js';
export * from './pool/service-pool.js';
export * from './pool/endpoint-instance.js';
export * from './pool/pool-registry.js';

export * from './routing/router.js';

export * from './backpressure/priority-queue.js';
export * from './backpressure/admission-controller.js';
export * from './backpressure/rate-limiter.js';
export * from './backpressure/resource-budget.js';

export * from './resilience/backoff.js';
export * from './resilience/circuit-breaker.js';
export * from './resilience/retry-manager.js';

export * from './security/credential-vault.js';
export * from './security/tenant-governor.js';
export * from './security/pii-guard.js';

export * from './adapters/base-adapter.js';
export * from './adapters/llm-adapter.js';
export * from './adapters/doc-intelligence-adapter.js';
export * from './adapters/generic-http-adapter.js';
export * from './adapters/embedding-adapter.js';
export * from './adapters/mock-adapter.js';

export * from './telemetry/metrics.js';
export * from './server/gateway-server.js';
export * from './config/load-config.js';
export * from './utils/tokenizer.js';
export * from './spill/blob-spill.js';
export * from './bootstrap.js';

import { ServicePool } from './pool/service-pool.js';
import { ServicePoolOptions } from './types/index.js';
import { ServiceAdapter } from './adapters/base-adapter.js';

/**
 * Convenient factory function to create a typed ServicePool
 */
export function createServicePool<TReq = unknown, TRes = unknown>(
  options: ServicePoolOptions,
  adapter: ServiceAdapter<TReq, TRes>
): ServicePool<TReq, TRes> {
  return new ServicePool(options, adapter);
}

