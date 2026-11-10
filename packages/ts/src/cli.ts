#!/usr/bin/env node
import { createServicePool, loadFactionConfig, poolOptionsFromFile } from './index.js';
import { MockServiceAdapter } from './adapters/mock-adapter.js';
import { JobPriority } from './types/index.js';
import { GatewayServer } from './server/gateway-server.js';
import { PoolRegistry } from './pool/pool-registry.js';

async function runDemo() {
  console.log('='.repeat(70));
  console.log('   FACTIONER: Universal Service Resource Pool & Resilience Engine');
  console.log('='.repeat(70));

  // Capacity precedence: runtime signals → FACTIONER_CONFIG/env → in-code defaults
  const loaded = loadFactionConfig();
  if (loaded.sourcePath) {
    console.log(`\n[config] Loaded FACTIONER_CONFIG from ${loaded.sourcePath}`);
  }

  // 1. Setup an LLM Pool with 3 endpoints across regions & providers
  console.log('\n[1] Initializing LLM Service Pool with Multi-Region Endpoints...');
  const llmAdapter = new MockServiceAdapter<{ prompt: string }, any>({
    baseLatencyMs: 30,
    jitterMs: 10,
    failureRatePercentage: 15
  });

  const filePool = loaded.file.pools?.[0];
  const llmPool = createServicePool<{ prompt: string }, any>(
    filePool
      ? poolOptionsFromFile(filePool, {
          routingStrategy: 'least_loaded',
          backoff: { strategy: 'full_jitter', initialDelayMs: 100, maxDelayMs: 2000 }
        })
      : {
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
        },
    llmAdapter
  );

  PoolRegistry.getInstance().register(llmPool);

  const tenants = loaded.file.tenants ?? [
    { id: 'tenant-enterprise-gold', maxRps: 30, maxConcurrency: 10 },
    { id: 'tenant-free-tier', maxRps: 3, maxConcurrency: 1, maxAllowedPriority: JobPriority.HIGH }
  ];
  for (const t of tenants) {
    llmPool.registerTenant(t);
  }

  // Listen to pool telemetry events
  llmPool.on('jobRetry', (info: any) => {
    console.log(`  [Retry & Backoff] Job ${info.jobId.slice(0, 8)} failed on '${info.failedEndpointId}'. Retry #${info.attempts} backing off for ${info.delayMs}ms...`);
  });

  // 2. Simulate high-concurrency burst traffic
  console.log('\n[2] Simulating high-concurrency burst traffic (50 concurrent jobs)...');
  const jobPromises: Promise<any>[] = [];

  for (let i = 0; i < 50; i++) {
    const priority = i % 10 === 0 ? JobPriority.CRITICAL : i % 3 === 0 ? JobPriority.HIGH : JobPriority.NORMAL;
    const tenantId = i % 2 === 0 ? 'tenant-enterprise-gold' : 'tenant-free-tier';

    const p = llmPool.submit(
      { prompt: `Document summarization request #${i + 1}` },
      { priority, tenantId }
    ).catch(err => ({ error: err.message }));

    jobPromises.push(p);
  }

  await Promise.all(jobPromises);

  // 3. Display Real-time Pool Telemetry
  console.log('\n[3] Execution Complete. Inspecting Real-Time Metrics Snapshot:');
  const metrics = PoolRegistry.getInstance().getAllMetrics();
  console.dir(metrics, { depth: null, colors: true });

  console.log('\n[4] Starting Factioner Gateway HTTP Server on port 8080...');
  const server = new GatewayServer({
    port: loaded.file.gateway?.port ?? 8080,
    host: loaded.file.gateway?.host ?? '127.0.0.1',
    apiKey: loaded.file.gateway?.apiKey ?? 'factioner-secret-key-prod',
    maxBodyBytes: loaded.file.gateway?.maxBodyBytes ?? 10 * 1024 * 1024
  });
  await server.start();
  console.log('✓ Factioner Gateway Server running at http://127.0.0.1:8080 (Auth Protected)');
  console.log('  - Health check:     GET  /v1/health');
  console.log('  - All Pool Metrics: GET  /v1/metrics (Requires Authorization: Bearer factioner-secret-key-prod)');
  console.log('  - Execute in Pool:  POST /v1/pools/llm-prod-pool/execute');
  console.log('  - Stream in Pool:   POST /v1/pools/llm-prod-pool/stream');

  setTimeout(async () => {
    console.log('\n[5] Draining pools and shutting down server cleanly...');
    await server.stop();
    await PoolRegistry.getInstance().drainAll();
    console.log('✓ All pools drained and gateway server stopped gracefully.');
    process.exit(0);
  }, 2000);
}

runDemo().catch(err => {
  console.error('Fatal CLI Error:', err);
  process.exit(1);
});
