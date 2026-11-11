#!/usr/bin/env node
import { bootstrapFromConfig, GatewayServer, JobPriority, PoolRegistry } from './index.js';

async function runDemo() {
  console.log('='.repeat(70));
  console.log('   FACTIONER: Universal Service Resource Pool & Resilience Engine');
  console.log('='.repeat(70));

  // Demo uses mocks so burst traffic does not call real providers.
  // Production: bootstrapFromConfig({ useMock: false }) or omit useMock.
  const boot = bootstrapFromConfig({ useMock: true });
  if (boot.sourcePath) {
    console.log(`\n[config] Loaded FACTIONER_CONFIG from ${boot.sourcePath}`);
  }
  console.log(
    `\n[1] Bootstrapped ${boot.pools.length} pool(s) via serviceType adapter factory (useMock=${boot.useMock})...`
  );

  const llmPool = boot.pools[0];
  llmPool.on('jobRetry', (info: { jobId: string; failedEndpointId: string; attempts: number; delayMs: number }) => {
    console.log(
      `  [Retry & Backoff] Job ${info.jobId.slice(0, 8)} failed on '${info.failedEndpointId}'. Retry #${info.attempts} backing off for ${info.delayMs}ms...`
    );
  });

  console.log('\n[2] Simulating high-concurrency burst traffic (50 concurrent jobs)...');
  const jobPromises: Promise<unknown>[] = [];
  for (let i = 0; i < 50; i++) {
    const priority = i % 10 === 0 ? JobPriority.CRITICAL : i % 3 === 0 ? JobPriority.HIGH : JobPriority.NORMAL;
    const tenantId = i % 2 === 0 ? 'tenant-enterprise-gold' : 'tenant-free-tier';
    jobPromises.push(
      llmPool
        .submit({ prompt: `Document summarization request #${i + 1}` }, { priority, tenantId })
        .catch((err: Error) => ({ error: err.message }))
    );
  }
  await Promise.all(jobPromises);

  console.log('\n[3] Execution Complete. Inspecting Real-Time Metrics Snapshot:');
  console.dir(PoolRegistry.getInstance().getAllMetrics(), { depth: null, colors: true });

  console.log('\n[4] Starting Factioner Gateway HTTP Server...');
  const server = new GatewayServer(boot.gatewayOptions);
  await server.start();
  const host = boot.gatewayOptions.host ?? '127.0.0.1';
  const port = boot.gatewayOptions.port ?? 8080;
  console.log(`✓ Factioner Gateway Server running at http://${host}:${port}`);
  console.log('  - Health check:     GET  /v1/health');
  console.log(`  - All Pool Metrics: GET  /v1/metrics`);
  for (const pool of boot.pools) {
    console.log(`  - Execute:          POST /v1/pools/${pool.name}/execute`);
  }

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
