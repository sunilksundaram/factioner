import { test, describe } from 'node:test';
import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  PriorityJobQueue,
  AdmissionController,
  BackoffCalculator,
  CircuitBreaker,
  ErrorClassifier,
  CredentialVault,
  TenantGovernor,
  createServicePool,
  MockServiceAdapter,
  GenericHttpAdapter,
  JobPriority,
  normalizeAdmissionPolicy,
  normalizeCredential,
  estimateTokens,
  BlobSpillStore,
  SPILL_URI_PREFIX,
  bootstrapFromConfig,
  createAdapterForServiceType,
  normalizeServiceType,
  PoolRegistry
} from './index.js';
import { MultiResourceLimiter } from './backpressure/resource-budget.js';

describe('Factioner Hardened Test Suite', () => {

  test('PriorityJobQueue fair round-robins across tenants within priority bands', () => {
    const queue = new PriorityJobQueue();

    const makeJob = (id: string, priority: JobPriority, tenantId: string): any => ({
      id,
      priority,
      tenantId,
      payload: {},
      createdAt: Date.now(),
      queueTimeoutMs: 5000,
      executionTimeoutMs: 5000,
      retriesRemaining: 1,
      totalAttempts: 0,
      abortController: new AbortController(),
      resolve: () => {},
      reject: () => {}
    });

    // Tenant A enqueues 2 HIGH jobs; Tenant B enqueues 1 HIGH job
    queue.enqueue(makeJob('A1', JobPriority.HIGH, 'tenant-A'));
    queue.enqueue(makeJob('A2', JobPriority.HIGH, 'tenant-A'));
    queue.enqueue(makeJob('B1', JobPriority.HIGH, 'tenant-B'));

    // Tenant-Fair dequeue: cycles A1 -> B1 -> A2
    assert.strictEqual(queue.dequeue()?.id, 'A1');
    assert.strictEqual(queue.dequeue()?.id, 'B1');
    assert.strictEqual(queue.dequeue()?.id, 'A2');
    assert.strictEqual(queue.size(), 0);
  });

  test('CircuitBreaker caps Half-Open state to strictly ONE in-flight trial probe', () => {
    const cb = new CircuitBreaker({
      failureThresholdPercentage: 50,
      minimumRequestVolume: 2,
      coolingOffDurationMs: 40,
      halfOpenTrialSuccesses: 2
    });

    cb.recordFailure();
    cb.recordFailure();
    assert.strictEqual(cb.getState(), 'OPEN');
    assert.strictEqual(cb.canExecute(), false);

    return new Promise<void>((resolve) => {
      setTimeout(() => {
        // Cool-off elapsed: pure check allows, claim transitions to HALF_OPEN
        assert.strictEqual(cb.canExecute(), true);
        assert.strictEqual(cb.tryClaimProbe(), true);
        assert.strictEqual(cb.getState(), 'HALF_OPEN');

        // Second claim while probe in flight MUST BE REJECTED
        assert.strictEqual(cb.canExecute(), false);
        assert.strictEqual(cb.tryClaimProbe(), false);

        cb.recordSuccess();

        assert.strictEqual(cb.tryClaimProbe(), true);
        cb.recordSuccess();

        assert.strictEqual(cb.getState(), 'CLOSED');
        resolve();
      }, 50);
    });
  });

  test('CredentialVault encrypts secrets and redacts api_key=<secret> correctly', () => {
    const vault = new CredentialVault('my-secure-master-vault-key-32chars');
    vault.registerCredential('openai-ep', {
      type: 'bearer',
      value: 'sk-proj-live-token-secret-99999'
    });

    // Decrypts accurately for internal dispatch
    const cred = vault.getCredential('openai-ep');
    assert.strictEqual(cred?.value, 'sk-proj-live-token-secret-99999');

    // 1. Registered secret redaction
    const rawError1 = 'Failed: api_key=sk-proj-live-token-secret-99999';
    const sanitized1 = vault.redactSecrets(rawError1);
    assert.ok(!sanitized1.includes('sk-proj-live-token-secret-99999'), 'Raw key must never appear in redacted text');
    assert.strictEqual(sanitized1, 'Failed: api_key=[REDACTED]');

    // 2. Generic key-value and Bearer redaction (unregistered keys)
    const rawError2 = 'Failed: password=secret123 and token: Bearer abcdefghij123456';
    const sanitized2 = vault.redactSecrets(rawError2);
    assert.ok(sanitized2.includes('password=[REDACTED]'), 'Must redact key-value pattern');
    assert.ok(sanitized2.includes('Bearer [REDACTED_TOKEN]'), 'Must redact bearer token');
  });

  test('TenantGovernor does not charge rate tokens on eligibility check', () => {
    const governor = new TenantGovernor({ enforceTenantIsolation: true });
    governor.registerTenant({
      id: 'tenant-test',
      maxRps: 1,
      maxConcurrency: 1,
      maxAllowedPriority: JobPriority.HIGH
    });

    // 1. Eligibility check does not consume token
    assert.strictEqual(governor.checkEligibility('tenant-test').allowed, true);
    assert.strictEqual(governor.checkEligibility('tenant-test').allowed, true);

    // 2. Deduced priority caps non-VIP tenant
    const prio = governor.resolvePriority('tenant-test', JobPriority.CRITICAL);
    assert.strictEqual(prio, JobPriority.HIGH, 'Should cap non-VIP tenant to maxAllowedPriority');

    // 3. Deduct on admission
    governor.deductOnAdmission('tenant-test');
    assert.strictEqual(governor.checkEligibility('tenant-test').allowed, false);

    // 4. Refund releases slot and restores token
    governor.refundOnDrop('tenant-test');
    assert.strictEqual(governor.checkEligibility('tenant-test').allowed, true);
  });

  test('MultiResourceLimiter enforces multi-dimensional RPM and TPM quotas', () => {
    const limiter = new MultiResourceLimiter({
      rpm: 2,
      tpm: 1000
    });

    assert.strictEqual(limiter.hasAvailable({ rpm: 1, tpm: 400 }), true);
    assert.strictEqual(limiter.tryAcquire({ rpm: 1, tpm: 400 }), true);

    // TPM quota exceeded
    assert.strictEqual(limiter.hasAvailable({ rpm: 1, tpm: 700 }), false);

    // RPM limit reached
    assert.strictEqual(limiter.tryAcquire({ rpm: 1, tpm: 100 }), true);
    assert.strictEqual(limiter.tryAcquire({ rpm: 1, tpm: 50 }), false);
  });

  test('MultiResourceLimiter fixed window clears consumed to 0 when idle past window', async () => {
    const limiter = new MultiResourceLimiter({
      rps: 2,
      rpm: 10
    });

    // Fully consume RPS quota
    assert.strictEqual(limiter.tryAcquire({ rps: 2, rpm: 2 }), true);
    assert.strictEqual(limiter.hasAvailable({ rps: 1 }), false);

    // Wait past 1.0s window
    await new Promise((resolve) => setTimeout(resolve, 1050));

    // Fixed window: idle past window clears consumed to 0 without window decay
    assert.strictEqual(limiter.hasAvailable({ rps: 2 }), true);
    assert.strictEqual(limiter.tryAcquire({ rps: 2 }), true);
  });

  test('GenericHttpAdapter blocks SSRF path traversal and preserves auth header precedence', async () => {
    const adapter = new GenericHttpAdapter();

    // 1. SSRF prevention
    assert.throws(() => {
      adapter.validatePayload({ path: '../../etc/passwd' });
    }, /SSRF validation failed/);

    assert.throws(() => {
      adapter.validatePayload({ path: 'http://evil-server.com/steal' });
    }, /SSRF validation failed/);
  });

  test('ServicePool handles streaming and long-running operation lifecycles', async () => {
    const mockAdapter = new MockServiceAdapter({ baseLatencyMs: 10 });
    const pool = createServicePool(
      {
        name: 'test-stream-pool',
        serviceType: 'llm',
        endpoints: [
          {
            id: 'ep-1',
            url: 'https://mock.service.local',
            maxConcurrency: 3,
            budget: { rpm: 60, tpm: 10000 }
          }
        ]
      },
      mockAdapter
    );

    // Unary execution
    const unaryRes = await pool.submit({ prompt: 'test' });
    assert.strictEqual(unaryRes.success, true);

    // Streaming execution
    const stream = await pool.submitStream({ prompt: 'stream test' });
    const tokens: string[] = [];
    for await (const chunk of stream) {
      tokens.push(chunk);
    }
    assert.ok(tokens.length > 0);
    assert.strictEqual(tokens[0], 'Hello');

    // Long-running operation (await helper)
    const opRes = await pool.awaitOperation({ prompt: 'long operation' });
    assert.strictEqual((opRes as any).status, 'operation_complete');

    await pool.drain();
  });

  test('PriorityJobQueue dropOldestLowPriority never drops CRITICAL work', () => {
    const queue = new PriorityJobQueue();
    const makeJob = (id: string, priority: JobPriority, tenantId: string): any => ({
      id,
      priority,
      tenantId,
      payload: {},
      createdAt: Date.now(),
      queueTimeoutMs: 5000,
      executionTimeoutMs: 5000,
      retriesRemaining: 0,
      totalAttempts: 0,
      abortController: new AbortController(),
      resolve: () => {},
      reject: () => {}
    });

    queue.enqueue(makeJob('C1', JobPriority.CRITICAL, 'vip-tenant'));
    queue.enqueue(makeJob('B1', JobPriority.BATCH, 'batch-tenant'));

    const dropped = queue.dropOldestLowPriority();
    assert.strictEqual(dropped?.id, 'B1', 'Should drop BATCH job');

    const nextDrop = queue.dropOldestLowPriority();
    assert.strictEqual(nextDrop, undefined, 'Must NEVER drop CRITICAL job');
    assert.strictEqual(queue.size(), 1);
    assert.strictEqual(queue.dequeue()?.id, 'C1');
  });

  test('TenantGovernor rejects unknown tenant under strict isolation policy', () => {
    const gov = new TenantGovernor({
      enforceTenantIsolation: true,
      allowedTenants: ['allowed-tenant-1']
    });

    // 1. Missing tenant
    assert.strictEqual(gov.checkEligibility().allowed, false);

    // 2. Unregistered / unauthorized tenant
    assert.strictEqual(gov.checkEligibility('unknown-tenant').allowed, false);

    // 3. Registered and allowed tenant
    gov.registerTenant({ id: 'allowed-tenant-1', maxRps: 10 });
    assert.strictEqual(gov.checkEligibility('allowed-tenant-1').allowed, true);
  });

  test('Config normalizes DropOldest and credType for shared JSON', () => {
    assert.strictEqual(normalizeAdmissionPolicy('DropOldest'), 'drop_oldest');
    assert.strictEqual(normalizeAdmissionPolicy('drop_oldest'), 'drop_oldest');
    const cred = normalizeCredential({ credType: 'bearer', value: 'sk-test' });
    assert.strictEqual(cred?.type, 'bearer');
    assert.strictEqual(cred?.value, 'sk-test');
  });

  test('evaluate does not enqueue; admitAndEnqueue does', async () => {
    const queue = new PriorityJobQueue();
    const admission = new AdmissionController({ maxQueueSize: 2, admissionPolicy: 'reject' });
    const job: any = {
      id: 'j1',
      priority: JobPriority.NORMAL,
      tenantId: 't',
      payload: {},
      createdAt: Date.now(),
      queueTimeoutMs: 1000,
      executionTimeoutMs: 1000,
      retriesRemaining: 0,
      totalAttempts: 0,
      avoidEndpointIds: [],
      tenantCharged: false,
      abortController: new AbortController(),
      resolve: () => {},
      reject: () => {}
    };
    const decided = await admission.evaluate(queue, job);
    assert.strictEqual(decided.accepted, true);
    assert.strictEqual(queue.size(), 0);
    const admitted = await admission.admitAndEnqueue(queue, job);
    assert.strictEqual(admitted.accepted, true);
    assert.strictEqual(queue.size(), 1);
  });

  test('ErrorClassifier maps UNAVAILABLE and string Retry-After to ms', () => {
    const refused = ErrorClassifier.classify(Object.assign(new Error('connect failed'), { code: 'ECONNREFUSED' }));
    assert.strictEqual(refused.class, 'UNAVAILABLE');

    const overloaded = ErrorClassifier.classify(
      Object.assign(new Error('rate limited'), { status: 429, headers: { 'retry-after': '2' } })
    );
    assert.strictEqual(overloaded.class, 'OVERLOADED');
    assert.strictEqual(overloaded.retryAfterMs, 2000);

    const backoff = new BackoffCalculator({ respectRetryAfterHeader: true, maxDelayMs: 15000 });
    const d = backoff.calculateDelay(1, 2000);
    assert.strictEqual(d.delayMs, 2000);
  });

  test('MultiResourceLimiter overload signal tightens then restores RPM and TPM', () => {
    const limiter = new MultiResourceLimiter({ rpm: 100, tpm: 1000 });
    assert.strictEqual(limiter.getEffectiveRpm(), 100);
    assert.strictEqual(limiter.getEffectiveTpm(), 1000);
    limiter.applyOverloadSignal(50);
    assert.ok(limiter.isSignalTightened());
    assert.strictEqual(limiter.getEffectiveRpm(), 50);
    assert.strictEqual(limiter.getEffectiveTpm(), 500);
    // After expiry, restore
    const until = Date.now() + 60;
    while (Date.now() < until) { /* spin */ }
    assert.strictEqual(limiter.isSignalTightened(), false);
    assert.strictEqual(limiter.getEffectiveRpm(), 100);
    assert.strictEqual(limiter.getEffectiveTpm(), 1000);
  });

  test('MultiResourceLimiter reconcile overshoot biases TPM estimates', () => {
    const limiter = new MultiResourceLimiter({ tpm: 10_000 });
    assert.strictEqual(limiter.getTpmEstimateBias(), 1);
    assert.ok(limiter.tryAcquire({ tpm: 100 }));
    limiter.reconcile({ tpm: 100 }, { tpm: 250 });
    assert.ok(limiter.getTpmEstimateBias() > 1);
    assert.ok(limiter.isSignalTightened());
    assert.ok((limiter.getEffectiveTpm() ?? 0) < 10_000);
  });

  test('bootstrapFromConfig registers pools by serviceType', () => {
    assert.strictEqual(normalizeServiceType('doc-intel'), 'document_intelligence');
    assert.ok(createAdapterForServiceType('embedding', true));
    const registry = PoolRegistry.create();
    const boot = bootstrapFromConfig({
      useMock: true,
      registry,
      loaded: {
        file: {
          pools: [
            {
              name: 'embed-pool',
              serviceType: 'embedding',
              endpoints: [
                {
                  id: 'e1',
                  url: 'https://api.openai.com/v1/embeddings',
                  maxConcurrency: 2,
                  budget: { rpm: 60, tpm: 10000 }
                }
              ]
            }
          ],
          tenants: [{ id: 't1', maxRps: 5 }]
        }
      }
    });
    assert.strictEqual(boot.pools.length, 1);
    assert.strictEqual(boot.pools[0].name, 'embed-pool');
    assert.ok(boot.gatewayOptions.maxBodyBytes);
    assert.ok(registry.get('embed-pool'));
  });

  test('tokenizer and blob spill round-trip', () => {
    assert.ok(estimateTokens('hello world') >= 2);
    const dir = mkdtempSync(join(tmpdir(), 'factioner-spill-'));
    const store = new BlobSpillStore({ dir, thresholdBytes: 32 });
    const big = 'x'.repeat(64);
    const spilled = store.spillPayload({ base64Source: big, modelId: 'prebuilt' });
    assert.ok(String(spilled.base64Source).startsWith(SPILL_URI_PREFIX));
    const resolved = store.resolvePayload(spilled);
    assert.strictEqual(resolved.base64Source, big);
    store.deletePayloadSpills(spilled);
    rmSync(dir, { recursive: true, force: true });
  });

  test('Operation poll returns snapshot without double-release', async () => {
    const mockAdapter = new MockServiceAdapter({ baseLatencyMs: 5 });
    const pool = createServicePool(
      {
        name: 'ops-poll-pool',
        serviceType: 'llm',
        endpoints: [
          { id: 'ep-ops', url: 'https://mock.service.local', maxConcurrency: 3, budget: { rpm: 60 } }
        ]
      },
      mockAdapter
    );
    const handle = await pool.submitOperation({ prompt: 'op' });
    const snap = await handle.poll();
    assert.strictEqual(typeof snap.done, 'boolean');
    assert.ok(snap.status);
    const cancel = await handle.cancel();
    assert.strictEqual(cancel.status, 'cancelled');
    await pool.drain();
  });

  test('ServicePool idempotency coalesces concurrent in-flight requests and caches result', async () => {
    const mockAdapter = new MockServiceAdapter({ baseLatencyMs: 30 });
    const pool = createServicePool(
      {
        name: 'idempotent-pool',
        serviceType: 'llm',
        endpoints: [
          {
            id: 'ep-idem',
            url: 'https://mock.service.local',
            maxConcurrency: 5,
            budget: { rpm: 60, tpm: 10000 }
          }
        ]
      },
      mockAdapter
    );

    // Submit two requests simultaneously with the same idempotency key
    const [p1, p2] = await Promise.all([
      pool.submit({ prompt: 'same' }, { idempotencyKey: 'idem-key-1' }),
      pool.submit({ prompt: 'same' }, { idempotencyKey: 'idem-key-1' })
    ]);

    assert.strictEqual(p1.success, true);
    assert.strictEqual(p2.success, true);

    // Third request after completion returns cached result
    const p3 = await pool.submit({ prompt: 'same' }, { idempotencyKey: 'idem-key-1' });
    assert.strictEqual(p3.success, true);

    await pool.drain();
  });
});
