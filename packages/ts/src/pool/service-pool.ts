import { EventEmitter } from 'node:events';
import { randomUUID } from 'node:crypto';
import {
  ServicePoolOptions,
  Job,
  JobPriority,
  JobResult,
  PoolMetrics,
  EndpointConfig,
  SubmitOptions,
  FailureClass,
  BudgetDeduction,
  CircuitBreakerConfig,
  LongRunningOperationHandle,
  OperationPollSnapshot
} from '../types/index.js';
import { EndpointInstance } from './endpoint-instance.js';
import { IPoolRouter, RouterFactory } from '../routing/router.js';
import { PriorityJobQueue } from '../backpressure/priority-queue.js';
import { AdmissionController } from '../backpressure/admission-controller.js';
import { BackoffCalculator } from '../resilience/backoff.js';
import { CredentialVault } from '../security/credential-vault.js';
import { TenantGovernor } from '../security/tenant-governor.js';
import { PIIGuard } from '../security/pii-guard.js';
import { ServiceAdapter } from '../adapters/base-adapter.js';
import { PoolTelemetry } from '../telemetry/metrics.js';

interface IdempotencyEntry<TRes> {
  promise?: Promise<JobResult<TRes>>;
  result?: JobResult<TRes>;
  expiresAt: number;
}

/**
 * ServicePool coordinates multi-dimensional resource budgets, tenant-fair priority queuing,
 * circuit breakers, secret redaction, and Unary / Streaming / Long-Running lifecycles.
 */
export class ServicePool<TReq = unknown, TRes = unknown> extends EventEmitter {
  public readonly name: string;
  public readonly serviceType: string;

  private readonly endpoints: Map<string, EndpointInstance> = new Map();
  private readonly router: IPoolRouter;
  private readonly queue: PriorityJobQueue<TReq, TRes> = new PriorityJobQueue();
  private readonly admissionController: AdmissionController;
  private readonly backoffCalculator: BackoffCalculator;
  private readonly credentialVault: CredentialVault;
  private readonly tenantGovernor: TenantGovernor;
  private readonly adapter: ServiceAdapter<TReq, TRes>;
  private readonly telemetry: PoolTelemetry;
  private readonly circuitBreakerConfig?: Partial<CircuitBreakerConfig>;
  private readonly enablePIIRedaction: boolean;
  private readonly idempotency: Map<string, IdempotencyEntry<TRes>> = new Map();
  private readonly operations: Map<string, { endpointId: string; operationId: string }> = new Map();

  private readonly maxRetries: number;
  private activeJobsCount: number = 0;
  private readonly recentDispatchOutcomes: boolean[] = []; // true = retry
  private isDraining: boolean = false;
  private healthCheckTimer?: NodeJS.Timeout;
  private coolOffWakeTimer?: NodeJS.Timeout;

  constructor(options: ServicePoolOptions, adapter: ServiceAdapter<TReq, TRes>) {
    super();
    this.name = options.name;
    this.serviceType = options.serviceType;
    this.adapter = adapter;
    this.maxRetries = options.maxRetries ?? 3;
    this.circuitBreakerConfig = options.circuitBreaker;
    this.enablePIIRedaction = options.security?.enablePIIRedaction ?? false;

    this.admissionController = new AdmissionController(options.backpressure);
    this.backoffCalculator = new BackoffCalculator(options.backoff);
    this.credentialVault = new CredentialVault(options.security?.credentialVaultEncryptionKey);
    this.tenantGovernor = new TenantGovernor(options.security);
    this.telemetry = new PoolTelemetry(this.name, this.serviceType);
    this.router = RouterFactory.create(options.routingStrategy ?? 'least_loaded');

    for (const epConfig of options.endpoints) {
      this.addEndpoint(epConfig);
    }

    if (options.healthCheckIntervalMs && options.healthCheckIntervalMs > 0) {
      this.healthCheckTimer = setInterval(() => this.runHealthChecks(), options.healthCheckIntervalMs);
    }
  }

  public addEndpoint(config: EndpointConfig): void {
    const instance = new EndpointInstance(config, this.circuitBreakerConfig);
    this.endpoints.set(config.id, instance);
    if (config.credential) {
      this.credentialVault.registerCredential(config.id, config.credential);
    }
    this.emit('endpointAdded', { endpointId: config.id, url: config.url });
    this.scheduleDispatch();
  }

  public async removeEndpoint(endpointId: string, waitMs: number = 5000): Promise<boolean> {
    const ep = this.endpoints.get(endpointId);
    if (!ep) return false;

    // Immediately mark unhealthy to block new dispatches while active leases drain
    ep.setHealth('unhealthy');

    const start = Date.now();
    while (ep.getActiveLeases() > 0 && Date.now() - start < waitMs) {
      await new Promise(r => setTimeout(r, 50));
    }

    this.endpoints.delete(endpointId);
    this.credentialVault.removeCredential(endpointId);
    this.emit('endpointRemoved', { endpointId });
    return true;
  }

  private resolveEstimate(payload: TReq, options: SubmitOptions): BudgetDeduction {
    if (options.estimatedBudget) return options.estimatedBudget;
    if (this.adapter.estimateBudget) return this.adapter.estimateBudget(payload);
    return { rpm: 1 };
  }

  private hasRetryBudget(): boolean {
    const pct = this.admissionController.getConfig().globalRetryBudgetPercentage ?? 20;
    const window = this.recentDispatchOutcomes.slice(-100);
    if (window.length === 0) return true;
    const retries = window.filter(Boolean).length;
    return (retries / window.length) * 100 <= pct;
  }

  private recordDispatchOutcome(wasRetry: boolean): void {
    this.recentDispatchOutcomes.push(wasRetry);
    if (this.recentDispatchOutcomes.length > 100) {
      this.recentDispatchOutcomes.shift();
    }
  }

  public async submit(payload: TReq, options: SubmitOptions = {}): Promise<JobResult<TRes>> {
    if (this.isDraining) {
      throw new Error(`ServicePool '${this.name}' is draining and not accepting new jobs`);
    }

    // Atomic idempotency claim: first writer inserts a placeholder; others await it.
    let idemPlaceholder: IdempotencyEntry<TRes> | undefined;
    if (options.idempotencyKey) {
      const existing = this.idempotency.get(options.idempotencyKey);
      if (existing) {
        if (existing.result && existing.expiresAt > Date.now()) return existing.result;
        if (existing.promise) return existing.promise;
      }
      idemPlaceholder = { expiresAt: Date.now() + 60_000 };
      this.idempotency.set(options.idempotencyKey, idemPlaceholder);
    }

    let processedPayload = payload;
    try {
      if (this.enablePIIRedaction) {
        processedPayload = PIIGuard.sanitizePayload(payload);
      }

      if (this.adapter.validatePayload) {
        this.adapter.validatePayload(processedPayload);
      }
    } catch (err) {
      if (options.idempotencyKey) this.idempotency.delete(options.idempotencyKey);
      throw err;
    }

    const estimate = this.resolveEstimate(processedPayload, options);
    const tenantCheck = this.tenantGovernor.checkEligibility(options.tenantId, estimate);
    if (!tenantCheck.allowed) {
      if (options.idempotencyKey) this.idempotency.delete(options.idempotencyKey);
      this.telemetry.recordRejection();
      const err: Error & { code?: string } = new Error(`Tenant admission denied: ${tenantCheck.reason}`);
      err.code = 'TENANT_DENIED';
      throw err;
    }

    const resolvedPriority = this.tenantGovernor.resolvePriority(
      options.tenantId,
      options.priority ?? JobPriority.NORMAL
    );

    const promise = new Promise<JobResult<TRes>>(async (resolve, reject) => {
      const abortController = new AbortController();
      const bpConfig = this.admissionController.getConfig();
      const queueTimeoutMs = options.queueTimeoutMs ?? bpConfig.queueTimeoutMs;
      const executionTimeoutMs = options.executionTimeoutMs ?? bpConfig.executionTimeoutMs;

      let queueTimer: NodeJS.Timeout | undefined;
      const clearQueueTimer = () => {
        if (queueTimer) {
          clearTimeout(queueTimer);
          queueTimer = undefined;
        }
      };

      const job: Job<TReq, TRes> = {
        id: options.idempotencyKey ?? randomUUID(),
        idempotencyKey: options.idempotencyKey,
        priority: resolvedPriority,
        payload: processedPayload,
        tenantId: options.tenantId,
        createdAt: Date.now(),
        queueTimeoutMs,
        executionTimeoutMs,
        estimatedBudget: estimate,
        retriesRemaining: options.maxRetries ?? this.maxRetries,
        totalAttempts: 0,
        avoidEndpointIds: [],
        tenantCharged: false,
        clearQueueTimer,
        abortController,
        metadata: options.metadata,
        resolve: (result) => {
          clearQueueTimer();
          if (job.tenantCharged) {
            this.tenantGovernor.releaseSlot(options.tenantId);
            job.tenantCharged = false;
          }
          if (options.idempotencyKey) {
            this.idempotency.set(options.idempotencyKey, {
              result,
              expiresAt: Date.now() + 60_000
            });
          }
          resolve(result);
        },
        reject: (err) => {
          clearQueueTimer();
          if (options.idempotencyKey) {
            this.idempotency.delete(options.idempotencyKey);
          }
          if (job.tenantCharged) {
            this.tenantGovernor.releaseSlot(options.tenantId);
            job.tenantCharged = false;
          }
          const rawMsg = err instanceof Error ? err.message : String(err);
          reject(new Error(this.credentialVault.redactSecrets(rawMsg)));
        }
      };

      queueTimer = setTimeout(() => {
        if (!abortController.signal.aborted) {
          abortController.abort();
          if (job.tenantCharged) {
            this.tenantGovernor.refundOnDrop(options.tenantId, estimate);
            job.tenantCharged = false;
          }
          reject(new Error(`Job timed out after ${queueTimeoutMs}ms waiting in queue of pool '${this.name}'`));
        }
      }, queueTimeoutMs);

      const decision = await this.admissionController.admitAndEnqueue(this.queue, job);
      if (!decision.accepted) {
        clearQueueTimer();
        this.telemetry.recordRejection();
        const err: Error & { code?: string } = new Error(decision.reason ?? 'Request rejected by admission controller');
        err.code = decision.code ?? 'QUEUE_FULL';
        reject(err);
        return;
      }

      if (decision.rejectedJob) {
        const dropped = decision.rejectedJob;
        if (dropped.tenantCharged) {
          this.tenantGovernor.refundOnDrop(dropped.tenantId, dropped.estimatedBudget);
          dropped.tenantCharged = false;
        }
        dropped.reject(new Error('Dropped from queue to make room for higher-priority work'));
      }

      this.tenantGovernor.deductOnAdmission(options.tenantId, estimate);
      job.tenantCharged = true;

      this.emit('jobEnqueued', { jobId: job.id, priority: job.priority, queueSize: this.queue.size() });
      this.scheduleDispatch();
    });

    if (options.idempotencyKey && idemPlaceholder) {
      idemPlaceholder.promise = promise;
      idemPlaceholder.expiresAt = Date.now() + 60_000;
    }

    return promise;
  }

  public async submitStream(
    payload: TReq,
    options: SubmitOptions = {}
  ): Promise<AsyncIterable<string> & { abort: () => void }> {
    if (this.isDraining) {
      throw new Error(`ServicePool '${this.name}' is draining and not accepting new jobs`);
    }
    if (!this.adapter.executeStream) {
      throw new Error(`Adapter '${this.adapter.name}' does not implement streaming`);
    }

    let processedPayload = payload;
    if (this.enablePIIRedaction) {
      processedPayload = PIIGuard.sanitizePayload(payload);
    }

    const estimate = this.resolveEstimate(processedPayload, options);
    const tenantCheck = this.tenantGovernor.checkEligibility(options.tenantId, estimate);
    if (!tenantCheck.allowed) {
      const err: Error & { code?: string } = new Error(`Tenant admission denied: ${tenantCheck.reason}`);
      err.code = 'TENANT_DENIED';
      throw err;
    }

    // Stream bypasses the fair queue; priority is capped for policy consistency only.
    void this.tenantGovernor.resolvePriority(options.tenantId, options.priority ?? JobPriority.NORMAL);
    this.tenantGovernor.deductOnAdmission(options.tenantId, estimate);

    const endpointList = Array.from(this.endpoints.values());
    const selectedEndpoint = this.router.selectEndpoint(endpointList, estimate);
    if (!selectedEndpoint || !selectedEndpoint.acquireSlot(estimate)) {
      this.tenantGovernor.refundOnDrop(options.tenantId, estimate);
      throw new Error(`No available endpoint capacity for streaming in pool '${this.name}'`);
    }

    const authHeaders = this.credentialVault.getAuthHeaders(selectedEndpoint.id);
    const abortController = new AbortController();
    const startTime = Date.now();

    try {
      const { stream, getBudgetDeduction } = await this.adapter.executeStream(
        selectedEndpoint.config,
        processedPayload,
        abortController.signal,
        authHeaders
      );

      const endpoint = selectedEndpoint;
      const self = this;
      let released = false;

      const release = (success: boolean) => {
        if (released) return;
        released = true;
        const durationMs = Date.now() - startTime;
        if (success) {
          endpoint.releaseSlot(true, durationMs, estimate, getBudgetDeduction());
        } else {
          endpoint.releaseSlot(false, durationMs, estimate, undefined, FailureClass.TRANSIENT);
        }
        self.tenantGovernor.releaseSlot(options.tenantId);
        self.scheduleDispatch();
      };

      const iterable = {
        abort: () => {
          abortController.abort();
          release(false);
        },
        async *[Symbol.asyncIterator]() {
          try {
            for await (const chunk of stream) {
              if (abortController.signal.aborted) {
                release(false);
                return;
              }
              yield chunk;
            }
            if (abortController.signal.aborted) {
              release(false);
            } else {
              release(true);
            }
          } catch (err) {
            release(false);
            throw err;
          }
        }
      };

      return iterable;
    } catch (err) {
      selectedEndpoint.releaseSlot(false, Date.now() - startTime, estimate, undefined, FailureClass.TRANSIENT);
      this.tenantGovernor.refundOnDrop(options.tenantId, estimate);
      this.scheduleDispatch();
      throw err;
    }
  }

  public async submitOperation(
    payload: TReq,
    options: SubmitOptions = {}
  ): Promise<LongRunningOperationHandle<TRes>> {
    if (this.isDraining) {
      throw new Error(`ServicePool '${this.name}' is draining and not accepting new jobs`);
    }
    if (!this.adapter.submitOperation || !this.adapter.pollOperation) {
      throw new Error(`Adapter '${this.adapter.name}' does not implement long-running operation protocol`);
    }

    let processedPayload = payload;
    if (this.enablePIIRedaction) {
      processedPayload = PIIGuard.sanitizePayload(payload);
    }

    const estimate = this.resolveEstimate(processedPayload, options);
    const tenantCheck = this.tenantGovernor.checkEligibility(options.tenantId, estimate);
    if (!tenantCheck.allowed) {
      const err: Error & { code?: string } = new Error(`Tenant admission denied: ${tenantCheck.reason}`);
      err.code = 'TENANT_DENIED';
      throw err;
    }
    // Ops bypass the fair queue; priority is capped for policy consistency only.
    void this.tenantGovernor.resolvePriority(options.tenantId, options.priority ?? JobPriority.NORMAL);
    this.tenantGovernor.deductOnAdmission(options.tenantId, estimate);

    const endpointList = Array.from(this.endpoints.values());
    const selectedEndpoint = this.router.selectEndpoint(endpointList, { rpm: 1 });
    if (!selectedEndpoint || !selectedEndpoint.acquireSlot({ rpm: 1 })) {
      this.tenantGovernor.refundOnDrop(options.tenantId, estimate);
      throw new Error(`No capacity available to submit long-running operation in pool '${this.name}'`);
    }

    const authHeaders = this.credentialVault.getAuthHeaders(selectedEndpoint.id);
    const abortController = new AbortController();
    let operationId: string;

    try {
      const submission = await this.adapter.submitOperation(
        selectedEndpoint.config,
        processedPayload,
        abortController.signal,
        authHeaders
      );
      operationId = submission.operationId;
      selectedEndpoint.releaseSlot(true, 50, { rpm: 1 }, submission.initialBudget);
    } catch (err) {
      selectedEndpoint.releaseSlot(false, 0, { rpm: 1 }, undefined, FailureClass.TRANSIENT);
      this.tenantGovernor.refundOnDrop(options.tenantId, estimate);
      this.scheduleDispatch();
      throw err;
    }

    this.scheduleDispatch();
    this.tenantGovernor.releaseSlot(options.tenantId);

    const handleId = randomUUID();
    this.operations.set(handleId, { endpointId: selectedEndpoint.id, operationId });

    const handle: LongRunningOperationHandle<TRes> = {
      operationId: handleId,
      status: 'running',
      poll: async () => {
        const snap = await this.pollOperation(handleId);
        if (snap.done) {
          handle.status = snap.status === 'succeeded' ? 'succeeded' : 'failed';
        }
        return snap;
      },
      cancel: async () => {
        const result = await this.cancelOperation(handleId);
        handle.status = 'cancelled';
        return result;
      }
    };
    return handle;
  }

  /** Poll status of an operation by handle id — single release per attempt. */
  public async pollOperation(handleId: string): Promise<OperationPollSnapshot<TRes>> {
    const entry = this.operations.get(handleId);
    if (!entry) {
      throw new Error(`Operation '${handleId}' not found in pool '${this.name}'`);
    }
    const endpoint = this.endpoints.get(entry.endpointId);
    if (!endpoint) {
      throw new Error(`Endpoint '${entry.endpointId}' not found for operation`);
    }
    const pollBudget = { rpm: 1 };
    if (!endpoint.acquireSlot(pollBudget)) {
      throw new Error('No capacity for poll');
    }
    let released = false;
    const releaseOnce = (success: boolean) => {
      if (released) return;
      released = true;
      if (success) {
        endpoint.releaseSlot(true, 10, pollBudget, pollBudget);
      } else {
        endpoint.releaseSlot(false, 10, pollBudget, undefined, FailureClass.TRANSIENT);
      }
    };
    try {
      const headers = this.credentialVault.getAuthHeaders(endpoint.id);
      const pollRes = await this.adapter.pollOperation!(
        endpoint.config,
        entry.operationId,
        new AbortController().signal,
        headers
      );
      releaseOnce(true);
      if (pollRes.done) {
        this.operations.delete(handleId);
      }
      return {
        done: pollRes.done,
        status: pollRes.status,
        data: pollRes.data
      };
    } catch (err) {
      releaseOnce(false);
      throw err;
    }
  }

  /** Cancel an operation by handle id */
  public async cancelOperation(handleId: string): Promise<{ status: 'cancelled'; upstreamCancelled: boolean }> {
    const entry = this.operations.get(handleId);
    if (!entry) {
      return { status: 'cancelled', upstreamCancelled: false };
    }
    let upstreamCancelled = false;
    const endpoint = this.endpoints.get(entry.endpointId);
    if (endpoint && this.adapter.cancelOperation) {
      try {
        const headers = this.credentialVault.getAuthHeaders(endpoint.id);
        await this.adapter.cancelOperation(endpoint.config, entry.operationId, new AbortController().signal, headers);
        upstreamCancelled = true;
      } catch {
        upstreamCancelled = false;
      }
    }
    this.operations.delete(handleId);
    return { status: 'cancelled', upstreamCancelled };
  }

  /** Convenience helper for demos/tests — blocks until done or timeout. */
  public async awaitOperation(payload: TReq, options: SubmitOptions & { maxPollTimeMs?: number } = {}): Promise<TRes> {
    const handle = await this.submitOperation(payload, options);
    const maxPollTimeMs = options.maxPollTimeMs ?? 120000;
    const start = Date.now();
    while (Date.now() - start < maxPollTimeMs) {
      const snap = await handle.poll();
      if (!snap.done) {
        await new Promise(r => setTimeout(r, 1000));
        continue;
      }
      if (snap.status === 'succeeded' && snap.data !== undefined) {
        return snap.data;
      }
      throw new Error(`Operation '${handle.operationId}' failed with status: ${snap.status}`);
    }
    throw new Error(`Operation timed out after ${maxPollTimeMs}ms`);
  }

  private scheduleDispatch(): void {
    setImmediate(() => this.dispatch());
  }

  private scheduleCoolOffWake(): void {
    if (this.coolOffWakeTimer) return;
    let minRemaining = Number.POSITIVE_INFINITY;
    for (const ep of this.endpoints.values()) {
      const rem = ep.coolOffRemainingMs();
      if (rem > 0 && rem < minRemaining) minRemaining = rem;
    }
    if (!Number.isFinite(minRemaining) || minRemaining <= 0) {
      // Eligible now or half-open — retry soon
      this.coolOffWakeTimer = setTimeout(() => {
        this.coolOffWakeTimer = undefined;
        this.scheduleDispatch();
      }, 50);
      return;
    }
    const delay = Math.min(minRemaining, 1000);
    this.coolOffWakeTimer = setTimeout(() => {
      this.coolOffWakeTimer = undefined;
      this.scheduleDispatch();
    }, delay);
  }

  private dispatch(): void {
    if (this.queue.isEmpty()) return;

    const endpointList = Array.from(this.endpoints.values());
    if (endpointList.length === 0) return;

    while (!this.queue.isEmpty()) {
      const job = this.queue.dequeue();
      if (!job) break;

      if (job.abortController.signal.aborted) {
        if (job.tenantCharged) {
          this.tenantGovernor.refundOnDrop(job.tenantId, job.estimatedBudget);
          job.tenantCharged = false;
        }
        this.admissionController.notifySlotAvailable();
        continue;
      }

      const estimate = job.estimatedBudget ?? { rpm: 1 };
      const selectedEndpoint = this.router.selectEndpoint(endpointList, estimate, job.avoidEndpointIds);

      if (!selectedEndpoint) {
        this.queue.enqueue(job, true);
        this.scheduleCoolOffWake();
        break;
      }

      if (!selectedEndpoint.acquireSlot(estimate)) {
        this.queue.enqueue(job, true);
        this.scheduleCoolOffWake();
        break;
      }

      job.clearQueueTimer?.();
      this.activeJobsCount++;
      this.admissionController.notifySlotAvailable();
      this.executeJob(job, selectedEndpoint, false);
    }
  }

  private async executeJob(
    job: Job<TReq, TRes>,
    endpoint: EndpointInstance,
    isRetry: boolean
  ): Promise<void> {
    const queueWaitMs = Date.now() - job.createdAt;
    const executionStartTime = Date.now();
    job.totalAttempts++;
    this.recordDispatchOutcome(isRetry);

    const execTimer = setTimeout(() => {
      job.abortController.abort();
    }, job.executionTimeoutMs);

    this.emit('jobDispatched', {
      jobId: job.id,
      endpointId: endpoint.id,
      attempt: job.totalAttempts,
      queueWaitMs
    });

    try {
      const authHeaders = this.credentialVault.getAuthHeaders(endpoint.id);
      const result = await this.adapter.execute(
        endpoint.config,
        job.payload,
        job.abortController.signal,
        authHeaders
      );

      clearTimeout(execTimer);
      const durationMs = Date.now() - executionStartTime;
      endpoint.releaseSlot(true, durationMs, job.estimatedBudget, result.budgetDeduction);
      this.activeJobsCount--;
      this.telemetry.recordCompletion(durationMs, queueWaitMs);

      job.resolve({
        success: true,
        data: result.data,
        endpointId: endpoint.id,
        attempts: job.totalAttempts,
        durationMs,
        queueWaitMs,
        budgetDeducted: result.budgetDeduction,
        timestamp: Date.now()
      });

      this.emit('jobCompleted', { jobId: job.id, endpointId: endpoint.id, durationMs });
    } catch (err: any) {
      clearTimeout(execTimer);
      const durationMs = Date.now() - executionStartTime;
      const classification = this.adapter.classifyError(err);

      endpoint.releaseSlot(false, durationMs, job.estimatedBudget, undefined, classification.class);
      this.activeJobsCount--;

      if (classification.class === FailureClass.OVERLOADED) {
        endpoint.applyOverloadSignal(classification.retryAfterMs ?? 1000);
      }

      if (classification.class === FailureClass.UNAVAILABLE) {
        job.avoidEndpointIds.push(endpoint.id);
      }

      const canRetry =
        (classification.class === FailureClass.TRANSIENT ||
          classification.class === FailureClass.OVERLOADED ||
          classification.class === FailureClass.UNAVAILABLE) &&
        job.retriesRemaining > 0 &&
        !job.abortController.signal.aborted &&
        this.hasRetryBudget();

      if (canRetry) {
        job.retriesRemaining--;
        this.telemetry.recordRetry();

        const retryAfterMs = classification.retryAfterMs;
        const backoffRes = this.backoffCalculator.calculateDelay(
          job.totalAttempts,
          retryAfterMs,
          job.previousDecorrelatedDelay
        );
        job.previousDecorrelatedDelay = backoffRes.nextDecorrelatedDelay;

        const deadline = job.createdAt + job.queueTimeoutMs + job.executionTimeoutMs;
        const remaining = Math.max(0, deadline - Date.now());
        const delayMs = Math.min(backoffRes.delayMs, remaining);

        const sanitizedErr = this.credentialVault.redactSecrets(classification.message);
        this.emit('jobRetry', {
          jobId: job.id,
          failedEndpointId: endpoint.id,
          attempts: job.totalAttempts,
          delayMs,
          reason: sanitizedErr
        });

        setTimeout(async () => {
          if (job.abortController.signal.aborted) {
            this.telemetry.recordFailure();
            job.reject(new Error(sanitizedErr));
            return;
          }
          const remainingQueueTime = Math.max(0, deadline - Date.now());
          if (remainingQueueTime <= 0) {
            this.telemetry.recordFailure();
            job.reject(new Error('Job deadline exceeded before retry dispatch'));
            return;
          }
          let retryTimer: NodeJS.Timeout | undefined = setTimeout(() => {
            if (!job.abortController.signal.aborted) {
              job.abortController.abort();
              if (job.tenantCharged) {
                this.tenantGovernor.refundOnDrop(job.tenantId, job.estimatedBudget);
                job.tenantCharged = false;
              }
              job.reject(new Error('Job timed out waiting in queue on retry'));
            }
          }, remainingQueueTime);
          job.clearQueueTimer = () => {
            if (retryTimer) {
              clearTimeout(retryTimer);
              retryTimer = undefined;
            }
          };
          const decision = await this.admissionController.admitAndEnqueue(this.queue, job);
          if (!decision.accepted) {
            job.clearQueueTimer?.();
            this.telemetry.recordFailure();
            job.reject(new Error(decision.reason ?? 'Retry rejected by admission controller'));
            return;
          }
          if (decision.rejectedJob) {
            const dropped = decision.rejectedJob;
            if (dropped.tenantCharged) {
              this.tenantGovernor.refundOnDrop(dropped.tenantId, dropped.estimatedBudget);
              dropped.tenantCharged = false;
            }
            dropped.reject(new Error('Dropped from queue to make room for higher-priority work'));
          }
          this.scheduleDispatch();
        }, delayMs);
      } else {
        this.telemetry.recordFailure();
        const sanitizedErr = this.credentialVault.redactSecrets(classification.message);
        job.reject(new Error(sanitizedErr));
        this.emit('jobFailed', { jobId: job.id, endpointId: endpoint.id, error: sanitizedErr });
      }
    } finally {
      this.scheduleDispatch();
    }
  }

  private async runHealthChecks(): Promise<void> {
    let anyHealthy = false;
    for (const ep of this.endpoints.values()) {
      const authHeaders = this.credentialVault.getAuthHeaders(ep.id);
      const isUp = await this.adapter.healthCheck(ep.config, authHeaders).catch(() => false);
      if (!isUp) {
        ep.setHealth('unhealthy');
      } else {
        // Successful probe always marks Healthy (including from degraded / half-open labeling)
        ep.setHealth('healthy');
        anyHealthy = true;
      }
    }
    if (anyHealthy) {
      this.scheduleDispatch();
    }
  }

  public getMetrics(): PoolMetrics {
    return this.telemetry.snapshot(
      this.activeJobsCount,
      this.queue.size(),
      this.admissionController.getConfig().maxQueueSize,
      Array.from(this.endpoints.values())
    );
  }

  public registerTenant(tenantConfig: Parameters<TenantGovernor['registerTenant']>[0]): void {
    this.tenantGovernor.registerTenant(tenantConfig);
  }

  public resolvePriority(tenantId: string | undefined, priority: JobPriority): JobPriority {
    return this.tenantGovernor.resolvePriority(tenantId, priority);
  }

  public async drain(timeoutMs: number = 10000): Promise<void> {
    this.isDraining = true;
    const startTime = Date.now();

    while (this.activeJobsCount > 0 || !this.queue.isEmpty()) {
      if (Date.now() - startTime >= timeoutMs) {
        this.queue.clear(new Error('Pool drained before all queued jobs could finish'));
        this.admissionController.notifySlotAvailable();
        break;
      }
      await new Promise(r => setTimeout(r, 100));
    }

    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
    }
    if (this.coolOffWakeTimer) {
      clearTimeout(this.coolOffWakeTimer);
      this.coolOffWakeTimer = undefined;
    }
  }

  public destroy(): void {
    this.isDraining = true;
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
    }
    if (this.coolOffWakeTimer) {
      clearTimeout(this.coolOffWakeTimer);
      this.coolOffWakeTimer = undefined;
    }
    this.queue.clear(new Error('ServicePool destroyed'));
    this.admissionController.notifySlotAvailable();
    this.removeAllListeners();
  }
}
