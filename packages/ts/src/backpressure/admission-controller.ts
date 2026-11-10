import { BackpressureConfig, Job, JobPriority, PoolErrorCode } from '../types/index.js';
import { PriorityJobQueue } from './priority-queue.js';
import { EventEmitter } from 'node:events';

export interface AdmissionDecision {
  accepted: boolean;
  rejectedJob?: Job<any, any>;
  reason?: string;
  code?: PoolErrorCode;
}

/**
 * AdmissionController with Timeout wait that re-checks capacity after each wake,
 * and a mutex so check + enqueue happen atomically via admitAndEnqueue.
 */
export class AdmissionController extends EventEmitter {
  private readonly config: BackpressureConfig;
  private admissionLock: Promise<void> = Promise.resolve();

  constructor(config: Partial<BackpressureConfig> = {}) {
    super();
    this.config = {
      maxQueueSize: config.maxQueueSize ?? 500,
      admissionPolicy: config.admissionPolicy ?? 'reject',
      queueTimeoutMs: config.queueTimeoutMs ?? 30000,
      executionTimeoutMs: config.executionTimeoutMs ?? 60000,
      admissionWaitTimeoutMs: config.admissionWaitTimeoutMs ?? 3000,
      sheddingThresholdPercentage: config.sheddingThresholdPercentage ?? 85,
      globalRetryBudgetPercentage: config.globalRetryBudgetPercentage ?? 20
    };
  }

  private async withAdmissionLock<T>(fn: () => Promise<T> | T): Promise<T> {
    let release!: () => void;
    const prev = this.admissionLock;
    this.admissionLock = new Promise<void>(r => { release = r; });
    await prev;
    try {
      return await fn();
    } finally {
      release();
    }
  }

  /** Decision-only (does not enqueue). drop_oldest may mutate the queue by dropping. */
  private decideLocked<TReq, TRes>(
    queue: PriorityJobQueue<TReq, TRes>,
    incomingJob: Job<TReq, TRes>
  ): AdmissionDecision | 'wait' {
    const capacity = this.config.maxQueueSize;
    const currentSize = queue.size();

    if (currentSize < capacity) {
      const saturationPercent = (currentSize / capacity) * 100;
      if (
        saturationPercent >= (this.config.sheddingThresholdPercentage ?? 85) &&
        incomingJob.priority === JobPriority.BATCH
      ) {
        return {
          accepted: false,
          code: 'SATURATED',
          reason: `Queue is ${Math.round(saturationPercent)}% saturated; batch jobs shed proactively`
        };
      }
      return { accepted: true };
    }

    switch (this.config.admissionPolicy) {
      case 'reject':
        return {
          accepted: false,
          code: 'QUEUE_FULL',
          reason: `Pool queue capacity reached (${capacity} jobs). Request rejected with backpressure.`
        };

      case 'drop_newest':
        return {
          accepted: false,
          code: 'QUEUE_FULL',
          reason: `Queue full (${capacity} jobs). Dropping newest incoming job.`
        };

      case 'drop_oldest': {
        if (incomingJob.priority < JobPriority.BATCH) {
          const dropped = queue.dropOldestLowPriority();
          if (dropped) {
            return { accepted: true, rejectedJob: dropped };
          }
        }
        return {
          accepted: false,
          code: 'QUEUE_FULL',
          reason: `Queue is full of critical/high priority work. Cannot drop oldest.`
        };
      }

      case 'timeout':
        return 'wait';

      default:
        return { accepted: false, code: 'QUEUE_FULL', reason: 'Queue full' };
    }
  }

  /**
   * Atomically evaluate admission policy and enqueue on accept.
   * Notify is NOT fired for DropOldest+enqueue (net depth unchanged).
   */
  public async admitAndEnqueue<TReq, TRes>(
    queue: PriorityJobQueue<TReq, TRes>,
    incomingJob: Job<TReq, TRes>
  ): Promise<AdmissionDecision> {
    const capacity = this.config.maxQueueSize;

    const immediate = await this.withAdmissionLock(() => {
      const decision = this.decideLocked(queue, incomingJob);
      if (decision === 'wait') return 'wait' as const;
      if (decision.accepted) {
        queue.enqueue(incomingJob);
      }
      return decision;
    });

    if (immediate !== 'wait') {
      return immediate;
    }

    const waitTimeoutMs = this.config.admissionWaitTimeoutMs ?? 3000;
    const deadline = Date.now() + waitTimeoutMs;

    while (Date.now() < deadline) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) break;

      await this.waitForSlotOnce(Math.min(remaining, 500));
      const claim = await this.withAdmissionLock(() => {
        if (queue.size() < capacity) {
          queue.enqueue(incomingJob);
          return { accepted: true } as AdmissionDecision;
        }
        return null;
      });
      if (claim) return claim;
    }

    return {
      accepted: false,
      code: 'QUEUE_FULL',
      reason: `Queue capacity reached (${capacity} jobs). Timed out waiting ${waitTimeoutMs}ms for admission slot.`
    };
  }

  /** Decision only — does not enqueue the incoming job. */
  public async evaluate<TReq, TRes>(
    queue: PriorityJobQueue<TReq, TRes>,
    incomingJob: Job<TReq, TRes>
  ): Promise<AdmissionDecision> {
    const capacity = this.config.maxQueueSize;
    const immediate = await this.withAdmissionLock(() => this.decideLocked(queue, incomingJob));
    if (immediate !== 'wait') {
      return immediate;
    }

    const waitTimeoutMs = this.config.admissionWaitTimeoutMs ?? 3000;
    const deadline = Date.now() + waitTimeoutMs;

    while (Date.now() < deadline) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) break;
      await this.waitForSlotOnce(Math.min(remaining, 500));
      const claim = await this.withAdmissionLock(() => {
        if (queue.size() < capacity) {
          return { accepted: true } as AdmissionDecision;
        }
        return null;
      });
      if (claim) return claim;
    }

    return {
      accepted: false,
      code: 'QUEUE_FULL',
      reason: `Queue capacity reached (${capacity} jobs). Timed out waiting ${waitTimeoutMs}ms for admission slot.`
    };
  }

  /** Notify waiters only when net queue depth decreased. */
  public notifySlotAvailable(): void {
    this.emit('slotAvailable');
  }

  private waitForSlotOnce(timeoutMs: number): Promise<boolean> {
    return new Promise(resolve => {
      const timer = setTimeout(() => {
        this.removeListener('slotAvailable', onSlot);
        resolve(false);
      }, timeoutMs);

      const onSlot = () => {
        clearTimeout(timer);
        this.removeListener('slotAvailable', onSlot);
        resolve(true);
      };

      this.once('slotAvailable', onSlot);
    });
  }

  public getConfig(): Readonly<BackpressureConfig> {
    return this.config;
  }
}
