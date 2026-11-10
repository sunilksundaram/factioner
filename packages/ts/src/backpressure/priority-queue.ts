import { Job, JobPriority } from '../types/index.js';

/**
 * Tenant-Fair Multi-Tier Priority Queue.
 * Within each priority band, jobs round-robin across tenants (stable sorted keys).
 */
export class PriorityJobQueue<TReq = unknown, TRes = unknown> {
  private readonly tiers: Map<JobPriority, Map<string, Job<TReq, TRes>[]>> = new Map();
  private readonly tenantPointers: Map<JobPriority, number> = new Map();
  private totalJobsCount: number = 0;

  constructor() {
    for (const p of [JobPriority.CRITICAL, JobPriority.HIGH, JobPriority.NORMAL, JobPriority.BATCH]) {
      this.tiers.set(p, new Map());
      this.tenantPointers.set(p, 0);
    }
  }

  public enqueue(job: Job<TReq, TRes>, front: boolean = false): void {
    const tierMap = this.tiers.get(job.priority) ?? this.tiers.get(JobPriority.NORMAL)!;
    const tenantKey = job.tenantId || '_default';

    let tenantJobs = tierMap.get(tenantKey);
    if (!tenantJobs) {
      tenantJobs = [];
      tierMap.set(tenantKey, tenantJobs);
    }

    if (front) {
      tenantJobs.unshift(job);
    } else {
      tenantJobs.push(job);
    }
    this.totalJobsCount++;
  }

  public dequeue(): Job<TReq, TRes> | undefined {
    if (this.totalJobsCount === 0) return undefined;

    const priorities = [JobPriority.CRITICAL, JobPriority.HIGH, JobPriority.NORMAL, JobPriority.BATCH];

    for (const priority of priorities) {
      const tierMap = this.tiers.get(priority)!;
      const tenantKeys = Array.from(tierMap.keys())
        .filter(k => (tierMap.get(k)?.length ?? 0) > 0)
        .sort();

      if (tenantKeys.length > 0) {
        let ptr = this.tenantPointers.get(priority) ?? 0;
        const tenantKey = tenantKeys[ptr % tenantKeys.length];
        this.tenantPointers.set(priority, ptr + 1);

        const tenantJobs = tierMap.get(tenantKey)!;
        const job = tenantJobs.shift();
        if (job) {
          this.totalJobsCount--;
          return job;
        }
      }
    }

    return undefined;
  }

  /** Drops oldest from most bloated low-priority tenant; never drops CRITICAL. */
  public dropOldestLowPriority(): Job<TReq, TRes> | undefined {
    if (this.totalJobsCount === 0) return undefined;

    const priorities = [JobPriority.BATCH, JobPriority.NORMAL, JobPriority.HIGH];

    for (const priority of priorities) {
      const tierMap = this.tiers.get(priority)!;
      let largestTenantKey: string | undefined;
      let maxCount = 0;

      for (const [key, jobs] of tierMap.entries()) {
        if (jobs.length > maxCount) {
          maxCount = jobs.length;
          largestTenantKey = key;
        }
      }

      if (largestTenantKey && maxCount > 0) {
        const jobs = tierMap.get(largestTenantKey)!;
        const dropped = jobs.shift();
        if (dropped) {
          this.totalJobsCount--;
          return dropped;
        }
      }
    }

    return undefined;
  }

  public size(): number {
    return this.totalJobsCount;
  }

  public isEmpty(): boolean {
    return this.totalJobsCount === 0;
  }

  public clear(rejectReason: Error): void {
    for (const tierMap of this.tiers.values()) {
      for (const jobs of tierMap.values()) {
        while (jobs.length > 0) {
          const job = jobs.shift()!;
          this.totalJobsCount--;
          job.reject(rejectReason);
        }
      }
    }
  }
}
