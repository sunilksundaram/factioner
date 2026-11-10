import { PoolMetrics, EndpointMetrics, ServiceType } from '../types/index.js';
import { EndpointInstance } from '../pool/endpoint-instance.js';

/**
 * Real-time Telemetry and Performance Metrics Aggregator
 */
export class PoolTelemetry {
  private readonly poolName: string;
  private readonly serviceType: ServiceType;

  private totalCompleted: number = 0;
  private totalFailed: number = 0;
  private totalRejected: number = 0;
  private totalRetries: number = 0;

  private recentLatencies: number[] = [];
  private recentQueueWaits: number[] = [];
  private completionTimestamps: number[] = [];

  constructor(poolName: string, serviceType: ServiceType) {
    this.poolName = poolName;
    this.serviceType = serviceType;
  }

  public recordCompletion(latencyMs: number, queueWaitMs: number): void {
    this.totalCompleted++;
    this.recentLatencies.push(latencyMs);
    if (this.recentLatencies.length > 200) {
      this.recentLatencies.shift();
    }
    this.recentQueueWaits.push(queueWaitMs);
    if (this.recentQueueWaits.length > 200) {
      this.recentQueueWaits.shift();
    }

    const now = Date.now();
    this.completionTimestamps.push(now);
    this.cleanCompletionWindow(now);
  }

  public getAverageQueueWaitMs(): number {
    if (this.recentQueueWaits.length === 0) return 0;
    const sum = this.recentQueueWaits.reduce((a, b) => a + b, 0);
    return Math.round(sum / this.recentQueueWaits.length);
  }

  public recordFailure(): void {
    this.totalFailed++;
  }

  public recordRejection(): void {
    this.totalRejected++;
  }

  public recordRetry(): void {
    this.totalRetries++;
  }

  private cleanCompletionWindow(now: number): void {
    const windowStart = now - 10000; // 10-second rolling window
    this.completionTimestamps = this.completionTimestamps.filter(t => t >= windowStart);
  }

  public getThroughputRps(): number {
    const now = Date.now();
    this.cleanCompletionWindow(now);
    if (this.completionTimestamps.length === 0) return 0;
    return Math.round((this.completionTimestamps.length / 10) * 10) / 10;
  }

  public getAverageLatency(): number {
    if (this.recentLatencies.length === 0) return 0;
    const sum = this.recentLatencies.reduce((a, b) => a + b, 0);
    return Math.round(sum / this.recentLatencies.length);
  }

  public getP95Latency(): number {
    if (this.recentLatencies.length === 0) return 0;
    const sorted = [...this.recentLatencies].sort((a, b) => a - b);
    const index = Math.floor(sorted.length * 0.95);
    return sorted[index] ?? sorted[sorted.length - 1];
  }

  public snapshot(
    activeJobs: number,
    queuedJobs: number,
    queueCapacity: number,
    endpoints: EndpointInstance[]
  ): PoolMetrics {
    const epMetrics: EndpointMetrics[] = endpoints.map(e => e.getMetrics());

    return {
      poolName: this.poolName,
      serviceType: this.serviceType,
      activeJobs,
      queuedJobs,
      queueCapacity,
      queueSaturationPercentage: Math.min(100, Math.round((queuedJobs / Math.max(1, queueCapacity)) * 100)),
      totalCompleted: this.totalCompleted,
      totalFailed: this.totalFailed,
      totalRejected: this.totalRejected,
      totalRetries: this.totalRetries,
      averageLatencyMs: this.getAverageLatency(),
      p95LatencyMs: this.getP95Latency(),
      throughputRps: this.getThroughputRps(),
      endpoints: epMetrics
    };
  }
}

