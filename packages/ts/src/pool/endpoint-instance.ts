import {
  EndpointConfig,
  EndpointHealth,
  CircuitState,
  EndpointMetrics,
  CircuitBreakerConfig,
  BudgetDeduction,
  FailureClass
} from '../types/index.js';
import { CircuitBreaker } from '../resilience/circuit-breaker.js';
import { MultiResourceLimiter } from '../backpressure/resource-budget.js';

/**
 * EndpointInstance manages state, concurrency slots, multi-dimensional budget
 * limiters (RPM, TPM, Pages, RPS), and circuit breaker for a single target endpoint.
 */
export class EndpointInstance {
  public readonly id: string;
  public readonly config: EndpointConfig;
  public readonly circuitBreaker: CircuitBreaker;
  public readonly budgetLimiter: MultiResourceLimiter;

  private activeLeases: number = 0;
  private totalRequests: number = 0;
  private successfulRequests: number = 0;
  private failedRequests: number = 0;
  private recentLatencies: number[] = [];
  private emaLatencyMs: number = 0;
  private healthStatus: EndpointHealth = 'healthy';
  private probeClaimed: boolean = false;

  constructor(config: EndpointConfig, circuitBreakerConfig?: Partial<CircuitBreakerConfig>) {
    this.id = config.id;
    this.config = config;
    this.circuitBreaker = new CircuitBreaker(circuitBreakerConfig);
    // maxConcurrency on endpoint is source of truth; budget.concurrency is ignored
    this.budgetLimiter = new MultiResourceLimiter(config.budget ?? {});
  }

  public isAvailable(estimate: BudgetDeduction = { rpm: 1 }): boolean {
    if (this.healthStatus === 'unhealthy') return false;
    if (this.activeLeases >= this.config.maxConcurrency) return false;
    if (!this.circuitBreaker.canExecute()) return false;
    if (!this.budgetLimiter.hasAvailable(estimate)) return false;
    return true;
  }

  public coolOffRemainingMs(): number {
    return this.circuitBreaker.coolOffRemainingMs();
  }

  public applyOverloadSignal(retryAfterMs: number): void {
    this.budgetLimiter.applyOverloadSignal(retryAfterMs);
  }

  public acquireSlot(estimate: BudgetDeduction = { rpm: 1 }): boolean {
    if (this.healthStatus === 'unhealthy') return false;
    if (this.activeLeases >= this.config.maxConcurrency) return false;
    if (!this.budgetLimiter.hasAvailable(estimate)) return false;

    if (!this.circuitBreaker.tryClaimProbe()) {
      return false;
    }

    if (!this.budgetLimiter.tryAcquire(estimate)) {
      this.circuitBreaker.releaseProbeClaim();
      return false;
    }

    this.probeClaimed = this.circuitBreaker.getState() === 'HALF_OPEN';
    this.activeLeases++;
    this.totalRequests++;
    return true;
  }

  /**
   * @param failureClass When set to FATAL, does not trip the circuit breaker.
   */
  public releaseSlot(
    success: boolean,
    durationMs: number,
    estimated?: BudgetDeduction,
    actual?: BudgetDeduction,
    failureClass?: FailureClass
  ): void {
    if (this.activeLeases > 0) {
      this.activeLeases--;
    }

    if (success) {
      this.successfulRequests++;
      this.circuitBreaker.recordSuccess();
      if (actual) {
        this.budgetLimiter.reconcile(estimated, actual);
      }
      if (this.healthStatus === 'unhealthy' || this.healthStatus === 'degraded' || this.healthStatus === 'cooling_off') {
        this.healthStatus = this.circuitBreaker.getState() === 'CLOSED' ? 'healthy' : 'degraded';
      } else if (this.circuitBreaker.getState() === 'CLOSED') {
        this.healthStatus = 'healthy';
      }
    } else {
      this.failedRequests++;
      const tripsBreaker =
        failureClass === undefined ||
        failureClass === FailureClass.TRANSIENT ||
        failureClass === FailureClass.OVERLOADED ||
        failureClass === FailureClass.UNAVAILABLE;

      if (tripsBreaker) {
        this.circuitBreaker.recordFailure();
      } else {
        this.circuitBreaker.releaseProbeClaim();
      }

      if (estimated && !actual) {
        this.budgetLimiter.refund(estimated);
      }

      const cbState = this.circuitBreaker.getState();
      if (cbState === 'OPEN') {
        this.healthStatus = 'cooling_off';
      } else if (cbState === 'HALF_OPEN') {
        this.healthStatus = 'degraded';
      }
    }

    this.probeClaimed = false;

    if (durationMs > 0) {
      this.recentLatencies.push(durationMs);
      if (this.recentLatencies.length > 50) {
        this.recentLatencies.shift();
      }

      if (this.emaLatencyMs === 0) {
        this.emaLatencyMs = durationMs;
      } else {
        const alpha = 0.2;
        this.emaLatencyMs = alpha * durationMs + (1 - alpha) * this.emaLatencyMs;
      }
    }
  }

  public getActiveLeases(): number {
    return this.activeLeases;
  }

  public getLatencyEma(): number {
    return Math.round(this.emaLatencyMs);
  }

  public getP95Latency(): number {
    if (this.recentLatencies.length === 0) return 0;
    const sorted = [...this.recentLatencies].sort((a, b) => a - b);
    const index = Math.floor(sorted.length * 0.95);
    return Math.round(sorted[index] ?? sorted[sorted.length - 1]);
  }

  public getCircuitState(): CircuitState {
    return this.circuitBreaker.getState();
  }

  public getHealth(): EndpointHealth {
    return this.healthStatus;
  }

  public setHealth(health: EndpointHealth): void {
    this.healthStatus = health;
  }

  public getMetrics(): EndpointMetrics {
    const usage = this.budgetLimiter.getUsage();
    return {
      id: this.id,
      url: this.config.url,
      health: this.healthStatus,
      circuitState: this.circuitBreaker.getState(),
      activeLeases: this.activeLeases,
      maxConcurrency: this.config.maxConcurrency,
      totalRequests: this.totalRequests,
      successfulRequests: this.successfulRequests,
      failedRequests: this.failedRequests,
      avgLatencyMs: this.getLatencyEma(),
      p95LatencyMs: this.getP95Latency(),
      budgetUsage: usage,
      effectiveRpm: this.budgetLimiter.getEffectiveRpm(),
      effectiveTpm: this.budgetLimiter.getEffectiveTpm(),
      tpmEstimateBias: this.budgetLimiter.getTpmEstimateBias(),
      signalTightened: this.budgetLimiter.isSignalTightened()
    };
  }
}
