import { CircuitState, CircuitBreakerConfig } from '../types/index.js';

interface RequestSample {
  timestamp: number;
  success: boolean;
}

/**
 * Three-state Circuit Breaker with strictly controlled Half-Open trial probing.
 * canExecute() is a pure check; tryClaimProbe() claims the single half-open slot.
 */
export class CircuitBreaker {
  private state: CircuitState = 'CLOSED';
  private readonly config: CircuitBreakerConfig;
  private samples: RequestSample[] = [];
  private stateChangedAt: number = Date.now();
  private halfOpenConsecutiveSuccesses: number = 0;
  private halfOpenProbesInFlight: number = 0;

  constructor(config: Partial<CircuitBreakerConfig> = {}) {
    this.config = {
      failureThresholdPercentage: config.failureThresholdPercentage ?? 50,
      minimumRequestVolume: config.minimumRequestVolume ?? 5,
      samplingWindowMs: config.samplingWindowMs ?? 10000,
      coolingOffDurationMs: config.coolingOffDurationMs ?? 15000,
      halfOpenTrialSuccesses: config.halfOpenTrialSuccesses ?? 3
    };
  }

  /** Pure eligibility check — does not claim a half-open probe. */
  public canExecute(): boolean {
    const now = Date.now();

    if (this.state === 'OPEN') {
      return now - this.stateChangedAt >= this.config.coolingOffDurationMs;
    }

    if (this.state === 'HALF_OPEN') {
      return this.halfOpenProbesInFlight < 1;
    }

    return true;
  }

  /**
   * Claims the single half-open probe slot (or allows CLOSED traffic).
   * Transitions OPEN → HALF_OPEN when cool-off has elapsed.
   */
  public tryClaimProbe(): boolean {
    const now = Date.now();

    if (this.state === 'OPEN') {
      if (now - this.stateChangedAt < this.config.coolingOffDurationMs) {
        return false;
      }
      this.transitionTo('HALF_OPEN');
      this.halfOpenProbesInFlight = 1;
      return true;
    }

    if (this.state === 'HALF_OPEN') {
      if (this.halfOpenProbesInFlight < 1) {
        this.halfOpenProbesInFlight = 1;
        return true;
      }
      return false;
    }

    return true; // CLOSED
  }

  public releaseProbeClaim(): void {
    if (this.state === 'HALF_OPEN' && this.halfOpenProbesInFlight > 0) {
      this.halfOpenProbesInFlight--;
    }
  }

  public recordSuccess(): void {
    const now = Date.now();
    this.cleanExpiredSamples(now);
    this.samples.push({ timestamp: now, success: true });

    if (this.state === 'HALF_OPEN') {
      if (this.halfOpenProbesInFlight > 0) this.halfOpenProbesInFlight--;
      this.halfOpenConsecutiveSuccesses++;
      if (this.halfOpenConsecutiveSuccesses >= this.config.halfOpenTrialSuccesses) {
        this.transitionTo('CLOSED');
      }
    }
  }

  public recordFailure(): void {
    const now = Date.now();
    this.cleanExpiredSamples(now);
    this.samples.push({ timestamp: now, success: false });

    if (this.state === 'HALF_OPEN') {
      if (this.halfOpenProbesInFlight > 0) this.halfOpenProbesInFlight--;
      this.transitionTo('OPEN');
      return;
    }

    if (this.state === 'CLOSED') {
      if (this.samples.length >= this.config.minimumRequestVolume) {
        const failures = this.samples.filter(s => !s.success).length;
        const failureRate = (failures / this.samples.length) * 100;

        if (failureRate >= this.config.failureThresholdPercentage) {
          this.transitionTo('OPEN');
        }
      }
    }
  }

  private transitionTo(newState: CircuitState): void {
    this.state = newState;
    this.stateChangedAt = Date.now();
    this.halfOpenProbesInFlight = 0;
    if (newState === 'HALF_OPEN') {
      this.halfOpenConsecutiveSuccesses = 0;
    } else if (newState === 'CLOSED') {
      this.samples = [];
      this.halfOpenConsecutiveSuccesses = 0;
    }
  }

  private cleanExpiredSamples(now: number): void {
    const cutoff = now - this.config.samplingWindowMs;
    this.samples = this.samples.filter(s => s.timestamp >= cutoff);
  }

  /** Read-only state; does not transition OPEN → HALF_OPEN. */
  public getState(): CircuitState {
    return this.state;
  }

  /** Milliseconds until OPEN cool-off elapses; 0 if already eligible / not OPEN. */
  public coolOffRemainingMs(now: number = Date.now()): number {
    if (this.state !== 'OPEN') return 0;
    const elapsed = now - this.stateChangedAt;
    return Math.max(0, this.config.coolingOffDurationMs - elapsed);
  }

  public reset(): void {
    this.transitionTo('CLOSED');
  }
}
