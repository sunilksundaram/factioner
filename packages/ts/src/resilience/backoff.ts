import { BackoffConfig, BackoffStrategy } from '../types/index.js';

export interface BackoffCalculationResult {
  delayMs: number;
  nextDecorrelatedDelay: number;
}

/**
 * Advanced Backoff calculator supporting Exponential, Full Jitter, Equal Jitter,
 * Decorrelated Jitter, and Retry-After (milliseconds) overrides.
 * Thread-safe: Decorrelated state is passed per-job rather than held in a shared instance field.
 */
export class BackoffCalculator {
  private readonly config: BackoffConfig;

  constructor(config: Partial<BackoffConfig> = {}) {
    this.config = {
      strategy: config.strategy ?? 'full_jitter',
      initialDelayMs: config.initialDelayMs ?? 250,
      maxDelayMs: config.maxDelayMs ?? 15000,
      factor: config.factor ?? 2.0,
      jitter: config.jitter ?? true,
      respectRetryAfterHeader: config.respectRetryAfterHeader ?? true
    };
  }

  /**
   * Computes delay in milliseconds for a given attempt index.
   * `retryAfterMs` is already in milliseconds when provided by the classifier.
   */
  public calculateDelay(
    attempt: number,
    retryAfterMs?: number,
    previousJobDelay?: number
  ): BackoffCalculationResult {
    if (this.config.respectRetryAfterHeader && retryAfterMs !== undefined && Number.isFinite(retryAfterMs)) {
      const delay = Math.min(this.config.maxDelayMs, Math.max(0, retryAfterMs));
      return { delayMs: delay, nextDecorrelatedDelay: delay };
    }

    const { initialDelayMs, maxDelayMs, factor, strategy } = this.config;
    const exponentialBase = Math.min(maxDelayMs, initialDelayMs * Math.pow(factor, attempt));
    const prev = previousJobDelay ?? initialDelayMs;

    switch (strategy) {
      case 'full_jitter': {
        const delay = Math.floor(Math.random() * exponentialBase);
        return { delayMs: delay, nextDecorrelatedDelay: delay };
      }

      case 'equal_jitter': {
        const half = exponentialBase / 2;
        const delay = Math.floor(half + Math.random() * half);
        return { delayMs: delay, nextDecorrelatedDelay: delay };
      }

      case 'decorrelated': {
        const low = initialDelayMs;
        const high = prev * 3;
        const delay = Math.min(maxDelayMs, Math.floor(low + Math.random() * Math.max(1, high - low)));
        return { delayMs: delay, nextDecorrelatedDelay: delay };
      }

      case 'exponential':
      default: {
        const delay = Math.floor(exponentialBase);
        return { delayMs: delay, nextDecorrelatedDelay: delay };
      }
    }
  }

  /** @deprecated Prefer classifier parseRetryAfterToMs; kept for string header helpers. */
  public parseRetryAfter(value: string | number): number | null {
    if (typeof value === 'number') {
      return value; // treat as already-ms when callers pass ms
    }

    const seconds = parseInt(value, 10);
    if (!isNaN(seconds) && /^\d+$/.test(value.trim())) {
      return seconds * 1000;
    }

    const dateParsed = Date.parse(value);
    if (!isNaN(dateParsed)) {
      const diff = dateParsed - Date.now();
      return Math.max(0, diff);
    }

    return null;
  }
}
