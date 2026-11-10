/**
 * TokenBucketRateLimiter implements the classic token bucket algorithm with sub-millisecond precision.
 * @deprecated For service pool and endpoint rate limiting, use {@link MultiResourceLimiter} which supports
 * multi-dimensional quotas (RPM, TPM, Pages/min, RPS) with atomic check-and-acquire.
 */
export class TokenBucketRateLimiter {
  private readonly capacity: number;
  private readonly refillRatePerSecond: number;
  private tokens: number;
  private lastRefillTimestamp: number;

  constructor(rateLimitRps: number, burstCapacity?: number) {
    this.refillRatePerSecond = Math.max(0.1, rateLimitRps);
    this.capacity = burstCapacity ? Math.max(burstCapacity, rateLimitRps) : Math.max(1, rateLimitRps);
    this.tokens = this.capacity;
    this.lastRefillTimestamp = Date.now();
  }

  /**
   * Refills tokens based on elapsed wall-clock time
   */
  private refill(): void {
    const now = Date.now();
    const elapsedSeconds = (now - this.lastRefillTimestamp) / 1000;
    if (elapsedSeconds > 0) {
      this.tokens = Math.min(this.capacity, this.tokens + elapsedSeconds * this.refillRatePerSecond);
      this.lastRefillTimestamp = now;
    }
  }

  /**
   * Attempts to consume tokens. Returns true if granted, false if rate limited.
   */
  public tryAcquire(count: number = 1): boolean {
    this.refill();
    if (this.tokens >= count) {
      this.tokens -= count;
      return true;
    }
    return false;
  }

  /**
   * Calculates estimated milliseconds until sufficient tokens become available
   */
  public getDelayUntilAvailable(count: number = 1): number {
    this.refill();
    if (this.tokens >= count) return 0;
    const missing = count - this.tokens;
    return Math.ceil((missing / this.refillRatePerSecond) * 1000);
  }

  /**
   * Returns current token count (rounded down to 2 decimals)
   */
  public getTokens(): number {
    this.refill();
    return Math.round(this.tokens * 100) / 100;
  }
}

