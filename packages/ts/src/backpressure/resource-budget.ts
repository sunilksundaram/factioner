import { ResourceBudgetConfig, BudgetDeduction } from '../types/index.js';

interface ResourceWindow {
  limit: number;
  windowMs: number;
  consumed: number;
  windowStart: number;
}

/** When actual TPM exceeds estimate by this ratio, apply temporary TPM pressure. */
const TPM_OVERSHOOT_RATIO = 1.25;
/** Cap on reconcile-driven estimate inflation (never unbounded). */
const MAX_TPM_ESTIMATE_BIAS = 2.0;
/** Duration of reconcile-driven TPM bias when no Retry-After is present. */
const TPM_BIAS_DEFAULT_MS = 30_000;

/**
 * MultiResourceLimiter enforces multi-dimensional quotas (RPM, TPM, Pages/min, RPS)
 * with fixed time-window reset and temporary RPM/TPM tightening from overload signals
 * and usage-reconcile overshoot (E3).
 */
export class MultiResourceLimiter {
  private readonly rpmWindow?: ResourceWindow;
  private readonly tpmWindow?: ResourceWindow;
  private readonly pagesWindow?: ResourceWindow;
  private readonly rpsWindow?: ResourceWindow;

  private readonly baselineRpm?: number;
  private effectiveRpm?: number;
  private rpmSignalUntilMs?: number;

  private readonly baselineTpm?: number;
  private effectiveTpm?: number;
  private tpmSignalUntilMs?: number;
  /** Multiplier applied to estimated TPM on acquire (from reconcile overshoot). */
  private tpmEstimateBias: number = 1;
  private signalTightened: boolean = false;

  constructor(config: ResourceBudgetConfig = {}) {
    if (config.rpm && config.rpm > 0) {
      this.baselineRpm = config.rpm;
      this.rpmWindow = { limit: config.rpm, windowMs: 60000, consumed: 0, windowStart: Date.now() };
    }
    if (config.tpm && config.tpm > 0) {
      this.baselineTpm = config.tpm;
      this.tpmWindow = { limit: config.tpm, windowMs: 60000, consumed: 0, windowStart: Date.now() };
    }
    if (config.pagesPerMin && config.pagesPerMin > 0) {
      this.pagesWindow = { limit: config.pagesPerMin, windowMs: 60000, consumed: 0, windowStart: Date.now() };
    }
    if (config.rps && config.rps > 0) {
      this.rpsWindow = { limit: config.rps, windowMs: 1000, consumed: 0, windowStart: Date.now() };
    }
  }

  private restoreSignalIfExpired(now: number): void {
    if (this.rpmSignalUntilMs !== undefined && now >= this.rpmSignalUntilMs) {
      this.effectiveRpm = undefined;
      this.rpmSignalUntilMs = undefined;
      if (this.rpmWindow && this.baselineRpm) {
        this.rpmWindow.limit = this.baselineRpm;
      }
    }
    if (this.tpmSignalUntilMs !== undefined && now >= this.tpmSignalUntilMs) {
      this.effectiveTpm = undefined;
      this.tpmSignalUntilMs = undefined;
      this.tpmEstimateBias = 1;
      if (this.tpmWindow && this.baselineTpm) {
        this.tpmWindow.limit = this.baselineTpm;
      }
    }
    this.signalTightened =
      this.rpmSignalUntilMs !== undefined || this.tpmSignalUntilMs !== undefined;
  }

  private currentRpmLimit(now: number): number | undefined {
    this.restoreSignalIfExpired(now);
    if (this.effectiveRpm !== undefined) return this.effectiveRpm;
    return this.baselineRpm;
  }

  private currentTpmLimit(now: number): number | undefined {
    this.restoreSignalIfExpired(now);
    if (this.effectiveTpm !== undefined) return this.effectiveTpm;
    return this.baselineTpm;
  }

  private tightenTpm(now: number, retryAfterMs: number, factor: number): void {
    if (!this.baselineTpm || !this.tpmWindow) return;
    const tightened = Math.min(
      this.baselineTpm,
      Math.max(1, Math.floor(this.baselineTpm * factor))
    );
    this.effectiveTpm = tightened;
    this.tpmWindow.limit = tightened;
    const until = now + Math.max(1, retryAfterMs);
    this.tpmSignalUntilMs =
      this.tpmSignalUntilMs !== undefined ? Math.max(this.tpmSignalUntilMs, until) : until;
    this.signalTightened = true;
  }

  /**
   * Temporarily tighten effective RPM and TPM on OVERLOADED + Retry-After.
   * Never raises above the declared baseline.
   */
  public applyOverloadSignal(retryAfterMs: number): void {
    const now = Date.now();
    const untilMs = Math.max(1, retryAfterMs);

    if (this.baselineRpm && this.rpmWindow) {
      const tightened = Math.min(this.baselineRpm, Math.max(1, this.baselineRpm / 2));
      this.effectiveRpm = tightened;
      this.rpmWindow.limit = tightened;
      this.rpmSignalUntilMs = now + untilMs;
      this.signalTightened = true;
    }

    // Mirror RPM path for TPM (halve effective cap).
    this.tightenTpm(now, untilMs, 0.5);
  }

  public getEffectiveRpm(): number | undefined {
    return this.currentRpmLimit(Date.now());
  }

  public getEffectiveTpm(): number | undefined {
    return this.currentTpmLimit(Date.now());
  }

  /** Current estimate inflation from usage overshoot (1 = none). */
  public getTpmEstimateBias(): number {
    this.restoreSignalIfExpired(Date.now());
    return this.tpmEstimateBias;
  }

  public isSignalTightened(): boolean {
    this.restoreSignalIfExpired(Date.now());
    return this.signalTightened;
  }

  private refreshWindow(w: ResourceWindow, now: number): void {
    if (now - w.windowStart >= w.windowMs) {
      w.consumed = 0;
      w.windowStart = now;
    }
  }

  private biasedTpmNeed(estimate: BudgetDeduction, now: number): number | undefined {
    if (estimate.tpm === undefined) return undefined;
    this.restoreSignalIfExpired(now);
    return estimate.tpm * this.tpmEstimateBias;
  }

  public hasAvailable(estimate: BudgetDeduction = { rpm: 1 }): boolean {
    const now = Date.now();
    this.restoreSignalIfExpired(now);

    if (this.rpmWindow) {
      this.refreshWindow(this.rpmWindow, now);
      const limit = this.currentRpmLimit(now) ?? this.rpmWindow.limit;
      if (this.rpmWindow.consumed + (estimate.rpm ?? 1) > limit) return false;
    }

    const tpmNeed = this.biasedTpmNeed(estimate, now);
    if (this.tpmWindow && tpmNeed !== undefined) {
      this.refreshWindow(this.tpmWindow, now);
      const limit = this.currentTpmLimit(now) ?? this.tpmWindow.limit;
      if (this.tpmWindow.consumed + tpmNeed > limit) return false;
    }

    if (this.pagesWindow && estimate.pages) {
      this.refreshWindow(this.pagesWindow, now);
      if (this.pagesWindow.consumed + estimate.pages > this.pagesWindow.limit) return false;
    }

    if (this.rpsWindow) {
      this.refreshWindow(this.rpsWindow, now);
      if (this.rpsWindow.consumed + (estimate.rps ?? 1) > this.rpsWindow.limit) return false;
    }

    return true;
  }

  public tryAcquire(estimate: BudgetDeduction = { rpm: 1 }): boolean {
    if (!this.hasAvailable(estimate)) return false;

    const now = Date.now();
    if (this.rpmWindow) {
      this.refreshWindow(this.rpmWindow, now);
      this.rpmWindow.consumed += estimate.rpm ?? 1;
    }
    const tpmNeed = this.biasedTpmNeed(estimate, now);
    if (this.tpmWindow && tpmNeed !== undefined) {
      this.refreshWindow(this.tpmWindow, now);
      this.tpmWindow.consumed += tpmNeed;
    }
    if (this.pagesWindow && estimate.pages) {
      this.refreshWindow(this.pagesWindow, now);
      this.pagesWindow.consumed += estimate.pages;
    }
    if (this.rpsWindow) {
      this.refreshWindow(this.rpsWindow, now);
      this.rpsWindow.consumed += estimate.rps ?? 1;
    }

    return true;
  }

  public reconcile(estimated: BudgetDeduction = { rpm: 1 }, actual: BudgetDeduction): void {
    const now = Date.now();

    if (this.tpmWindow) {
      this.refreshWindow(this.tpmWindow, now);
      const estTpm = estimated.tpm ?? 0;
      const actTpm = actual.tpm ?? 0;
      this.tpmWindow.consumed = Math.max(0, this.tpmWindow.consumed + (actTpm - estTpm));

      // E3 usage→TPM bias: repeated/large overshoot tightens effective TPM + inflates future estimates.
      if (estTpm > 0 && actTpm > estTpm * TPM_OVERSHOOT_RATIO && this.baselineTpm) {
        const ratio = actTpm / estTpm;
        this.tpmEstimateBias = Math.min(MAX_TPM_ESTIMATE_BIAS, Math.max(this.tpmEstimateBias, ratio));
        this.tightenTpm(now, TPM_BIAS_DEFAULT_MS, 1 / Math.min(ratio, 2));
      }
    }

    if (this.pagesWindow) {
      this.refreshWindow(this.pagesWindow, now);
      const estPages = estimated.pages ?? 0;
      const actPages = actual.pages ?? 0;
      this.pagesWindow.consumed = Math.max(0, this.pagesWindow.consumed + (actPages - estPages));
    }
  }

  public refund(deduction: BudgetDeduction = { rpm: 1 }): void {
    if (this.rpmWindow) {
      this.rpmWindow.consumed = Math.max(0, this.rpmWindow.consumed - (deduction.rpm ?? 1));
    }
    if (this.tpmWindow && deduction.tpm) {
      const bias = this.tpmEstimateBias || 1;
      this.tpmWindow.consumed = Math.max(0, this.tpmWindow.consumed - deduction.tpm * bias);
    }
    if (this.pagesWindow && deduction.pages) {
      this.pagesWindow.consumed = Math.max(0, this.pagesWindow.consumed - deduction.pages);
    }
    if (this.rpsWindow) {
      this.rpsWindow.consumed = Math.max(0, this.rpsWindow.consumed - (deduction.rps ?? 1));
    }
  }

  public getUsage(): { rpmConsumed: number; tpmConsumed: number; pagesConsumed: number } {
    const now = Date.now();
    if (this.rpmWindow) this.refreshWindow(this.rpmWindow, now);
    if (this.tpmWindow) this.refreshWindow(this.tpmWindow, now);
    if (this.pagesWindow) this.refreshWindow(this.pagesWindow, now);

    return {
      rpmConsumed: Math.round(this.rpmWindow?.consumed ?? 0),
      tpmConsumed: Math.round(this.tpmWindow?.consumed ?? 0),
      pagesConsumed: Math.round(this.pagesWindow?.consumed ?? 0)
    };
  }
}
