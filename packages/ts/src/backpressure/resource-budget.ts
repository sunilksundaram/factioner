import { ResourceBudgetConfig, BudgetDeduction } from '../types/index.js';

interface ResourceWindow {
  limit: number;
  windowMs: number;
  consumed: number;
  windowStart: number;
}

/**
 * MultiResourceLimiter enforces multi-dimensional quotas (RPM, TPM, Pages/min, RPS)
 * with fixed time-window reset and optional temporary RPM tightening from overload signals.
 */
export class MultiResourceLimiter {
  private readonly rpmWindow?: ResourceWindow;
  private readonly tpmWindow?: ResourceWindow;
  private readonly pagesWindow?: ResourceWindow;
  private readonly rpsWindow?: ResourceWindow;

  private readonly baselineRpm?: number;
  private effectiveRpm?: number;
  private rpmSignalUntilMs?: number;
  private signalTightened: boolean = false;

  constructor(config: ResourceBudgetConfig = {}) {
    if (config.rpm && config.rpm > 0) {
      this.baselineRpm = config.rpm;
      this.rpmWindow = { limit: config.rpm, windowMs: 60000, consumed: 0, windowStart: Date.now() };
    }
    if (config.tpm && config.tpm > 0) {
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
      this.signalTightened = false;
      if (this.rpmWindow && this.baselineRpm) {
        this.rpmWindow.limit = this.baselineRpm;
      }
    }
  }

  private currentRpmLimit(now: number): number | undefined {
    this.restoreSignalIfExpired(now);
    if (this.effectiveRpm !== undefined) return this.effectiveRpm;
    return this.baselineRpm;
  }

  /**
   * Temporarily tighten effective RPM on OVERLOADED + Retry-After.
   * Never raises above the declared baseline.
   */
  public applyOverloadSignal(retryAfterMs: number): void {
    if (!this.baselineRpm || !this.rpmWindow) return;
    const now = Date.now();
    const tightened = Math.min(this.baselineRpm, Math.max(1, this.baselineRpm / 2));
    this.effectiveRpm = tightened;
    this.rpmWindow.limit = tightened;
    this.rpmSignalUntilMs = now + Math.max(1, retryAfterMs);
    this.signalTightened = true;
  }

  public getEffectiveRpm(): number | undefined {
    return this.currentRpmLimit(Date.now());
  }

  public getEffectiveTpm(): number | undefined {
    return this.tpmWindow?.limit;
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

  public hasAvailable(estimate: BudgetDeduction = { rpm: 1 }): boolean {
    const now = Date.now();
    this.restoreSignalIfExpired(now);

    if (this.rpmWindow) {
      this.refreshWindow(this.rpmWindow, now);
      const limit = this.currentRpmLimit(now) ?? this.rpmWindow.limit;
      if (this.rpmWindow.consumed + (estimate.rpm ?? 1) > limit) return false;
    }

    if (this.tpmWindow && estimate.tpm) {
      this.refreshWindow(this.tpmWindow, now);
      if (this.tpmWindow.consumed + estimate.tpm > this.tpmWindow.limit) return false;
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
    if (this.tpmWindow && estimate.tpm) {
      this.refreshWindow(this.tpmWindow, now);
      this.tpmWindow.consumed += estimate.tpm;
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
      this.tpmWindow.consumed = Math.max(0, this.tpmWindow.consumed - deduction.tpm);
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
