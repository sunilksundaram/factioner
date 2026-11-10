import { TenantConfig, SecurityConfig, JobPriority, BudgetDeduction } from '../types/index.js';

interface TenantState {
  config: TenantConfig;
  tokens: number;
  lastRefillTime: number;
  activeConcurrency: number;
  dailyConsumed: number;
  lastDailyReset: number;
  tpmTokens: number;
  lastTpmRefill: number;
}

export class TenantGovernor {
  private readonly tenants: Map<string, TenantState> = new Map();
  private readonly securityConfig: SecurityConfig;

  constructor(securityConfig: Partial<SecurityConfig> = {}) {
    this.securityConfig = {
      redactSecretsInLogs: securityConfig.redactSecretsInLogs ?? true,
      allowedTenants: securityConfig.allowedTenants,
      enforceTenantIsolation: securityConfig.enforceTenantIsolation ?? false,
      enablePIIRedaction: securityConfig.enablePIIRedaction ?? false
    };
  }

  public registerTenant(config: TenantConfig): void {
    const maxRps = config.maxRps ?? 50;
    this.tenants.set(config.id, {
      config,
      tokens: maxRps,
      lastRefillTime: Date.now(),
      activeConcurrency: 0,
      dailyConsumed: 0,
      lastDailyReset: Date.now(),
      tpmTokens: config.maxTpm ?? Number.POSITIVE_INFINITY,
      lastTpmRefill: Date.now()
    });
  }

  public checkEligibility(tenantId?: string, estimate?: BudgetDeduction): { allowed: boolean; reason?: string } {
    if (!tenantId) {
      if (this.securityConfig.enforceTenantIsolation) {
        return { allowed: false, reason: 'Tenant ID is required under strict tenant isolation policy' };
      }
      return { allowed: true };
    }

    if (this.securityConfig.allowedTenants && !this.securityConfig.allowedTenants.includes(tenantId)) {
      return { allowed: false, reason: `Tenant '${tenantId}' is not authorized to access this service pool` };
    }

    let state = this.tenants.get(tenantId);
    if (!state) {
      if (this.securityConfig.enforceTenantIsolation) {
        return { allowed: false, reason: `Tenant '${tenantId}' is not registered and auto-registration is disabled` };
      }
      this.registerTenant({ id: tenantId });
      state = this.tenants.get(tenantId)!;
    }

    const now = Date.now();
    if (now - state.lastDailyReset > 86400000) {
      state.dailyConsumed = 0;
      state.lastDailyReset = now;
    }

    if (state.config.dailyQuota && state.dailyConsumed >= state.config.dailyQuota) {
      return { allowed: false, reason: `Tenant '${tenantId}' exceeded daily quota of ${state.config.dailyQuota} calls` };
    }

    if (state.config.maxConcurrency && state.activeConcurrency >= state.config.maxConcurrency) {
      return { allowed: false, reason: `Tenant '${tenantId}' reached maximum concurrent jobs limit (${state.config.maxConcurrency})` };
    }

    const maxRps = state.config.maxRps ?? 50;
    const elapsedSeconds = (now - state.lastRefillTime) / 1000;
    const availableTokens = Math.min(maxRps, state.tokens + elapsedSeconds * maxRps);
    if (availableTokens < 1) {
      return { allowed: false, reason: `Tenant '${tenantId}' rate-limited (${maxRps} RPS exceeded)` };
    }

    if (state.config.maxTpm && estimate?.tpm) {
      const tpmElapsed = (now - state.lastTpmRefill) / 1000;
      const availableTpm = Math.min(state.config.maxTpm, state.tpmTokens + tpmElapsed * (state.config.maxTpm / 60));
      if (availableTpm < estimate.tpm) {
        return { allowed: false, reason: `Tenant '${tenantId}' TPM budget exceeded` };
      }
    }

    return { allowed: true };
  }

  public deductOnAdmission(tenantId?: string, estimate?: BudgetDeduction): void {
    if (!tenantId) return;
    const state = this.tenants.get(tenantId);
    if (!state) return;

    const now = Date.now();
    const maxRps = state.config.maxRps ?? 50;
    const elapsedSeconds = (now - state.lastRefillTime) / 1000;
    state.tokens = Math.min(maxRps, state.tokens + elapsedSeconds * maxRps);
    state.lastRefillTime = now;

    state.tokens = Math.max(0, state.tokens - 1);
    state.activeConcurrency += 1;
    state.dailyConsumed += 1;

    if (state.config.maxTpm && estimate?.tpm) {
      const tpmElapsed = (now - state.lastTpmRefill) / 1000;
      state.tpmTokens = Math.min(state.config.maxTpm, state.tpmTokens + tpmElapsed * (state.config.maxTpm / 60));
      state.lastTpmRefill = now;
      state.tpmTokens = Math.max(0, state.tpmTokens - estimate.tpm);
    }
  }

  public refundOnDrop(tenantId?: string, estimate?: BudgetDeduction): void {
    if (!tenantId) return;
    const state = this.tenants.get(tenantId);
    if (!state) return;

    if (state.activeConcurrency > 0) state.activeConcurrency -= 1;
    if (state.dailyConsumed > 0) state.dailyConsumed -= 1;
    state.tokens += 1;
    if (state.config.maxTpm && estimate?.tpm) {
      state.tpmTokens += estimate.tpm;
    }
  }

  public releaseSlot(tenantId?: string): void {
    if (!tenantId) return;
    const state = this.tenants.get(tenantId);
    if (state && state.activeConcurrency > 0) {
      state.activeConcurrency -= 1;
    }
  }

  public resolvePriority(tenantId: string | undefined, requested: JobPriority): JobPriority {
    if (!tenantId) {
      // Unregistered / anonymous: cannot elevate to CRITICAL
      return requested === JobPriority.CRITICAL ? JobPriority.HIGH : requested;
    }
    const state = this.tenants.get(tenantId);
    if (!state) {
      return requested === JobPriority.CRITICAL ? JobPriority.HIGH : requested;
    }

    let priority = requested;

    if (state.config.maxAllowedPriority !== undefined && priority < state.config.maxAllowedPriority) {
      priority = state.config.maxAllowedPriority;
    }

    if (state.config.priorityBonus) {
      const adjusted = priority + state.config.priorityBonus;
      priority = Math.max(JobPriority.CRITICAL, Math.min(JobPriority.BATCH, adjusted));
    }

    return priority;
  }
}
