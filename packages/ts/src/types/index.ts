/**
 * Core type definitions for Factioner Service Pool System
 */

export type ServiceType =
  | 'llm'
  | 'document_intelligence'
  | 'form_recognizer'
  | 'ocr'
  | 'speech'
  | 'embedding'
  | 'custom'
  | string;

export enum JobPriority {
  CRITICAL = 0, // Health-checks, VIP users, synchronous interactive calls
  HIGH = 1,     // Interactive workflows
  NORMAL = 2,   // Standard operations (default)
  BATCH = 3     // Background bulk processing
}

export type AdmissionPolicy = 'reject' | 'drop_oldest' | 'drop_newest' | 'timeout';

export type BackoffStrategy = 'full_jitter' | 'equal_jitter' | 'decorrelated' | 'exponential';

export type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export type RoutingStrategy =
  | 'round_robin'
  | 'least_loaded'
  | 'weighted_latency'
  | 'priority_failover'
  | 'cost_optimized';

export type EndpointHealth = 'healthy' | 'degraded' | 'unhealthy' | 'cooling_off';

export enum FailureClass {
  TRANSIENT = 'TRANSIENT',     // Network glitch, 503, retryable
  OVERLOADED = 'OVERLOADED',   // 429, TPM/RPM limit hit, retryable with backoff / Retry-After
  UNAVAILABLE = 'UNAVAILABLE', // Endpoint down or circuit open, failover to next endpoint
  FATAL = 'FATAL'              // 400 Bad Request, 401 Unauthorized, unrecoverable
}

export interface FailureClassification {
  class: FailureClass;
  retryAfterMs?: number;
  message: string;
}

/**
 * Multi-dimensional capacity budget for heterogeneous AI & cloud services
 */
export interface ResourceBudgetConfig {
  rpm?: number;         // Requests Per Minute
  tpm?: number;         // Tokens Per Minute (LLMs / Embeddings)
  pagesPerMin?: number; // Pages Per Minute (Document Intelligence / Form Recognizer)
  rps?: number;         // Requests Per Second (Generic HTTP)
  concurrency?: number; // Max concurrent in-flight leases
}

export interface BudgetDeduction {
  rpm?: number;
  tpm?: number;
  pages?: number;
  rps?: number;
}

export interface SecretCredential {
  type: 'api_key' | 'bearer' | 'oauth2' | 'custom';
  value: string;
  headerName?: string; // defaults to 'Authorization' or 'api-key'
  maskedValue?: string;
}

export interface EndpointConfig {
  id: string;
  name?: string;
  url: string;
  credential?: SecretCredential;
  maxConcurrency: number;              // Max in-flight requests on this endpoint
  budget?: ResourceBudgetConfig;       // Multi-dimensional capacity limits (RPM, TPM, pages, etc.)
  weight?: number;                     // For weighted routing (default: 1)
  priorityTier?: number;               // 0 = primary, 1 = secondary/failover, etc.
  costPerRequest?: number;             // Normalized cost score (for cost-optimized router)
  timeoutMs?: number;                  // Individual request timeout in ms
  region?: string;                     // Regional label e.g. "eastus", "westeurope"
  metadata?: Record<string, unknown>;
}

export interface CircuitBreakerConfig {
  failureThresholdPercentage: number; // e.g. 50 (50% failures trigger trip)
  minimumRequestVolume: number;       // min requests in sampling window to trip (e.g. 5)
  samplingWindowMs: number;           // window for measuring errors (e.g. 10000 ms)
  coolingOffDurationMs: number;       // time to stay OPEN before HALF_OPEN (e.g. 15000 ms)
  halfOpenTrialSuccesses: number;     // successful consecutive probes to close (e.g. 3)
}

export interface BackoffConfig {
  strategy: BackoffStrategy;
  initialDelayMs: number; // e.g. 200 ms
  maxDelayMs: number;     // e.g. 10000 ms
  factor: number;         // e.g. 2.0
  jitter: boolean;        // whether to add jitter
  respectRetryAfterHeader: boolean;
}

export interface BackpressureConfig {
  maxQueueSize: number;                // Max pending jobs in queue
  admissionPolicy: AdmissionPolicy;
  queueTimeoutMs: number;              // Max time a job can wait in queue before expiring
  executionTimeoutMs: number;          // Max time for execution once dispatched
  admissionWaitTimeoutMs?: number;     // For 'timeout' admission policy: wait for space before failing
  sheddingThresholdPercentage?: number;// E.g. 85% full triggers proactive batch shedding
  globalRetryBudgetPercentage?: number;// Max % of jobs that can be retries (default: 20%)
}

export interface TenantConfig {
  id: string;
  maxRps?: number;
  maxTpm?: number;
  maxConcurrency?: number;
  dailyQuota?: number;
  maxAllowedPriority?: JobPriority;    // Prevents non-VIP tenants from claiming CRITICAL
  priorityBonus?: number;              // Priority boost (-1 to promote, +1 to demote)
}

export interface SecurityConfig {
  credentialVaultEncryptionKey?: string;
  redactSecretsInLogs: boolean;
  allowedTenants?: string[];
  enforceTenantIsolation: boolean;
  enablePIIRedaction: boolean;         // PII sanitization middleware
}

export interface ServicePoolOptions {
  name: string;
  serviceType: ServiceType;
  endpoints: EndpointConfig[];
  routingStrategy?: RoutingStrategy;
  backpressure?: Partial<BackpressureConfig>;
  circuitBreaker?: Partial<CircuitBreakerConfig>;
  backoff?: Partial<BackoffConfig>;
  security?: Partial<SecurityConfig>;
  maxRetries?: number;
  healthCheckIntervalMs?: number;
}

export interface SubmitOptions {
  priority?: JobPriority;
  tenantId?: string;
  queueTimeoutMs?: number;
  executionTimeoutMs?: number;
  maxRetries?: number;
  idempotencyKey?: string;
  estimatedBudget?: BudgetDeduction;    // Estimated token/page footprint for admission
  metadata?: Record<string, unknown>;
}

export interface Job<TReq = unknown, TRes = unknown> {
  id: string;
  idempotencyKey?: string;
  priority: JobPriority;
  payload: TReq;
  tenantId?: string;
  createdAt: number;
  queueTimeoutMs: number;
  executionTimeoutMs: number;
  estimatedBudget?: BudgetDeduction;
  retriesRemaining: number;
  totalAttempts: number;
  previousDecorrelatedDelay?: number;
  avoidEndpointIds: string[];
  tenantCharged: boolean;
  clearQueueTimer?: () => void;
  resolve: (value: JobResult<TRes>) => void;
  reject: (reason: unknown) => void;
  abortController: AbortController;
  metadata?: Record<string, unknown>;
}

export interface JobResult<TRes = unknown> {
  success: boolean;
  data?: TRes;
  error?: Error;
  endpointId?: string;
  attempts: number;
  durationMs: number;
  queueWaitMs: number;
  budgetDeducted?: BudgetDeduction;
  timestamp: number;
}

export interface OperationPollSnapshot<TRes = unknown> {
  done: boolean;
  status: string;
  data?: TRes;
}

export type PoolErrorCode = 'QUEUE_FULL' | 'SATURATED' | 'TENANT_DENIED';

export interface LongRunningOperationHandle<TRes = unknown> {
  operationId: string;
  status: 'running' | 'succeeded' | 'failed' | 'cancelled';
  /** Snapshot poll — running returns `{ done: false }` (does not throw). */
  poll: () => Promise<OperationPollSnapshot<TRes>>;
  cancel: () => Promise<{ status: 'cancelled'; upstreamCancelled: boolean }>;
}

export interface EndpointMetrics {
  id: string;
  url: string;
  health: EndpointHealth;
  circuitState: CircuitState;
  activeLeases: number;
  maxConcurrency: number;
  totalRequests: number;
  successfulRequests: number;
  failedRequests: number;
  avgLatencyMs: number;
  p95LatencyMs: number;
  budgetUsage?: {
    rpmConsumed: number;
    tpmConsumed: number;
    pagesConsumed: number;
  };
  effectiveRpm?: number;
  effectiveTpm?: number;
  signalTightened?: boolean;
}

export interface PoolMetrics {
  poolName: string;
  serviceType: ServiceType;
  activeJobs: number;
  queuedJobs: number;
  queueCapacity: number;
  queueSaturationPercentage: number;
  totalCompleted: number;
  totalFailed: number;
  totalRejected: number;
  totalRetries: number;
  averageLatencyMs: number;
  p95LatencyMs: number;
  throughputRps: number;
  endpoints: EndpointMetrics[];
}
