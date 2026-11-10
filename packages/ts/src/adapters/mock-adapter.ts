import {
  ServiceAdapter,
  UnaryExecutionResult,
  StreamExecutionResult,
  OperationSubmissionResult,
  OperationPollResult
} from './base-adapter.js';
import { EndpointConfig, FailureClassification } from '../types/index.js';
import { ErrorClassifier } from '../resilience/retry-manager.js';

export interface MockAdapterOptions {
  baseLatencyMs?: number;
  jitterMs?: number;
  failureRatePercentage?: number;
  failWithStatus?: number;
}

export class MockServiceAdapter<TReq = unknown, TRes = unknown> implements ServiceAdapter<TReq, TRes> {
  public readonly name = 'mock-service-adapter';
  private readonly options: MockAdapterOptions;
  public totalCalls: number = 0;

  constructor(options: MockAdapterOptions = {}) {
    this.options = {
      baseLatencyMs: options.baseLatencyMs ?? 20,
      jitterMs: options.jitterMs ?? 10,
      failureRatePercentage: options.failureRatePercentage ?? 0,
      failWithStatus: options.failWithStatus ?? 429
    };
  }

  public estimateBudget(_payload: TReq) {
    return { rpm: 1, tpm: 120 };
  }

  public async execute(
    endpoint: EndpointConfig,
    payload: TReq,
    signal: AbortSignal,
    _authHeaders: Record<string, string>
  ): Promise<UnaryExecutionResult<TRes>> {
    this.totalCalls++;

    const jitter = Math.floor(Math.random() * (this.options.jitterMs ?? 10));
    const delay = (this.options.baseLatencyMs ?? 20) + jitter;

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => resolve(), delay);
      signal.addEventListener('abort', () => {
        clearTimeout(timer);
        reject(new Error('Operation aborted'));
      });
    });

    if (this.options.failureRatePercentage && Math.random() * 100 < this.options.failureRatePercentage) {
      const status = this.options.failWithStatus ?? 429;
      const err: any = new Error(`Mock endpoint '${endpoint.id}' simulated failure with HTTP ${status}`);
      err.status = status;
      err.headers = status === 429 ? { 'retry-after': '1' } : {};
      throw err;
    }

    const data = {
      status: 'success',
      endpointId: endpoint.id,
      endpointUrl: endpoint.url,
      receivedPayload: payload,
      processedAt: new Date().toISOString()
    } as unknown as TRes;

    return {
      data,
      budgetDeduction: { rpm: 1, tpm: 120 }
    };
  }

  public async executeStream(
    endpoint: EndpointConfig,
    _payload: TReq,
    signal: AbortSignal,
    _authHeaders: Record<string, string>
  ): Promise<StreamExecutionResult> {
    this.totalCalls++;
    const tokens = ['Hello', ' from', ' Factioner', ' streaming', ' pool!'];

    const stream = {
      async *[Symbol.asyncIterator]() {
        for (const token of tokens) {
          if (signal.aborted) throw new Error('Stream aborted');
          await new Promise(r => setTimeout(r, 20));
          yield token;
        }
      }
    };

    return {
      stream,
      getBudgetDeduction: () => ({ rpm: 1, tpm: 50 })
    };
  }

  public async submitOperation(
    _endpoint: EndpointConfig,
    _payload: TReq,
    _signal: AbortSignal,
    _authHeaders: Record<string, string>
  ): Promise<OperationSubmissionResult> {
    this.totalCalls++;
    return {
      operationId: 'mock-op-12345',
      initialBudget: { rpm: 1, pages: 1 }
    };
  }

  public async pollOperation(
    _endpoint: EndpointConfig,
    _operationId: string,
    _signal: AbortSignal,
    _authHeaders: Record<string, string>
  ): Promise<OperationPollResult<TRes>> {
    return {
      done: true,
      status: 'succeeded',
      data: { status: 'operation_complete' } as unknown as TRes,
      budgetDeduction: { pages: 2, rpm: 1 }
    };
  }

  public classifyError(err: unknown): FailureClassification {
    return ErrorClassifier.classify(err);
  }

  public async healthCheck(_endpoint: EndpointConfig, _authHeaders: Record<string, string>): Promise<boolean> {
    return true;
  }
}
