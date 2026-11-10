import { EndpointConfig, BudgetDeduction, FailureClassification } from '../types/index.js';

export interface UnaryExecutionResult<TRes> {
  data: TRes;
  budgetDeduction: BudgetDeduction;
}

export interface StreamExecutionResult {
  stream: AsyncIterable<string>;
  getBudgetDeduction: () => BudgetDeduction;
}

export interface OperationSubmissionResult {
  operationId: string;
  initialBudget: BudgetDeduction;
}

export interface OperationPollResult<TRes> {
  done: boolean;
  data?: TRes;
  status: 'running' | 'succeeded' | 'failed' | 'cancelled';
  budgetDeduction?: BudgetDeduction;
}

/**
 * ServiceAdapter defines the translation and execution contract
 * between generic pool jobs and target service protocols.
 */
export interface ServiceAdapter<TReq = unknown, TRes = unknown> {
  name: string;

  /** Estimate multi-dimensional budget before admission / acquire. */
  estimateBudget?(payload: TReq): BudgetDeduction;

  execute(
    endpoint: EndpointConfig,
    payload: TReq,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<UnaryExecutionResult<TRes>>;

  executeStream?(
    endpoint: EndpointConfig,
    payload: TReq,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<StreamExecutionResult>;

  submitOperation?(
    endpoint: EndpointConfig,
    payload: TReq,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<OperationSubmissionResult>;

  pollOperation?(
    endpoint: EndpointConfig,
    operationId: string,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<OperationPollResult<TRes>>;

  cancelOperation?(
    endpoint: EndpointConfig,
    operationId: string,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<void>;

  classifyError(err: unknown): FailureClassification;

  healthCheck(endpoint: EndpointConfig, authHeaders: Record<string, string>): Promise<boolean>;

  validatePayload?(payload: TReq): void;
}
