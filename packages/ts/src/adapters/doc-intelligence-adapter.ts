import {
  ServiceAdapter,
  UnaryExecutionResult,
  OperationSubmissionResult,
  OperationPollResult
} from './base-adapter.js';
import { EndpointConfig, FailureClassification, BudgetDeduction } from '../types/index.js';
import { ErrorClassifier } from '../resilience/retry-manager.js';
import { headersToRecord } from '../utils/headers.js';

export interface DocumentAnalysisRequest {
  modelId?: string;
  urlSource?: string;
  base64Source?: string;
  pages?: string;
}

export interface DocumentAnalysisResponse {
  modelId: string;
  status: 'succeeded' | 'failed' | 'running';
  content?: string;
  pages?: Array<{
    pageNumber: number;
    lines?: string[];
  }>;
  tables?: Array<{
    rowCount: number;
    columnCount: number;
  }>;
  raw?: unknown;
}

function assertPollUrlAllowed(endpoint: EndpointConfig, operationId: string): void {
  if (operationId === 'direct') return;
  let pollUrl: URL;
  try {
    pollUrl = new URL(operationId);
  } catch {
    throw new Error(`Invalid operation-location URL: ${operationId}`);
  }
  const endpointUrl = new URL(endpoint.url);
  const allowlist = (endpoint.metadata?.pollOriginAllowlist as string[] | undefined) ?? [endpointUrl.origin];
  if (!allowlist.includes(pollUrl.origin)) {
    throw new Error(`SSRF blocked: operation-location origin '${pollUrl.origin}' is not allowed`);
  }
}

export class DocumentIntelligenceAdapter
  implements ServiceAdapter<DocumentAnalysisRequest, DocumentAnalysisResponse>
{
  public readonly name = 'document-intelligence-adapter';

  public estimateBudget(_payload: DocumentAnalysisRequest): BudgetDeduction {
    return { rpm: 1, pages: 1 };
  }

  public validatePayload(payload: DocumentAnalysisRequest): void {
    if (!payload.urlSource && !payload.base64Source) {
      throw new Error('Document analysis requires either urlSource or base64Source');
    }
  }

  public async execute(
    endpoint: EndpointConfig,
    payload: DocumentAnalysisRequest,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<UnaryExecutionResult<DocumentAnalysisResponse>> {
    const sub = await this.submitOperation(endpoint, payload, signal, authHeaders);

    if (sub.operationId === 'direct') {
      return {
        data: { modelId: payload.modelId ?? 'prebuilt', status: 'succeeded' },
        budgetDeduction: sub.initialBudget
      };
    }

    const maxPolls = 120;
    for (let i = 0; i < maxPolls; i++) {
      if (signal.aborted) throw new Error('Aborted');
      const pollResult = await this.pollOperation(endpoint, sub.operationId, signal, authHeaders);
      if (pollResult.done) {
        if (pollResult.status === 'succeeded' && pollResult.data) {
          return {
            data: pollResult.data,
            budgetDeduction: pollResult.budgetDeduction ?? { rpm: 1, pages: 1 }
          };
        }
        throw new Error(`Document analysis failed with status ${pollResult.status}`);
      }
      await new Promise(r => setTimeout(r, 1000));
    }
    throw new Error('Document analysis timed out while polling');
  }

  public async submitOperation(
    endpoint: EndpointConfig,
    payload: DocumentAnalysisRequest,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<OperationSubmissionResult> {
    const modelId = payload.modelId || (endpoint.metadata?.defaultModel as string) || 'prebuilt-layout';
    const targetUrl = endpoint.url.includes('{modelId}')
      ? endpoint.url.replace('{modelId}', encodeURIComponent(modelId))
      : `${endpoint.url.replace(/\/$/, '')}/documentModels/${encodeURIComponent(modelId)}:analyze?api-version=2024-02-29-preview`;

    const requestBody = payload.urlSource
      ? JSON.stringify({ urlSource: payload.urlSource })
      : JSON.stringify({ base64Source: payload.base64Source });

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'User-Agent': 'Factioner-DocIntel/1.0',
      ...authHeaders
    };

    const res = await fetch(targetUrl, {
      method: 'POST',
      headers,
      body: requestBody,
      signal
    });

    if (res.status === 202) {
      const operationLocation = res.headers.get('operation-location');
      if (!operationLocation) {
        throw new Error('Document Intelligence returned 202 without operation-location header');
      }
      assertPollUrlAllowed(endpoint, operationLocation);
      return {
        operationId: operationLocation,
        initialBudget: { rpm: 1, pages: 1 }
      };
    }

    if (!res.ok) {
      const errText = await res.text().catch(() => '');
      const err: any = new Error(`Document Intelligence failed (${res.status} ${res.statusText}): ${errText}`);
      err.status = res.status;
      err.headers = headersToRecord(res.headers);
      throw err;
    }

    return {
      operationId: 'direct',
      initialBudget: { rpm: 1, pages: 1 }
    };
  }

  public async pollOperation(
    endpoint: EndpointConfig,
    operationId: string,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<OperationPollResult<DocumentAnalysisResponse>> {
    if (operationId === 'direct') {
      return {
        done: true,
        status: 'succeeded',
        data: { modelId: 'prebuilt', status: 'succeeded' },
        budgetDeduction: { rpm: 1, pages: 1 }
      };
    }

    assertPollUrlAllowed(endpoint, operationId);

    const res = await fetch(operationId, {
      method: 'GET',
      headers: { ...authHeaders },
      signal
    });

    if (!res.ok) {
      const errText = await res.text().catch(() => '');
      const err: any = new Error(`Polling failed with status ${res.status}: ${errText}`);
      err.status = res.status;
      throw err;
    }

    const data: any = await res.json();
    const status = data.status;

    if (status === 'succeeded') {
      const analyzeResult = data.analyzeResult ?? data;
      const pageCount = analyzeResult.pages?.length ?? 1;

      return {
        done: true,
        status: 'succeeded',
        data: {
          modelId: data.modelId ?? 'prebuilt',
          status: 'succeeded',
          content: analyzeResult.content ?? '',
          pages: analyzeResult.pages?.map((p: any) => ({
            pageNumber: p.pageNumber,
            lines: p.lines?.map((l: any) => l.content)
          })),
          tables: analyzeResult.tables?.map((t: any) => ({
            rowCount: t.rowCount,
            columnCount: t.columnCount
          })),
          raw: data
        },
        budgetDeduction: { pages: pageCount, rpm: 1 }
      };
    }

    if (status === 'failed') {
      return {
        done: true,
        status: 'failed',
        data: { modelId: 'prebuilt', status: 'failed', raw: data.error }
      };
    }

    return {
      done: false,
      status: 'running'
    };
  }

  public classifyError(err: unknown): FailureClassification {
    return ErrorClassifier.classify(err);
  }

  public async healthCheck(endpoint: EndpointConfig, authHeaders: Record<string, string>): Promise<boolean> {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 3000);
      const res = await fetch(`${endpoint.url.replace(/\/$/, '')}/info?api-version=2024-02-29-preview`, {
        method: 'GET',
        headers: { ...authHeaders },
        signal: controller.signal
      });
      clearTimeout(timer);
      return res.status < 500;
    } catch {
      return false;
    }
  }
}
