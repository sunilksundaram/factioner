import { ServiceAdapter, UnaryExecutionResult } from './base-adapter.js';
import { EndpointConfig, FailureClassification } from '../types/index.js';
import { ErrorClassifier } from '../resilience/retry-manager.js';
import { headersToRecord } from '../utils/headers.js';

export interface EmbeddingRequest {
  model?: string;
  input: string | string[];
  dimensions?: number;
  encodingFormat?: 'float' | 'base64';
}

export interface EmbeddingItem {
  index: number;
  embedding: number[];
}

export interface EmbeddingResponse {
  model: string;
  data: EmbeddingItem[];
  usage?: {
    promptTokens: number;
    totalTokens: number;
  };
  raw?: unknown;
}

/**
 * High-throughput Adapter for Vector Embedding models
 */
export class EmbeddingAdapter implements ServiceAdapter<EmbeddingRequest, EmbeddingResponse> {
  public readonly name = 'embedding-service-adapter';

  public estimateBudget(payload: EmbeddingRequest) {
    const textLen = typeof payload.input === 'string'
      ? payload.input.length
      : payload.input.reduce((n, s) => n + (s?.length ?? 0), 0);
    return { rpm: 1, tpm: Math.max(1, Math.ceil(textLen / 4)) };
  }

  public validatePayload(payload: EmbeddingRequest): void {
    if (!payload.input) {
      throw new Error('Embedding input is required (string or array of strings)');
    }
    if (Array.isArray(payload.input) && payload.input.length === 0) {
      throw new Error('Embedding input array must not be empty');
    }
  }

  public async execute(
    endpoint: EndpointConfig,
    payload: EmbeddingRequest,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<UnaryExecutionResult<EmbeddingResponse>> {
    const defaultModel = (endpoint.metadata?.model as string) || payload.model || 'text-embedding-3-small';
    const body = JSON.stringify({
      model: defaultModel,
      input: payload.input,
      dimensions: payload.dimensions,
      encoding_format: payload.encodingFormat ?? 'float'
    });

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'User-Agent': 'Factioner-Embedding/1.0',
      ...authHeaders
    };

    const res = await fetch(endpoint.url, {
      method: 'POST',
      headers,
      body,
      signal
    });

    if (!res.ok) {
      const errText = await res.text().catch(() => '');
      const err: any = new Error(`Embedding endpoint '${endpoint.id}' failed (${res.status} ${res.statusText}): ${errText}`);
      err.status = res.status;
      err.headers = headersToRecord(res.headers);
      throw err;
    }

    const data: any = await res.json();
    const totalTokens = data.usage?.total_tokens ?? (typeof payload.input === 'string' ? Math.ceil(payload.input.length / 4) : 100);

    return {
      data: {
        model: data.model ?? defaultModel,
        data: data.data?.map((d: any) => ({
          index: d.index ?? 0,
          embedding: d.embedding
        })) ?? [],
        usage: {
          promptTokens: data.usage?.prompt_tokens ?? 0,
          totalTokens
        },
        raw: data
      },
      budgetDeduction: {
        rpm: 1,
        tpm: totalTokens
      }
    };
  }

  public classifyError(err: unknown): FailureClassification {
    return ErrorClassifier.classify(err);
  }

  public async healthCheck(endpoint: EndpointConfig, authHeaders: Record<string, string>): Promise<boolean> {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 2000);
      const res = await fetch(endpoint.url, {
        method: 'HEAD',
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
