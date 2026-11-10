import { ServiceAdapter, UnaryExecutionResult } from './base-adapter.js';
import { EndpointConfig, FailureClassification } from '../types/index.js';
import { ErrorClassifier } from '../resilience/retry-manager.js';
import { headersToRecord } from '../utils/headers.js';

export interface GenericHttpRequest {
  path?: string;
  method?: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH' | 'HEAD';
  headers?: Record<string, string>;
  body?: unknown;
  queryParams?: Record<string, string | number>;
}

/**
 * Hardened Generic HTTP Adapter with SSRF protection and header precedence defense.
 */
export class GenericHttpAdapter implements ServiceAdapter<GenericHttpRequest, unknown> {
  public readonly name = 'generic-http-adapter';

  public estimateBudget() {
    return { rps: 1, rpm: 1 };
  }

  public validatePayload(payload: GenericHttpRequest): void {
    if (payload.path) {
      // SSRF defense: Prevent path traversal and external URL injection
      if (
        payload.path.includes('://') ||
        payload.path.startsWith('//') ||
        payload.path.includes('..') ||
        !/^[a-zA-Z0-9_\-\.\/]*$/.test(payload.path)
      ) {
        throw new Error(`SSRF validation failed: Invalid or dangerous path expression '${payload.path}'`);
      }
    }
  }

  public async execute(
    endpoint: EndpointConfig,
    payload: GenericHttpRequest,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<UnaryExecutionResult<unknown>> {
    this.validatePayload(payload);

    const safePath = payload.path ? payload.path.replace(/^\/+/, '') : '';
    const base = endpoint.url.endsWith('/') ? endpoint.url : `${endpoint.url}/`;
    const targetUrl = new URL(safePath, base);

    if (payload.queryParams) {
      for (const [key, val] of Object.entries(payload.queryParams)) {
        targetUrl.searchParams.append(key, String(val));
      }
    }

    // Security: Caller headers are applied FIRST, official authHeaders are applied LAST so they cannot be overwritten!
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'User-Agent': 'Factioner-HttpGateway/1.0',
      ...(payload.headers ?? {}),
      ...authHeaders // Overrides any spoofed authorization headers from caller
    };

    const method = payload.method ?? 'POST';
    const body = payload.body
      ? (typeof payload.body === 'string' ? payload.body : JSON.stringify(payload.body))
      : undefined;

    const res = await fetch(targetUrl.toString(), {
      method,
      headers,
      body: method !== 'GET' && method !== 'HEAD' ? body : undefined,
      signal
    });

    if (!res.ok) {
      const errText = await res.text().catch(() => '');
      const err: any = new Error(`Endpoint '${endpoint.id}' failed (${res.status} ${res.statusText}): ${errText}`);
      err.status = res.status;
      err.headers = headersToRecord(res.headers);
      throw err;
    }

    const contentType = res.headers.get('content-type') || '';
    let data: unknown;
    if (contentType.includes('application/json')) {
      data = await res.json();
    } else {
      data = await res.text();
    }

    return {
      data,
      budgetDeduction: { rps: 1 }
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
