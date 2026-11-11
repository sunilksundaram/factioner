import { ServiceAdapter, UnaryExecutionResult, StreamExecutionResult } from './base-adapter.js';
import { EndpointConfig, BudgetDeduction, FailureClassification } from '../types/index.js';
import { ErrorClassifier } from '../resilience/retry-manager.js';
import { headersToRecord } from '../utils/headers.js';
import { estimateTokensFromParts } from '../utils/tokenizer.js';
import { resolveSpilledPayload } from '../spill/blob-spill.js';

export interface LLMMessage {
  role: 'system' | 'user' | 'assistant' | 'tool';
  content: string;
  name?: string;
}

export interface LLMRequest {
  model?: string;
  messages: LLMMessage[];
  temperature?: number;
  maxTokens?: number;
  topP?: number;
}

export interface LLMResponse {
  id?: string;
  model?: string;
  content: string;
  usage?: {
    promptTokens: number;
    completionTokens: number;
    totalTokens: number;
  };
  raw?: unknown;
}

/**
 * Universal LLM Adapter with native provider formatting for OpenAI,
 * Anthropic, and Gemini, reporting accurate TPM/RPM budgets.
 */
export class LLMAdapter implements ServiceAdapter<LLMRequest, LLMResponse> {
  public readonly name = 'llm-universal-adapter';

  public estimateBudget(payload: LLMRequest) {
    const promptTokens = estimateTokensFromParts(payload.messages.map(m => m.content));
    return { rpm: 1, tpm: promptTokens + (payload.maxTokens ?? 1024) };
  }

  public validatePayload(payload: LLMRequest): void {
    if (!payload.messages || !Array.isArray(payload.messages) || payload.messages.length === 0) {
      throw new Error('LLM payload requires a non-empty messages array');
    }
  }

  public async execute(
    endpoint: EndpointConfig,
    payload: LLMRequest,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<UnaryExecutionResult<LLMResponse>> {
    payload = resolveSpilledPayload(payload as unknown as Record<string, unknown>) as unknown as LLMRequest;
    const isAnthropic = endpoint.url.includes('anthropic.com');
    const isGemini = endpoint.url.includes('googleapis.com');

    let bodyStr: string;
    let headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'User-Agent': 'Factioner-ServicePool/1.0',
      ...authHeaders
    };

    const defaultModel = (endpoint.metadata?.model as string) || payload.model || 'gpt-4o';

    if (isAnthropic) {
      headers['anthropic-version'] = '2023-06-01';
      if (authHeaders['api-key']) {
        headers['x-api-key'] = authHeaders['api-key'];
        delete headers['api-key'];
      }
      bodyStr = JSON.stringify({
        model: defaultModel,
        messages: payload.messages.filter(m => m.role !== 'system'),
        system: payload.messages.find(m => m.role === 'system')?.content,
        max_tokens: payload.maxTokens ?? 1024,
        temperature: payload.temperature ?? 0.7
      });
    } else if (isGemini) {
      const contents = payload.messages.map(m => ({
        role: m.role === 'assistant' ? 'model' : 'user',
        parts: [{ text: m.content }]
      }));
      bodyStr = JSON.stringify({ contents });
    } else {
      // OpenAI / Azure OpenAI / vLLM / Ollama
      bodyStr = JSON.stringify({
        model: defaultModel,
        messages: payload.messages,
        temperature: payload.temperature ?? 0.7,
        max_tokens: payload.maxTokens ?? 1024,
        stream: false
      });
    }

    const res = await fetch(endpoint.url, {
      method: 'POST',
      headers,
      body: bodyStr,
      signal
    });

    if (!res.ok) {
      const errText = await res.text().catch(() => '');
      const err: any = new Error(`LLM endpoint '${endpoint.id}' failed (${res.status} ${res.statusText}): ${errText}`);
      err.status = res.status;
      err.headers = headersToRecord(res.headers);
      throw err;
    }

    const data: any = await res.json();

    let content = '';
    let promptTokens = 0;
    let completionTokens = 0;

    if (isAnthropic) {
      content = data.content?.[0]?.text ?? '';
      promptTokens = data.usage?.input_tokens ?? 0;
      completionTokens = data.usage?.output_tokens ?? 0;
    } else if (isGemini) {
      content = data.candidates?.[0]?.content?.parts?.[0]?.text ?? '';
      promptTokens = data.usageMetadata?.promptTokenCount ?? 0;
      completionTokens = data.usageMetadata?.candidatesTokenCount ?? 0;
    } else {
      content = data.choices?.[0]?.message?.content ?? '';
      promptTokens = data.usage?.prompt_tokens ?? 0;
      completionTokens = data.usage?.completion_tokens ?? 0;
    }

    const totalTokens = promptTokens + completionTokens || Math.ceil((bodyStr.length + content.length) / 4);

    return {
      data: {
        id: data.id,
        model: data.model ?? defaultModel,
        content,
        usage: {
          promptTokens,
          completionTokens,
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

  public async executeStream(
    endpoint: EndpointConfig,
    payload: LLMRequest,
    signal: AbortSignal,
    authHeaders: Record<string, string>
  ): Promise<StreamExecutionResult> {
    payload = resolveSpilledPayload(payload as unknown as Record<string, unknown>) as unknown as LLMRequest;
    const isAnthropic = endpoint.url.includes('anthropic.com');
    const isGemini = endpoint.url.includes('googleapis.com');
    const defaultModel = (endpoint.metadata?.model as string) || payload.model || 'gpt-4o';

    let bodyStr: string;
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'User-Agent': 'Factioner-LLM-Stream/1.0',
      ...authHeaders
    };

    if (isAnthropic) {
      headers['anthropic-version'] = '2023-06-01';
      if (authHeaders['api-key']) {
        headers['x-api-key'] = authHeaders['api-key'];
        delete headers['api-key'];
      }
      bodyStr = JSON.stringify({
        model: defaultModel,
        messages: payload.messages.filter(m => m.role !== 'system'),
        system: payload.messages.find(m => m.role === 'system')?.content,
        max_tokens: payload.maxTokens ?? 1024,
        temperature: payload.temperature ?? 0.7,
        stream: true
      });
    } else if (isGemini) {
      const contents = payload.messages.map(m => ({
        role: m.role === 'assistant' ? 'model' : 'user',
        parts: [{ text: m.content }]
      }));
      bodyStr = JSON.stringify({
        contents,
        generationConfig: {
          temperature: payload.temperature ?? 0.7,
          maxOutputTokens: payload.maxTokens ?? 1024
        }
      });
    } else {
      bodyStr = JSON.stringify({
        model: defaultModel,
        messages: payload.messages,
        temperature: payload.temperature ?? 0.7,
        max_tokens: payload.maxTokens ?? 1024,
        stream: true
      });
    }

    const streamUrl = isGemini && !endpoint.url.includes(':streamGenerateContent')
      ? (endpoint.url.includes(':generateContent')
          ? endpoint.url.replace(':generateContent', ':streamGenerateContent')
          : `${endpoint.url}${endpoint.url.includes('?') ? '&' : '?'}alt=sse`)
      : endpoint.url;

    const res = await fetch(streamUrl, {
      method: 'POST',
      headers,
      body: bodyStr,
      signal
    });

    if (!res.ok) {
      const errText = await res.text().catch(() => '');
      const err: any = new Error(`LLM streaming failed (${res.status} ${res.statusText}): ${errText}`);
      err.status = res.status;
      err.headers = headersToRecord(res.headers);
      throw err;
    }

    if (!res.body) {
      throw new Error('LLM response body is empty');
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder('utf-8');
    let totalStreamedChars = 0;

    const stream = {
      async *[Symbol.asyncIterator]() {
        let buffer = '';
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() ?? '';

            for (const line of lines) {
              const trimmed = line.trim();
              if (!trimmed || trimmed.startsWith(':')) continue;
              if (trimmed === 'data: [DONE]') return;

              if (trimmed.startsWith('data: ')) {
                try {
                  const parsed = JSON.parse(trimmed.slice(6));
                  // OpenAI / Anthropic / Gemini stream shapes
                  const delta =
                    parsed.choices?.[0]?.delta?.content ??
                    parsed.delta?.text ??
                    (parsed.type === 'content_block_delta' ? parsed.delta?.text : undefined) ??
                    parsed.candidates?.[0]?.content?.parts?.[0]?.text ??
                    '';
                  if (delta) {
                    totalStreamedChars += delta.length;
                    yield delta;
                  }
                } catch {
                  // ignore JSON parse errors in stream chunks
                }
              }
            }
          }
        } finally {
          reader.releaseLock();
        }
      }
    };

    return {
      stream,
      getBudgetDeduction: () => ({
        rpm: 1,
        tpm: Math.ceil((bodyStr.length + totalStreamedChars) / 4)
      })
    };
  }

  public classifyError(err: unknown): FailureClassification {
    return ErrorClassifier.classify(err);
  }

  public async healthCheck(endpoint: EndpointConfig, authHeaders: Record<string, string>): Promise<boolean> {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 3000);
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
