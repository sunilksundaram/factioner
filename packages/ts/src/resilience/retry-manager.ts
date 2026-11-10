import { FailureClass, FailureClassification } from '../types/index.js';

/**
 * Standard protocol error classifier that adapters can invoke or extend.
 */
export class ErrorClassifier {
  private static readonly TRANSIENT_HTTP_STATUSES = new Set([408, 500, 502, 503, 504]);
  private static readonly OVERLOADED_HTTP_STATUSES = new Set([429]);
  private static readonly FATAL_HTTP_STATUSES = new Set([400, 401, 403, 404, 422]);

  /** Transport failures that usually mean "try another endpoint". */
  private static readonly UNAVAILABLE_ERROR_CODES = new Set([
    'ECONNREFUSED',
    'ENOTFOUND',
    'EAI_AGAIN'
  ]);

  private static readonly TRANSIENT_ERROR_CODES = new Set([
    'ECONNRESET',
    'ETIMEDOUT',
    'EPIPE',
    'UND_ERR_CONNECT_TIMEOUT'
  ]);

  public static classify(error: unknown): FailureClassification {
    const rawMsg = error instanceof Error ? error.message : String(error);

    if (!error || typeof error !== 'object') {
      return { class: FailureClass.FATAL, message: rawMsg };
    }

    const status = (error as { status?: number; statusCode?: number }).status ??
                   (error as { status?: number; statusCode?: number }).statusCode;

    const retryAfterMs = this.parseRetryAfterToMs(this.extractRetryAfter(error));

    if (typeof status === 'number') {
      if (this.OVERLOADED_HTTP_STATUSES.has(status)) {
        return {
          class: FailureClass.OVERLOADED,
          retryAfterMs: retryAfterMs ?? 1000,
          message: rawMsg
        };
      }

      if (this.TRANSIENT_HTTP_STATUSES.has(status)) {
        return { class: FailureClass.TRANSIENT, message: rawMsg };
      }

      if (this.FATAL_HTTP_STATUSES.has(status)) {
        return { class: FailureClass.FATAL, message: rawMsg };
      }
    }

    const code = (error as { code?: string }).code;
    if (typeof code === 'string' && this.UNAVAILABLE_ERROR_CODES.has(code)) {
      return { class: FailureClass.UNAVAILABLE, message: rawMsg };
    }
    if (typeof code === 'string' && this.TRANSIENT_ERROR_CODES.has(code)) {
      return { class: FailureClass.TRANSIENT, message: rawMsg };
    }

    const lower = rawMsg.toLowerCase();
    if (lower.includes('rate limit') || lower.includes('quota exceeded') || lower.includes('too many requests')) {
      return { class: FailureClass.OVERLOADED, retryAfterMs: retryAfterMs ?? 1000, message: rawMsg };
    }

    if (
      lower.includes('circuit open') ||
      lower.includes('endpoint down') ||
      lower.includes('econnrefused') ||
      lower.includes('enotfound') ||
      lower.includes('unhealthy')
    ) {
      return { class: FailureClass.UNAVAILABLE, message: rawMsg };
    }

    if (lower.includes('timeout') || lower.includes('fetch failed') || lower.includes('socket hang up') || lower.includes('overloaded') || lower.includes('econnreset')) {
      return { class: FailureClass.TRANSIENT, message: rawMsg };
    }

    return { class: FailureClass.FATAL, message: rawMsg };
  }

  /** Parse HTTP Retry-After delta-seconds (string or number) into milliseconds. */
  public static parseRetryAfterToMs(value: string | number | undefined): number | undefined {
    if (value === undefined) return undefined;
    if (typeof value === 'number' && Number.isFinite(value)) {
      return Math.max(0, value * 1000);
    }
    const str = String(value).trim();
    if (/^\d+$/.test(str)) {
      return Math.max(0, parseInt(str, 10) * 1000);
    }
    const dateParsed = Date.parse(str);
    if (!Number.isNaN(dateParsed)) {
      return Math.max(0, dateParsed - Date.now());
    }
    return undefined;
  }

  public static extractRetryAfter(error: unknown): string | number | undefined {
    if (!error || typeof error !== 'object') return undefined;

    const headers = (error as { headers?: Record<string, string | number> }).headers;
    if (headers) {
      const val = headers['retry-after'] ?? headers['Retry-After'];
      if (val !== undefined) return val;
    }

    const response = (error as { response?: { headers?: Record<string, string | number> } }).response;
    if (response?.headers) {
      const val = response.headers['retry-after'] ?? response.headers['Retry-After'];
      if (val !== undefined) return val;
    }

    return undefined;
  }
}
