/**
 * PIIGuard provides automated detection and redaction of Personally Identifiable Information
 * (PII) before requests leave the perimeter to external AI/Cloud services.
 */
export class PIIGuard {
  private static readonly EMAIL_REGEX = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  private static readonly PHONE_REGEX = /(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
  private static readonly SSN_REGEX = /\b\d{3}-\d{2}-\d{4}\b/g;
  private static readonly CREDIT_CARD_REGEX = /\b(?:\d{4}[-\s]?){3}\d{4}\b/g;

  /**
   * Sanitizes string by replacing detected PII with placeholders
   */
  public static sanitizeText(text: string): { sanitized: string; piiDetectedCount: number } {
    let count = 0;
    let sanitized = text;

    sanitized = sanitized.replace(this.EMAIL_REGEX, () => {
      count++;
      return '[REDACTED_EMAIL]';
    });

    sanitized = sanitized.replace(this.PHONE_REGEX, () => {
      count++;
      return '[REDACTED_PHONE]';
    });

    sanitized = sanitized.replace(this.SSN_REGEX, () => {
      count++;
      return '[REDACTED_SSN]';
    });

    sanitized = sanitized.replace(this.CREDIT_CARD_REGEX, () => {
      count++;
      return '[REDACTED_CARD]';
    });

    return { sanitized, piiDetectedCount: count };
  }

  /**
   * Recursively sanitizes any object or payload
   */
  public static sanitizePayload<T>(payload: T): T {
    if (typeof payload === 'string') {
      return this.sanitizeText(payload).sanitized as unknown as T;
    }

    if (Array.isArray(payload)) {
      return payload.map(item => this.sanitizePayload(item)) as unknown as T;
    }

    if (payload !== null && typeof payload === 'object') {
      const sanitizedObj: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(payload)) {
        sanitizedObj[key] = this.sanitizePayload(value);
      }
      return sanitizedObj as unknown as T;
    }

    return payload;
  }
}

