import { mkdirSync, writeFileSync, readFileSync, unlinkSync, existsSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';

export const SPILL_URI_PREFIX = 'spill://';

let defaultSpillStore: BlobSpillStore | undefined;

/** Process-wide spill store used by adapters to resolve spill:// URIs. */
export function configureSpillStore(store: BlobSpillStore | undefined): void {
  defaultSpillStore = store;
}

export function getSpillStore(): BlobSpillStore | undefined {
  return defaultSpillStore;
}

export function resolveSpilledPayload<T extends Record<string, unknown>>(payload: T): T {
  return defaultSpillStore ? defaultSpillStore.resolvePayload(payload) : payload;
}

export function cleanupSpilledPayload(payload: unknown): void {
  defaultSpillStore?.deletePayloadSpills(payload);
}

export interface BlobSpillOptions {
  /** Directory for spilled blobs (created if missing). */
  dir: string;
  /** Spill string fields at or above this size (default 256 KiB). */
  thresholdBytes?: number;
  /** Delete spills older than this (default 1h). */
  ttlMs?: number;
}

export interface SpillRef {
  uri: string;
  path: string;
  bytes: number;
}

/**
 * Local filesystem spill so large inline payloads (base64 docs, big prompts)
 * are not retained in the in-memory job queue. Adapters resolve spill:// URIs at execute time.
 */
export class BlobSpillStore {
  private readonly dir: string;
  private readonly thresholdBytes: number;
  private readonly ttlMs: number;

  constructor(options: BlobSpillOptions) {
    this.dir = resolve(options.dir);
    this.thresholdBytes = options.thresholdBytes ?? 256 * 1024;
    this.ttlMs = options.ttlMs ?? 60 * 60 * 1000;
    mkdirSync(this.dir, { recursive: true });
  }

  public getThresholdBytes(): number {
    return this.thresholdBytes;
  }

  public put(data: string | Buffer, ext = 'bin'): SpillRef {
    this.cleanupExpired();
    const id = randomUUID();
    const path = join(this.dir, `${id}.${ext}`);
    const buf = typeof data === 'string' ? Buffer.from(data, 'utf8') : data;
    writeFileSync(path, buf);
    return { uri: `${SPILL_URI_PREFIX}${id}.${ext}`, path, bytes: buf.length };
  }

  public isSpillUri(value: string): boolean {
    return value.startsWith(SPILL_URI_PREFIX);
  }

  public resolvePath(uri: string): string {
    if (!this.isSpillUri(uri)) {
      throw new Error(`Not a spill URI: ${uri}`);
    }
    const name = uri.slice(SPILL_URI_PREFIX.length);
    if (name.includes('..') || name.includes('/') || name.includes('\\')) {
      throw new Error('Invalid spill URI');
    }
    return join(this.dir, name);
  }

  public readString(uri: string): string {
    return readFileSync(this.resolvePath(uri), 'utf8');
  }

  public readBuffer(uri: string): Buffer {
    return readFileSync(this.resolvePath(uri));
  }

  public delete(uri: string): void {
    try {
      const p = this.resolvePath(uri);
      if (existsSync(p)) unlinkSync(p);
    } catch {
      /* best-effort */
    }
  }

  public cleanupExpired(now = Date.now()): number {
    let removed = 0;
    if (!existsSync(this.dir)) return 0;
    for (const name of readdirSync(this.dir)) {
      const p = join(this.dir, name);
      try {
        const st = statSync(p);
        if (now - st.mtimeMs > this.ttlMs) {
          unlinkSync(p);
          removed++;
        }
      } catch {
        /* ignore */
      }
    }
    return removed;
  }

  /**
   * Rewrite known large fields to spill:// refs when over threshold.
   * Supports DocIntel `base64Source`, Embedding `input`, LLM `messages[].content`.
   */
  public spillPayload<T extends Record<string, unknown>>(payload: T): T {
    const out = { ...payload } as Record<string, unknown>;

    if (typeof out.base64Source === 'string' && out.base64Source.length >= this.thresholdBytes) {
      const ref = this.put(out.base64Source, 'b64');
      out.base64Source = ref.uri;
    }

    if (typeof out.input === 'string' && out.input.length >= this.thresholdBytes) {
      const ref = this.put(out.input, 'txt');
      out.input = ref.uri;
    } else if (Array.isArray(out.input)) {
      out.input = out.input.map((item) => {
        if (typeof item === 'string' && item.length >= this.thresholdBytes) {
          return this.put(item, 'txt').uri;
        }
        return item;
      });
    }

    if (Array.isArray(out.messages)) {
      out.messages = out.messages.map((m) => {
        if (!m || typeof m !== 'object') return m;
        const msg = { ...(m as Record<string, unknown>) };
        if (typeof msg.content === 'string' && msg.content.length >= this.thresholdBytes) {
          msg.content = this.put(msg.content, 'txt').uri;
        }
        return msg;
      });
    }

    return out as T;
  }

  /** Resolve spill:// fields back to inline content for adapter HTTP calls. */
  public resolvePayload<T extends Record<string, unknown>>(payload: T): T {
    const out = { ...payload } as Record<string, unknown>;

    if (typeof out.base64Source === 'string' && this.isSpillUri(out.base64Source)) {
      out.base64Source = this.readString(out.base64Source);
    }

    if (typeof out.input === 'string' && this.isSpillUri(out.input)) {
      out.input = this.readString(out.input);
    } else if (Array.isArray(out.input)) {
      out.input = out.input.map((item) =>
        typeof item === 'string' && this.isSpillUri(item) ? this.readString(item) : item
      );
    }

    if (Array.isArray(out.messages)) {
      out.messages = out.messages.map((m) => {
        if (!m || typeof m !== 'object') return m;
        const msg = { ...(m as Record<string, unknown>) };
        if (typeof msg.content === 'string' && this.isSpillUri(msg.content)) {
          msg.content = this.readString(msg.content);
        }
        return msg;
      });
    }

    return out as T;
  }

  /** Collect spill URIs nested in a payload for cleanup. */
  public collectUris(payload: unknown, into: string[] = []): string[] {
    if (typeof payload === 'string') {
      if (this.isSpillUri(payload)) into.push(payload);
      return into;
    }
    if (Array.isArray(payload)) {
      for (const item of payload) this.collectUris(item, into);
      return into;
    }
    if (payload && typeof payload === 'object') {
      for (const v of Object.values(payload as Record<string, unknown>)) {
        this.collectUris(v, into);
      }
    }
    return into;
  }

  public deletePayloadSpills(payload: unknown): void {
    for (const uri of this.collectUris(payload)) {
      this.delete(uri);
    }
  }
}
