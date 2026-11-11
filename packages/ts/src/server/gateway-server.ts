import * as http from 'node:http';
import { PoolRegistry } from '../pool/pool-registry.js';
import { JobPriority } from '../types/index.js';
import {
  BlobSpillStore,
  cleanupSpilledPayload,
  configureSpillStore
} from '../spill/blob-spill.js';

export interface GatewayServerOptions {
  port?: number;
  host?: string;
  apiKey?: string;
  allowUnauthenticated?: boolean;
  corsOrigins?: string[];
  /** Max request body size in bytes (default 10 MiB). */
  maxBodyBytes?: number;
  /** Local directory for large-payload spill (enables blob spill). */
  spillDir?: string;
  /** Spill string fields at/above this size (default 256 KiB). */
  spillThresholdBytes?: number;
}

/**
 * Hardened Gateway Server exposing protected REST and SSE Streaming endpoints.
 */
export class GatewayServer {
  private server?: http.Server;
  private readonly port: number;
  private readonly host: string;
  private readonly apiKey?: string;
  private readonly allowUnauthenticated: boolean;
  private readonly corsOrigins: string[];
  private readonly maxBodyBytes: number;
  private readonly spillStore?: BlobSpillStore;
  private readonly registry: PoolRegistry;
  /** Spilled payloads retained for in-flight LROs until done/cancel. */
  private readonly operationSpills = new Map<string, Record<string, unknown>>();

  constructor(options: GatewayServerOptions = {}, registry: PoolRegistry = PoolRegistry.getInstance()) {
    this.port = options.port ?? 8080;
    this.host = options.host ?? '127.0.0.1';
    this.apiKey = options.apiKey;
    this.allowUnauthenticated = options.allowUnauthenticated ?? false;
    this.corsOrigins = options.corsOrigins ?? [];
    this.maxBodyBytes = options.maxBodyBytes ?? 10 * 1024 * 1024;
    this.registry = registry;
    if (options.spillDir) {
      this.spillStore = new BlobSpillStore({
        dir: options.spillDir,
        thresholdBytes: options.spillThresholdBytes
      });
      configureSpillStore(this.spillStore);
    }
  }

  private operationSpillKey(poolName: string, operationId: string): string {
    return `${poolName}::${operationId}`;
  }

  private retainOperationSpill(poolName: string, operationId: string, payload: Record<string, unknown>): void {
    if (!this.spillStore) return;
    this.operationSpills.set(this.operationSpillKey(poolName, operationId), payload);
  }

  private releaseOperationSpill(poolName: string, operationId: string): void {
    const key = this.operationSpillKey(poolName, operationId);
    const payload = this.operationSpills.get(key);
    if (!payload) return;
    this.operationSpills.delete(key);
    cleanupSpilledPayload(payload);
  }

  public start(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server = http.createServer(async (req, res) => {
        try {
          await this.handleRequest(req, res);
        } catch (err: any) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: err.message || 'Internal Server Error' }));
        }
      });

      this.server.listen(this.port, this.host, () => resolve());
      this.server.on('error', reject);
    });
  }

  public stop(): Promise<void> {
    return new Promise((resolve) => {
      if (this.server) {
        this.server.close(() => resolve());
      } else {
        resolve();
      }
    });
  }

  private authenticate(req: http.IncomingMessage): boolean {
    if (this.allowUnauthenticated && !this.apiKey) return true;
    if (!this.apiKey) return false;

    const authHeader = req.headers['authorization'];
    if (authHeader) {
      const match = authHeader.match(/^Bearer\s+(.+)$/i);
      if (match && match[1] === this.apiKey) return true;
    }

    if (req.headers['x-api-key'] === this.apiKey) return true;
    return false;
  }

  private applyCors(req: http.IncomingMessage, res: http.ServerResponse): void {
    const origin = req.headers['origin'];
    if (origin && this.corsOrigins.includes(origin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Vary', 'Origin');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
      res.setHeader(
        'Access-Control-Allow-Headers',
        'Content-Type, Authorization, x-api-key, x-tenant-id, x-priority, x-timeout-ms, x-idempotency-key'
      );
    }
  }

  private async handleRequest(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', `http://${req.headers.host || 'localhost'}`);
    const method = req.method ?? 'GET';

    this.applyCors(req, res);

    if (method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    if (url.pathname === '/v1/health' || url.pathname === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', uptime: process.uptime() }));
      return;
    }

    if (!this.authenticate(req)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized: Invalid or missing API key' }));
      return;
    }

    if ((url.pathname === '/' || url.pathname === '/dashboard') && method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(this.renderDashboardHtml());
      return;
    }

    if (url.pathname === '/v1/metrics') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(this.registry.getAllMetrics(), null, 2));
      return;
    }

    const poolMetricsMatch = url.pathname.match(/^\/v1\/pools\/([^/]+)\/metrics$/);
    if (poolMetricsMatch && method === 'GET') {
      const pool = this.registry.get(decodeURIComponent(poolMetricsMatch[1]));
      if (!pool) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Pool not found' }));
        return;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(pool.getMetrics(), null, 2));
      return;
    }

    const poolStreamMatch = url.pathname.match(/^\/v1\/pools\/([^/]+)\/stream$/);
    if (poolStreamMatch && method === 'POST') {
      const pool = this.registry.get(decodeURIComponent(poolStreamMatch[1]));
      if (!pool) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Pool not found' }));
        return;
      }

      const payload = await this.readJsonPayload(req);
      const opts = this.extractSubmitOptions(req);

      try {
        const tokenStream = await pool.submitStream(payload, opts);
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive'
        });

        const onClose = () => tokenStream.abort();
        req.on('close', onClose);

        for await (const chunk of tokenStream) {
          res.write(`data: ${JSON.stringify({ token: chunk })}\n\n`);
        }
        res.write('data: [DONE]\n\n');
        res.end();
        req.off('close', onClose);
      } catch (err: any) {
        if (!res.headersSent) {
          this.writeError(res, err);
        } else {
          res.end(JSON.stringify({ error: err.message }));
        }
      } finally {
        cleanupSpilledPayload(payload);
      }
      return;
    }

    // POST /v1/pools/:name/operations — async submit
    const opsMatch = url.pathname.match(/^\/v1\/pools\/([^/]+)\/operations$/);
    if (opsMatch && method === 'POST') {
      const pool = this.registry.get(decodeURIComponent(opsMatch[1]));
      if (!pool) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Pool not found' }));
        return;
      }
      const poolName = decodeURIComponent(opsMatch[1]);
      const payload = await this.readJsonPayload(req);
      const opts = this.extractSubmitOptions(req);
      try {
        const handle = await pool.submitOperation(payload, opts);
        this.retainOperationSpill(poolName, handle.operationId, payload);
        res.writeHead(202, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ operationId: handle.operationId, status: 'running' }));
      } catch (err: any) {
        cleanupSpilledPayload(payload);
        this.writeError(res, err);
      }
      return;
    }

    // GET/DELETE /v1/pools/:name/operations/:id
    const opIdMatch = url.pathname.match(/^\/v1\/pools\/([^/]+)\/operations\/([^/]+)$/);
    if (opIdMatch) {
      const poolName = decodeURIComponent(opIdMatch[1]);
      const operationId = decodeURIComponent(opIdMatch[2]);
      const pool = this.registry.get(poolName);
      if (!pool) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Pool not found' }));
        return;
      }

      if (method === 'GET') {
        try {
          const pollRes = await pool.pollOperation(operationId);
          if (pollRes.done) {
            this.releaseOperationSpill(poolName, operationId);
          }
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            operationId,
            status: pollRes.status,
            done: pollRes.done,
            data: pollRes.data
          }));
        } catch (err: any) {
          const status = err.message?.includes('not found') ? 404 : 500;
          res.writeHead(status, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ operationId, error: err.message }));
        }
        return;
      }
      if (method === 'DELETE') {
        try {
          const result = await pool.cancelOperation(operationId);
          this.releaseOperationSpill(poolName, operationId);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            operationId,
            status: result.status,
            upstreamCancelled: result.upstreamCancelled
          }));
        } catch (err: any) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ operationId, error: err.message }));
        }
        return;
      }
    }

    const poolExecMatch = url.pathname.match(/^\/v1\/pools\/([^/]+)\/execute$/);
    if (poolExecMatch && method === 'POST') {
      const pool = this.registry.get(decodeURIComponent(poolExecMatch[1]));
      if (!pool) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Pool not found' }));
        return;
      }

      const payload = await this.readJsonPayload(req);
      // Pass raw priority — pool.resolvePriority is the single cap point
      const opts = this.extractSubmitOptions(req);

      try {
        const result = await pool.submit(payload, opts);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (err: any) {
        this.writeError(res, err);
      } finally {
        cleanupSpilledPayload(payload);
      }
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Endpoint not found' }));
  }

  private extractSubmitOptions(req: http.IncomingMessage): {
    tenantId?: string;
    priority?: JobPriority;
    queueTimeoutMs?: number;
    idempotencyKey?: string;
  } {
    const tenantId = (req.headers['x-tenant-id'] as string) || undefined;
    const priorityHeader = req.headers['x-priority'] as string;
    let priority = JobPriority.NORMAL;
    if (priorityHeader === 'critical' || priorityHeader === '0') priority = JobPriority.CRITICAL;
    else if (priorityHeader === 'high' || priorityHeader === '1') priority = JobPriority.HIGH;
    else if (priorityHeader === 'batch' || priorityHeader === '3') priority = JobPriority.BATCH;
    const idempotencyKey = (req.headers['x-idempotency-key'] as string) || undefined;
    const queueTimeoutMs = req.headers['x-timeout-ms']
      ? parseInt(req.headers['x-timeout-ms'] as string, 10)
      : undefined;
    return { tenantId, priority, queueTimeoutMs, idempotencyKey };
  }

  private writeError(res: http.ServerResponse, err: any): void {
    const code = err?.code as string | undefined;
    const isBackpressure =
      code === 'QUEUE_FULL' ||
      code === 'SATURATED' ||
      code === 'TENANT_DENIED' ||
      err.message?.includes('capacity') ||
      err.message?.includes('backpressure') ||
      err.message?.includes('saturated') ||
      err.message?.includes('Tenant admission denied');
    const statusCode = isBackpressure ? 429 : 502;
    res.writeHead(statusCode, {
      'Content-Type': 'application/json',
      ...(isBackpressure ? { 'Retry-After': '2' } : {})
    });
    res.end(JSON.stringify({
      error: err.message,
      code: code ?? (isBackpressure ? 'QUEUE_FULL' : undefined),
      backpressure: isBackpressure
    }));
  }

  private readBody(req: http.IncomingMessage): Promise<string> {
    const limit = this.maxBodyBytes;
    return new Promise((resolve, reject) => {
      let data = '';
      req.on('data', chunk => {
        data += chunk;
        if (data.length > limit) {
          reject(new Error(`Request payload too large (max ${limit} bytes)`));
        }
      });
      req.on('end', () => resolve(data));
      req.on('error', reject);
    });
  }

  /** Parse JSON body and optionally spill large fields to disk. */
  private async readJsonPayload(req: http.IncomingMessage): Promise<Record<string, unknown>> {
    const bodyText = await this.readBody(req);
    const payload = bodyText ? (JSON.parse(bodyText) as Record<string, unknown>) : {};
    return this.spillStore ? this.spillStore.spillPayload(payload) : payload;
  }

  private renderDashboardHtml(): string {
    return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Factioner Control Plane</title>
  <style>
    body { background: #0f172a; color: #f8fafc; font-family: sans-serif; padding: 24px; }
    .card { background: #1e293b; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
    h2 { color: #38bdf8; margin-bottom: 12px; }
  </style>
</head>
<body>
  <h2>Factioner Service Pool Control Plane</h2>
  <div class="card">
    <p>POST /v1/pools/:name/execute | /stream | /operations — GET /v1/metrics — GET /v1/health</p>
  </div>
</body>
</html>`;
  }
}
