import { ServicePool } from './service-pool.js';
import { PoolMetrics } from '../types/index.js';

/**
 * PoolRegistry coordinates multiple service pools in a distributed application or gateway.
 */
export class PoolRegistry {
  private static instance?: PoolRegistry;
  private readonly pools: Map<string, ServicePool<any, any>> = new Map();

  /** Fresh registry for tests / multi-tenant hosts (preferred over singleton). */
  public static create(): PoolRegistry {
    return new PoolRegistry();
  }

  public static getInstance(): PoolRegistry {
    if (!PoolRegistry.instance) {
      PoolRegistry.instance = new PoolRegistry();
    }
    return PoolRegistry.instance;
  }

  public register(pool: ServicePool<any, any>): void {
    if (this.pools.has(pool.name)) {
      throw new Error(`ServicePool '${pool.name}' is already registered`);
    }
    this.pools.set(pool.name, pool);
  }

  public get<TReq = unknown, TRes = unknown>(name: string): ServicePool<TReq, TRes> | undefined {
    return this.pools.get(name);
  }

  public remove(name: string): boolean {
    const pool = this.pools.get(name);
    if (pool) {
      pool.destroy();
      return this.pools.delete(name);
    }
    return false;
  }

  public getAllMetrics(): Record<string, PoolMetrics> {
    const result: Record<string, PoolMetrics> = {};
    for (const [name, pool] of this.pools.entries()) {
      result[name] = pool.getMetrics();
    }
    return result;
  }

  public async drainAll(timeoutMs: number = 10000): Promise<void> {
    const drainPromises = Array.from(this.pools.values()).map(p => p.drain(timeoutMs));
    await Promise.all(drainPromises);
  }

  public destroyAll(): void {
    for (const pool of this.pools.values()) {
      pool.destroy();
    }
    this.pools.clear();
  }
}

