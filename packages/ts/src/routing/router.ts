import { RoutingStrategy, BudgetDeduction } from '../types/index.js';
import { EndpointInstance } from '../pool/endpoint-instance.js';

export interface IPoolRouter {
  selectEndpoint(
    endpoints: EndpointInstance[],
    estimate?: BudgetDeduction,
    avoidEndpointIds?: string[]
  ): EndpointInstance | undefined;
}

function filterAvailable(
  endpoints: EndpointInstance[],
  estimate?: BudgetDeduction,
  avoidEndpointIds?: string[]
): EndpointInstance[] {
  const avoid = new Set(avoidEndpointIds ?? []);
  return endpoints.filter(e => !avoid.has(e.id) && e.isAvailable(estimate));
}

/** Smooth weighted round-robin respecting endpoint weights (zero allocation) */
export class RoundRobinRouter implements IPoolRouter {
  private currentWeights: Map<string, number> = new Map();

  public selectEndpoint(
    endpoints: EndpointInstance[],
    estimate?: BudgetDeduction,
    avoidEndpointIds?: string[]
  ): EndpointInstance | undefined {
    const available = filterAvailable(endpoints, estimate, avoidEndpointIds);
    if (available.length === 0) return undefined;
    if (available.length === 1) return available[0];

    let totalWeight = 0;
    let bestEp: EndpointInstance = available[0];
    let maxWeight = -Infinity;

    for (const ep of available) {
      const weight = Math.max(1, Math.round(ep.config.weight ?? 1));
      totalWeight += weight;
      const current = (this.currentWeights.get(ep.id) ?? 0) + weight;
      this.currentWeights.set(ep.id, current);

      if (current > maxWeight) {
        maxWeight = current;
        bestEp = ep;
      }
    }

    this.currentWeights.set(bestEp.id, maxWeight - totalWeight);
    return bestEp;
  }
}

export class LeastLoadedRouter implements IPoolRouter {
  public selectEndpoint(
    endpoints: EndpointInstance[],
    estimate?: BudgetDeduction,
    avoidEndpointIds?: string[]
  ): EndpointInstance | undefined {
    const available = filterAvailable(endpoints, estimate, avoidEndpointIds);
    if (available.length === 0) return undefined;

    let bestEndpoint = available[0];
    const weight0 = bestEndpoint.config.weight ?? 1;
    let minLoadRatio = (bestEndpoint.getActiveLeases() / bestEndpoint.config.maxConcurrency) / weight0;

    for (let i = 1; i < available.length; i++) {
      const ep = available[i];
      const weight = ep.config.weight ?? 1;
      const loadRatio = (ep.getActiveLeases() / ep.config.maxConcurrency) / weight;
      if (loadRatio < minLoadRatio) {
        minLoadRatio = loadRatio;
        bestEndpoint = ep;
      }
    }

    return bestEndpoint;
  }
}

export class WeightedLatencyRouter implements IPoolRouter {
  public selectEndpoint(
    endpoints: EndpointInstance[],
    estimate?: BudgetDeduction,
    avoidEndpointIds?: string[]
  ): EndpointInstance | undefined {
    const available = filterAvailable(endpoints, estimate, avoidEndpointIds);
    if (available.length === 0) return undefined;
    if (available.length === 1) return available[0];

    const scores = available.map(ep => {
      const lat = Math.max(1, ep.getLatencyEma());
      const weight = ep.config.weight ?? 1;
      return (1000 / lat) * weight;
    });

    const totalScore = scores.reduce((a, b) => a + b, 0);
    const randomPoint = Math.random() * totalScore;

    let cumulative = 0;
    for (let i = 0; i < available.length; i++) {
      cumulative += scores[i];
      if (randomPoint <= cumulative) {
        return available[i];
      }
    }

    return available[0];
  }
}

export class PriorityFailoverRouter implements IPoolRouter {
  public selectEndpoint(
    endpoints: EndpointInstance[],
    estimate?: BudgetDeduction,
    avoidEndpointIds?: string[]
  ): EndpointInstance | undefined {
    const available = filterAvailable(endpoints, estimate, avoidEndpointIds);
    if (available.length === 0) return undefined;

    const sorted = [...available].sort((a, b) => {
      const tierA = a.config.priorityTier ?? 0;
      const tierB = b.config.priorityTier ?? 0;
      return tierA - tierB;
    });

    const topTier = sorted[0].config.priorityTier ?? 0;
    const sameTierEndpoints = sorted.filter(e => (e.config.priorityTier ?? 0) === topTier);

    return new LeastLoadedRouter().selectEndpoint(sameTierEndpoints, estimate);
  }
}

export class CostOptimizedRouter implements IPoolRouter {
  public selectEndpoint(
    endpoints: EndpointInstance[],
    estimate?: BudgetDeduction,
    avoidEndpointIds?: string[]
  ): EndpointInstance | undefined {
    const available = filterAvailable(endpoints, estimate, avoidEndpointIds);
    if (available.length === 0) return undefined;

    const sorted = [...available].sort((a, b) => {
      const costA = a.config.costPerRequest ?? 1.0;
      const costB = b.config.costPerRequest ?? 1.0;
      if (Number.isNaN(costA) && Number.isNaN(costB)) return 0;
      if (Number.isNaN(costA)) return 1;
      if (Number.isNaN(costB)) return -1;
      return costA - costB;
    });

    return sorted[0];
  }
}

export class RouterFactory {
  public static create(strategy: RoutingStrategy = 'least_loaded'): IPoolRouter {
    switch (strategy) {
      case 'round_robin':
        return new RoundRobinRouter();
      case 'least_loaded':
        return new LeastLoadedRouter();
      case 'weighted_latency':
        return new WeightedLatencyRouter();
      case 'priority_failover':
        return new PriorityFailoverRouter();
      case 'cost_optimized':
        return new CostOptimizedRouter();
      default: {
        const _exhaustive: never = strategy;
        void _exhaustive;
        return new LeastLoadedRouter();
      }
    }
  }
}
