# Factioner Gateway HTTP Contract

Shared route set for TypeScript (`packages/ts`) and Rust (`packages/rust`) gateways.

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/v1/health` or `/health` | No | Liveness `{ status, uptime }` |
| GET | `/v1/metrics` | Yes | All pool metrics (camelCase + nested `budgetUsage`) |
| GET | `/v1/pools/:name/metrics` | Yes | Single pool metrics |
| POST | `/v1/pools/:name/execute` | Yes | Unary submit |
| POST | `/v1/pools/:name/stream` | Yes | SSE `text/event-stream`: `data: {"token":...}` then `data: [DONE]`; abort on disconnect releases lease |
| POST | `/v1/pools/:name/operations` | Yes | Submit long-running op → `{ operationId, status: "running" }` (opaque handle; no `endpointId`) |
| GET | `/v1/pools/:name/operations/:id` | Yes | Poll snapshot `{ operationId, status, done, data? }` — no `x-endpoint-id` |
| DELETE | `/v1/pools/:name/operations/:id` | Yes | Cancel when supported (`upstreamCancelled`) |

## Auth

- `Authorization: Bearer <apiKey>` (case-insensitive scheme) or `x-api-key: <apiKey>`
- Require `apiKey` unless `allowUnauthenticated: true`
- Default bind: `127.0.0.1`
- CORS: only configured origins (default: none)

## Headers (execute / stream / operations)

- `x-tenant-id`
- `x-priority` — capped by tenant `maxAllowedPriority` inside the pool (single cap point)
- `x-timeout-ms` — queue wait timeout
- `x-idempotency-key`

## Errors / backpressure

Admission and tenant denials return **429** with `error`, `code` (`QUEUE_FULL` | `SATURATED` | `TENANT_DENIED`), `backpressure: true`, and `Retry-After: 2`. Other upstream failures return **502**.

## Capacity precedence

1. **Runtime signals** — overload + Retry-After temporarily tighten effective RPM
2. **Config / env** — `FACTIONER_CONFIG` (+ legacy `FACTION_CONFIG`) and endpoint overlays
3. **In-code** defaults

## Deferred

- Multi-replica shared TPM/RPM
- OAuth2 refresh / Managed Identity
- Payload spill to object storage
- OpenTelemetry exporters
- Sticky routing implementation
- E1 QuotaProbe learn-caps
