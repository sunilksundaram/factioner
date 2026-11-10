---
name: Faction pool fixes
overview: Fix shared critical bugs in TypeScript and Rust first, then align unary pool behavior across both, then bring Rust to full feature parity (streaming, long-running operations, HTTP gateway) while closing remaining TypeScript gaps.
todos:
  - id: phase1-halfopen
    content: "Phase 1: Split circuit breaker check vs probe claim; fix TS+Rust acquire path; add recovery tests"
    status: completed
  - id: phase1-budget
    content: "Phase 1: Fix MultiResourceLimiter window + refund on failure (TS+Rust)"
    status: completed
  - id: phase1-ts-dispatch
    content: "Phase 1: TS dispatch uses estimatedBudget; clear queue timer; fix aborted release/refund"
    status: completed
  - id: phase1-rust-atomic
    content: "Phase 1: Atomic acquire_slot in Rust endpoint"
    status: completed
  - id: phase2-rust-queue
    content: "Phase 2: Wire Rust PriorityQueue + dispatcher; fix RR/failover/drop-oldest/admission timeout"
    status: completed
  - id: phase2-tenant-secrets-deadlines
    content: "Phase 2: Align tenant isolation/quota, vault encryption/redact, deadlines, idempotency, metrics, Unavailable routing"
    status: completed
  - id: phase3-ts-stream-ops-gw
    content: "Phase 3: TS stream/ops controls, doc-intel fixes, gateway auth/CORS/ops route, LLM stream providers"
    status: completed
  - id: phase4-rust-parity
    content: "Phase 4: Rust adapters + submit_stream/submit_operation + registry + Axum gateway + PII/security config"
    status: completed
  - id: glue-admission-retry-lifecycle
    content: "Glue: admission waiter race, DropOldest refund, retry requeue caps, removeEndpoint drain, Retry-After units"
    status: completed
  - id: glue-security-ops-ssrf
    content: "Glue: operation-location SSRF allowlist, vault redact without plaintext mirror, Fatal vs breaker"
    status: completed
  - id: glue-contract-tests-docs
    content: "Glue: shared gateway route contract, cross-lang golden scenarios, README sync; note deferred items"
    status: completed
isProject: false
---

# Faction Pool: Fix Conflicts, Gaps & Issues

Scope: **1B** (Rust full parity with stream + operations + gateway) delivered in **2A** phases (critical bugs → conflicts → feature gaps).

```mermaid
flowchart LR
  phase1[Phase1_CriticalBugs]
  phase2[Phase2_AlignUnary]
  phase3[Phase3_TsGaps]
  phase4[Phase4_RustParity]
  phase1 --> phase2 --> phase3 --> phase4
```

## Phase 1 — Shared critical bugs (ship first)

### 1.1 Half-open probe double-claim
Root cause: `canExecute` / `can_execute` both **checks and claims** the probe; routers call it via `isAvailable`/`is_available`, then `acquireSlot`/`acquire_slot` calls it again and fails permanently.

- Split into pure check + claim:
  - TypeScript: [`packages/ts/src/resilience/circuit-breaker.ts`](packages/ts/src/resilience/circuit-breaker.ts)
  - Rust: [`packages/rust/src/resilience/circuit_breaker.rs`](packages/rust/src/resilience/circuit_breaker.rs)
- API: `canExecute()` / `can_execute()` become read-only; add `tryClaimProbe()` / `try_claim_probe()` used **only** inside `acquireSlot` / `acquire_slot`.
- `getState()` / `get_state()` must not transition Open→HalfOpen as a side effect of metrics reads (only cool-off eligibility check, or transition only when claiming).
- Update [`packages/ts/src/pool/endpoint-instance.ts`](packages/ts/src/pool/endpoint-instance.ts) and [`packages/rust/src/pool/endpoint.rs`](packages/rust/src/pool/endpoint.rs): `isAvailable` uses pure check; `acquireSlot` claims probe then budget then lease, with rollback on failure.
- Tests: prove select → acquire → success closes circuit with a single endpoint.

### 1.2 Budget sliding window
Both [`packages/ts/src/backpressure/resource-budget.ts`](packages/ts/src/backpressure/resource-budget.ts) and [`packages/rust/src/backpressure/resource_budget.rs`](packages/rust/src/backpressure/resource_budget.rs) decay incorrectly by advancing `windowStart` on every refresh.

- Replace with fixed-window reset: if `now - windowStart >= windowMs`, set `consumed = 0` and `windowStart = now`; otherwise leave start alone.
- On failure path, call `refund(estimated)` when no successful upstream usage; keep `reconcile` for success only.
- Wire refund into endpoint `releaseSlot` when `success === false` and `actual` is absent.

### 1.3 TypeScript dispatch budget + queue timer
In [`packages/ts/src/pool/service-pool.ts`](packages/ts/src/pool/service-pool.ts):

- Peek or dequeue first, then `selectEndpoint(list, job.estimatedBudget)` and `acquireSlot(job.estimatedBudget)`.
- Clear `queueTimer` when the job is dequeued/dispatched (not only on admission reject).
- Call `refundOnDrop` only after a prior `deductOnAdmission` (track with a flag on the job or call order).
- Aborted-at-dequeue: `releaseSlot(false, …)` + budget refund, never `releaseSlot(true, 0)`.
- Dispatch algorithm (concrete): `dequeue()` first; if aborted, refund tenant if charged and continue; else `selectEndpoint(endpoints, job.estimatedBudget, job.avoidEndpointIds)`; if none available, **re-enqueue at front of that tenant’s tier** (or dedicated hold) and stop the loop (do not drop the job); else `acquireSlot(estimate)` and clear the job’s queue timer before `executeJob`.

### 1.4 Rust lease/budget race
In [`packages/rust/src/pool/endpoint.rs`](packages/rust/src/pool/endpoint.rs): make `acquire_slot` atomic under one lock (or compare-and-swap loop) covering availability, probe claim, budget acquire, and lease increment, with full rollback.

---

## Phase 1.5 — Admission / retry / lifecycle glue (both)

These are logic holes between modules, not named in the original conflict table.

- **Admission multi-waiter race:** `notifySlotAvailable` / `Notify` can wake N waiters for 1 free slot. After wake, re-check size; if still full, wait again (loop until timeout). Prefer a single admission mutex around “check + enqueue” so two Timeout waiters cannot both pass.
- **DropOldest refund:** When an older job is dropped to admit a newer one, call `refundOnDrop` / reject that job’s promise — the dropped job was already `deductOnAdmission`’d at enqueue time.
- **Retry re-enqueue vs queue cap:** Retries currently bypass admission and can grow the queue past `maxQueueSize`. Re-run admission (or reserve a retry sub-budget of the queue, e.g. 10%) before re-enqueue; if rejected, fail the job.
- **Retry sleep vs deadlines:** After backoff sleep, re-check queue+execution deadline before dispatching; do not sleep past the remaining deadline (clamp delay).
- **Retry-After unit consistency:** Fix Rust path that divides ms by 1000 then multiplies by 1000 again inconsistently in [`backoff.rs`](packages/rust/src/resilience/backoff.rs) / [`service_pool.rs`](packages/rust/src/pool/service_pool.rs). Standardize on milliseconds end-to-end.
- **Global retry budget:** Replace lifetime ratio `retries/dispatched` (permanent starvation after a burst) with a sliding window (e.g. last 100 dispatches or 60s).
- **`removeEndpoint` / drain:** Wait for or abort in-flight leases on that endpoint; refund budgets; remove vault entry only after no in-flight auth use. Add Rust `remove_endpoint` + `drain`.
- **Breaker vs Fatal:** `releaseSlot(false)` on HTTP 400 should not trip the circuit (record as rejected client error, not endpoint failure). Only `Transient` / `Overloaded` / `Unavailable` (and transport failures) call `recordFailure`.
- **Budget dual config:** Treat `EndpointConfig.maxConcurrency` as source of truth; ignore or sync `budget.concurrency` to avoid two limits.
- **Default estimate:** Adapters expose `estimateBudget(payload)` (LLM≈tokens, DocIntel≈pages, HTTP≈rps:1); pool uses that instead of hard-coded `{rpm:1,tpm:250}` for every service type.
- **Cost router NaN:** Use total-order compare (`partial_cmp` unwrap_or Equal); never panic.
- **Orphan `TokenBucketRateLimiter`:** Deprecate or delete once `MultiResourceLimiter.rps` is the only RPS path (both packages).

---

## Phase 2 — Align unary pool behavior (conflicts)

Target: same semantics for a unary `submit` in TS and Rust.

| Conflict | Fix |
|---|---|
| Rust has no real queue | Wire [`PriorityQueue`](packages/rust/src/backpressure/priority_queue.rs) into `ServicePool::submit`: enqueue after admission; background dispatch task dequeues with tenant fair RR; stop per-caller spin loops |
| RoundRobin always picks `[0]` | Add rotating index (atomic) matching TS |
| Failover ignores weight | Least-loaded-by-weight within top tier |
| DropOldest never drops | Call `drop_oldest_low_priority` and reject the dropped job; never drop Critical |
| Admission Timeout | After notify, re-check queue size before accept (both sides) |
| Tenant isolation | Pass security config into Rust `TenantGovernor`; support `enforce_isolation` + `allowed_tenants`; no auto-register when isolation on |
| Daily quota | Reset after 24h in Rust (match TS) |
| Error classification | Rust `ErrorClassifier` accept structured status/headers (or adapter returns `FailureClassification`); stop relying on substring `"429"` alone |
| Secrets | Rust vault: AES-256-GCM at rest (e.g. `aes-gcm` crate) + redact set; redact all key=value hits, not first only |
| Deadlines | Honor `queue_timeout_ms` and `execution_timeout_ms` with `tokio::time::timeout` / AbortSignal; apply endpoint `timeout_ms` in adapters |
| Idempotency | In-memory map: same key while in-flight waits on one future; completed key returns stored `JobResult` for TTL window (both languages) |
| Metrics | Rust compute throughput from recent completions; add p95; TS record `queueWaitMs` in telemetry |
| Unavailable class | On `Unavailable`, exclude failed endpoint for that retry attempt (sticky avoid list on job) |

Also align:

- Drop-oldest: never drop `CRITICAL` (fix TS `dropOldestLowPriority` and Rust equivalent).
- Round-robin: respect `weight` via weighted RR (both).
- `maxTpm` / `max_tpm`: enforce on tenant governor when estimate/actual TPM is known.
- Health: failed probe sets Unhealthy and must not be overwritten to Healthy by breaker Closed until a successful probe or successful call.

Shared contract note: keep types mirrored in [`packages/ts/src/types/index.ts`](packages/ts/src/types/index.ts) and [`packages/rust/src/types.rs`](packages/rust/src/types.rs); document field meaning once in README only if behavior changes.

---

## Phase 3 — Remaining TypeScript gaps

### 3.1 Stream path through full controls
[`submitStream`](packages/ts/src/pool/service-pool.ts): same front door as unary — tenant eligibility + deduct, PII, priority resolve, estimated budget via `adapter.estimateBudget`, capacity acquire (bypass fair queue for streams that hold a lease for the whole iterator life, but still subject to endpoint budgets + tenant concurrency). Abort on client disconnect; release lease + refund unused estimate when iterator ends/errors or consumer drops. Gateway SSE: wire `AbortController` to `req` `close`/`aborted`.

### 3.2 Operation path (concrete API)
Do **not** block the HTTP request for the full poll. Gateway contract:

- `POST /v1/pools/:name/operations` → `{ operationId, status: "running" }` after submit; release submit lease immediately.
- `GET /v1/pools/:name/operations/:id` → poll snapshot.
- `DELETE /v1/pools/:name/operations/:id` → cancel when adapter implements `cancelOperation`.

Pool `submitOperation` returns a handle `{ operationId, poll(), cancel() }` matching [`LongRunningOperationHandle`](packages/ts/src/types/index.ts). Internal poll loop is only for convenience helper `awaitOperation(id, opts)` used by demos/tests — not the gateway default.

Also:

- On submit failure: `releaseSlot(false, …)` + budget refund.
- Poll under separate lighter budget (`rpm` only), respect abort + configurable max poll time.
- Fix [`DocumentIntelligenceAdapter.execute`](packages/ts/src/adapters/doc-intelligence-adapter.ts): do not invent `succeeded` when poll is still `running`; handle `operationId === 'direct'` without fetching URL `"direct"`.
- **operation-location SSRF:** only follow poll URLs whose origin matches the configured endpoint URL (or an allowlist on `EndpointConfig`); reject absolute URLs to other hosts.

### 3.3 Gateway hardening
[`packages/ts/src/server/gateway-server.ts`](packages/ts/src/server/gateway-server.ts):

- Require `apiKey` unless explicit `allowUnauthenticated: true` (do not key off `NODE_ENV` alone).
- Do not trust client `x-priority` for elevation: map header through `resolvePriority` only; unregistered tenants cannot request CRITICAL.
- CORS: configurable `corsOrigins` (default empty = no ACAO header).
- Routes: execute, stream, operations CRUD as above, metrics, health.

### 3.4 LLM / embedding stream provider parity
[`packages/ts/src/adapters/llm-adapter.ts`](packages/ts/src/adapters/llm-adapter.ts): `executeStream` uses same Anthropic/Gemini/OpenAI body branching as unary `execute`. Keep embedding adapter unary-only unless streaming is required later.

### 3.5 Vault redaction without plaintext mirror
[`CredentialVault`](packages/ts/src/security/credential-vault.ts) still keeps `rawSecretsSet` in memory after AES encrypt. Replace with: decrypt-on-use for dispatch; for redaction, store SHA-256 hashes + short suffixes for detection, or redact only via regex patterns plus decrypt-compare against known endpoint ids when logging. Same design in Rust vault.

---

## Phase 4 — Rust full parity (1B)

### 4.1 Expand `ServiceAdapter`
[`packages/rust/src/adapters/mod.rs`](packages/rust/src/adapters/mod.rs) — capability trait with defaults that return “unsupported”:

```rust
fn estimate_budget(&self, payload: &TReq) -> BudgetDeduction;
async fn execute(...) -> Result<(TRes, BudgetDeduction), AdapterError>; // AdapterError carries FailureClassification
async fn execute_stream(...) -> Result<StreamExecution, AdapterError>; // default Err(Unsupported)
async fn submit_operation(...) -> Result<OperationSubmission, AdapterError>;
async fn poll_operation(...) -> Result<OperationPollResult<TRes>, AdapterError>;
async fn cancel_operation(...) -> Result<(), AdapterError>;
async fn health_check(...);
```

Adapters: Mock (all paths), Generic HTTP (SSRF + auth-last headers), LLM (provider branching + stream), Document Intelligence (202 + poll + cancel stub), Embedding (unary). Use `reqwest`.

### 4.2 Pool APIs
On [`ServicePool`](packages/rust/src/pool/service_pool.rs): `submit`, `submit_stream`, `submit_operation` / `poll_operation` / `cancel_operation` matching Phase 3. Fair queue + dispatcher from Phase 2. Periodic health checks. `drain` + `remove_endpoint`.

### 4.3 Gateway + registry
- `packages/rust/src/pool/registry.rs` — multi-pool registry (instance-based, not process singleton, for testability; TS registry gains `create()` alongside `getInstance()`).
- `packages/rust/src/server/gateway.rs` — Axum: same route set and auth/CORS rules as TS gateway.
- Demo wire-up in [`packages/rust/src/main.rs`](packages/rust/src/main.rs).

### 4.4 Security extras
PII sanitization hook gated by config. Tenant allow-list + isolation from pool options. Mutex poison: prefer `lock().unwrap_or_else(|e| e.into_inner())` or propagate errors on vault/breaker paths that must not panic the process.

### 4.5 Dependencies
[`packages/rust/Cargo.toml`](packages/rust/Cargo.toml): `aes-gcm`, `sha2`, `reqwest`, `axum`, `tokio-stream`, `futures`, `tower-http` (CORS) as needed.

### 4.6 Cross-language contract
Shared markdown or JSON fixture of golden scenarios (priorities, drop-oldest, half-open recover, idempotent key) run by both test suites. Document HTTP routes once in README so TS and Rust gateways stay aligned.

---

## Explicitly deferred (out of this plan)

- Multi-process / Redis shared TPM-RPM budgets across replicas.
- Real OAuth2 refresh / Azure Managed Identity (keep static bearer/api_key).
- Large payload spill to object storage for base64 documents.
- OpenTelemetry exporters (keep in-process metrics snapshots).
- gRPC / non-HTTP adapters beyond the HTTP adapter trait.

---

## Test plan (per phase)

- Phase 1: circuit probe claim/acquire; budget window after 60s idle → consumed 0; TS timer cleared on dispatch; Rust concurrent acquire does not oversubscribe.
- Phase 1.5: two Timeout waiters + one slot → only one admitted; DropOldest refunds dropped tenant; retry respects queue cap and deadline clamp; Fatal 400 does not open breaker.
- Phase 2: fair queue with stable tenant key order (`BTreeMap` or sorted keys); DropOldest leaves Critical; isolation rejects unknown tenant; idempotent double-submit; Unavailable skips failed endpoint; Retry-After ms consistent.
- Phase 3: stream aborts on disconnect; operations are async handle + poll route; doc intel SSRF reject; gateway 401 without key; vault logs never contain raw secret after redact.
- Phase 4: Rust mirrors TS golden scenarios; gateway smoke for execute/stream/operations/metrics.

Exit criteria: both packages pass their test suites; unary Critical under load matches across TS and Rust; both expose the same HTTP route set for execute, stream, operations, metrics, and health.