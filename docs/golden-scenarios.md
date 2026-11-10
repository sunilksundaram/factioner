# Cross-language golden scenarios

Both `packages/ts` and `packages/rust` test suites should cover:

1. **Fair queue** — Tenants A (2 HIGH) and B (1 HIGH) dequeue as A1 → B1 → A2.
2. **Half-open probe** — After cool-off, `canExecute` is pure; only `tryClaimProbe` claims; second claim fails; two successes close.
3. **Budget window** — Fixed window: idle past window clears consumed to 0 (no decay advance).
4. **DropOldest** — Never drops CRITICAL; refunds dropped tenant quota.
5. **Tenant isolation** — Unknown tenant rejected when `enforceTenantIsolation` is on.
6. **Idempotency** — Same key returns one in-flight / cached result.
7. **Stream + operation** — Stream yields tokens; operation submit + poll succeeds.
8. **Secret redact** — `api_key=secret` → `api_key=[REDACTED]`; raw secret never in redacted text.
