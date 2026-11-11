# Factioner Monorepo

> **Universal Adaptive Service Resource Pooling, Backpressure & Resilience Engine for AI & Cloud Services**

Factioner is a multi-language monorepo providing high-performance service resource pools, multi-tier priority queuing, backpressure admission control, jittered exponential backoffs, and 3-state circuit breakers for AI and cloud services (LLMs, Document Intelligence, Form Recognizers, OCR, and REST APIs).

---

## Monorepo Structure

```
factioner/
├── packages/
│   ├── ts/                  # TypeScript Implementation (@factioner/ts)
│   ├── rust/                # Rust crate (factioner)
│   └── python/              # Python package (factioner) — PyPI stub / future SDK
├── .github/workflows/       # CI/CD and release pipelines
├── package.json
└── README.md
```

---

## Feature Parity Matrix (TypeScript & Rust)

| Capability | TypeScript (`@factioner/ts`) | Rust (`factioner`) |
| :--- | :--- | :--- |
| **Concurrency & Async Engine** | Node.js 18+ Web Standards | Tokio |
| **Multi-Dimensional Budgets** | RPM, TPM, Pages, RPS | RPM, TPM, Pages, RPS |
| **Circuit Breakers** | 3-state + single half-open probe | Same |
| **Priority Queue + Tenant Fair RR** | Yes | Yes |
| **HTTP Gateway** | execute / SSE stream / operations | Axum (same contract) |

Shared docs: [gateway contract](docs/gateway-contract.md) · [golden scenarios](docs/golden-scenarios.md)

### Capacity precedence

1. **Runtime signals** (429 / Retry-After → tighten effective RPM/TPM; usage overshoot → TPM estimate bias; then restore)
2. **Config / env** (`FACTIONER_CONFIG` JSON + `FACTIONER_ENDPOINT_<ID>_TPM` / `_RPM` / `_API_KEY`; legacy `FACTION_*` still accepted)
3. **In-code** endpoint / tenant defaults

Local blob spill (`gateway.spillDir`) and the shared tokenizer are supported. Sticky/affinity routing and QuotaProbe learn-caps (E1) remain deferred.

**Deferred:** multi-replica shared TPM/RPM, OAuth2 / Managed Identity, remote object-storage spill, OpenTelemetry exporters.

---

## Bootstrap

```ts
import { bootstrapFromConfig, GatewayServer } from '@factioner/ts';

// Production: real adapters selected by pools[].serviceType
const boot = bootstrapFromConfig({ useMock: false });
const server = new GatewayServer(boot.gatewayOptions, boot.registry);
await server.start();
```

`serviceType` → adapter: `llm`, `document_intelligence` (aliases: `ocr`, `form_recognizer`), `embedding`, `custom`. Set `FACTIONER_USE_MOCK=1` or `{ useMock: true }` for offline demos.

## Quick Commands

```bash
npm test
npm run test:ts
npm run test:rust
npm run demo:ts
npm run demo:rust
```

---

## Packages

| Ecosystem | Package | Registry |
| :--- | :--- | :--- |
| npm | `@factioner/ts` | npmjs.com |
| crates.io | `factioner` | crates.io |
| PyPI | `factioner` | pypi.org |

---

## License

Apache-2.0

## Author

Sunil K Sundaram \<sunil.ksundaram@gmail.com\>
