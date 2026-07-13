# s3 / jurisdiction-gating

**Scenario:** Capability-gated parameter admission, exercised
from every s3 implementation in the matrix (the gating decision
reads config discriminators, not the backing service).

The `jurisdiction` parameter is honoured only when the backend
declares `implementation: r2` in its config. Against any other
implementation (`aws`, `minio`, `versitygw`, or no
discriminator), admission rejects with a clear message.

This scenario is **admission-only** in CI: we do not exercise
real R2 (the harness has no Cloudflare credentials). The
controller's webhook decision against the backend's
implementation discriminator is the contract this scenario
enforces. A separate manual-test recipe under
`examples/s3/r2/manual/` (future) covers real-R2 provisioning.

**Harness:** This scenario requires two backends defined in
the controller config:

- `s3` (driver-typed; the matrix-driven backend pointing at
  versitygw or minio).
- `r2-fake` (driver `s3`, `implementation: r2`, with bogus
  endpoint/keys). The controller MUST accept this config
  even if it cannot reach the endpoint; the discriminator
  is set on registration and used by admission.

The harness's per-implementation overlays add `r2-fake`
unconditionally so the admission check is reproducible.

**Assertions** (`assert.sh`):

1. Applying `accepted.yaml` (Buckety against `r2-fake` with
   `jurisdiction: eu`) is accepted by admission. The
   resource may not reach Ready (no real R2), but
   admission MUST NOT reject.
2. Applying `rejected-on-non-r2.yaml` (Buckety against
   `s3` with `jurisdiction: eu`) is rejected. Message
   names `jurisdiction` and the actual implementation.
