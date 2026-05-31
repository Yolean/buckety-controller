# kadm / parameter-mutation

**Scenario:** SPEC.md §End-to-end coverage #3 — Parameter
mutation.

Mutable parameters (e.g. `config.retention.ms`) are reconciled
to the backend when the Buckety spec changes. Immutable
parameters (the kadm driver declares `replicationFactor`
immutable post-create) are rejected by the admission webhook.

**Demonstrates:**

- Initial apply with `config.retention.ms: "604800000"` (7 days).
- Patch to `config.retention.ms: "3600000"` (1 hour).
- The broker's effective topic config reflects the new value
  on the next reconcile (drift reconciled in-place).
- Patch to `replicationFactor: "3"` is rejected with a webhook
  error citing the immutable field.

**Assertions** (`assert.sh`):

1. `Buckety/cfg-mut` reaches `Ready=True` with retention=7d.
2. Patch retention to 1h; wait for `observedGeneration` to
   catch up and `Ready=True` again.
3. Broker reports `retention.ms=3600000` for the resolved
   topic.
4. `kubectl patch` with `replicationFactor: "3"` exits non-zero
   and the error message names `replicationFactor`.
