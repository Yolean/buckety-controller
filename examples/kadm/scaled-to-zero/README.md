# kadm / scaled-to-zero

**Scenario:** SPEC.md §End-to-end coverage #5 — Operator
scaled to zero.

The operator is **off the data path** (SPEC.md §Off the data
path). Once a Secret exists, the consumer talks to the broker
directly; scaling the operator to zero must not affect
running consumers.

**Demonstrates:**

- A `Buckety` is provisioned and its Secret is materialised.
- The `buckety-controller` Deployment is scaled to 0 replicas.
- A consumer Job runs and produces+consumes a message via
  the issued Secret while no operator is running.
- The operator is scaled back to 1.

**Assertions** (`assert.sh`):

1. `Buckety/dial-tone` reaches `Ready=True`; Secret exists.
2. Scale Deployment `buckety-controller` (namespace
   `$E2E_CONTROLLER_NS`) to 0; wait for zero ready replicas.
3. Apply the consumer Job; it completes successfully.
4. Scale the operator back to 1.

This is the property COSI demonstrated for versitygw (see
ystack/e2e/agents-clusterautomation-acceptance-linux-amd64.sh)
and that Buckety inherits.
