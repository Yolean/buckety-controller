# kadm / retention-policy

**Scenario:** SPEC.md §End-to-end coverage #6 — Retention
policy.

Two Bucketys are created against the same backend with
different `retentionPolicy`. After deletion:

- `Retain` Buckety: backend topic survives, Secret is gone.
- `Delete` Buckety: backend topic gone, Secret is gone.

**Demonstrates:**

- `retentionPolicy: Retain` is the safe default per SPEC.
- Per-resource policy (no class-level setting).

**Assertions** (`assert.sh`):

1. Both Bucketys reach `Ready=True`; both Secrets exist; both
   topics exist on the broker.
2. Capture both topic names.
3. Delete the `Retain` Buckety. Wait for resource removal.
   Verify the topic still exists on the broker. Verify the
   `BucketyAccess` and its Secret are gone (owner-ref GC).
4. Delete the `Delete` Buckety. Wait for resource removal.
   Verify the topic is gone from the broker.
