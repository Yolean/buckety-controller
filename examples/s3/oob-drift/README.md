# s3 / oob-drift

**Scenario:** SPEC.md §End-to-end coverage #4 — Out-of-band
drift, for the `s3` driver.

When the backend bucket is removed out-of-band, the controller
observes the absence on its next re-check and recreates it.
The minted Secret is unchanged (same name, same endpoint), so
consumers don't need to re-read it.

**Assertions** (`assert.sh`):

1. Buckety reaches Ready; bucket exists.
2. Directly delete the bucket via the backend's API (root
   creds, ephemeral aws-cli pod).
3. Within the re-check window, the controller recreates the
   bucket. `Ready=True` again; the Secret's `bucket` value is
   unchanged (sticky `backendResourceName`).

Note: v1alpha1 has no "ParameterDrift" surface for s3 since
there are no reconcile-controlled bucket parameters yet. This
scenario only covers presence drift.
