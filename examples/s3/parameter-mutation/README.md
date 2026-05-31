# s3 / parameter-mutation

**Scenario:** SPEC.md §End-to-end coverage #3 — Parameter
mutation, for the `s3` driver.

v1alpha1 s3 has no common-shape mutable parameters
(capability-gated ones like `jurisdiction` are set-on-create
and immutable). The scenario therefore exercises the
schema's `additionalProperties: false` guard: an unknown
parameter key is rejected at admission, with the rejection
message naming the offending key.

This is a smaller scenario than the kadm equivalent by design;
the contract is the same.

**Assertions** (`assert.sh`):

1. `Buckety/shape` reaches Ready.
2. Patch `spec.parameters` to add `unknownKey: "x"`. Webhook
   rejects with a message that names `unknownKey`.
