# s3 / multi-consumer

**Scenario:** SPEC.md §End-to-end coverage #2 — Multiple
consumers, different roles, for the `s3` driver.

Same lifecycle test as `kadm/multi-consumer/`; in v1alpha1 the
three Secrets carry identical credentials (no per-consumer IAM
minting), and `ScopingNotImplemented=True` surfaces on
non-ReadWrite accesses.

**Assertions** (`assert.sh`):

1. Buckety reaches Ready.
2. Three BucketyAccess resources reach Ready.
3. Three Secrets exist, each with the full s3 key set.
4. `bucket` and `endpoint` are identical across all three
   Secrets (same backing resource).
5. `ScopingNotImplemented=True` on Reader and Writer accesses;
   not present on the ReadWrite one.
