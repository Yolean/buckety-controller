# s3 / retention-policy

**Scenario:** SPEC.md §End-to-end coverage #6 — Retention
policy, for the `s3` driver.

Same shape as `kadm/retention-policy/`. With S3, "Delete"
means the bucket is emptied and removed; the driver MUST
handle non-empty buckets honestly (refuse to delete or
empty-then-delete; impl-time decision).

**Assertions** (`assert.sh`):

1. Both Bucketys reach Ready; both Secrets exist; both
   buckets exist at the backend.
2. Delete the `Retain` Buckety; bucket survives.
3. Delete the `Delete` Buckety; bucket is gone (or the
   driver surfaces a clear condition explaining a non-empty
   bucket; assert one or the other).
