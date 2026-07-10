# s3 / backend-stickiness

**Scenario:** SPEC "Buckety shape" sticky status fields plus the
deletion-block rule from SPEC "Lifecycle and deletion".

1. A Buckety against backend `s3` reaches Ready; `status.backend`
   is stamped.
2. The controller config is swapped for one that renames the
   backend to `s3-renamed`. The existing resource surfaces
   `BackendUnavailable`, keeps `status.backend=s3`, and a fresh
   Buckety against `s3-renamed` works.
3. Deleting the stamped Buckety (retentionPolicy=Delete) while its
   backend is missing MUST block rather than silently orphan the
   bucket: the finalizer stays until the original config is
   restored, then deletion completes and the bucket is removed.
