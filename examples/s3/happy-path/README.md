# s3 / happy-path

**Scenario:** SPEC.md §End-to-end coverage #1 — Single-consumer
happy path, for the `s3` driver.

A `Buckety` with `defaultAccess` provisions an S3 bucket and
materialises the Secret a single workload consumes. Runs
against any S3-compatible implementation; the harness's
per-implementation overlay decides which.

**Demonstrates:**

- A `Buckety` against backend `s3` (driver-typed name).
- The minted Secret carries the documented s3 keys:
  `endpoint`, `bucket` (resource-type key), optional `region`,
  `accessKeyID`, `secretAccessKey`.
- A consumer `Job` puts an object using credentials sourced
  from the Secret and reads it back.

**Assertions** (`assert.sh`):

1. `Buckety/orders` reaches `Ready=True`.
2. `Secret/orders-bucket` exists with keys `endpoint`,
   `bucket`, `accessKeyID`, `secretAccessKey`.
3. The bucket named in `Secret/.data.bucket` exists at the
   endpoint.
4. The consumer `Job` completes successfully (puts and gets
   an object).
