# gcs happy path

SPEC scenario 1 for the gcs driver: a `Buckety` with
`defaultAccess` provisions a bucket through the native GCS JSON
API and mints a Secret with the documented keys (`endpoint`,
`bucket` — the resource-type key —, `project`, `accessKeyID`,
`secretAccessKey`). A consumer Job round-trips an object using
only the Secret.

`parameters.versioning` proves the create path carries native
parameters. The consumer Job uses the JSON API via curl because
e2e runs against fake-gcs-server, which does not implement the
S3-interop XML data path; against real GCS the same Secret works
with any S3 client via SigV4 (see the driver package docs).
