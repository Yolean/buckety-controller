# portable blobs CR (s3)

SPEC "Driver families" scenario: `buckety.yaml` is byte-identical
to `examples/gcs/portable-blobs-cr/buckety.yaml` (assert.sh
enforces it) and provisions on every bucket-family implementation.
The CR carries only family-common parameters (`lifecycle`) against
the use-case-named backend `objects`; driver-specific and
site-wide defaults (`versioning: "true"`) come from that backend's
`parameters:` in the overlay config.

Per-implementation assertions:

- minio implements both family parameters, so the CR's lifecycle
  rules and the backend-default versioning are asserted on the
  bucket itself.
- versitygw's posix backend implements neither; the driver skips
  them fail-safe and Ready + Secret + bucket existence is the
  assertion.
