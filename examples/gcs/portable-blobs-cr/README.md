# portable blobs CR (gcs)

SPEC "Driver families" scenario: `buckety.yaml` is byte-identical
to `examples/s3/portable-blobs-cr/buckety.yaml` (assert.sh
enforces it) and provisions on every bucket-family implementation.
The CR carries only family-common parameters (`lifecycle`) against
the use-case-named backend `objects`; driver-specific defaults
(`uniformBucketLevelAccess`) and site-wide defaults
(`versioning: "true"`) come from that backend's `parameters:` in
the overlay config.

The emulator persists versioning, which is the observable proof
that backend parameter defaults merged into the ensure. It does
not persist lifecycle or UBLA (see parameter-mutation's README);
their persistence is asserted on minio and was verified against
real GCS.
