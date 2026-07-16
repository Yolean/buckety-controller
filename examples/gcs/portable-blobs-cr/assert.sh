#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

# The portable contract: the CR applied here is the same file the
# s3 runs apply. A diff means someone forked the "portable" CR.
here="$(cd "$(dirname "$0")" && pwd)"
cmp "$here/buckety.yaml" "$here/../../s3/portable-blobs-cr/buckety.yaml" \
  || fail "portable CR diverged from examples/s3/portable-blobs-cr/buckety.yaml"

wait_ready buckety/userdata 120s
secret_has_keys userdata-blobs endpoint bucket project region accessKeyID secretAccessKey
secret_owned_label userdata-blobs

bucket="$(secret_value userdata-blobs bucket)"
endpoint="$(secret_value userdata-blobs endpoint)"
log "resolved bucket=$bucket on $endpoint"
gcs_bucket_exists "$bucket" "$endpoint"

# The "objects" backend declares versioning=true in its parameters;
# the emulator persists versioning, so this proves backend
# parameter defaults merged into the ensure. The CR's lifecycle and
# the backend's uniformBucketLevelAccess are accepted and applied
# too, but fake-gcs-server does not persist either attribute -
# their persistence is asserted on minio and was verified against
# real GCS.
gcs_bucket_versioning_enabled "$bucket" "$endpoint" \
  || fail "backend parameter default versioning=true not applied"

log "portable-blobs-cr PASS (gcs/$E2E_IMPLEMENTATION)"
