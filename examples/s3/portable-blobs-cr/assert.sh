#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

# The portable contract: the CR applied here is the same file the
# gcs run applies. A diff means someone forked the "portable" CR.
here="$(cd "$(dirname "$0")" && pwd)"
cmp "$here/buckety.yaml" "$here/../../gcs/portable-blobs-cr/buckety.yaml" \
  || fail "portable CR diverged from examples/gcs/portable-blobs-cr/buckety.yaml"

wait_ready buckety/userdata 120s
secret_has_keys userdata-blobs endpoint bucket accessKeyID secretAccessKey
secret_owned_label userdata-blobs

bucket="$(secret_value userdata-blobs bucket)"
endpoint="$(secret_value userdata-blobs endpoint)"
access="$(secret_value userdata-blobs accessKeyID)"
secret="$(secret_value userdata-blobs secretAccessKey)"
log "resolved bucket=$bucket on $endpoint"
s3_bucket_exists "$bucket" "$endpoint" "$access" "$secret"

case "$E2E_IMPLEMENTATION" in
  minio)
    # MinIO (SNSD) implements both family parameters: prove the
    # CR's lifecycle and the backend-default versioning=true (from
    # the "objects" backend's parameters) reached the bucket.
    out="$(s3_api "$endpoint" "$access" "$secret" \
      get-bucket-versioning --bucket "$bucket")" \
      || { printf '%s\n' "$out" >&2; fail "get-bucket-versioning failed"; }
    printf '%s' "$out" | grep -q '"Status": "Enabled"' \
      || fail "backend parameter default versioning=true not applied: $out"

    out="$(s3_api "$endpoint" "$access" "$secret" \
      get-bucket-lifecycle-configuration --bucket "$bucket")" \
      || { printf '%s\n' "$out" >&2; fail "get-bucket-lifecycle-configuration failed"; }
    rules="$(printf '%s' "$out" | grep -c '"Expiration"')" || true
    [[ "$rules" == 2 ]] \
      || fail "expected 2 lifecycle expiration rules, got $rules: $out"
    if ! printf '%s' "$out" | grep -qF 'board-prints/' \
        || ! printf '%s' "$out" | grep -qF '.staging/'; then
      fail "lifecycle prefixes missing: $out"
    fi
    ;;
  *)
    # versitygw (posix): family parameters the gateway cannot
    # express are skipped fail-safe (SPEC "Driver families") - the
    # bucket still provisions, which wait_ready above proved.
    log "family parameter persistence not asserted on $E2E_IMPLEMENTATION (fail-safe backend)"
    ;;
esac

log "portable-blobs-cr PASS (s3/$E2E_IMPLEMENTATION)"
