#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/orders 120s
secret_has_keys orders-bucket endpoint bucket project region accessKeyID secretAccessKey

bucket="$(secret_value orders-bucket bucket)"
endpoint="$(secret_value orders-bucket endpoint)"
log "resolved bucket=$bucket on $endpoint"
gcs_bucket_exists "$bucket" "$endpoint"
gcs_bucket_versioning_enabled "$bucket" "$endpoint" \
  || fail "parameters.versioning=true not reflected on the backend"

kc wait --for=condition=Complete --timeout=120s job/orders-roundtrip \
  || { kc logs job/orders-roundtrip >&2 || true; fail "roundtrip Job did not complete"; }

log "happy-path PASS"
