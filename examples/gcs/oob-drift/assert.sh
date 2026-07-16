#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/drift 120s
bucket="$(secret_value drift-bucket bucket)"
endpoint="$(secret_value drift-bucket endpoint)"
log "initial bucket=$bucket"
gcs_bucket_exists "$bucket" "$endpoint"

log "out-of-band: deleting bucket $bucket directly"
gcs_bucket_delete "$bucket" "$endpoint"

# Wait up to 90s for the controller to recreate.
deadline=$(( $(date +%s) + 90 ))
recreated=0
while (( $(date +%s) < deadline )); do
  if gcs_bucket_exists_quiet "$bucket" "$endpoint"; then
    recreated=1
    break
  fi
  sleep 5
done
(( recreated == 1 )) \
  || fail "controller did not recreate bucket $bucket within 90s"

bucket_after="$(secret_value drift-bucket bucket)"
[[ "$bucket_after" == "$bucket" ]] \
  || fail "bucket name in Secret changed from $bucket to $bucket_after; backendResourceName is supposed to be sticky"

log "oob-drift PASS"
