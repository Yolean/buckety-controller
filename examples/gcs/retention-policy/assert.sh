#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/keep-me 120s
wait_ready buckety/drop-me 120s

keep_bucket="$(secret_value keep-me-bucket bucket)"
drop_bucket="$(secret_value drop-me-bucket bucket)"
endpoint="$(secret_value keep-me-bucket endpoint)"
gcs_bucket_exists "$keep_bucket" "$endpoint"
gcs_bucket_exists "$drop_bucket" "$endpoint"

# Retain: bucket survives Buckety deletion.
log "deleting Buckety/keep-me (Retain)"
kc delete buckety/keep-me --wait=true --timeout=60s
resource_absent secret/keep-me-bucket 30s
gcs_bucket_exists "$keep_bucket" "$endpoint"

# Delete: bucket gone after Buckety deletion.
log "deleting Buckety/drop-me (Delete)"
kc delete buckety/drop-me --wait=true --timeout=60s
resource_absent secret/drop-me-bucket 30s
if gcs_bucket_exists_quiet "$drop_bucket" "$endpoint"; then
  fail "bucket $drop_bucket still present after Buckety with retentionPolicy=Delete was deleted"
fi

log "retention-policy PASS"
