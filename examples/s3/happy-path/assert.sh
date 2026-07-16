#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/orders 120s
secret_has_keys orders-bucket endpoint bucket accessKeyID secretAccessKey
secret_owned_label orders-bucket

bucket="$(secret_value orders-bucket bucket)"
endpoint="$(secret_value orders-bucket endpoint)"
access="$(secret_value orders-bucket accessKeyID)"
secret="$(secret_value orders-bucket secretAccessKey)"
log "resolved bucket=$bucket on $endpoint"
s3_bucket_exists "$bucket" "$endpoint" "$access" "$secret"

kc wait --for=condition=Complete --timeout=120s job/orders-roundtrip \
  || { kc logs job/orders-roundtrip >&2 || true; fail "roundtrip Job did not complete"; }

log "happy-path PASS"
