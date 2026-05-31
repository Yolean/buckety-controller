#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/keep-me 120s
wait_ready buckety/drop-me 120s

keep_bucket="$(secret_value keep-me-bucket bucket)"
drop_bucket="$(secret_value drop-me-bucket bucket)"
endpoint="$(secret_value keep-me-bucket endpoint)"
access="$(secret_value keep-me-bucket accessKeyID)"
secret="$(secret_value keep-me-bucket secretAccessKey)"
s3_bucket_exists "$keep_bucket" "$endpoint" "$access" "$secret"
s3_bucket_exists "$drop_bucket" "$endpoint" "$access" "$secret"

# Retain: bucket survives Buckety deletion.
log "deleting Buckety/keep-me (Retain)"
kc delete buckety/keep-me --wait=true --timeout=60s
resource_absent secret/keep-me-bucket 30s
s3_bucket_exists "$keep_bucket" "$endpoint" "$access" "$secret"

# Delete: bucket gone after Buckety deletion.
log "deleting Buckety/drop-me (Delete)"
kc delete buckety/drop-me --wait=true --timeout=60s
resource_absent secret/drop-me-bucket 30s
if kcg run -n "$E2E_CONTROLLER_NS" --rm -i --restart=Never --quiet \
    --image=public.ecr.aws/aws-cli/aws-cli:latest \
    --env="AWS_ACCESS_KEY_ID=$access" \
    --env="AWS_SECRET_ACCESS_KEY=$secret" \
    "awscli-deleted-$RANDOM" -- \
    s3api head-bucket --bucket "$drop_bucket" --endpoint-url "$endpoint" >/dev/null 2>&1; then
  fail "bucket $drop_bucket still present after Buckety with retentionPolicy=Delete was deleted"
fi

log "retention-policy PASS"
