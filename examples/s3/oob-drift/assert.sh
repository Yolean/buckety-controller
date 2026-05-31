#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/drift 120s
bucket="$(secret_value drift-bucket bucket)"
endpoint="$(secret_value drift-bucket endpoint)"
access="$(secret_value drift-bucket accessKeyID)"
secret="$(secret_value drift-bucket secretAccessKey)"
log "initial bucket=$bucket"
s3_bucket_exists "$bucket" "$endpoint" "$access" "$secret"

log "out-of-band: deleting bucket $bucket directly"
kcg run -n "$E2E_CONTROLLER_NS" --rm -i --restart=Never --quiet \
  --image=public.ecr.aws/aws-cli/aws-cli:latest \
  --env="AWS_ACCESS_KEY_ID=$access" \
  --env="AWS_SECRET_ACCESS_KEY=$secret" \
  --env="AWS_REGION=us-east-1" \
  "awscli-oob-$RANDOM" -- \
  s3api delete-bucket --bucket "$bucket" --endpoint-url "$endpoint"

# Wait up to 90s for the controller to recreate.
deadline=$(( $(date +%s) + 90 ))
recreated=0
while (( $(date +%s) < deadline )); do
  if kcg run -n "$E2E_CONTROLLER_NS" --rm -i --restart=Never --quiet \
      --image=public.ecr.aws/aws-cli/aws-cli:latest \
      --env="AWS_ACCESS_KEY_ID=$access" \
      --env="AWS_SECRET_ACCESS_KEY=$secret" \
      --env="AWS_REGION=us-east-1" \
      "awscli-recheck-$RANDOM" -- \
      s3api head-bucket --bucket "$bucket" --endpoint-url "$endpoint" >/dev/null 2>&1; then
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
