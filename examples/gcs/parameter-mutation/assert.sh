#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/shape 120s
bucket="$(secret_value shape-bucket bucket)"
endpoint="$(secret_value shape-bucket endpoint)"
gcs_bucket_versioning_enabled "$bucket" "$endpoint" \
  || fail "initial parameters.versioning=true not reflected on the backend"

log "mutating parameters.versioning true -> false"
kc patch buckety/shape --type=merge \
  -p '{"spec":{"parameters":{"versioning":"false"}}}'

deadline=$(( $(date +%s) + 90 ))
reconciled=0
while (( $(date +%s) < deadline )); do
  if ! gcs_bucket_versioning_enabled "$bucket" "$endpoint"; then
    reconciled=1
    break
  fi
  sleep 5
done
(( reconciled == 1 )) \
  || fail "versioning=false was not reconciled to the backend within 90s"

log "expecting admission to reject unknown parameter 'unknownKey'"
if kc patch buckety/shape --type=merge \
    -p '{"spec":{"parameters":{"unknownKey":"x"}}}' 2>/tmp/gcs-pm.err; then
  fail "admission accepted unknown parameter 'unknownKey'"
fi
grep -q "unknownKey" /tmp/gcs-pm.err \
  || fail "rejection message should reference 'unknownKey': $(cat /tmp/gcs-pm.err)"

log "expecting admission to reject a location change (immutable post-create)"
if kc patch buckety/shape --type=merge \
    -p '{"spec":{"parameters":{"location":"US"}}}' 2>/tmp/gcs-loc.err; then
  fail "admission accepted a location change"
fi
grep -q "immutable" /tmp/gcs-loc.err \
  || fail "rejection message should explain immutability: $(cat /tmp/gcs-loc.err)"

log "parameter-mutation PASS"
