#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/sticky-orig 120s
sticky_backend="$(kc get buckety/sticky-orig -o jsonpath='{.status.backend}')"
[[ "$sticky_backend" == "gcs" ]] \
  || fail "status.backend stamped as '$sticky_backend', expected 'gcs'"

: "${E2E_RENAMED_CONFIG:?harness must set E2E_RENAMED_CONFIG to a yaml file that defines backend 'gcs-renamed' instead of 'gcs'}"
: "${E2E_ORIGINAL_CONFIG:?harness must set E2E_ORIGINAL_CONFIG to the unmodified controller config}"

apply_config() {
  kcg -n "$E2E_CONTROLLER_NS" create secret generic buckety-controller-config \
    --from-file=buckety-controller.yaml="$1" \
    --dry-run=client -o yaml | kcg apply -f -
  kcg -n "$E2E_CONTROLLER_NS" rollout restart deploy/buckety-controller
  kcg -n "$E2E_CONTROLLER_NS" rollout status  deploy/buckety-controller --timeout=60s
}

restore() {
  log "restoring original controller config"
  apply_config "$E2E_ORIGINAL_CONFIG"
}
trap restore EXIT

log "swapping controller config: gcs -> gcs-renamed"
apply_config "$E2E_RENAMED_CONFIG"

wait_condition buckety/sticky-orig BackendUnavailable True 90s
[[ "$(condition_status buckety/sticky-orig Ready)" == "False" ]] \
  || fail "sticky-orig Ready should be False under BackendUnavailable"
still_sticky="$(kc get buckety/sticky-orig -o jsonpath='{.status.backend}')"
[[ "$still_sticky" == "gcs" ]] \
  || fail "status.backend mutated to '$still_sticky'; stickiness violated"

# A fresh Buckety against the renamed backend still works.
log "applying new Buckety against gcs-renamed"
kc apply -f "$(dirname "$0")/renamed.yaml"
wait_ready buckety/sticky-new 120s

# Deletion with retentionPolicy=Delete blocks while the backend is
# missing: removing the finalizer would silently orphan the bucket.
log "deleting sticky-orig while its backend is missing; expecting the deletion to block"
kc delete buckety/sticky-orig --wait=false
blocked=""
for _ in $(seq 1 20); do
  blocked="$(kc get buckety/sticky-orig \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null || true)"
  [[ "$blocked" == *"deletion blocked"* ]] && break
  sleep 3
done
[[ "$blocked" == *"deletion blocked"* ]] \
  || fail "sticky-orig deletion did not surface 'deletion blocked' (Ready message: '$blocked')"
kc get buckety/sticky-orig >/dev/null 2>&1 \
  || fail "sticky-orig disappeared while its backend was missing; the bucket would be orphaned"

# Restoring the backend unblocks the deletion and removes the bucket.
restore
kc wait --for=delete buckety/sticky-orig --timeout=90s \
  || fail "sticky-orig deletion did not complete after the backend was restored"

log "backend-stickiness PASS"
