#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/sticky-orig 120s
sticky_backend="$(kc get buckety/sticky-orig -o jsonpath='{.status.backend}')"
[[ "$sticky_backend" == "kafka" ]] \
  || fail "status.backend stamped as '$sticky_backend', expected 'kafka'"

# Save current controller config Secret and patch a rename.
# The harness provides a renamed buckety-controller.yaml under
# $E2E_RENAMED_CONFIG; this scenario applies it, restarts the
# controller, and restores the original at the end.
: "${E2E_RENAMED_CONFIG:?harness must set E2E_RENAMED_CONFIG to a yaml file that defines backend 'kafka-renamed' instead of 'kafka'}"
: "${E2E_ORIGINAL_CONFIG:?harness must set E2E_ORIGINAL_CONFIG to the unmodified controller config}"

restore() {
  log "restoring original controller config"
  kcg -n "$E2E_CONTROLLER_NS" create secret generic buckety-controller-config \
    --from-file=buckety-controller.yaml="$E2E_ORIGINAL_CONFIG" \
    --dry-run=client -o yaml | kcg apply -f -
  rollout_restart deploy/buckety-controller
  kcg -n "$E2E_CONTROLLER_NS" rollout status  deploy/buckety-controller --timeout=60s
}
trap restore EXIT

log "swapping controller config: kafka -> kafka-renamed"
kcg -n "$E2E_CONTROLLER_NS" create secret generic buckety-controller-config \
  --from-file=buckety-controller.yaml="$E2E_RENAMED_CONFIG" \
  --dry-run=client -o yaml | kcg apply -f -
rollout_restart deploy/buckety-controller
kcg -n "$E2E_CONTROLLER_NS" rollout status  deploy/buckety-controller --timeout=60s

wait_condition buckety/sticky-orig BackendUnavailable True 90s
[[ "$(condition_status buckety/sticky-orig Ready)" == "False" ]] \
  || fail "sticky-orig Ready should be False under BackendUnavailable"
still_sticky="$(kc get buckety/sticky-orig -o jsonpath='{.status.backend}')"
[[ "$still_sticky" == "kafka" ]] \
  || fail "status.backend mutated to '$still_sticky'; stickiness violated"

# A fresh Buckety against the renamed backend still works.
log "applying new Buckety against kafka-renamed"
kc apply -f "$(dirname "$0")/renamed.yaml"
wait_ready buckety/sticky-new 120s

# Deletion with retentionPolicy=Delete blocks while the backend is
# missing: removing the finalizer would silently orphan the topic.
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
  || fail "sticky-orig disappeared while its backend was missing; the topic would be orphaned"

# Restoring the backend unblocks the deletion and removes the topic.
restore
kc wait --for=delete buckety/sticky-orig --timeout=90s \
  || fail "sticky-orig deletion did not complete after the backend was restored"

log "backend-stickiness PASS"
