#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/dial-tone 120s
secret_has_keys dial-tone-bucket endpoint bucket accessKeyID secretAccessKey

# Scale operator to zero before the consumer Job runs.
log "scaling buckety-controller to 0 in $E2E_CONTROLLER_NS"
kcg -n "$E2E_CONTROLLER_NS" scale deploy/buckety-controller --replicas=0
kcg -n "$E2E_CONTROLLER_NS" wait --for=delete pod \
  -l app.kubernetes.io/name=buckety-controller --timeout=60s

# Apply the consumer Job while the operator is down.
log "applying consumer Job (operator is down)"
kc apply -f "$(dirname "$0")/consumer-job.yaml"
kc wait --for=condition=Complete --timeout=120s job/dial-tone-roundtrip \
  || { kc logs job/dial-tone-roundtrip >&2 || true; fail "roundtrip Job failed while operator was scaled to 0"; }

# Restore replica count so subsequent scenarios see a running
# controller.
log "restoring buckety-controller to 1"
kcg -n "$E2E_CONTROLLER_NS" scale deploy/buckety-controller --replicas=1
kcg -n "$E2E_CONTROLLER_NS" rollout status deploy/buckety-controller --timeout=60s

log "scaled-to-zero PASS"
