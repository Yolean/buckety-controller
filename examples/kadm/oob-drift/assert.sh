#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/drift 120s
topic_name="$(secret_value drift-topic topic)"
bootstrap="$(secret_value drift-topic bootstrap)"

# Out-of-band reconcilable change. Spec said retention.ms=3600000;
# directly poke retention.ms=1 on the broker, then wait for the
# controller's next periodic re-check to restore it.
log "out-of-band: setting retention.ms=1 directly on broker"
kcg run -n "${E2E_KAFKA_NAMESPACE:-redpanda}" --rm -i --restart=Never --quiet \
  --image=ghcr.io/yolean/redpanda:v24.2.22@sha256:5132085d4fe35b0fd6ddedc7f0fe3d3ba7be12c5e3829e1a2b986cd41b1d3538 \
  "rpk-oob-set-$RANDOM" -- \
  rpk topic alter-config "$topic_name" --set retention.ms=1 --brokers "$bootstrap"

log "expecting controller to reconcile retention.ms back to 3600000"
for _ in $(seq 1 60); do
  current="$(kcg run -n "${E2E_KAFKA_NAMESPACE:-redpanda}" --rm -i --restart=Never --quiet \
    --image=ghcr.io/yolean/redpanda:v24.2.22@sha256:5132085d4fe35b0fd6ddedc7f0fe3d3ba7be12c5e3829e1a2b986cd41b1d3538 \
    "rpk-oob-check-$RANDOM" -- \
    rpk topic describe -p "$topic_name" --brokers "$bootstrap" 2>/dev/null \
    | awk '/retention.ms/ {print $2}')"
  [[ "$current" == "3600000" ]] && break
  sleep 5
done
[[ "$current" == "3600000" ]] \
  || fail "controller did not reapply retention.ms=3600000 (broker reports $current)"

# Spec-level unsafe change. Reduce partitions in the spec; the
# driver cannot shrink Kafka partitions in place, so surfaces
# ParameterDrift instead of attempting destructive action.
log "spec change: reducing partitions from 3 to 2 (unsafe)"
kc patch buckety/drift --type=merge -p '{"spec":{"parameters":{"partitions":"2"}}}'

wait_condition buckety/drift ParameterDrift True 90s
[[ "$(condition_status buckety/drift Ready)" == "False" ]] \
  || fail "Buckety/drift Ready should be False under ParameterDrift"

drift_msg="$(kc get buckety/drift \
  -o jsonpath='{.status.conditions[?(@.type=="ParameterDrift")].message}')"
grep -qi "partitions" <<<"$drift_msg" \
  || fail "ParameterDrift message should reference 'partitions', got: $drift_msg"

log "oob-drift PASS"
