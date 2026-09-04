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
kc run --rm -i --restart=Never --quiet \
  --image=ghcr.io/yolean/redpanda:v24.2.22@sha256:5132085d4fe35b0fd6ddedc7f0fe3d3ba7be12c5e3829e1a2b986cd41b1d3538 \
  "rpk-oob-set-$RANDOM" -- \
  topic alter-config "$topic_name" --set retention.ms=1 --brokers "$bootstrap"

log "expecting controller to reconcile retention.ms back to 3600000"
deadline=$(( $(date +%s) + 90 ))
current=""
while (( $(date +%s) < deadline )); do
  current="$(kc run --rm -i --restart=Never --quiet \
    --image=ghcr.io/yolean/redpanda:v24.2.22@sha256:5132085d4fe35b0fd6ddedc7f0fe3d3ba7be12c5e3829e1a2b986cd41b1d3538 \
    "rpk-oob-check-$RANDOM" -- \
    topic describe -c "$topic_name" --brokers "$bootstrap" 2>/dev/null \
    | awk '$1 == "retention.ms" {print $2}')"
  [[ "$current" == "3600000" ]] && break
  sleep 5
done
[[ "$current" == "3600000" ]] \
  || fail "controller did not reapply retention.ms=3600000 within 90s (broker reports $current)"

# Out-of-band unreconcilable change: grow the partition count on
# the broker. Kafka cannot shrink partitions, so the spec's 3 can
# never be re-applied; the controller surfaces ParameterDrift and
# waits for a human instead of attempting anything destructive.
log "out-of-band: adding 2 partitions on the broker (3 -> 5)"
rpk_run oob-parts topic add-partitions "$topic_name" --num 2 --brokers "$bootstrap" </dev/null \
  || fail "could not add partitions out of band"

wait_condition buckety/drift ParameterDrift True 90s
[[ "$(condition_status buckety/drift Ready)" == "False" ]] \
  || fail "Buckety/drift Ready should be False under ParameterDrift"

drift_msg="$(kc get buckety/drift \
  -o jsonpath='{.status.conditions[?(@.type=="ParameterDrift")].message}')"
grep -qi "partitions" <<<"$drift_msg" \
  || fail "ParameterDrift message should reference 'partitions', got: $drift_msg"

log "oob-drift PASS"
