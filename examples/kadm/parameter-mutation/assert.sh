#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/cfg-mut 120s
secret_has_keys cfg-mut-topic bootstrap topic
topic_name="$(secret_value cfg-mut-topic topic)"

# Phase 1: patch to a new retention. The impl branch must
# reconcile this to the broker; this assert checks the broker
# state via rpk.
log "patching retention.ms to 3600000"
kc patch buckety/cfg-mut --type=merge -p \
  '{"spec":{"parameters":{"config.retention.ms":"3600000"}}}'

# Wait for observedGeneration to advance past the patch.
gen_after="$(kc get buckety/cfg-mut -o jsonpath='{.metadata.generation}')"
for _ in $(seq 1 60); do
  observed="$(kc get buckety/cfg-mut -o jsonpath='{.status.observedGeneration}' 2>/dev/null || echo 0)"
  [[ "$observed" -ge "$gen_after" ]] && break
  sleep 2
done
[[ "$observed" -ge "$gen_after" ]] \
  || fail "observedGeneration ($observed) did not catch up to generation ($gen_after)"
wait_ready buckety/cfg-mut 60s

# Broker reflects the new retention.
log "checking broker for retention.ms=3600000 on topic '$topic_name'"
bootstrap="$(secret_value cfg-mut-topic bootstrap)"
broker_retention="$(kcg run -n "${E2E_KAFKA_NAMESPACE:-redpanda}" --rm -i --restart=Never --quiet \
  --image=ghcr.io/yolean/redpanda:v24.2.22@sha256:5132085d4fe35b0fd6ddedc7f0fe3d3ba7be12c5e3829e1a2b986cd41b1d3538 \
  "rpk-cfgcheck-$RANDOM" -- \
  rpk topic describe -p "$topic_name" --brokers "$bootstrap" 2>/dev/null \
  | awk '/retention.ms/ {print $2}')"
[[ "$broker_retention" == "3600000" ]] \
  || fail "broker retention.ms=$broker_retention, expected 3600000"

# Immutable-parameter rejection. Patching replicationFactor must
# fail at the admission webhook.
log "expecting admission to reject replicationFactor change"
if kc patch buckety/cfg-mut --type=merge -p \
    '{"spec":{"parameters":{"replicationFactor":"3"}}}' 2>/tmp/patch-err.txt; then
  fail "admission webhook accepted an immutable-parameter change"
fi
grep -q "replicationFactor" /tmp/patch-err.txt \
  || fail "rejection message does not mention 'replicationFactor': $(cat /tmp/patch-err.txt)"

log "parameter-mutation PASS"
