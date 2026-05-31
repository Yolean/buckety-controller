#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/keep-me 120s
wait_ready buckety/drop-me 120s

keep_topic="$(secret_value keep-me-topic topic)"
drop_topic="$(secret_value drop-me-topic topic)"
log "keep=$keep_topic drop=$drop_topic"
kafka_topic_exists "$keep_topic"
kafka_topic_exists "$drop_topic"

# Retain: backend survives.
log "deleting Buckety/keep-me (Retain)"
kc delete buckety/keep-me --wait=true --timeout=60s
resource_absent secret/keep-me-topic 30s
log "verifying topic '$keep_topic' survived"
kafka_topic_exists "$keep_topic"

# Delete: backend gone.
log "deleting Buckety/drop-me (Delete)"
kc delete buckety/drop-me --wait=true --timeout=60s
resource_absent secret/drop-me-topic 30s
log "verifying topic '$drop_topic' was removed from broker"
if kcg run -n "${E2E_KAFKA_NAMESPACE:-redpanda}" --rm -i --restart=Never --quiet \
    --image=ghcr.io/yolean/redpanda:v24.2.22@sha256:5132085d4fe35b0fd6ddedc7f0fe3d3ba7be12c5e3829e1a2b986cd41b1d3538 \
    "rpk-deleted-$RANDOM" -- \
    rpk topic list --brokers "${E2E_KAFKA_BOOTSTRAP:-redpanda.redpanda.svc.cluster.local:9093}" 2>/dev/null \
    | grep -qE "^\s*$drop_topic\s"; then
  fail "topic '$drop_topic' still present after Buckety with retentionPolicy=Delete was deleted"
fi

log "retention-policy PASS"
