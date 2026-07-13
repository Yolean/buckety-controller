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
kafka_topic_absent "$drop_topic"

log "retention-policy PASS"
