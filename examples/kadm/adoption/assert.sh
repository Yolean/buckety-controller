#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"
here="$(cd "$(dirname "$0")" && pwd)"

# Fresh name: proves provenance=Created. Kafka pre-creation needs
# no credentials (rpk talks straight to the broker), so ordering
# only matters for the colliding names.
kc apply -f "$here/fresh.yaml"
wait_ready buckety/adopt-fresh 120s
assert_provenance adopt-fresh Created

log "pre-creating topic adopt-pre (with a record) and adopt-void (empty)"
kafka_topic_create adopt-pre
kafka_topic_produce adopt-pre "predates the CR"
kafka_topic_create adopt-void

# Non-empty pre-existing topic: the gate refuses, nothing is
# stamped, no Secret is minted.
kc apply -f "$here/pre.yaml"
wait_ready_reason buckety/adopt-pre BackendResourceExists 60
if kc get secret/adopt-pre-creds >/dev/null 2>&1; then
  fail "Secret minted while adoption was refused"
fi

# Empty pre-existing topic: adopts under the default policy.
kc apply -f "$here/void.yaml"
wait_ready buckety/adopt-void 120s
assert_provenance adopt-void Adopted

# Explicit opt-in claims the non-empty topic.
log "unblocking adopt-pre with spec.adoption=Adopt"
kc patch buckety/adopt-pre --type=merge -p '{"spec":{"adoption":"Adopt"}}'
wait_ready buckety/adopt-pre 120s
assert_provenance adopt-pre Adopted
secret_has_keys adopt-pre-creds bootstrap topic

# Deleting an adopted Buckety retains the topic even with
# retentionPolicy=Delete.
log "deleting adopted Bucketys (retentionPolicy=Delete must degrade to Retain)"
kc delete buckety/adopt-pre --wait=true --timeout=90s
resource_absent secret/adopt-pre-creds 30s
kafka_topic_exists adopt-pre
kc delete buckety/adopt-void --wait=true --timeout=90s
kafka_topic_exists adopt-void

# Created topics still delete normally.
kc delete buckety/adopt-fresh --wait=true --timeout=90s
kafka_topic_absent adopt-fresh

# The retained topics are this scenario's responsibility.
log "cleaning up retained topics out-of-band"
kafka_topic_delete adopt-pre
kafka_topic_delete adopt-void

log "adoption PASS"
