#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/orders 120s

# Implicit BucketyAccess of the same name exists with the label
# and owner-ref to the Buckety.
implicit_label="$(kc get bucketyaccess/orders -o jsonpath='{.metadata.labels.buckety\.yolean\.se/implicit}')"
[[ "$implicit_label" == "true" ]] \
  || fail "BucketyAccess/orders missing label buckety.yolean.se/implicit=true (got '$implicit_label')"
owner_kind="$(kc get bucketyaccess/orders -o jsonpath='{.metadata.ownerReferences[0].kind}')"
[[ "$owner_kind" == "Buckety" ]] \
  || fail "BucketyAccess/orders ownerReferences[0].kind=$owner_kind, expected Buckety"

# Secret shape per SPEC §Secret output (kadm driver).
secret_has_keys orders-topic bootstrap topic

topic_name="$(secret_value orders-topic topic)"
log "resolved topic name: $topic_name"
kafka_topic_exists "$topic_name"

# Consumer Job round-trips a message.
kc wait --for=condition=Complete --timeout=120s job/orders-roundtrip \
  || { kc logs job/orders-roundtrip >&2 || true; fail "consumer Job did not complete"; }

log "happy-path PASS"
