#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

# Stage 1: defaultAccess materialises an implicit access.
wait_ready buckety/orders 120s

implicit_label="$(kc get bucketyaccess/orders \
  -o jsonpath='{.metadata.labels.buckety\.yolean\.se/implicit}')"
[[ "$implicit_label" == "true" ]] \
  || fail "implicit BucketyAccess missing the implicit=true label (got '$implicit_label')"
owner_kind="$(kc get bucketyaccess/orders -o jsonpath='{.metadata.ownerReferences[0].kind}')"
owner_name="$(kc get bucketyaccess/orders -o jsonpath='{.metadata.ownerReferences[0].name}')"
[[ "$owner_kind" == "Buckety" && "$owner_name" == "orders" ]] \
  || fail "implicit BucketyAccess owner-ref is ($owner_kind, $owner_name); expected (Buckety, orders)"

secret_has_keys orig-secret bootstrap topic
topic_before="$(secret_value orig-secret topic)"
log "topic before migration: $topic_before"

# Stage 2: drop defaultAccess from the Buckety, add an explicit
# BucketyAccess with a fresh Secret name.
log "applying stage 2 (defaultAccess removed, explicit access added)"
kc apply -k "$(dirname "$0")/migrated"

# Implicit reclaim within 60s.
resource_absent bucketyaccess/orders 60s
resource_absent secret/orig-secret  60s

# Explicit access reconciles.
wait_ready bucketyaccess/orders-explicit 60s
secret_has_keys explicit-secret bootstrap topic

# Same underlying backend topic; the Buckety did not get
# recreated when defaultAccess was removed.
topic_after="$(secret_value explicit-secret topic)"
[[ "$topic_after" == "$topic_before" ]] \
  || fail "topic name changed across migration (before=$topic_before after=$topic_after); status.backendResourceName is supposed to be sticky on the Buckety"

log "access-migration PASS"
