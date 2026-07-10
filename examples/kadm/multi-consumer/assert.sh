#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/orders 120s
wait_ready bucketyaccess/orders-reader 60s
wait_ready bucketyaccess/orders-writer 60s
wait_ready bucketyaccess/orders-rw 60s

for secret in orders-reader orders-writer orders-rw; do
  secret_has_keys "$secret" bootstrap topic
done

# All three Secrets resolve the same backend topic; per-consumer
# scoping is not implemented in v1alpha1.
t_reader="$(secret_value orders-reader topic)"
t_writer="$(secret_value orders-writer topic)"
t_rw="$(secret_value orders-rw     topic)"
[[ "$t_reader" == "$t_writer" && "$t_writer" == "$t_rw" ]] \
  || fail "expected identical topic across Secrets, got reader=$t_reader writer=$t_writer rw=$t_rw"

kafka_topic_exists "$t_rw"

# ScopingNotImplemented surfaces honestly on non-ReadWrite roles
# and is absent on the ReadWrite one.
[[ "$(condition_status bucketyaccess/orders-reader ScopingNotImplemented)" == "True" ]] \
  || fail "BucketyAccess/orders-reader should have ScopingNotImplemented=True"
[[ "$(condition_status bucketyaccess/orders-writer ScopingNotImplemented)" == "True" ]] \
  || fail "BucketyAccess/orders-writer should have ScopingNotImplemented=True"
[[ -z "$(condition_status bucketyaccess/orders-rw ScopingNotImplemented)" \
  || "$(condition_status bucketyaccess/orders-rw ScopingNotImplemented)" == "False" ]] \
  || fail "BucketyAccess/orders-rw should not have ScopingNotImplemented=True"

# Deletion blocks on explicit accesses (SPEC "Lifecycle and
# deletion"): the controller surfaces BlockedByAccesses and does
# not cascade. Removing the accesses unblocks promptly via the
# access watch.
log "deleting buckety/orders; expecting BlockedByAccesses"
kc delete buckety/orders --wait=false
wait_condition buckety/orders BlockedByAccesses True 60s
kc get buckety/orders >/dev/null 2>&1 \
  || fail "orders deleted while explicit accesses existed; cascade is not allowed"
log "deleting the accesses; expecting orders deletion to complete"
kc delete bucketyaccess --all --wait=true --timeout=60s
kc wait --for=delete buckety/orders --timeout=60s \
  || fail "orders deletion did not complete after accesses were removed"

log "multi-consumer PASS"
