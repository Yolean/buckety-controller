#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/shared 120s
for s in shared-rw shared-reader; do
  wait_ready "bucketyaccess/$s" 120s
  secret_has_keys "$s" host port database username password jdbcUrl url
  sni="$(condition_status "bucketyaccess/$s" ScopingNotImplemented)"
  [[ -z "$sni" || "$sni" == "False" ]] || fail "$s: ScopingNotImplemented is '$sni'; the mysql driver scopes per role"
done

[[ "$(secret_value shared-rw database)" == "$(secret_value shared-reader database)" ]] \
  || fail "the accesses name different databases"
[[ "$(secret_value shared-rw username)" != "$(secret_value shared-reader username)" ]] \
  || fail "the accesses share a user; each access must get its own"

# Writer is refused, and nothing is minted for it.
wait_ready_reason bucketyaccess/shared-writer GrantFailed 60
if kc get secret/shared-writer >/dev/null 2>&1; then
  fail "a Secret was minted for the refused Writer access"
fi

kc wait --for=condition=Complete --timeout=120s job/shared-roles \
  || { kc logs job/shared-roles >&2 || true; fail "roles Job did not complete"; }

# Deletion blocks on explicit accesses; removing them drops their
# users, then the database (retentionPolicy=Delete).
log "deleting buckety/shared; expecting BlockedByAccesses"
kc delete buckety/shared --wait=false
wait_condition buckety/shared BlockedByAccesses True 60s
kc delete bucketyaccess --all --wait=true --timeout=90s
kc wait --for=delete buckety/shared --timeout=90s \
  || fail "shared deletion did not complete after accesses were removed"

log "multi-consumer PASS"
