#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/clash 120s

# The access must surface SecretConflict, not adopt the Secret.
reason=""
for _ in $(seq 1 20); do
  reason="$(kc get bucketyaccess/clash-access \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true)"
  [[ "$reason" == "SecretConflict" ]] && break
  sleep 3
done
[[ "$reason" == "SecretConflict" ]] \
  || { kc get bucketyaccess/clash-access -o yaml >&2; fail "expected Ready reason=SecretConflict, got '$reason'"; }

# The user's Secret is untouched: original data intact, no minted
# keys, no adopted owner reference.
[[ "$(secret_value occupied precious)" == "user-data-that-must-survive" ]] \
  || fail "pre-existing Secret data was overwritten"
if kc get secret/occupied -o jsonpath='{.data.bootstrap}' 2>/dev/null | grep -q .; then
  fail "controller wrote minted keys into the pre-existing Secret"
fi
[[ -z "$(kc get secret/occupied -o jsonpath='{.metadata.ownerReferences}')" ]] \
  || fail "controller adopted the pre-existing Secret via ownerReferences"

# A Warning Event accompanies the condition (kubectl describe UX).
event_found=""
for _ in $(seq 1 10); do
  if kc get events --field-selector reason=SecretConflict -o name 2>/dev/null | grep -q .; then
    event_found=yes
    break
  fi
  sleep 3
done
[[ "$event_found" == "yes" ]] \
  || fail "no SecretConflict Event recorded for the access"

# Removing the conflicting Secret resolves the conflict on the next
# periodic re-check.
log "deleting the conflicting Secret; expecting the access to recover"
kc delete secret/occupied
wait_ready bucketyaccess/clash-access 90s
secret_has_keys occupied bootstrap topic
owner_kind="$(kc get secret/occupied -o jsonpath='{.metadata.ownerReferences[0].kind}')"
[[ "$owner_kind" == "BucketyAccess" ]] \
  || fail "minted Secret should be owned by the BucketyAccess, got '$owner_kind'"

log "secret-conflict PASS"
