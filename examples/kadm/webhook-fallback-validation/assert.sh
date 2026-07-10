#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

dir="$(cd "$(dirname "$0")" && pwd)"
vwc_backup="/tmp/buckety-vwc-backup-$$.json"

# Server-populated metadata (uid, resourceVersion, ...) must be
# stripped or the re-create after deletion is rejected.
kcg get validatingwebhookconfiguration buckety-controller -o json \
  | jq 'del(.metadata.uid, .metadata.resourceVersion, .metadata.creationTimestamp, .metadata.generation, .metadata.managedFields)' \
  > "$vwc_backup" \
  || fail "cannot back up the ValidatingWebhookConfiguration"
[[ -s "$vwc_backup" ]] || fail "empty ValidatingWebhookConfiguration backup"

restore() {
  log "restoring ValidatingWebhookConfiguration"
  kcg apply -f "$vwc_backup" >/dev/null
}
trap restore EXIT

log "removing ValidatingWebhookConfiguration (simulates --enable-webhook=false)"
kcg delete validatingwebhookconfiguration buckety-controller

wait_ready buckety/base 120s

log "applying invalid parameters without admission in the way"
kc apply -f "$dir/invalid-params.yaml"
kc apply -f "$dir/invalid-access.yaml"

expect_invalid() {
  local target="$1"
  local reason=""
  for _ in $(seq 1 20); do
    reason="$(kc get "$target" \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true)"
    [[ "$reason" == "InvalidParameters" ]] && break
    sleep 3
  done
  [[ "$reason" == "InvalidParameters" ]] \
    || { kc get "$target" -o yaml >&2; fail "$target Ready reason='$reason', expected InvalidParameters"; }
  local msg
  msg="$(kc get "$target" -o jsonpath='{.status.conditions[?(@.type=="Ready")].message}')"
  [[ "$msg" == *"bogus"* ]] \
    || fail "$target InvalidParameters message should name the offending key: '$msg'"
}

expect_invalid buckety/invalid
expect_invalid bucketyaccess/bad-access

# The invalid access must not have minted a Secret.
if kc get secret/bad-access >/dev/null 2>&1; then
  fail "secret/bad-access minted despite invalid parameters"
fi

restore
# Prove admission is back before later scenarios rely on it: an
# invalid apply must be rejected again (propagation can lag a
# moment after the configuration reappears).
rejected=""
for _ in $(seq 1 10); do
  if ! kc apply -f "$dir/invalid-params.yaml" --dry-run=server >/dev/null 2>&1; then
    rejected=yes
    break
  fi
  sleep 3
done
[[ "$rejected" == "yes" ]] \
  || fail "admission did not resume rejecting after the webhook configuration was restored"

log "webhook-fallback-validation PASS"
