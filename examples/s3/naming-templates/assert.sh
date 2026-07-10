#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

# Harness's overlay sets the backend's defaults.zone to a known
# value so the resolved name is predictable. Default 'e2e' if
# not provided.
zone="${E2E_BACKEND_ZONE:-e2e}"

wait_ready buckety/templated 120s
resolved="$(kc get buckety/templated -o jsonpath='{.status.backendResourceName}')"
expected="${zone}-${E2E_NAMESPACE}-templated-v003"
[[ "$resolved" == "$expected" ]] \
  || fail "backendResourceName='$resolved', expected '$expected'"

# Stickiness: mutating the label MUST NOT change the resolved
# backend name.
log "patching label yolean.se/generation -> 042"
kc patch buckety/templated --type=merge -p \
  '{"metadata":{"labels":{"yolean.se/generation":"042"}}}'
sleep 5
resolved_after="$(kc get buckety/templated -o jsonpath='{.status.backendResourceName}')"
[[ "$resolved_after" == "$expected" ]] \
  || fail "backendResourceName changed to '$resolved_after' after label patch; should be sticky"

# Admission rejects a template that references a missing label.
log "applying bad-template.yaml; expect admission rejection"
if kc apply -f "$(dirname "$0")/bad-template.yaml" 2>/tmp/bad-template.err; then
  fail "admission accepted a template referencing missing label 'no.such/label'"
fi
grep -q "no.such/label" /tmp/bad-template.err \
  || fail "rejection message should reference 'no.such/label': $(cat /tmp/bad-template.err)"

# Admission rejects a template that resolves to an invalid bucket
# name (driver name validation).
log "applying bad-name.yaml; expect admission rejection on bucket name rules"
if kc apply -f "$(dirname "$0")/bad-name.yaml" 2>/tmp/bad-name.err; then
  fail "admission accepted a template resolving to an invalid bucket name"
fi
grep -q "lowercase" /tmp/bad-name.err \
  || fail "rejection message should explain bucket name rules: $(cat /tmp/bad-name.err)"

log "naming-templates PASS"
