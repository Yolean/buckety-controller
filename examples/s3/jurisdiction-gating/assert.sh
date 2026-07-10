#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

dir="$(cd "$(dirname "$0")" && pwd)"

log "applying accepted.yaml (jurisdiction on r2 backend)"
kc apply -f "$dir/accepted.yaml" \
  || fail "admission rejected jurisdiction against an r2 backend"

log "applying rejected-on-non-r2.yaml (jurisdiction on non-r2 backend) - expecting rejection"
if kc apply -f "$dir/rejected-on-non-r2.yaml" 2>/tmp/r2-rej.err; then
  fail "admission accepted jurisdiction against a non-r2 backend"
fi
grep -q "jurisdiction" /tmp/r2-rej.err \
  || fail "rejection message should reference 'jurisdiction': $(cat /tmp/r2-rej.err)"

log "jurisdiction-gating PASS"
