#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/shape 120s

log "expecting admission to reject unknown parameter 'unknownKey'"
if kc patch buckety/shape --type=merge \
    -p '{"spec":{"parameters":{"unknownKey":"x"}}}' 2>/tmp/s3-pm.err; then
  fail "admission accepted unknown parameter 'unknownKey'"
fi
grep -q "unknownKey" /tmp/s3-pm.err \
  || fail "rejection message should reference 'unknownKey': $(cat /tmp/s3-pm.err)"

log "parameter-mutation PASS"
