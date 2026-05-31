#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/shared 120s
for s in shared-reader shared-writer shared-rw; do
  wait_ready "bucketyaccess/$s" 60s
  secret_has_keys "$s" endpoint bucket accessKeyID secretAccessKey
done

b1="$(secret_value shared-reader bucket)"
b2="$(secret_value shared-writer bucket)"
b3="$(secret_value shared-rw     bucket)"
[[ "$b1" == "$b2" && "$b2" == "$b3" ]] \
  || fail "expected identical bucket across Secrets, got reader=$b1 writer=$b2 rw=$b3"

[[ "$(condition_status bucketyaccess/shared-reader ScopingNotImplemented)" == "True" ]] \
  || fail "shared-reader should have ScopingNotImplemented=True"
[[ "$(condition_status bucketyaccess/shared-writer ScopingNotImplemented)" == "True" ]] \
  || fail "shared-writer should have ScopingNotImplemented=True"
sni_rw="$(condition_status bucketyaccess/shared-rw ScopingNotImplemented)"
[[ -z "$sni_rw" || "$sni_rw" == "False" ]] \
  || fail "shared-rw should not have ScopingNotImplemented=True (got '$sni_rw')"

log "multi-consumer PASS"
