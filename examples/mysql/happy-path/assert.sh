#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/orders 120s
wait_ready bucketyaccess/orders 120s
secret_has_keys orders-db host port database username password jdbcUrl url
secret_owned_label orders-db

want="b_${E2E_NAMESPACE}_orders"
db="$(secret_value orders-db database)"
[[ "$db" == "$want" ]] || fail "Secret database is '$db', expected '$want'"
resolved="$(kc get buckety/orders -o jsonpath='{.status.backendResourceName}')"
[[ "$resolved" == "$want" ]] || fail "status.backendResourceName is '$resolved', expected '$want'"

user="$(secret_value orders-db username)"
[[ "$user" == b_* && ${#user} -le 32 ]] || fail "username '$user' is not a b_ name of at most 32 characters"
principal="$(kc get bucketyaccess/orders -o jsonpath='{.status.principal}')"
[[ "$principal" == "$user@%" ]] || fail "status.principal is '$principal', expected '$user@%'"
jdbc="$(secret_value orders-db jdbcUrl)"
[[ "$jdbc" == "jdbc:mariadb://mysql.mysql.svc.cluster.local:3306/$want" ]] || fail "jdbcUrl is '$jdbc'"

# The mysql driver scopes per role: no ScopingNotImplemented.
sni="$(condition_status bucketyaccess/orders ScopingNotImplemented)"
[[ -z "$sni" || "$sni" == "False" ]] || fail "ScopingNotImplemented is '$sni'"

kc wait --for=condition=Complete --timeout=120s job/orders-roundtrip \
  || { kc logs job/orders-roundtrip >&2 || true; fail "roundtrip Job did not complete"; }

log "happy-path PASS"
