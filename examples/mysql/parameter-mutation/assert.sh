#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/shape 120s
db="$(secret_value shape-db database)"
[[ "$(mysql_root "SELECT CONCAT(DEFAULT_CHARACTER_SET_NAME, ' ', DEFAULT_COLLATION_NAME) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$db'")" == "utf8mb4 utf8mb4_unicode_ci" ]] \
  || fail "database $db was not created with the requested character set and collation"

err="$(mktemp)"
trap 'rm -f "$err"' EXIT

# expect_refused <what> <regex> <kubectl args...>
expect_refused() {
  local what="$1" regex="$2"; shift 2
  log "expecting admission to refuse: $what"
  if "$@" 2>"$err"; then
    fail "admission accepted $what"
  fi
  grep -qE "$regex" "$err" || fail "refusal of $what should match /$regex/: $(cat "$err")"
}

expect_refused "a characterSet change (immutable)" "immutable" \
  kc patch buckety/shape --type=merge -p '{"spec":{"parameters":{"characterSet":"latin1"}}}'
expect_refused "removing collation (immutable)" "immutable" \
  kc patch buckety/shape --type=json -p '[{"op":"remove","path":"/spec/parameters/collation"}]'
expect_refused "an unknown parameter" "unknownKey" \
  kc patch buckety/shape --type=merge -p '{"spec":{"parameters":{"unknownKey":"x"}}}'

# New resources: the ambiguous utf8, and a name outside namePrefix.
expect_refused "characterSet utf8" "ambiguous" kc apply -f - <<'YAML'
apiVersion: buckety.yolean.se/v1alpha1
kind: Buckety
metadata:
  name: shape-utf8
spec:
  backend: mysql
  name: "b_${namespace}_${name}"
  parameters:
    characterSet: utf8
YAML
expect_refused "a database name outside namePrefix" "namePrefix" kc apply -f - <<'YAML'
apiVersion: buckety.yolean.se/v1alpha1
kind: Buckety
metadata:
  name: shape-noprefix
spec:
  backend: mysql
YAML
# BucketyAccess parameters are checked by the reconciler, which can
# resolve the driver (admission may run before the Buckety exists).
kc apply -f - <<'YAML'
apiVersion: buckety.yolean.se/v1alpha1
kind: BucketyAccess
metadata:
  name: shape-params
spec:
  bucketyRef:
    name: shape
  credentialsSecretName: shape-params
  parameters:
    host: "%"
YAML
wait_ready_reason bucketyaccess/shape-params InvalidParameters 60
if kc get secret/shape-params >/dev/null 2>&1; then
  fail "a Secret was minted for an access with parameters"
fi
kc delete bucketyaccess/shape-params --wait=true --timeout=60s

# Nothing was changed on the server by the refused updates.
[[ "$(mysql_root "SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$db'")" == utf8mb4 ]] \
  || fail "database $db character set changed"
wait_ready buckety/shape 30s

log "parameter-mutation PASS"
