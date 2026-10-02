#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"
here="$(cd "$(dirname "$0")" && pwd)"

pre="b_${E2E_NAMESPACE}_adopt-pre"
void="b_${E2E_NAMESPACE}_adopt-void"

kc apply -f "$here/fresh.yaml"
wait_ready buckety/adopt-fresh 120s
assert_provenance adopt-fresh Created
fresh="$(secret_value adopt-fresh-db database)"

log "pre-creating $pre (with a table) and $void (empty)"
mysql_root "CREATE DATABASE \`$pre\`; CREATE TABLE \`$pre\`.precious (note VARCHAR(40)); INSERT INTO \`$pre\`.precious VALUES ('predates the CR')"
mysql_root "CREATE DATABASE \`$void\`"

# Non-empty: refused, nothing stamped, no Secret.
kc apply -f "$here/pre.yaml"
wait_ready_reason buckety/adopt-pre BackendResourceExists 60
if kc get secret/adopt-pre-db >/dev/null 2>&1; then
  fail "Secret minted while adoption was refused"
fi

# Empty: adopted under the default policy.
kc apply -f "$here/void.yaml"
wait_ready buckety/adopt-void 120s
assert_provenance adopt-void Adopted

# Explicit opt-in claims the database; its rows are untouched and
# the minted user can read them.
log "unblocking adopt-pre with spec.adoption=Adopt"
kc patch buckety/adopt-pre --type=merge -p '{"spec":{"adoption":"Adopt"}}'
wait_ready buckety/adopt-pre 120s
assert_provenance adopt-pre Adopted
wait_ready bucketyaccess/adopt-pre 120s
[[ "$(mysql_login adopt-pre-db "SELECT note FROM precious")" == "predates the CR" ]] \
  || fail "the adopted database's rows are not readable with the minted Secret"

# Deleting adopted Bucketys retains the databases despite
# retentionPolicy=Delete.
log "deleting adopted Bucketys (retentionPolicy=Delete must degrade to Retain)"
kc delete buckety/adopt-pre --wait=true --timeout=90s
kc delete buckety/adopt-void --wait=true --timeout=90s
mysql_database_exists "$pre" || fail "adopted database $pre was dropped"
mysql_database_exists "$void" || fail "adopted database $void was dropped"
[[ "$(mysql_root "SELECT COUNT(*) FROM \`$pre\`.precious")" == 1 ]] \
  || fail "adopted database $pre lost its rows"

# Created databases still delete normally.
kc delete buckety/adopt-fresh --wait=true --timeout=90s
if mysql_database_exists "$fresh"; then
  fail "created database $fresh survived retentionPolicy=Delete"
fi

log "cleaning up retained databases out-of-band"
mysql_root "DROP DATABASE \`$pre\`; DROP DATABASE \`$void\`"

log "adoption PASS"
