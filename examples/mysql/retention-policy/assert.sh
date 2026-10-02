#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

wait_ready buckety/keep-me 120s
wait_ready buckety/drop-me 120s
keep_db="$(secret_value keep-me-db database)"
drop_db="$(secret_value drop-me-db database)"
keep_user="$(secret_value keep-me-db username)"
drop_user="$(secret_value drop-me-db username)"
for s in keep-me-db drop-me-db; do
  mysql_login "$s" "CREATE TABLE t (id INT PRIMARY KEY); INSERT INTO t VALUES (1)" \
    || fail "$s: cannot write"
done

user_count() { mysql_root "SELECT COUNT(*) FROM mysql.user WHERE User = '$1'"; }

# Retain: the database and its data survive; the user does not.
log "deleting Buckety/keep-me (Retain)"
kc delete buckety/keep-me --wait=true --timeout=90s
resource_absent secret/keep-me-db 30s
mysql_database_exists "$keep_db" || fail "retained database $keep_db is gone"
[[ "$(mysql_root "SELECT COUNT(*) FROM \`$keep_db\`.t")" == 1 ]] \
  || fail "retained database $keep_db lost its rows"
[[ "$(user_count "$keep_user")" == 0 ]] \
  || fail "user $keep_user survived its access; a retained database keeps no credentials"

# Delete: the database goes, with its tables, and the user.
log "deleting Buckety/drop-me (Delete)"
kc delete buckety/drop-me --wait=true --timeout=90s
resource_absent secret/drop-me-db 30s
if mysql_database_exists "$drop_db"; then
  fail "database $drop_db still present after retentionPolicy=Delete"
fi
[[ "$(user_count "$drop_user")" == 0 ]] || fail "user $drop_user survived its access"

# The retained database is this scenario's to clean up.
mysql_root "DROP DATABASE \`$keep_db\`"

log "retention-policy PASS"
