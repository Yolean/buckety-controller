#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

# The e2e controller re-checks every 15s (--periodic-recheck), so
# every repair below is expected within a few re-checks.

wait_ready buckety/drift 120s
wait_ready bucketyaccess/drift-rw 120s
wait_ready bucketyaccess/drift-reader 120s
db="$(secret_value drift-db database)"
rw="$(secret_value drift-db username)"
ro="$(secret_value drift-reader-db username)"
mysql_login drift-db "CREATE TABLE t (id INT PRIMARY KEY); INSERT INTO t VALUES (1)" \
  || fail "the ReadWrite user cannot create a table"

# A user dropped out of band is recreated with the Secret's password.
log "out-of-band: DROP USER $rw"
mysql_root "DROP USER '$rw'@'%'"
if mysql_login drift-db "SELECT 1" 2>/dev/null; then
  fail "login still works after DROP USER"
fi
wait_until 90 "$rw recreated with the Secret's password" \
  mysql_login drift-db "SELECT COUNT(*) FROM t"

# Privileges granted out of band are revoked, GRANT OPTION too.
pattern="${db//_/\\_}"
log "out-of-band: GRANT INSERT ... WITH GRANT OPTION to $ro"
mysql_root "GRANT INSERT ON \`$pattern\`.* TO '$ro'@'%' WITH GRANT OPTION"
reader_is_reader() {
  local g
  g="$(mysql_root "SHOW GRANTS FOR '$ro'@'%'")"
  ! grep -qE 'INSERT|GRANT OPTION' <<<"$g" && grep -q 'GRANT SELECT ON' <<<"$g"
}
wait_until 90 "INSERT and GRANT OPTION revoked from $ro" reader_is_reader

# A database dropped out of band is recreated (empty).
log "out-of-band: DROP DATABASE $db"
mysql_root "DROP DATABASE \`$db\`"
wait_until 90 "database $db recreated" mysql_database_exists "$db"
mysql_login drift-db "CREATE TABLE t (id INT PRIMARY KEY)" \
  || fail "the ReadWrite user cannot use the recreated database"

# A changed character set is reported, never converted.
log "out-of-band: ALTER DATABASE $db CHARACTER SET latin1"
mysql_root "ALTER DATABASE \`$db\` CHARACTER SET latin1 COLLATE latin1_swedish_ci"
wait_condition buckety/drift ParameterDrift True 90s
[[ "$(condition_status buckety/drift Ready)" == "False" ]] \
  || fail "Ready should be False under ParameterDrift"
[[ "$(mysql_root "SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$db'")" == latin1 ]] \
  || fail "the controller altered the database's character set"
# ParameterDrift pauses re-checks until the Buckety changes: the
# documented resolution is to fix it out of band, then touch it.
mysql_root "ALTER DATABASE \`$db\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
kc annotate buckety/drift e2e.buckety.yolean.se/recheck="$(date +%s)" --overwrite
wait_ready buckety/drift 90s

# Deleting the Secret rotates the password.
before="$(secret_value drift-db password)"
log "deleting Secret/drift-db to rotate the password"
kc delete secret/drift-db
rotated() {
  kc get secret/drift-db >/dev/null 2>&1 \
    && [[ "$(secret_value drift-db password)" != "$before" ]] \
    && mysql_login drift-db "SELECT 1"
}
wait_until 90 "a new password in Secret/drift-db that logs in" rotated

# A password written into the Secret is applied to the server.
pw="$(head -c 24 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')"
patch="$(mktemp)"
trap 'rm -f "$patch"' EXIT
printf '{"stringData":{"password":"%s"}}' "$pw" >"$patch"
log "writing a new password into Secret/drift-db"
kc patch secret/drift-db --type=merge --patch-file="$patch"
wait_until 90 "the written password logs in" mysql_login drift-db "SELECT 1"

log "oob-drift PASS"
