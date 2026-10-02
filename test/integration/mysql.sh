#!/usr/bin/env bash
#
# Runs the mysql driver's integration test
# (pkg/drivers/mysql/integration_test.go) against real servers in
# Docker, one container per image. CI runs this same script.
#
# Each server gets the controller's account the way a cluster
# does: examples/mysql/bootstrap/initdb/buckety-account.sh runs
# from /docker-entrypoint-initdb.d on first boot, as root on the
# local socket, with root allowed only from localhost. So this
# also tests the bootstrap script against every server image.
#
# Inputs (env):
#   MYSQL_IMAGES  Space-separated images. Default: every image in
#                 DEFAULT_IMAGES below.
#   KEEP          true leaves the containers running.
#
# Needs docker and go. Never prints a password.

set -euo pipefail

here() { cd "$(dirname "${BASH_SOURCE[0]}")" && pwd; }
REPO="$(cd "$(here)/../.." && pwd)"
KEEP="${KEEP:-false}"

# The MariaDB the e2e backing service runs, the next MariaDB LTS,
# and both MySQL lines the driver supports.
DEFAULT_IMAGES="
ghcr.io/yolean/mariadb:10.11.19-jammy@sha256:7f22313fc130a377a44999965bcb0a08dd5b21e8502824c1b864f792f9bc66ab
mariadb:11.4.13@sha256:03744ebde1e401fabe7ae7243ae2536d83f4f5ef14b87769cd269e072556c4c3
mysql:8.0.46@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b
mysql:8.4.11@sha256:6ea90827b1100f8f2ae306a539f86d2c264a26ed435a2a9f75551dd5c3aeb242
"
read -r -a IMAGES <<<"$(echo ${MYSQL_IMAGES:-$DEFAULT_IMAGES})"

log() { printf '[mysql-it] %s\n' "$*" >&2; }
fail() { printf '[mysql-it][FAIL] %s\n' "$*" >&2; exit 1; }

WORK="$(mktemp -d)"
CONTAINERS=()
cleanup() {
  if [[ "$KEEP" != true ]]; then
    for c in "${CONTAINERS[@]}"; do docker rm -f "$c" >/dev/null 2>&1 || true; done
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

# The password file is mounted like the Secret in a cluster. The
# server's init scripts run as the mysql user, hence world-readable
# inside a directory only this run knows about.
password="$(head -c 24 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')"
mkdir -p "$WORK/secret"
printf '%s' "$password" >"$WORK/secret/password"
chmod 0755 "$WORK" "$WORK/secret"
chmod 0644 "$WORK/secret/password"

# start <image> <name>: boots a server with the bootstrap script.
start() {
  local image="$1" name="$2"
  docker run -d --name "$name" \
    -p 127.0.0.1::3306 \
    -e MYSQL_ALLOW_EMPTY_PASSWORD=yes \
    -e MYSQL_ROOT_HOST=localhost \
    -e MYSQL_INITDB_SKIP_TZINFO=yes \
    -v "$REPO/examples/mysql/bootstrap/initdb/buckety-account.sh:/docker-entrypoint-initdb.d/buckety-account.sh:ro" \
    -v "$WORK/secret:/run/secrets/buckety:ro" \
    "$image" >/dev/null
}

# wait_account <name>: the account logs in over TCP, which only the
# final server accepts (the entrypoint's init server is socket-only).
wait_account() {
  local name="$1" client
  for _ in $(seq 1 90); do
    client="$(docker exec "$name" sh -c 'command -v mariadb || command -v mysql' 2>/dev/null || true)"
    if [[ -n "$client" ]] && docker exec -e MYSQL_PWD="$password" "$name" \
        "$client" --protocol=tcp -h127.0.0.1 -ubuckety -e 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    if [[ "$(docker inspect -f '{{.State.Running}}' "$name")" != true ]]; then
      docker logs --tail 50 "$name" >&2 || true
      fail "$name exited"
    fi
    sleep 2
  done
  docker logs --tail 50 "$name" >&2 || true
  fail "$name: the buckety account cannot log in after 180s"
}

failed=()
for i in "${!IMAGES[@]}"; do
  image="${IMAGES[$i]}"
  name="buckety-mysql-it-$$-$i"
  CONTAINERS+=("$name")
  log "=== ${image%@*}"
  start "$image" "$name"
  wait_account "$name"
  addr="$(docker port "$name" 3306/tcp | head -n1)"
  log "server up at $addr"
  # auto: the test uses the address the server sees it connect
  # from as a second userHost, which covers moving accounts.
  if BUCKETY_MYSQL_TEST_DSN="buckety:${password}@tcp(${addr})/" \
     BUCKETY_MYSQL_TEST_ALT_USER_HOST=auto \
     go -C "$REPO" test ./pkg/drivers/mysql -run '^TestIntegration$' -count=1 -v; then
    log "PASS ${image%@*}"
  else
    log "FAIL ${image%@*}"
    docker logs --tail 50 "$name" >&2 || true
    failed+=("${image%@*}")
  fi
done

if (( ${#failed[@]} > 0 )); then
  fail "failed against: ${failed[*]}"
fi
log "all ${#IMAGES[@]} servers PASS"
