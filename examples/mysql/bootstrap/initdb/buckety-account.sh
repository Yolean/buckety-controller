#!/bin/bash
# Creates buckety-controller's account in a fresh MariaDB/MySQL.
#
# The image entrypoint runs this from /docker-entrypoint-initdb.d
# once, when it initialises an empty data directory. The password
# is read from a mounted Secret file, so no password is in source;
# the controller gets the same Secret through ${...} substitution.
#
# Environment (all optional):
#   BUCKETY_PASSWORD_FILE  default /run/secrets/buckety/password
#   BUCKETY_ADMIN_USER     default buckety
#   BUCKETY_NAME_PREFIX    default b_ (the backend's namePrefix)
#
# The body runs in a subshell, so the entrypoint may execute or
# source this file without inheriting its options or variables.

buckety_account() (
  set -euo pipefail
  password_file="${BUCKETY_PASSWORD_FILE:-/run/secrets/buckety/password}"
  user="${BUCKETY_ADMIN_USER:-buckety}"
  prefix="${BUCKETY_NAME_PREFIX:-b_}"

  if [ ! -s "$password_file" ]; then
    echo "buckety-account: $password_file is missing or empty" >&2
    exit 1
  fi
  # Keep a trailing newline, so the check below refuses it: the
  # controller's env gets the Secret verbatim and would not match.
  password="$(cat "$password_file"; printf x)"
  password="${password%x}"
  # The driver's rule for passwords: characters that need no
  # escaping inside a SQL string literal.
  if [ ${#password} -lt 16 ] || [ ${#password} -gt 128 ] || [[ "$password" =~ [^A-Za-z0-9._~-] ]]; then
    echo "buckety-account: the password must be 16-128 characters of A-Z a-z 0-9 . _ ~ - and no trailing newline" >&2
    exit 1
  fi
  if [[ ! "$user" =~ ^[A-Za-z0-9]{1,32}$ ]]; then
    echo "buckety-account: BUCKETY_ADMIN_USER must be 1-32 letters or digits" >&2
    exit 1
  fi
  if [[ ! "$prefix" =~ ^[a-z][a-z0-9]{0,8}_$ ]]; then
    echo "buckety-account: BUCKETY_NAME_PREFIX must be a lowercase letter, letters or digits, and a trailing _" >&2
    exit 1
  fi
  # GRANT ... ON db.* reads _ and % in the name as wildcards.
  pattern="${prefix//_/\\_}%"

  client=mariadb
  command -v mariadb >/dev/null 2>&1 || client=mysql
  # Root's password when the image was given one; empty otherwise.
  MYSQL_PWD="${MARIADB_ROOT_PASSWORD:-${MYSQL_ROOT_PASSWORD:-}}" "$client" --protocol=socket -uroot <<SQL
CREATE USER IF NOT EXISTS '${user}'@'%' IDENTIFIED BY '${password}';
GRANT CREATE USER ON *.* TO '${user}'@'%';
GRANT ALL PRIVILEGES ON \`${pattern}\`.* TO '${user}'@'%' WITH GRANT OPTION;
SQL
  echo "buckety-account: ${user}@% may create users and owns databases matching ${pattern}"
)

buckety_account
