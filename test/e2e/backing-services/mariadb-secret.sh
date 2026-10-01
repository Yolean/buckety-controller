#!/usr/bin/env bash
#
# Creates the controller account's password for the MariaDB backing
# service: generated once into Secret mysql-buckety in namespace
# mysql (mounted by MariaDB's first-boot script) and copied to the
# controller namespace (env for ${MYSQL_BUCKETY_PASSWORD}). A pod can
# only mount Secrets from its own namespace, hence two copies.
# Idempotent; never prints the password. Run before
# `kubectl apply -k test/e2e/backing-services`.

set -euo pipefail

CONTROLLER_NS="${CONTROLLER_NS:-buckety}"

for ns in mysql "$CONTROLLER_NS"; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done

if ! kubectl -n mysql get secret mysql-buckety >/dev/null 2>&1; then
  # 32 characters of [A-Za-z0-9_-], within the bootstrap script's rule.
  head -c 24 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n' \
    | kubectl -n mysql create secret generic mysql-buckety --from-file=password=/dev/stdin >/dev/null
  echo "created secret mysql/mysql-buckety"
fi

kubectl -n mysql get secret mysql-buckety -o json \
  | jq --arg ns "$CONTROLLER_NS" '{apiVersion, kind, type, data, metadata: {name: .metadata.name, namespace: $ns}}' \
  | kubectl apply -f - >/dev/null
echo "secret mysql-buckety present in namespaces mysql and $CONTROLLER_NS"
