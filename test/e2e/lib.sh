#!/usr/bin/env bash
# Helper functions shared by per-scenario assert.sh files.
# Sourced; not directly executable.
#
# Inputs (env vars set by the harness or by the contributor):
#   E2E_NAMESPACE       The namespace the scenario was applied into.
#   E2E_KUBECONFIG      kubeconfig path (falls back to $KUBECONFIG).
#   E2E_CONTROLLER_NS   Namespace the buckety-controller runs in (default: buckety).
#   E2E_IMPLEMENTATION  versitygw | minio | redpanda (informational; some assertions branch on it).
#
# Downstream consumers writing their own platform e2e against
# buckety-controller: do NOT source this file from outside this
# repo. The helpers split into two camps:
#
#   Portable across deployments (cherry-pick these into your own lib):
#     wait_ready, wait_condition, condition_status, secret_has_keys,
#     secret_value, resource_absent
#
#   Coupled to this repo's harness (assume namespace/service layout):
#     kafka_topic_exists, s3_bucket_exists
#
# The coupled helpers kubectl-run pods in conventional namespaces
# (redpanda/, buckety/) reading bootstrap addresses from harness
# env defaults. In a downstream platform you typically own those
# backing services and will write equivalent helpers against your
# own coordinates. Copy the helper shape, not the harness coupling.

set -euo pipefail

E2E_NAMESPACE="${E2E_NAMESPACE:?E2E_NAMESPACE must be set}"
E2E_CONTROLLER_NS="${E2E_CONTROLLER_NS:-buckety}"
KUBECONFIG="${E2E_KUBECONFIG:-${KUBECONFIG:-}}"
export KUBECONFIG

kc() { kubectl -n "$E2E_NAMESPACE" "$@"; }
kcg() { kubectl "$@"; }

log() { printf '[assert] %s\n' "$*" >&2; }
fail() { printf '[assert][FAIL] %s\n' "$*" >&2; exit 1; }

# wait_ready <kind/name> [timeout]
# Waits for .status.conditions[type=Ready].status == True.
wait_ready() {
  local target="$1" timeout="${2:-90s}"
  log "waiting for $target Ready=True (timeout $timeout)"
  kc wait --for=condition=Ready "$target" --timeout="$timeout" \
    || { kc get "$target" -o yaml >&2; fail "$target did not reach Ready=True"; }
}

# secret_has_keys <secret-name> <key>...
# Verifies the Secret exists and contains all listed keys (non-empty values).
secret_has_keys() {
  local secret="$1"; shift
  log "checking Secret/$secret has keys: $*"
  local missing=()
  for k in "$@"; do
    if ! kc get "secret/$secret" -o "jsonpath={.data.$k}" 2>/dev/null | grep -q .; then
      missing+=("$k")
    fi
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    kc get "secret/$secret" -o yaml >&2 || true
    fail "Secret/$secret missing keys: ${missing[*]}"
  fi
}

# secret_value <secret-name> <key>
# Echoes the base64-decoded value to stdout.
secret_value() {
  kc get "secret/$1" -o "jsonpath={.data.$2}" | base64 -d
}

# kafka_topic_exists <topic> [kafka-namespace] [bootstrap-svc]
# Verifies the topic exists on the broker via an ephemeral rpk pod.
# The harness sideloads a redpanda-equipped image as `e2e-rpk:latest`.
kafka_topic_exists() {
  local topic="$1"
  local kns="${2:-${E2E_KAFKA_NAMESPACE:-redpanda}}"
  local bootstrap="${3:-${E2E_KAFKA_BOOTSTRAP:-redpanda.${kns}.svc.cluster.local:9093}}"
  log "verifying Kafka topic '$topic' on $bootstrap"
  # ghcr.io/yolean/redpanda's ENTRYPOINT is rpk; pass args
  # without the leading `rpk` to avoid `rpk rpk topic ...`.
  if ! kcg run -n "$kns" --rm -i --restart=Never --quiet \
      --image=ghcr.io/yolean/redpanda:v24.2.22@sha256:5132085d4fe35b0fd6ddedc7f0fe3d3ba7be12c5e3829e1a2b986cd41b1d3538 \
      "rpk-check-$RANDOM" -- \
      topic list --brokers "$bootstrap" 2>/dev/null | grep -qE "^\s*$topic\s"; then
    fail "Kafka topic '$topic' not found on $bootstrap"
  fi
}

# s3_bucket_exists <bucket> <endpoint> <access> <secret>
# Verifies the bucket exists via an ephemeral aws-cli pod.
s3_bucket_exists() {
  local bucket="$1" endpoint="$2" access="$3" secret="$4"
  log "verifying S3 bucket '$bucket' at $endpoint"
  if ! kcg run -n "$E2E_CONTROLLER_NS" --rm -i --restart=Never --quiet \
      --image=public.ecr.aws/aws-cli/aws-cli:latest \
      --env="AWS_ACCESS_KEY_ID=$access" \
      --env="AWS_SECRET_ACCESS_KEY=$secret" \
      "awscli-check-$RANDOM" -- \
      s3api head-bucket --bucket "$bucket" --endpoint-url "$endpoint" >/dev/null 2>&1; then
    fail "S3 bucket '$bucket' not found at $endpoint"
  fi
}

# condition_status <kind/name> <conditionType>
# Echoes True | False | Unknown | <empty> for the named condition.
condition_status() {
  kc get "$1" -o "jsonpath={.status.conditions[?(@.type=='$2')].status}" 2>/dev/null
}

# wait_condition <kind/name> <conditionType> <expectedStatus> [timeout]
wait_condition() {
  local target="$1" ctype="$2" expect="$3" timeout="${4:-60s}"
  log "waiting for $target condition $ctype=$expect (timeout $timeout)"
  kc wait --for=condition="$ctype"="$expect" "$target" --timeout="$timeout" \
    || { kc get "$target" -o yaml >&2; fail "$target $ctype did not reach $expect"; }
}

# resource_absent <kind/name> [timeout]
# Waits for the resource to be deleted (NotFound).
resource_absent() {
  local target="$1" timeout="${2:-30s}"
  log "waiting for $target to be deleted (timeout $timeout)"
  kc wait --for=delete "$target" --timeout="$timeout" \
    || fail "$target was not deleted within $timeout"
}
