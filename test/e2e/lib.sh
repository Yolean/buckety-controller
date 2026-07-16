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

# rpk_topic_list [kafka-namespace] [bootstrap-svc]
# Prints `rpk topic list` output via an ephemeral rpk pod. Output
# is captured, not piped: under `set -o pipefail` an early `grep -q`
# exit SIGPIPEs kubectl and fails the pipeline even on a match.
rpk_topic_list() {
  local kns="${1:-${E2E_KAFKA_NAMESPACE:-redpanda}}"
  local bootstrap="${2:-${E2E_KAFKA_BOOTSTRAP:-redpanda.${kns}.svc.cluster.local:9093}}"
  # ghcr.io/yolean/redpanda's ENTRYPOINT is rpk; pass args
  # without the leading `rpk` to avoid `rpk rpk topic ...`.
  kcg run -n "$kns" --rm -i --restart=Never --quiet \
    --image=ghcr.io/yolean/redpanda:v24.2.22@sha256:5132085d4fe35b0fd6ddedc7f0fe3d3ba7be12c5e3829e1a2b986cd41b1d3538 \
    "rpk-check-$RANDOM" -- \
    topic list --brokers "$bootstrap" </dev/null 2>&1
}

# kafka_topic_exists <topic> [kafka-namespace] [bootstrap-svc]
# Polls until the topic appears on the broker. Topic creation is
# acked before a fresh metadata request necessarily lists it, so a
# single-shot check flakes (seen on retention-policy in CI).
kafka_topic_exists() {
  local topic="$1"
  local kns="${2:-${E2E_KAFKA_NAMESPACE:-redpanda}}"
  local bootstrap="${3:-${E2E_KAFKA_BOOTSTRAP:-redpanda.${kns}.svc.cluster.local:9093}}"
  log "verifying Kafka topic '$topic' on $bootstrap"
  local out=""
  for _ in $(seq 1 6); do
    if out="$(rpk_topic_list "$kns" "$bootstrap")" \
        && grep -qE "^[[:space:]]*${topic}[[:space:]]" <<<"$out"; then
      return 0
    fi
    sleep 5
  done
  printf '%s\n' "$out" >&2
  fail "Kafka topic '$topic' not found on $bootstrap"
}

# kafka_topic_absent <topic> [kafka-namespace] [bootstrap-svc]
# Polls until the topic no longer appears on the broker.
kafka_topic_absent() {
  local topic="$1"
  local kns="${2:-${E2E_KAFKA_NAMESPACE:-redpanda}}"
  local bootstrap="${3:-${E2E_KAFKA_BOOTSTRAP:-redpanda.${kns}.svc.cluster.local:9093}}"
  log "verifying Kafka topic '$topic' is gone from $bootstrap"
  local out=""
  for _ in $(seq 1 6); do
    if out="$(rpk_topic_list "$kns" "$bootstrap")" \
        && ! grep -qE "^[[:space:]]*${topic}[[:space:]]" <<<"$out"; then
      return 0
    fi
    sleep 5
  done
  printf '%s\n' "$out" >&2
  fail "Kafka topic '$topic' still present on $bootstrap"
}

# s3_bucket_exists <bucket> <endpoint> <access> <secret>
# Verifies the bucket exists via an ephemeral aws-cli pod.
s3_bucket_exists() {
  local bucket="$1" endpoint="$2" access="$3" secret="$4"
  log "verifying S3 bucket '$bucket' at $endpoint"
  local out
  if ! out="$(kcg run -n "$E2E_CONTROLLER_NS" --rm -i --restart=Never --quiet \
      --image=public.ecr.aws/aws-cli/aws-cli:latest \
      --env="AWS_ACCESS_KEY_ID=$access" \
      --env="AWS_SECRET_ACCESS_KEY=$secret" \
      "awscli-check-$RANDOM" -- \
      s3api head-bucket --bucket "$bucket" --endpoint-url "$endpoint" </dev/null 2>&1)"; then
    printf '%s\n' "$out" >&2
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

# ---- gcs helpers (coupled camp: assume the fake-gcs-server e2e
# backing service, which accepts unauthenticated JSON API calls;
# against real GCS these curls would need an OAuth token) ----

# gcs_api <url>
# GETs the URL via an ephemeral curl pod, echoing the body.
# Returns non-zero on HTTP >= 400 (curl -f). Secret endpoint
# values are bare hosts (schemes are the consumer's choice);
# these emulator-coupled helpers speak plain http.
gcs_api() {
  kcg run -n "$E2E_CONTROLLER_NS" --rm -i --restart=Never --quiet \
    --image=curlimages/curl:8.17.0@sha256:935d9100e9ba842cdb060de42472c7ca90cfe9a7c96e4dacb55e79e560b3ff40 \
    "curl-check-$RANDOM" -- \
    -sfS "$1" </dev/null 2>&1
}

# gcs_bucket_exists <bucket> <endpoint>
# Verifies the bucket exists on the GCS JSON API.
gcs_bucket_exists() {
  local bucket="$1" endpoint="$2"
  log "verifying GCS bucket '$bucket' at $endpoint"
  local out
  if ! out="$(gcs_api "http://$endpoint/storage/v1/b/$bucket")"; then
    printf '%s\n' "$out" >&2
    fail "GCS bucket '$bucket' not found at $endpoint"
  fi
}

# gcs_bucket_exists_quiet <bucket> <endpoint>
# Existence probe without failing the scenario; for polling loops
# and negative assertions.
gcs_bucket_exists_quiet() {
  gcs_api "http://$2/storage/v1/b/$1" >/dev/null 2>&1
}

# gcs_bucket_delete <bucket> <endpoint>
# Deletes the bucket directly (out-of-band mutation).
gcs_bucket_delete() {
  local bucket="$1" endpoint="$2"
  kcg run -n "$E2E_CONTROLLER_NS" --rm -i --restart=Never --quiet \
    --image=curlimages/curl:8.17.0@sha256:935d9100e9ba842cdb060de42472c7ca90cfe9a7c96e4dacb55e79e560b3ff40 \
    "curl-oob-$RANDOM" -- \
    -sfS -X DELETE "http://$endpoint/storage/v1/b/$bucket" </dev/null
}

# gcs_bucket_versioning_enabled <bucket> <endpoint>
# Returns 0 when the bucket reports versioning enabled. Whitespace
# is stripped before matching so the check is independent of the
# server's JSON formatting.
gcs_bucket_versioning_enabled() {
  local bucket="$1" endpoint="$2"
  local out
  out="$(gcs_api "http://$endpoint/storage/v1/b/$bucket")" || {
    printf '%s\n' "$out" >&2
    fail "GCS bucket '$bucket' not readable at $endpoint"
  }
  printf '%s' "$out" | tr -d '[:space:]' | grep -q '"versioning":{"enabled":true}'
}

# secret_owned_label <secret-name>
# Every controller-minted Secret carries buckety.yolean.se/owned=true;
# the manager cache is scoped to it (issue #10), so a missing label
# means the Secret would silently fall out of the controller's view.
secret_owned_label() {
  local secret="$1"
  local v
  v="$(kc get "secret/$secret" -o jsonpath='{.metadata.labels.buckety\.yolean\.se/owned}')"
  [[ "$v" == "true" ]] \
    || fail "Secret/$secret missing buckety.yolean.se/owned=true label (got '$v')"
}

# gcs_object_put <bucket> <endpoint> <key> <content>
# Uploads via the JSON API (unauthenticated emulator).
gcs_object_put() {
  local bucket="$1" endpoint="$2" key="$3" content="$4"
  kcg run -n "$E2E_CONTROLLER_NS" --rm -i --restart=Never --quiet \
    --image=curlimages/curl:8.17.0@sha256:935d9100e9ba842cdb060de42472c7ca90cfe9a7c96e4dacb55e79e560b3ff40 \
    "curl-put-$RANDOM" -- \
    -sfS -o /dev/null -X POST \
    -H "Content-Type: text/plain" \
    --data-raw "$content" \
    "http://$endpoint/upload/storage/v1/b/$bucket/o?uploadType=media&name=$key" </dev/null
}
