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

# Images the helper pods run. Pinned like everything else the
# suite pulls: a floating tag flakes every helper at once when the
# upstream image changes.
AWSCLI_IMAGE="public.ecr.aws/aws-cli/aws-cli:2.36.39@sha256:df8b292f3ae0092a2861eb4ad37f46d755ffd082615f99bb404bb0d118e6e1f1"

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
  kc run --rm -i --restart=Never --quiet \
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

# rollout_restart <deploy> [namespace]
# kubectl refuses a second restart within the same second (the
# restartedAt annotation has 1s granularity), which fast machines
# hit when scenario loops re-trigger back to back; GHA runners
# never did. Retry briefly instead of failing the scenario.
rollout_restart() {
  local deploy="$1" ns="${2:-$E2E_CONTROLLER_NS}"
  local out=""
  for _ in $(seq 1 5); do
    if out="$(kcg -n "$ns" rollout restart "$deploy" 2>&1)"; then
      printf '%s\n' "$out"
      return 0
    fi
    grep -q "within the past second" <<<"$out" || break
    sleep 1
  done
  printf '%s\n' "$out" >&2
  fail "rollout restart $deploy failed"
}

# rpk_run <name-prefix> <rpk args...>
# Runs an arbitrary rpk command via an ephemeral pod. stdin is
# forwarded, so `printf msg | rpk_run produce topic produce ...`
# feeds the producer. Caller checks the exit code.
rpk_run() {
  local prefix="$1"
  shift
  kc run --rm -i --restart=Never --quiet \
    --image=ghcr.io/yolean/redpanda:v24.2.22@sha256:5132085d4fe35b0fd6ddedc7f0fe3d3ba7be12c5e3829e1a2b986cd41b1d3538 \
    "rpk-${prefix}-$RANDOM" -- \
    "$@" 2>&1
}

# kafka_topic_create <topic>
kafka_topic_create() {
  local topic="$1"
  local bootstrap="${E2E_KAFKA_BOOTSTRAP:-redpanda.${E2E_KAFKA_NAMESPACE:-redpanda}.svc.cluster.local:9093}"
  rpk_run create topic create "$topic" --brokers "$bootstrap" </dev/null \
    || fail "could not create Kafka topic '$topic'"
}

# kafka_topic_produce <topic> <message>
kafka_topic_produce() {
  local topic="$1" msg="$2"
  local bootstrap="${E2E_KAFKA_BOOTSTRAP:-redpanda.${E2E_KAFKA_NAMESPACE:-redpanda}.svc.cluster.local:9093}"
  printf '%s\n' "$msg" | rpk_run produce topic produce "$topic" --brokers "$bootstrap" \
    || fail "could not produce to Kafka topic '$topic'"
}

# kafka_topic_delete <topic>
kafka_topic_delete() {
  local topic="$1"
  local bootstrap="${E2E_KAFKA_BOOTSTRAP:-redpanda.${E2E_KAFKA_NAMESPACE:-redpanda}.svc.cluster.local:9093}"
  rpk_run delete topic delete "$topic" --brokers "$bootstrap" </dev/null \
    || fail "could not delete Kafka topic '$topic'"
}

# s3_bucket_exists <bucket> <endpoint> <access> <secret>
# Verifies the bucket exists via an ephemeral aws-cli pod.
s3_bucket_exists() {
  local bucket="$1" endpoint="$2" access="$3" secret="$4"
  log "verifying S3 bucket '$bucket' at $endpoint"
  local out
  if ! out="$(kc run --rm -i --restart=Never --quiet \
      --image="$AWSCLI_IMAGE" \
      --env="AWS_ACCESS_KEY_ID=$access" \
      --env="AWS_SECRET_ACCESS_KEY=$secret" \
      "awscli-check-$RANDOM" -- \
      s3api head-bucket --bucket "$bucket" --endpoint-url "$endpoint" </dev/null 2>&1)"; then
    printf '%s\n' "$out" >&2
    fail "S3 bucket '$bucket' not found at $endpoint"
  fi
}

# s3_api <endpoint> <access> <secret> <s3api subcommand and args...>
# Runs an arbitrary s3api call via an ephemeral aws-cli pod and
# echoes its combined output. Caller checks the exit code.
s3_api() {
  local endpoint="$1" access="$2" secret="$3"
  shift 3
  kc run --rm -i --restart=Never --quiet \
    --image="$AWSCLI_IMAGE" \
    --env="AWS_ACCESS_KEY_ID=$access" \
    --env="AWS_SECRET_ACCESS_KEY=$secret" \
    "awscli-api-$RANDOM" -- \
    s3api "$@" --endpoint-url "$endpoint" </dev/null 2>&1
}

# s3_object_exists <bucket> <key> <endpoint> <access> <secret>
# Exit-code-only presence check via head-object. Do NOT assert
# object presence by parsing list output: `kubectl run -i` can
# miss a fast pod's stdout entirely (attach race) while the exit
# code, taken from pod status, stays reliable - seen as an empty
# list-objects-v2 capture failing the adoption scenario on a
# bucket that provably held the object.
s3_object_exists() {
  local bucket="$1" key="$2" endpoint="$3" access="$4" secret="$5"
  log "verifying S3 object '$bucket/$key' at $endpoint"
  s3_api "$endpoint" "$access" "$secret" head-object --bucket "$bucket" --key "$key" >/dev/null \
    || fail "S3 object '$bucket/$key' not found at $endpoint"
}

# condition_status <kind/name> <conditionType>
# Echoes True | False | Unknown | <empty> for the named condition.
condition_status() {
  kc get "$1" -o "jsonpath={.status.conditions[?(@.type=='$2')].status}" 2>/dev/null
}

# wait_ready_reason <kind/name> <reason> [timeoutSeconds]
# Waits until the Ready condition carries the given reason,
# whatever its status. For asserting specific False states
# (BackendResourceExists, ...) that kubectl wait cannot express.
wait_ready_reason() {
  local target="$1" reason="$2" timeout="${3:-60}"
  log "waiting for $target Ready reason=$reason (timeout ${timeout}s)"
  local deadline=$(( $(date +%s) + timeout )) got=""
  while (( $(date +%s) < deadline )); do
    got="$(kc get "$target" -o "jsonpath={.status.conditions[?(@.type=='Ready')].reason}" 2>/dev/null || true)"
    [[ "$got" == "$reason" ]] && return 0
    sleep 2
  done
  kc get "$target" -o yaml >&2
  fail "$target Ready reason is '$got', wanted '$reason'"
}

# assert_provenance <buckety-name> <Created|Adopted>
assert_provenance() {
  local name="$1" want="$2" got
  got="$(kc get "buckety/$name" -o jsonpath='{.status.provenance}')"
  [[ "$got" == "$want" ]] \
    || fail "buckety/$name provenance is '$got', wanted '$want'"
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
  kc run --rm -i --restart=Never --quiet \
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
  kc run --rm -i --restart=Never --quiet \
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

# gcs_object_exists <bucket> <endpoint> <key>
# Exit-code-only presence check via the object metadata GET (curl
# -f exits non-zero on 404). Same attach-race rationale as
# s3_object_exists. Slash-free keys only.
gcs_object_exists() {
  local bucket="$1" endpoint="$2" key="$3"
  log "verifying GCS object '$bucket/$key' at $endpoint"
  gcs_api "http://$endpoint/storage/v1/b/$bucket/o/$key" >/dev/null \
    || fail "GCS object '$bucket/$key' not found at $endpoint"
}

# gcs_object_delete <bucket> <endpoint> <key>
# Deletes one object directly (out-of-band). Slash-free keys only;
# the JSON API wants slashes percent-encoded in the object path.
gcs_object_delete() {
  local bucket="$1" endpoint="$2" key="$3"
  kc run --rm -i --restart=Never --quiet \
    --image=curlimages/curl:8.17.0@sha256:935d9100e9ba842cdb060de42472c7ca90cfe9a7c96e4dacb55e79e560b3ff40 \
    "curl-rmobj-$RANDOM" -- \
    -sfS -o /dev/null -X DELETE \
    "http://$endpoint/storage/v1/b/$bucket/o/$key" </dev/null \
    || fail "could not delete GCS object '$bucket/$key'"
}

# gcs_bucket_create <bucket> <endpoint> [project]
# Creates the bucket directly via the JSON API (out-of-band).
gcs_bucket_create() {
  local bucket="$1" endpoint="$2" project="${3:-e2e-project}"
  kc run --rm -i --restart=Never --quiet \
    --image=curlimages/curl:8.17.0@sha256:935d9100e9ba842cdb060de42472c7ca90cfe9a7c96e4dacb55e79e560b3ff40 \
    "curl-mkbucket-$RANDOM" -- \
    -sfS -o /dev/null -X POST \
    -H "Content-Type: application/json" \
    --data-raw "{\"name\": \"$bucket\"}" \
    "http://$endpoint/storage/v1/b?project=$project" </dev/null \
    || fail "could not create GCS bucket '$bucket'"
}

# gcs_object_put <bucket> <endpoint> <key> <content>
# Uploads via the JSON API (unauthenticated emulator).
gcs_object_put() {
  local bucket="$1" endpoint="$2" key="$3" content="$4"
  kc run --rm -i --restart=Never --quiet \
    --image=curlimages/curl:8.17.0@sha256:935d9100e9ba842cdb060de42472c7ca90cfe9a7c96e4dacb55e79e560b3ff40 \
    "curl-put-$RANDOM" -- \
    -sfS -o /dev/null -X POST \
    -H "Content-Type: text/plain" \
    --data-raw "$content" \
    "http://$endpoint/upload/storage/v1/b/$bucket/o?uploadType=media&name=$key" </dev/null
}

# ---- controller lifecycle helpers ----

# controller_config_apply <file>
# Installs <file> as the controller config Secret and restarts the
# controller - the operation a platform performs on a config
# change. Scenarios that swap the config trap
# controller_config_restore so later scenarios see the original.
controller_config_apply() {
  kcg -n "$E2E_CONTROLLER_NS" create secret generic buckety-controller-config \
    --from-file=buckety-controller.yaml="$1" \
    --dry-run=client -o yaml | kcg apply -f -
  rollout_restart deploy/buckety-controller
  kcg -n "$E2E_CONTROLLER_NS" rollout status deploy/buckety-controller --timeout=120s
}

controller_config_restore() {
  log "restoring original controller config"
  controller_config_apply "${E2E_ORIGINAL_CONFIG:?harness must set E2E_ORIGINAL_CONFIG}"
}

# controller_set_image <image>
controller_set_image() {
  log "switching controller image to $1"
  kcg -n "$E2E_CONTROLLER_NS" set image deploy/buckety-controller "controller=$1"
  kcg -n "$E2E_CONTROLLER_NS" rollout status deploy/buckety-controller --timeout=120s
}

# ---- shared scenario bodies ----
#
# Some scenarios exercise controller behaviour that does not
# depend on the driver, and their assert.sh files were
# byte-identical across drivers apart from a name or a key list.
# Their bodies live here so a fix lands once; each
# examples/<driver>/<scenario>/assert.sh stays runnable on its
# own and reads as the one-line statement of what it asserts.

# backend_stickiness_scenario <backend> <renamed-buckety.yaml>
# SPEC §End-to-end coverage #7. buckety/sticky-orig was applied
# against <backend>; the harness's E2E_RENAMED_CONFIG declares
# <backend>-renamed instead, and <renamed-buckety.yaml> targets it.
backend_stickiness_scenario() {
  local backend="$1" renamed_cr="$2"
  : "${E2E_RENAMED_CONFIG:?harness must set E2E_RENAMED_CONFIG (a config declaring '${backend}-renamed' instead of '${backend}')}"
  : "${E2E_ORIGINAL_CONFIG:?harness must set E2E_ORIGINAL_CONFIG}"

  wait_ready buckety/sticky-orig 120s
  local sticky_backend
  sticky_backend="$(kc get buckety/sticky-orig -o jsonpath='{.status.backend}')"
  [[ "$sticky_backend" == "$backend" ]] \
    || fail "status.backend stamped as '$sticky_backend', expected '$backend'"

  trap controller_config_restore EXIT
  log "swapping controller config: $backend -> $backend-renamed"
  controller_config_apply "$E2E_RENAMED_CONFIG"

  wait_condition buckety/sticky-orig BackendUnavailable True 90s
  [[ "$(condition_status buckety/sticky-orig Ready)" == "False" ]] \
    || fail "sticky-orig Ready should be False under BackendUnavailable"
  local still_sticky
  still_sticky="$(kc get buckety/sticky-orig -o jsonpath='{.status.backend}')"
  [[ "$still_sticky" == "$backend" ]] \
    || fail "status.backend mutated to '$still_sticky'; stickiness violated"

  # A fresh Buckety against the renamed backend still works.
  log "applying new Buckety against $backend-renamed"
  kc apply -f "$renamed_cr"
  wait_ready buckety/sticky-new 120s

  # Deletion with retentionPolicy=Delete blocks while the backend
  # is missing: removing the finalizer would silently orphan the
  # backend resource.
  log "deleting sticky-orig while its backend is missing; expecting the deletion to block"
  kc delete buckety/sticky-orig --wait=false
  local blocked=""
  for _ in $(seq 1 20); do
    blocked="$(kc get buckety/sticky-orig \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null || true)"
    [[ "$blocked" == *"deletion blocked"* ]] && break
    sleep 3
  done
  [[ "$blocked" == *"deletion blocked"* ]] \
    || fail "sticky-orig deletion did not surface 'deletion blocked' (Ready message: '$blocked')"
  kc get buckety/sticky-orig >/dev/null 2>&1 \
    || fail "sticky-orig disappeared while its backend was missing; the backend resource would be orphaned"

  # Restoring the backend unblocks the deletion.
  controller_config_restore
  kc wait --for=delete buckety/sticky-orig --timeout=90s \
    || fail "sticky-orig deletion did not complete after the backend was restored"

  log "backend-stickiness PASS"
}

# driver_version_scenario <buckety-name>
# SPEC §End-to-end coverage #9. Needs E2E_IMAGE_BASE/PATCH/MAJOR
# (test/e2e/build-rotation-images.sh); skips loudly otherwise -
# switching to a nonexistent image would wedge the controller for
# every scenario after this one. The base driver version is
# whatever the base image stamps, and the rotation images derive
# from the same binary (X.Y.Z+1 and (X+1).0.0 per driver), so the
# expectations derive from status too: nothing here knows what
# any driver's current version is.
driver_version_scenario() {
  local name="$1"
  if [[ -z "${E2E_IMAGE_BASE:-}" || -z "${E2E_IMAGE_PATCH:-}" || -z "${E2E_IMAGE_MAJOR:-}" ]]; then
    log "driver-version SKIPPED: E2E_IMAGE_BASE/PATCH/MAJOR not set (CI provides these; test/e2e/build-rotation-images.sh builds them locally)"
    exit 0
  fi
  trap 'controller_set_image "$E2E_IMAGE_BASE"' EXIT

  controller_set_image "$E2E_IMAGE_BASE"
  wait_ready "buckety/$name" 120s
  local base x y z
  base="$(kc get "buckety/$name" -o jsonpath='{.status.driverBuildVersion}')"
  IFS=. read -r x y z <<<"$base"
  [[ -n "$z" ]] || fail "status.driverBuildVersion '$base' is not X.Y.Z"
  local patch="$x.$y.$((z + 1))"
  [[ "$(kc get "buckety/$name" -o jsonpath='{.status.driverMajor}')" == "$x" ]] \
    || fail "status.driverMajor is not the major of driverBuildVersion $base"

  # Patch bump: auto-applied; buildVersion advances; major unchanged.
  controller_set_image "$E2E_IMAGE_PATCH"
  local bv=""
  for _ in $(seq 1 30); do
    bv="$(kc get "buckety/$name" -o jsonpath='{.status.driverBuildVersion}' 2>/dev/null || echo)"
    [[ "$bv" == "$patch" ]] && break
    sleep 2
  done
  [[ "$bv" == "$patch" ]] \
    || fail "after patch-rotate, driverBuildVersion=$bv, expected $patch"
  [[ "$(kc get "buckety/$name" -o jsonpath='{.status.driverMajor}')" == "$x" ]] \
    || fail "driverMajor changed after patch bump"
  [[ "$(condition_status "buckety/$name" Ready)" == "True" ]] \
    || fail "Ready not True after patch-rotate"

  # Major bump: incompatible; surfaces DriverVersionIncompatible.
  controller_set_image "$E2E_IMAGE_MAJOR"
  wait_condition "buckety/$name" DriverVersionIncompatible True 90s
  [[ "$(kc get "buckety/$name" -o jsonpath='{.status.driverMajor}')" == "$x" ]] \
    || fail "driverMajor changed under major bump"
  [[ "$(condition_status "buckety/$name" Ready)" == "False" ]] \
    || fail "Ready should be False under DriverVersionIncompatible"

  log "driver-version PASS"
}

# misconfigured_startup_scenario <scenario-dir>
# SPEC §End-to-end coverage #10. Every <dir>/broken-configs/<file>
# listed in <dir>/expectations.txt is installed as the controller
# config; the controller MUST refuse to start with a log line
# matching the listed regex.
misconfigured_startup_scenario() {
  local dir="$1" broken_dir="$1/broken-configs" expectations="$1/expectations.txt"
  : "${E2E_ORIGINAL_CONFIG:?harness must set E2E_ORIGINAL_CONFIG}"
  trap controller_config_restore EXIT

  local variant regex file
  while read -r variant regex; do
    [[ "$variant" =~ ^# ]] && continue
    [[ -z "$variant" ]] && continue
    file="$broken_dir/$variant"
    [[ -f "$file" ]] || fail "broken-config variant not found: $file"

    log "applying broken config: $variant (expect regex: /$regex/)"
    kcg -n "$E2E_CONTROLLER_NS" create secret generic buckety-controller-config \
      --from-file=buckety-controller.yaml="$file" \
      --dry-run=client -o yaml | kcg apply -f -
    rollout_restart deploy/buckety-controller

    # Wait until ANY pod's logs (current or previous container)
    # carry the expected regex. During a rollout there can be two
    # pods (old + new); after the new one has crashed once it lands
    # in CrashLoopBackOff and the message is in either log stream.
    local deadline=$(( $(date +%s) + 90 )) matched=0 pods pod arg
    while (( $(date +%s) < deadline )); do
      pods=$(kcg -n "$E2E_CONTROLLER_NS" get pods \
        -l app.kubernetes.io/name=buckety-controller \
        -o jsonpath='{range .items[*]}{.metadata.name} {end}' 2>/dev/null || true)
      for pod in $pods; do
        for arg in "" "--previous"; do
          if kcg -n "$E2E_CONTROLLER_NS" logs "$pod" $arg 2>/dev/null \
              | grep -E "$regex" >/dev/null 2>&1; then
            matched=1; break 3
          fi
        done
      done
      sleep 2
    done
    if (( matched == 0 )); then
      log "------ controller logs ($variant) ------"
      kcg -n "$E2E_CONTROLLER_NS" logs -l app.kubernetes.io/name=buckety-controller \
        --tail=200 --previous 2>/dev/null || true
      fail "variant '$variant' did not produce log matching /$regex/"
    fi
  done < "$expectations"

  log "misconfigured-startup PASS"
}

# scaled_to_zero_scenario <buckety> <secret> <job-file> <job-name> <secret-keys...>
# SPEC §End-to-end coverage #5. The consumer Job in <job-file>
# round-trips through the backend using only the Secret, with the
# controller scaled to zero.
scaled_to_zero_scenario() {
  local bky="$1" secret="$2" job_file="$3" job="$4"
  shift 4
  wait_ready "buckety/$bky" 120s
  secret_has_keys "$secret" "$@"

  log "scaling buckety-controller to 0 in $E2E_CONTROLLER_NS"
  kcg -n "$E2E_CONTROLLER_NS" scale deploy/buckety-controller --replicas=0
  kcg -n "$E2E_CONTROLLER_NS" wait --for=delete pod \
    -l app.kubernetes.io/name=buckety-controller --timeout=60s

  log "applying consumer Job (operator is down)"
  kc apply -f "$job_file"
  kc wait --for=condition=Complete --timeout=120s "job/$job" \
    || { kc logs "job/$job" >&2 || true; fail "roundtrip Job failed while operator was scaled to 0"; }

  # Restore replica count so subsequent scenarios see a running
  # controller.
  log "restoring buckety-controller to 1"
  kcg -n "$E2E_CONTROLLER_NS" scale deploy/buckety-controller --replicas=1
  kcg -n "$E2E_CONTROLLER_NS" rollout status deploy/buckety-controller --timeout=120s

  log "scaled-to-zero PASS"
}
