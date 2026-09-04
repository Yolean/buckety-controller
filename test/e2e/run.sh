#!/usr/bin/env bash
#
# Buckety controller e2e harness.
#
# Same script in CI and on a contributor laptop. The cluster
# (k3d / k3s / etc.) is provisioned by a separate step; this
# script orchestrates scenarios against an already-running
# cluster with backing services available.
#
# Inputs (env):
#   IMPLEMENTATIONS    Comma-separated. Default: redpanda,versitygw,minio,fakegcs.
#                      Each maps via $IMPL_DRIVER below to a driver.
#   CONTROLLER_IMAGE   Cluster-side image reference the deployment is
#                      patched to before rollout. Required.
#   OCI_DIR            Optional local OCI layout to push before applying.
#                      Requires PUSH_AS.
#   PUSH_AS            Host-reachable registry target for OCI_DIR
#                      (e.g. localhost:5000/yolean/buckety-controller:dev).
#                      The cluster-side equivalent goes in CONTROLLER_IMAGE;
#                      they differ when push and pull traverse different
#                      hostnames (k3d local registry on a docker network
#                      reaches as k3d-<name>:5000 from inside the cluster
#                      but as localhost:<port> from the host).
#   KEEP_FAILED        If true, scenario namespaces are kept even on
#                      PASS. Failed scenarios always leave their
#                      namespace standing for `kubectl describe` /
#                      `kubectl logs` (SPEC.md section "E2E harness
#                      and parity" #3).
#   E2E_IMAGE_BASE / E2E_IMAGE_PATCH / E2E_IMAGE_MAJOR
#   E2E_VERSION_BASE / E2E_VERSION_PATCH / E2E_VERSION_MAJOR
#                      Controller images built with rotated driver
#                      versions, for the driver-version scenario. CI
#                      builds and pushes these; when unset the
#                      scenario logs SKIPPED and exits 0.
#   KUBECONFIG         Cluster the harness writes to.
#   CONTROLLER_NS      Namespace the buckety-controller runs in.
#                      Default: buckety.
#
# The controller is deployed once from overlay/ (the release base
# plus webhook-certgen, args and backing-service credentials).
# Each implementation then only swaps the controller config
# (configs/<impl>.yaml) and restarts the controller - the same
# operation a platform performs on a config change.
#
# Scenario discovery: every directory under examples/<driver>/...
# that contains both kustomization.yaml AND assert.sh is a
# scenario. Driver-agnostic scenarios live under examples/<driver>/
# directly; implementation-specific ones live one level deeper
# (e.g. examples/s3/r2/jurisdiction/).

set -euo pipefail

here() { cd "$(dirname "${BASH_SOURCE[0]}")" && pwd; }
HERE="$(here)"
REPO="$(cd "$HERE/../.." && pwd)"
CONTROLLER_NS="${CONTROLLER_NS:-buckety}"
KEEP_FAILED="${KEEP_FAILED:-false}"
IMPLEMENTATIONS="${IMPLEMENTATIONS:-redpanda,versitygw,minio,fakegcs}"

# Map implementation -> driver. Scenario discovery uses this to
# pick which examples/<driver>/* to run for each implementation.
declare -A IMPL_DRIVER=(
  [redpanda]=kadm
  [versitygw]=s3
  [minio]=s3
  [fakegcs]=gcs
)
# Map implementation -> the backend name its config declares for
# the driver-agnostic scenarios. backend-stickiness renames it to
# prove status.backend stickiness; the renamed config is derived
# from configs/<impl>.yaml rather than kept as a second copy.
declare -A IMPL_BACKEND=(
  [redpanda]=kafka
  [versitygw]=s3
  [minio]=s3
  [fakegcs]=gcs
)

log() { printf '[run.sh] %s\n' "$*" >&2; }
fail() { printf '[run.sh][ERR] %s\n' "$*" >&2; exit 1; }

# ---- image setup ---------------------------------------------

sideload_image() {
  if [[ -n "${OCI_DIR:-}" ]]; then
    [[ -d "$OCI_DIR" ]] || fail "OCI_DIR=$OCI_DIR not a directory"
    [[ -n "${PUSH_AS:-}" ]] || fail "OCI_DIR set but PUSH_AS unset"
    command -v crane >/dev/null || fail "crane not on PATH"
    log "pushing $OCI_DIR -> $PUSH_AS"
    crane push --insecure "$OCI_DIR" "$PUSH_AS"
  fi
  [[ -n "${CONTROLLER_IMAGE:-}" ]] || fail "CONTROLLER_IMAGE required (cluster-side image ref)"
  log "controller image: $CONTROLLER_IMAGE"
}

# ---- per-implementation lifecycle ----------------------------

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK" "$HERE/.work"' EXIT

# apply_config installs a config file as the controller's config
# Secret - the operation a platform performs on a config change,
# and the one the config-swapping scenarios perform too.
apply_config() {
  kubectl -n "$CONTROLLER_NS" create secret generic buckety-controller-config \
    --from-file=buckety-controller.yaml="$1" \
    --dry-run=client -o yaml | kubectl apply -f -
}

# switch_config points the controller at implementation $1's
# config and derives the renamed variant for backend-stickiness.
# Sets E2E_ORIGINAL_CONFIG / E2E_RENAMED_CONFIG for the scenarios.
switch_config() {
  local impl="$1" backend="${IMPL_BACKEND[$1]}"
  E2E_ORIGINAL_CONFIG="$HERE/configs/$impl.yaml"
  E2E_RENAMED_CONFIG="$WORK/$impl.renamed.yaml"
  sed "s/^- name: ${backend}\$/- name: ${backend}-renamed/" "$E2E_ORIGINAL_CONFIG" >"$E2E_RENAMED_CONFIG"
  grep -q -- "^- name: ${backend}-renamed" "$E2E_RENAMED_CONFIG" \
    || fail "configs/$impl.yaml declares no backend named '$backend'"
  log "controller config: $E2E_ORIGINAL_CONFIG"
  apply_config "$E2E_ORIGINAL_CONFIG"
}

# deploy_controller applies overlay/ once, with CONTROLLER_IMAGE
# set through a kustomize images transformer: `kubectl set image`
# after an apply would first roll out the base's placeholder tag
# (and leave an ImagePullBackOff pod behind on every re-apply).
# The certgen Jobs mint the webhook TLS Secret; the controller
# restarts until it exists and turns Ready only once its webhook
# listener answers, which is what `rollout status` waits for.
deploy_controller() {
  [[ "$CONTROLLER_IMAGE" != *@* ]] \
    || fail "CONTROLLER_IMAGE must be a tag reference, got a digest: $CONTROLLER_IMAGE"
  local name="${CONTROLLER_IMAGE%:*}" tag="${CONTROLLER_IMAGE##*:}"
  # Generated next to overlay/ (gitignored): kustomize wants the
  # resource path relative, and refuses one above its root.
  mkdir -p "$HERE/.work"
  cat >"$HERE/.work/kustomization.yaml" <<EOF
resources:
- ../overlay
images:
- name: ghcr.io/yolean/buckety-controller
  newName: $name
  newTag: "$tag"
EOF
  log "deploying controller as $CONTROLLER_IMAGE"
  kubectl apply -k "$HERE/.work"
  kubectl -n "$CONTROLLER_NS" rollout status deploy/buckety-controller --timeout=180s
}

# restart_controller picks up a swapped config. Deleting the
# overlay between implementations is not an option: CRDs with
# terminating scenario CRs would hang, and the next apply would be
# rejected while the CRD is terminating.
restart_controller() {
  kubectl -n "$CONTROLLER_NS" rollout restart deploy/buckety-controller
  kubectl -n "$CONTROLLER_NS" rollout status deploy/buckety-controller --timeout=180s
}

# ---- scenario discovery and execution -------------------------

scenarios_for_driver() {
  local driver="$1"
  # Top-level scenarios (examples/<driver>/<scenario>/).
  find "$REPO/examples/$driver" -mindepth 2 -maxdepth 2 -type f -name assert.sh \
    -exec dirname {} \; | sort
  # Implementation-nested scenarios (examples/<driver>/<impl>/<scenario>/).
  # Filtered downstream so only the matching implementation runs them.
  find "$REPO/examples/$driver" -mindepth 3 -maxdepth 3 -type f -name assert.sh \
    -exec dirname {} \; | sort
}

scenario_matches_impl() {
  local scenario="$1" impl="$2" driver="$3"
  local rel="${scenario#$REPO/examples/$driver/}"
  case "$rel" in
    */*) # implementation-nested: <impl-from-path>/<scenario>
      local impl_in_path="${rel%%/*}"
      [[ "$impl_in_path" == "$impl" ]];;
    *) true;;  # top-level scenarios run for every impl of the driver
  esac
}

# run_scenario is invoked in an `if` condition, which makes bash
# ignore `set -e` for everything inside the function body. Every
# step therefore propagates failure explicitly with `|| return 1`;
# do not add a step here without one.
run_scenario() {
  local scenario="$1" impl="$2" driver="$3"
  local name="$(basename "$scenario")"
  local parent="$(basename "$(dirname "$scenario")")"
  local ns="e2e-${driver}-${impl}-${parent}-${name}"
  # Sanitise (k8s namespace charset).
  ns="$(echo "$ns" | tr 'A-Z_' 'a-z-' | head -c 60 | sed 's/-$//')"

  log "=== scenario: $scenario -> $ns (impl=$impl) ==="
  kubectl create namespace "$ns" || return 1

  # Render first: config-only scenarios (misconfigured-startup)
  # declare `resources: []` and apply nothing, which plain
  # `kubectl apply -k` rejects with "no objects passed to apply".
  local rendered
  rendered="$(kubectl kustomize "$scenario")" || {
    log "scenario FAILED (kustomize build): $scenario; namespace $ns left standing"
    return 1
  }
  if [[ -n "${rendered//[$'\n\r\t ']/}" ]]; then
    # The controller Pod is Ready only once its webhook listener
    # answers, but the apiserver resolves the webhook Service
    # through an informer cache that can lag a beat after the
    # controller Pod churns - seen as "no endpoints available" on
    # the first apply after scaled-to-zero. The error is transient
    # and apply is idempotent: retry it. Admission DENIALS say
    # "denied the request", not "failed calling webhook", so real
    # rejections still fail fast.
    local applied=0 apply_out=""
    for _ in $(seq 1 15); do
      if apply_out="$(printf '%s\n' "$rendered" | kubectl apply -n "$ns" -f - 2>&1)"; then
        applied=1
        printf '%s\n' "$apply_out"
        break
      fi
      grep -q "failed calling webhook" <<<"$apply_out" || break
      sleep 2
    done
    if (( ! applied )); then
      printf '%s\n' "$apply_out" >&2
      log "scenario FAILED (apply): $scenario; namespace $ns left standing"
      return 1
    fi
  else
    log "scenario applies no namespaced resources (config-only)"
  fi

  # </dev/null: assert.sh helpers use `kubectl run -i`, which would
  # otherwise forward and consume this shell's stdin. When stdin is
  # the scenario list of a `while read` caller, that silently
  # truncates the run.
  if ! E2E_NAMESPACE="$ns" \
    E2E_CONTROLLER_NS="$CONTROLLER_NS" \
    E2E_IMPLEMENTATION="$impl" \
    E2E_LIB="$HERE" \
    E2E_BACKEND_ZONE="${E2E_BACKEND_ZONE:-e2e}" \
    E2E_KAFKA_NAMESPACE="${E2E_KAFKA_NAMESPACE:-redpanda}" \
    E2E_KAFKA_BOOTSTRAP="${E2E_KAFKA_BOOTSTRAP:-redpanda.redpanda.svc.cluster.local:9093}" \
    E2E_ORIGINAL_CONFIG="$E2E_ORIGINAL_CONFIG" \
    E2E_RENAMED_CONFIG="$E2E_RENAMED_CONFIG" \
    E2E_VERSION_BASE="${E2E_VERSION_BASE:-}" \
    E2E_VERSION_PATCH="${E2E_VERSION_PATCH:-}" \
    E2E_VERSION_MAJOR="${E2E_VERSION_MAJOR:-}" \
    E2E_IMAGE_BASE="${E2E_IMAGE_BASE:-}" \
    E2E_IMAGE_PATCH="${E2E_IMAGE_PATCH:-}" \
    E2E_IMAGE_MAJOR="${E2E_IMAGE_MAJOR:-}" \
    bash "$scenario/assert.sh" </dev/null; then
    log "scenario FAILED (assert): $scenario; namespace $ns left standing"
    return 1
  fi

  log "--- scenario PASS: $scenario (impl=$impl) ---"
  if [[ "$KEEP_FAILED" == "true" ]]; then
    log "KEEP_FAILED=true; namespace $ns left for inspection"
  else
    kubectl delete namespace "$ns" --wait=false || true
  fi
  return 0
}

# ---- main -----------------------------------------------------

sideload_image

declare -i fails=0 deployed=0
declare -A results
IFS=',' read -ra impl_list <<<"$IMPLEMENTATIONS"
for impl in "${impl_list[@]}"; do
  driver="${IMPL_DRIVER[$impl]:-}"
  [[ -n "$driver" ]] || fail "unknown implementation '$impl' (no driver mapping)"

  log "============================================================"
  log "implementation=$impl  driver=$driver"
  log "============================================================"
  switch_config "$impl"
  if (( deployed )); then
    restart_controller
  else
    deploy_controller
    deployed=1
  fi

  # The scenario list is materialised up front instead of streamed
  # on stdin, so nothing a scenario runs can consume the remainder
  # of the list.
  mapfile -t scenario_list < <(scenarios_for_driver "$driver")
  for scenario in "${scenario_list[@]}"; do
    [[ -z "$scenario" ]] && continue
    scenario_matches_impl "$scenario" "$impl" "$driver" || continue
    if run_scenario "$scenario" "$impl" "$driver"; then
      results["$impl/$scenario"]=PASS
    else
      results["$impl/$scenario"]=FAIL
      fails=$((fails + 1))
    fi
  done
done

log "============================================================"
log "results"
log "============================================================"
for key in "${!results[@]}"; do
  printf '  %s  %s\n' "${results[$key]}" "$key" >&2
done

if (( fails > 0 )); then
  fail "$fails scenario(s) failed"
fi
log "all scenarios PASS"
