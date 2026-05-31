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
#   IMPLEMENTATIONS    Comma-separated. Default: redpanda,versitygw,minio.
#                      Each maps via $IMPL_DRIVER below to a driver.
#   OCI_DIR            Local OCI layout (preferred over CONTROLLER_IMAGE
#                      when both are set). Sideloaded via
#                      `y-cluster images load`.
#   CONTROLLER_IMAGE   Image reference (e.g. ghcr.io/...@sha256:...).
#                      Used when OCI_DIR is not set.
#   KEEP_FAILED        If true, scenario namespaces are kept on failure
#                      for `kubectl describe` / `kubectl logs`.
#   KUBECONFIG         Cluster the harness writes to.
#   CONTROLLER_NS      Namespace the buckety-controller runs in.
#                      Default: buckety.
#   OVERLAYS_DIR       Where per-implementation overlays live.
#                      Default: <repo>/test/e2e/overlays.
#
# Each per-implementation overlay is a kustomize bundle that:
#   - applies deploy/kustomize/base
#   - patches the controller image (overlay or set-image at runtime)
#   - provides a buckety-controller-config Secret with the
#     backends this implementation should expose
#   - applies any implementation-specific bootstrap manifests
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
OVERLAYS_DIR="${OVERLAYS_DIR:-$HERE/overlays}"
CONTROLLER_NS="${CONTROLLER_NS:-buckety}"
KEEP_FAILED="${KEEP_FAILED:-false}"
IMPLEMENTATIONS="${IMPLEMENTATIONS:-redpanda,versitygw,minio}"

# Map implementation -> driver. Scenario discovery uses this to
# pick which examples/<driver>/* to run for each implementation.
declare -A IMPL_DRIVER=(
  [redpanda]=kadm
  [versitygw]=s3
  [minio]=s3
)

log() { printf '[run.sh] %s\n' "$*" >&2; }
fail() { printf '[run.sh][ERR] %s\n' "$*" >&2; exit 1; }

# ---- image setup ---------------------------------------------

sideload_image() {
  if [[ -n "${OCI_DIR:-}" ]]; then
    [[ -d "$OCI_DIR" ]] || fail "OCI_DIR=$OCI_DIR not a directory"
    log "sideloading $OCI_DIR via y-cluster images load"
    y-cluster images load "$OCI_DIR"
    return
  fi
  if [[ -n "${CONTROLLER_IMAGE:-}" ]]; then
    log "using CONTROLLER_IMAGE=$CONTROLLER_IMAGE (no sideload)"
    return
  fi
  fail "set OCI_DIR (sideloaded local build) or CONTROLLER_IMAGE (pre-pushed digest)"
}

# ---- per-implementation lifecycle ----------------------------

apply_overlay() {
  local impl="$1"
  local overlay="$OVERLAYS_DIR/$impl"
  [[ -d "$overlay" ]] || fail "overlay missing for implementation '$impl': $overlay"
  log "applying overlay $overlay"
  kubectl apply -k "$overlay"
  kubectl -n "$CONTROLLER_NS" rollout status deploy/buckety-controller --timeout=120s
}

teardown_overlay() {
  local impl="$1"
  local overlay="$OVERLAYS_DIR/$impl"
  # Best-effort: ignore missing resources.
  kubectl delete -k "$overlay" --ignore-not-found --wait=false || true
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

run_scenario() {
  local scenario="$1" impl="$2" driver="$3"
  local name="$(basename "$scenario")"
  local parent="$(basename "$(dirname "$scenario")")"
  local ns="e2e-${driver}-${impl}-${parent}-${name}"
  # Sanitise (k8s namespace charset).
  ns="$(echo "$ns" | tr 'A-Z_' 'a-z-' | head -c 60 | sed 's/-$//')"

  log "=== scenario: $scenario -> $ns (impl=$impl) ==="
  kubectl create namespace "$ns"
  trap 'on_scenario_exit '"$ns"' '"$KEEP_FAILED"'' EXIT

  kubectl apply -k "$scenario" -n "$ns"

  E2E_NAMESPACE="$ns" \
  E2E_CONTROLLER_NS="$CONTROLLER_NS" \
  E2E_IMPLEMENTATION="$impl" \
  E2E_LIB="$HERE" \
  E2E_BACKEND_ZONE="${E2E_BACKEND_ZONE:-e2e}" \
  E2E_KAFKA_NAMESPACE="${E2E_KAFKA_NAMESPACE:-redpanda}" \
  E2E_KAFKA_BOOTSTRAP="${E2E_KAFKA_BOOTSTRAP:-redpanda.redpanda.svc.cluster.local:9093}" \
  E2E_ORIGINAL_CONFIG="$OVERLAYS_DIR/$impl/buckety-controller.yaml" \
  E2E_RENAMED_CONFIG="$OVERLAYS_DIR/$impl/buckety-controller.renamed.yaml" \
  E2E_VERSION_BASE="${E2E_VERSION_BASE:-0.1.0}" \
  E2E_VERSION_PATCH="${E2E_VERSION_PATCH:-0.1.1}" \
  E2E_VERSION_MAJOR="${E2E_VERSION_MAJOR:-1.0.0}" \
  E2E_IMAGE_BASE="${E2E_IMAGE_BASE:-ghcr.io/yolean/buckety-controller:dev}" \
  E2E_IMAGE_PATCH="${E2E_IMAGE_PATCH:-ghcr.io/yolean/buckety-controller:dev}" \
  E2E_IMAGE_MAJOR="${E2E_IMAGE_MAJOR:-ghcr.io/yolean/buckety-controller:dev}" \
  bash "$scenario/assert.sh"

  log "--- scenario PASS: $scenario (impl=$impl) ---"
  kubectl delete namespace "$ns" --wait=false || true
  trap - EXIT
}

on_scenario_exit() {
  local ns="$1" keep="$2"
  if [[ "$keep" == "true" ]]; then
    log "KEEP_FAILED=true; namespace $ns left for inspection"
    return
  fi
  kubectl delete namespace "$ns" --wait=false --ignore-not-found || true
}

# ---- main -----------------------------------------------------

sideload_image

declare -i fails=0
declare -A results
IFS=',' read -ra impl_list <<<"$IMPLEMENTATIONS"
for impl in "${impl_list[@]}"; do
  driver="${IMPL_DRIVER[$impl]:-}"
  [[ -n "$driver" ]] || fail "unknown implementation '$impl' (no driver mapping)"

  log "============================================================"
  log "implementation=$impl  driver=$driver"
  log "============================================================"
  apply_overlay "$impl"

  while IFS= read -r scenario; do
    [[ -z "$scenario" ]] && continue
    scenario_matches_impl "$scenario" "$impl" "$driver" || continue
    if run_scenario "$scenario" "$impl" "$driver"; then
      results["$impl/$scenario"]=PASS
    else
      results["$impl/$scenario"]=FAIL
      fails=$((fails + 1))
    fi
  done < <(scenarios_for_driver "$driver")

  teardown_overlay "$impl"
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
