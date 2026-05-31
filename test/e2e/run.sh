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
    [[ -n "${PUSH_AS:-}" ]] || fail "OCI_DIR set but PUSH_AS unset"
    command -v crane >/dev/null || fail "crane not on PATH"
    log "pushing $OCI_DIR -> $PUSH_AS"
    crane push --insecure "$OCI_DIR" "$PUSH_AS"
  fi
  [[ -n "${CONTROLLER_IMAGE:-}" ]] || fail "CONTROLLER_IMAGE required (cluster-side image ref)"
  log "controller image: $CONTROLLER_IMAGE"
}

# ---- per-implementation lifecycle ----------------------------

apply_overlay() {
  local impl="$1"
  local overlay="$OVERLAYS_DIR/$impl"
  [[ -d "$overlay" ]] || fail "overlay missing for implementation '$impl': $overlay"
  log "applying overlay $overlay"
  kubectl apply -k "$overlay"
  ensure_webhook_tls
  # Overlays ship the baked-in default image; CI and local k3d both push
  # to a per-run registry, so patch the deployment to CONTROLLER_IMAGE
  # before rollout instead of carrying a stale tag.
  kubectl -n "$CONTROLLER_NS" set image deploy/buckety-controller \
    controller="$CONTROLLER_IMAGE"
  kubectl -n "$CONTROLLER_NS" rollout status deploy/buckety-controller --timeout=180s
}

# The base overlay ships a ValidatingWebhookConfiguration that
# expects cert-manager to populate caBundle. In e2e there is no
# cert-manager, so generate a one-shot self-signed cert, mount it
# into the controller, and patch the VWC's caBundle to trust it.
# parameter-mutation and similar scenarios actively probe the
# webhook so disabling it via --enable-webhook=false would mask
# real regressions.
WEBHOOK_TLS_DIR=""
ensure_webhook_tls() {
  if [[ -z "$WEBHOOK_TLS_DIR" ]]; then
    command -v openssl >/dev/null || fail "openssl required for self-signed webhook TLS"
    log "generating self-signed webhook TLS"
    WEBHOOK_TLS_DIR="$(mktemp -d)"
    openssl req -x509 -newkey rsa:2048 -days 365 -nodes \
      -keyout "$WEBHOOK_TLS_DIR/tls.key" -out "$WEBHOOK_TLS_DIR/tls.crt" \
      -subj "/CN=buckety-controller-webhook.${CONTROLLER_NS}.svc" \
      -addext "subjectAltName=DNS:buckety-controller-webhook.${CONTROLLER_NS}.svc,DNS:buckety-controller-webhook.${CONTROLLER_NS}.svc.cluster.local" \
      >/dev/null 2>&1
  fi
  kubectl -n "$CONTROLLER_NS" create secret tls buckety-controller-webhook-tls \
    --cert="$WEBHOOK_TLS_DIR/tls.crt" --key="$WEBHOOK_TLS_DIR/tls.key" \
    --dry-run=client -o yaml | kubectl apply -f -
  local ca_bundle
  ca_bundle="$(base64 -w0 <"$WEBHOOK_TLS_DIR/tls.crt")"
  kubectl patch validatingwebhookconfiguration buckety-controller --type=json \
    -p="[{\"op\":\"replace\",\"path\":\"/webhooks/0/clientConfig/caBundle\",\"value\":\"${ca_bundle}\"}]"
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
