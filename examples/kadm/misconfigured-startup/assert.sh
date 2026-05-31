#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

: "${E2E_ORIGINAL_CONFIG:?harness must set E2E_ORIGINAL_CONFIG to a valid buckety-controller.yaml on disk}"

dir="$(cd "$(dirname "$0")" && pwd)"
broken_dir="$dir/broken-configs"
expectations="$dir/expectations.txt"

restore() {
  log "restoring original controller config"
  kcg -n "$E2E_CONTROLLER_NS" create secret generic buckety-controller-config \
    --from-file=buckety-controller.yaml="$E2E_ORIGINAL_CONFIG" \
    --dry-run=client -o yaml | kcg apply -f -
  kcg -n "$E2E_CONTROLLER_NS" rollout restart deploy/buckety-controller
  kcg -n "$E2E_CONTROLLER_NS" rollout status  deploy/buckety-controller --timeout=60s
}
trap restore EXIT

apply_broken() {
  local file="$1"
  kcg -n "$E2E_CONTROLLER_NS" create secret generic buckety-controller-config \
    --from-file=buckety-controller.yaml="$file" \
    --dry-run=client -o yaml | kcg apply -f -
  kcg -n "$E2E_CONTROLLER_NS" rollout restart deploy/buckety-controller
}

# Iterate variants, each MUST produce a log line matching the
# documented regex on the controller's most recent attempt.
while read -r variant regex; do
  [[ "$variant" =~ ^# ]] && continue
  [[ -z "$variant" ]]    && continue
  file="$broken_dir/$variant"
  [[ -f "$file" ]] || fail "broken-config variant not found: $file"

  log "applying broken config: $variant (expect regex: /$regex/)"
  apply_broken "$file"

  # Wait until a pod has terminated at least once with non-zero
  # exit (or the rollout is failing). We allow up to 60s.
  deadline=$(( $(date +%s) + 60 ))
  matched=0
  while (( $(date +%s) < deadline )); do
    pod="$(kcg -n "$E2E_CONTROLLER_NS" get pods \
      -l app.kubernetes.io/name=buckety-controller \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [[ -n "$pod" ]] && kcg -n "$E2E_CONTROLLER_NS" logs "$pod" --previous 2>/dev/null \
        | grep -E "$regex" >/dev/null 2>&1; then
      matched=1; break
    fi
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
