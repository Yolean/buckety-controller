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

  # Wait until ANY pod's logs (current or previous container)
  # carry the expected regex. During a rollout there can be two
  # pods (old + new); after the new one has crashed once it lands
  # in CrashLoopBackOff and the message is in either log stream.
  deadline=$(( $(date +%s) + 90 ))
  matched=0
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
