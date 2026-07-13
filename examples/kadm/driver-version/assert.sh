#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

# This scenario needs three controller images with rotated driver
# versions (see .github/workflows/e2e.yaml "rotation images" step
# for the build recipe). Without them there is nothing meaningful
# to assert - switching the Deployment to a nonexistent image would
# wedge the controller for every scenario after this one - so skip
# loudly rather than fail.
if [[ -z "${E2E_IMAGE_BASE:-}" || -z "${E2E_IMAGE_PATCH:-}" || -z "${E2E_IMAGE_MAJOR:-}" ]]; then
  log "driver-version SKIPPED: E2E_IMAGE_BASE/PATCH/MAJOR not set (CI provides these; build rotated images to run locally)"
  exit 0
fi
: "${E2E_VERSION_BASE:?must be set alongside E2E_IMAGE_BASE, e.g. 0.1.0}"
: "${E2E_VERSION_PATCH:?must be set alongside E2E_IMAGE_PATCH, e.g. 0.1.1}"
: "${E2E_VERSION_MAJOR:?must be set alongside E2E_IMAGE_MAJOR, e.g. 1.0.0}"

set_image() {
  local img="$1"
  log "switching controller image to $img"
  kcg -n "$E2E_CONTROLLER_NS" set image deploy/buckety-controller "controller=$img"
  kcg -n "$E2E_CONTROLLER_NS" rollout status deploy/buckety-controller --timeout=60s
}

restore() { set_image "$E2E_IMAGE_BASE"; }
trap restore EXIT

set_image "$E2E_IMAGE_BASE"
wait_ready buckety/pinned 120s

base_major="${E2E_VERSION_BASE%%.*}"
stamped_major="$(kc get buckety/pinned -o jsonpath='{.status.driverMajor}')"
[[ "$stamped_major" == "$base_major" ]] \
  || fail "status.driverMajor=$stamped_major, expected $base_major"

build_version="$(kc get buckety/pinned -o jsonpath='{.status.driverBuildVersion}')"
[[ "$build_version" == "$E2E_VERSION_BASE" ]] \
  || fail "status.driverBuildVersion=$build_version, expected $E2E_VERSION_BASE"

# Patch bump: auto-applied; buildVersion advances; major unchanged.
set_image "$E2E_IMAGE_PATCH"
for _ in $(seq 1 30); do
  bv="$(kc get buckety/pinned -o jsonpath='{.status.driverBuildVersion}' 2>/dev/null || echo)"
  [[ "$bv" == "$E2E_VERSION_PATCH" ]] && break
  sleep 2
done
[[ "$bv" == "$E2E_VERSION_PATCH" ]] \
  || fail "after patch-rotate, driverBuildVersion=$bv, expected $E2E_VERSION_PATCH"
[[ "$(kc get buckety/pinned -o jsonpath='{.status.driverMajor}')" == "$base_major" ]] \
  || fail "driverMajor changed after patch bump"
[[ "$(condition_status buckety/pinned Ready)" == "True" ]] \
  || fail "Ready not True after patch-rotate"

# Major bump: incompatible; surfaces DriverVersionIncompatible.
set_image "$E2E_IMAGE_MAJOR"
wait_condition buckety/pinned DriverVersionIncompatible True 90s
[[ "$(kc get buckety/pinned -o jsonpath='{.status.driverMajor}')" == "$base_major" ]] \
  || fail "driverMajor changed under major bump"
[[ "$(condition_status buckety/pinned Ready)" == "False" ]] \
  || fail "Ready should be False under DriverVersionIncompatible"

log "driver-version PASS"
