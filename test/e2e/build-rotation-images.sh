#!/usr/bin/env bash
#
# Build and push the two controller images the driver-version
# scenario rotates through:
#
#   build-rotation-images.sh <push-ref-without-tag>
#
# e.g. build-rotation-images.sh localhost:5000/yolean/buckety-controller
# pushes <ref>:dv-patch and <ref>:dv-major.
#
# Versions are derived from the binary itself (`buckety --version`)
# so no file needs to know what each driver's current version is:
# for every driver X.Y.Z, dv-patch carries X.Y.(Z+1) and dv-major
# (X+1).0.0. The scenario derives the same expectations from the
# status.driverBuildVersion the base image stamps.

set -euo pipefail

REF="${1:?usage: build-rotation-images.sh <push-ref-without-tag>}"
here() { cd "$(dirname "${BASH_SOURCE[0]}")" && pwd; }
cd "$(here)/../.."

# "driver <name> <X.Y.Z>" lines -> -X flags per rotation.
patch_flags=() major_flags=()
while read -r kind name ver; do
  [[ "$kind" == driver ]] || continue
  IFS=. read -r x y z <<<"$ver"
  pkg="github.com/Yolean/buckety-controller/pkg/drivers/${name}.version"
  patch_flags+=("-X" "${pkg}=${x}.${y}.$((z + 1))")
  major_flags+=("-X" "${pkg}=$((x + 1)).0.0")
done < <(go run ./cmd/buckety --version)
(( ${#patch_flags[@]} > 0 )) || { echo "no drivers reported by --version" >&2; exit 1; }

scripts/build-image.sh dv-patch ./oci-dv-patch "${patch_flags[@]}" >/dev/null
crane push --insecure ./oci-dv-patch "${REF}:dv-patch"
scripts/build-image.sh dv-major ./oci-dv-major "${major_flags[@]}" >/dev/null
crane push --insecure ./oci-dv-major "${REF}:dv-major"
