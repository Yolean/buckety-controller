#!/usr/bin/env bash
#
# Build the controller binary and its OCI layout, reproducibly.
#
#   scripts/build-image.sh <tag> [out-dir] [extra -ldflags...]
#
# The ONE place the build recipe lives. CI's e2e job, the publish
# job and scripts/bump-release.sh all go through here, so the
# digest CI asserts against the release pin is a function of this
# file plus the source and the go.mod toolchain - never of which
# copy of the flags a step happened to carry.
#
# -buildvcs=false is the reproducibility lever: with it on (the
# default) Go embeds the commit SHA and a "modified" flag, which
# makes the digest depend on whether the tree had uncommitted
# files at build time. Off, the binary is a pure function of
# source + flags + Go version.

set -euo pipefail

TAG="${1:?usage: build-image.sh <tag> [out-dir] [extra -ldflags...]}"
OUT="${2:-./oci}"
shift $(( $# >= 2 ? 2 : $# ))

here() { cd "$(dirname "${BASH_SOURCE[0]}")" && pwd; }
cd "$(here)/.."

rm -rf target/linux/amd64 "$OUT"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -trimpath \
  -buildvcs=false \
  -ldflags "-s -w -X main.version=${TAG} $*" \
  -o target/linux/amd64/buckety \
  ./cmd/buckety

IMAGE="ghcr.io/yolean/buckety-controller:${TAG}" \
  contain build --output "$OUT" --push=false >/dev/null

jq -r '.manifests[0].digest' "$OUT/index.json"
