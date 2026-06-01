#!/usr/bin/env bash
#
# Bump the release pin in deploy/kustomize/release/kustomization.yaml.
# Mints a UTC-seconds ISO tag, builds the OCI deterministically with
# that tag baked into main.version, extracts the manifest digest from
# the OCI layout, and writes both back into the kustomization. CI's
# e2e job rebuilds with the same tag and asserts the produced digest
# matches; the publish job craned-pushes the same OCI to ghcr.io.
#
# Run this before committing whenever the controller binary or its
# base image changes; the assertion in CI catches you otherwise.

set -euo pipefail

here() { cd "$(dirname "${BASH_SOURCE[0]}")" && pwd; }
REPO="$(cd "$(here)/.." && pwd)"

command -v contain >/dev/null || { echo "contain not on PATH" >&2; exit 1; }
command -v jq      >/dev/null || { echo "jq not on PATH" >&2; exit 1; }
command -v kustomize >/dev/null || { echo "kustomize not on PATH" >&2; exit 1; }

cd "$REPO"
TAG="$(date -u +%Y%m%dT%H%M%SZ)"

rm -rf target/linux/amd64 oci
# -buildvcs=false is the reproducibility lever: with it on (the default),
# Go embeds the commit SHA and a "modified" flag, which makes the digest
# depend on whether the tree had uncommitted files at build time. Off,
# the binary is a pure function of source + flags + Go version.
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -trimpath \
  -buildvcs=false \
  -ldflags "-s -w -X main.version=${TAG}" \
  -o target/linux/amd64/buckety \
  ./cmd/buckety

IMAGE="ghcr.io/yolean/buckety-controller:${TAG}" \
  contain build --output ./oci --push=false >/dev/null

DIGEST="$(jq -r '.manifests[0].digest' oci/index.json)"
[[ "$DIGEST" == sha256:* ]] || { echo "unexpected digest: $DIGEST" >&2; exit 1; }

( cd deploy/kustomize/release \
  && kustomize edit set image \
       "ghcr.io/yolean/buckety-controller=ghcr.io/yolean/buckety-controller:${TAG}@${DIGEST}" )

printf 'release bumped: ghcr.io/yolean/buckety-controller:%s@%s\n' "$TAG" "$DIGEST"
