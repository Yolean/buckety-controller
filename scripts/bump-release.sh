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

# The digest is a function of the compiler, and CI rebuilds with
# the go.mod toolchain directive (setup-go go-version-file). A bump
# built with any other toolchain pins a digest CI cannot reproduce,
# so drift fails here with the remedy instead of in the assertion.
WANT="$(go mod edit -json | jq -r '.Toolchain // empty')"
GOT="$(go env GOVERSION)"
if [[ -z "$WANT" || "$GOT" != "$WANT" ]]; then
  echo "local toolchain ${GOT} does not match the go.mod toolchain directive '${WANT:-<missing>}'" >&2
  echo "align them first (go mod edit -toolchain=${GOT}) so CI reproduces the digest" >&2
  exit 1
fi

TAG="$(date -u +%Y%m%dT%H%M%SZ)"

DIGEST="$(scripts/build-image.sh "$TAG" ./oci)"
[[ "$DIGEST" == sha256:* ]] || { echo "unexpected digest: $DIGEST" >&2; exit 1; }

( cd deploy/kustomize/release \
  && kustomize edit set image \
       "ghcr.io/yolean/buckety-controller=ghcr.io/yolean/buckety-controller:${TAG}@${DIGEST}" )

printf 'release bumped: ghcr.io/yolean/buckety-controller:%s@%s\n' "$TAG" "$DIGEST"
