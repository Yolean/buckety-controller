#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

# Body in test/e2e/lib.sh: controller behaviour, identical for every driver.
backend_stickiness_scenario s3 "$(dirname "$0")/renamed.yaml"
