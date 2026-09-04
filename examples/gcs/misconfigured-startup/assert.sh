#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

# Body in test/e2e/lib.sh: controller behaviour, identical for every driver.
misconfigured_startup_scenario "$(cd "$(dirname "$0")" && pwd)"
