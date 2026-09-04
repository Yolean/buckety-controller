#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"

# Body in test/e2e/lib.sh: controller behaviour, identical for every driver.
scaled_to_zero_scenario dial-tone dial-tone-bucket "$(dirname "$0")/consumer-job.yaml" dial-tone-roundtrip endpoint bucket accessKeyID secretAccessKey
