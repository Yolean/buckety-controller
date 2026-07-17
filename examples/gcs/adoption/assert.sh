#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"
here="$(cd "$(dirname "$0")" && pwd)"

# Fresh name first: proves provenance=Created and yields the
# emulator endpoint for the out-of-band pre-creation below.
kc apply -f "$here/fresh.yaml"
wait_ready buckety/adopt-fresh 120s
assert_provenance adopt-fresh Created
endpoint="$(secret_value adopt-fresh-creds endpoint)"

log "pre-creating bucket adopt-pre (with content) and adopt-void (empty)"
gcs_bucket_create adopt-pre "$endpoint"
gcs_object_put adopt-pre "$endpoint" precious.txt "predates the CR"
gcs_bucket_create adopt-void "$endpoint"

# Non-empty pre-existing resource: the gate refuses, nothing is
# stamped, no Secret is minted.
kc apply -f "$here/pre.yaml"
wait_ready_reason buckety/adopt-pre BackendResourceExists 60
if kc get secret/adopt-pre-creds >/dev/null 2>&1; then
  fail "Secret minted while adoption was refused"
fi

# Empty pre-existing resource: adopts under the default policy.
kc apply -f "$here/void.yaml"
wait_ready buckety/adopt-void 120s
assert_provenance adopt-void Adopted

# Explicit opt-in claims the non-empty bucket; content untouched.
log "unblocking adopt-pre with spec.adoption=Adopt"
kc patch buckety/adopt-pre --type=merge -p '{"spec":{"adoption":"Adopt"}}'
wait_ready buckety/adopt-pre 120s
assert_provenance adopt-pre Adopted
secret_has_keys adopt-pre-creds endpoint bucket project region accessKeyID secretAccessKey
gcs_object_exists adopt-pre "$endpoint" precious.txt

# Deleting an adopted Buckety retains the backend resource and its
# content even with retentionPolicy=Delete.
log "deleting adopted Bucketys (retentionPolicy=Delete must degrade to Retain)"
kc delete buckety/adopt-pre --wait=true --timeout=90s
resource_absent secret/adopt-pre-creds 30s
gcs_bucket_exists adopt-pre "$endpoint"
gcs_object_exists adopt-pre "$endpoint" precious.txt
kc delete buckety/adopt-void --wait=true --timeout=90s
gcs_bucket_exists adopt-void "$endpoint"

# Created resources still delete normally.
kc delete buckety/adopt-fresh --wait=true --timeout=90s
if gcs_bucket_exists_quiet adopt-fresh "$endpoint"; then
  fail "created bucket adopt-fresh survived retentionPolicy=Delete"
fi

# The retained buckets are this scenario's responsibility.
log "cleaning up retained buckets out-of-band"
gcs_object_delete adopt-pre "$endpoint" precious.txt
gcs_bucket_delete adopt-pre "$endpoint"
gcs_bucket_delete adopt-void "$endpoint"

log "adoption PASS"
