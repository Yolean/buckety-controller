#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:-$(cd "$(dirname "$0")/../../../test/e2e" && pwd)}/lib.sh"
here="$(cd "$(dirname "$0")" && pwd)"

# Fresh name first: proves provenance=Created, and its Secret
# supplies the backend root credentials (v1alpha1 copies them into
# every Secret) for the out-of-band pre-creation below.
kc apply -f "$here/fresh.yaml"
wait_ready buckety/adopt-fresh 120s
assert_provenance adopt-fresh Created
endpoint="$(secret_value adopt-fresh-creds endpoint)"
access="$(secret_value adopt-fresh-creds accessKeyID)"
secret="$(secret_value adopt-fresh-creds secretAccessKey)"

log "pre-creating bucket adopt-pre (with content) and adopt-void (empty)"
s3_api "$endpoint" "$access" "$secret" create-bucket --bucket adopt-pre >/dev/null \
  || fail "out-of-band create-bucket adopt-pre failed"
s3_api "$endpoint" "$access" "$secret" put-object --bucket adopt-pre --key precious.txt >/dev/null \
  || fail "out-of-band put-object failed"
s3_api "$endpoint" "$access" "$secret" create-bucket --bucket adopt-void >/dev/null \
  || fail "out-of-band create-bucket adopt-void failed"

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
secret_has_keys adopt-pre-creds endpoint bucket accessKeyID secretAccessKey
s3_object_exists adopt-pre precious.txt "$endpoint" "$access" "$secret"

# Deleting an adopted Buckety retains the backend resource and its
# content even with retentionPolicy=Delete.
log "deleting adopted Bucketys (retentionPolicy=Delete must degrade to Retain)"
kc delete buckety/adopt-pre --wait=true --timeout=90s
resource_absent secret/adopt-pre-creds 30s
s3_bucket_exists adopt-pre "$endpoint" "$access" "$secret"
s3_object_exists adopt-pre precious.txt "$endpoint" "$access" "$secret"
kc delete buckety/adopt-void --wait=true --timeout=90s
s3_bucket_exists adopt-void "$endpoint" "$access" "$secret"

# Created resources still delete normally.
kc delete buckety/adopt-fresh --wait=true --timeout=90s
if s3_api "$endpoint" "$access" "$secret" head-bucket --bucket adopt-fresh >/dev/null; then
  fail "created bucket adopt-fresh survived retentionPolicy=Delete"
fi

# The retained buckets are this scenario's responsibility.
log "cleaning up retained buckets out-of-band"
s3_api "$endpoint" "$access" "$secret" delete-object --bucket adopt-pre --key precious.txt >/dev/null || true
s3_api "$endpoint" "$access" "$secret" delete-bucket --bucket adopt-pre >/dev/null \
  || fail "cleanup of adopt-pre failed"
s3_api "$endpoint" "$access" "$secret" delete-bucket --bucket adopt-void >/dev/null \
  || fail "cleanup of adopt-void failed"

log "adoption PASS"
