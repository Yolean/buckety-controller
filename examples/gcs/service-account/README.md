# gcs per-bucket service account (opt-in, driver >= 0.2)

A `Buckety` that declares `parameters.serviceAccount` gets a
dedicated GCP service account with `roles/storage.objectAdmin` on
its bucket only, and every access Secret additionally carries:

| key | value |
| --- | --- |
| `serviceAccountKey` | SA key JSON — mount it and point `GOOGLE_APPLICATION_CREDENTIALS` at it for OAuth2 bearer-token auth with native GCS clients and V4 signed URLs |
| `serviceAccountEmail` | `orders-<namespace>@<identity-project>.iam.gserviceaccount.com` |
| `serviceAccountKeyId` | the key's `private_key_id`, for audit and rotation tooling |

The static HMAC pair stays in the Secret unchanged (additive keys
per SPEC §Secret output), so S3-interop consumers keep working;
the SA credential is what shrinks blast radius from
"every bucket on the backend" to "this bucket". Once no consumer
of a bucket needs the S3-interop path, add `hmac: "false"`
(mutable) and its Secrets drop the backend-wide pair entirely -
per CR, or as a backend parameter default to make HMAC opt-in
across the backend. A CR can conversely opt out of a
backend-default SA with `serviceAccount: ""`.

## Backend prerequisites

This example has no `assert.sh` deliberately: fake-gcs-server
implements neither `iam.googleapis.com` nor bucket IAM policies,
so the feature is unit-tested (`pkg/drivers/gcs/serviceaccount_test.go`)
and exercised against real GCS. The backend needs:

```yaml
backends:
- name: gcs
  driver: gcs
  config:
    project: my-bucket-project
    accessKeyID: ${GCS_HMAC_ID}
    secretAccessKey: ${GCS_HMAC_SECRET}
    serviceAccounts:
      # STRONGLY RECOMMENDED: a dedicated identity project. Key
      # creation equals impersonation, so the controller's IAM
      # grants must be confined to a project whose only identities
      # are the ones buckety mints. Cross-project bucket bindings
      # make the split free.
      project: my-buckety-identities
  # Or impose the naming convention for all buckets, letting CRs
  # omit the parameter:
  # parameters:
  #   serviceAccount: ${name}-${namespace}
```

Controller credential grants:

- on the identity project: a custom role with
  `iam.serviceAccounts.{create,get,delete}` and
  `iam.serviceAccountKeys.{create,list,delete}` (the predefined
  `roles/iam.serviceAccountAdmin` + `roles/iam.serviceAccountKeyAdmin`
  work but carry more than needed)
- on the bucket project: `storage.buckets.getIamPolicy` +
  `storage.buckets.setIamPolicy` on top of the existing bucket
  CRUD grant

Orgs that set `constraints/iam.disableServiceAccountKeyCreation`
block this feature by design; the grant fails with an actionable
`GrantFailed` condition.

## Rotation (manual, until scheduled rotation lands)

```
kubectl -n <namespace> delete secret <credentialsSecretName>
```

The controller reconciles the `BucketyAccess` on the Secret's
deletion: it mints a fresh key into a new Secret and then revokes
the previous key, recording a `PrincipalReplaced` event. Pods that
loaded the key once (a mounted file read at boot) keep using the
revoked one until restarted, so restart them after the new Secret
appears.

Deleting the key server-side instead (`gcloud iam service-accounts
keys delete`) does NOT trigger a re-mint: the controller cannot
tell a deleted key from one that `keys.list` has not caught up
with yet, and it never replaces a key on missing evidence. Delete
the Secret afterwards to re-mint.

Deleting the `BucketyAccess` revokes its key (`status.principal`
is the key's resource name); deleting the `Buckety` with
`retentionPolicy=Delete` removes the service account with the
bucket.
