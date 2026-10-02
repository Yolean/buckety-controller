# mysql backend stickiness

SPEC scenario 7 for the mysql driver: `status.backend` is stamped
at first reconcile and sticky. Renaming the backend in the
controller config surfaces `BackendUnavailable` on existing
resources without mutating their stamped backend; deletion with
`retentionPolicy: Delete` blocks while the backend is missing (the
database would be orphaned otherwise) and completes once the
backend is restored.

With mysql the implicit access holds a user of its own, so the
block shows on the access first (`cannot revoke principal ...`,
reason `BackendUnavailable`) and the `Buckety` waits for it. The
scenario deletes `sticky-new` before restoring the original config:
once its backend is unregistered, nothing could revoke its user.
