# gcs backend stickiness

SPEC scenario 7 for the gcs driver: `status.backend` is stamped at
first reconcile and sticky. Renaming the backend in the controller
config surfaces `BackendUnavailable` on existing resources without
mutating their stamped backend; deletion with
`retentionPolicy: Delete` blocks while the backend is missing (the
bucket would be orphaned otherwise) and completes once the backend
is restored.
