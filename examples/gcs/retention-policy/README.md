# gcs retention policy

SPEC scenario 6 for the gcs driver: deleting a `Buckety` with
`retentionPolicy: Retain` leaves the bucket on the backend;
deleting one with `Delete` removes it.
