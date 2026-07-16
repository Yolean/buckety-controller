# gcs naming templates

SPEC scenario 8 for the gcs driver: a `spec.name` template with
`${backend.zone}` + `${namespace}` + `${label[...]}` resolves into
`status.backendResourceName` and is sticky against later label
changes. A template referencing a missing label, and one that
resolves to an illegal GCS bucket name, are rejected at admission.
