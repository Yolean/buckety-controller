# gcs parameter mutation

SPEC scenario 3 for the gcs driver:

- A mutable parameter change (`versioning` true -> false) is
  reconciled to the backend in place.
- An unknown parameter is rejected at admission.
- The immutable `location` parameter is rejected at admission on
  change.

`lifecycle` and `uniformBucketLevelAccess` follow the same
mutable-parameter path in the driver but are asserted in unit
tests only: fake-gcs-server does not faithfully round-trip those
bucket attributes, and asserting against a lossy emulator would
test the emulator, not the driver.
