# s3 / driver-version

**Scenario:** SPEC "Driver versioning" for the s3 driver, same
image-rotation shape as `examples/kadm/driver-version` (the
rotation images bump every driver's version).

Patch bump auto-applies and advances `status.driverBuildVersion`;
major bump surfaces `DriverVersionIncompatible` and pauses
reconcile with `status.driverMajor` unchanged.
