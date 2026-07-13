# s3 / driver-version

**Scenario:** SPEC "Driver versioning" for the s3 driver, same
image-rotation shape as `examples/kadm/driver-version` (the CI
rotation images inject both drivers' version vars).

Patch bump auto-applies and advances `status.driverBuildVersion`;
major bump surfaces `DriverVersionIncompatible` and pauses
reconcile with `status.driverMajor` unchanged.
