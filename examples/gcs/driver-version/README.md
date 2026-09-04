# gcs / driver-version

**Scenario:** SPEC "Driver versioning" for the gcs driver, the
same image rotation as `examples/kadm/driver-version`.

Patch bump auto-applies and advances `status.driverBuildVersion`;
major bump surfaces `DriverVersionIncompatible` and pauses
reconcile with `status.driverMajor` unchanged. The expected
versions derive from what the base image stamps, so the scenario
needs no knowledge of the driver's current version.
