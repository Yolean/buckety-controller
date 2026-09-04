# kadm / driver-version

**Scenario:** SPEC.md §End-to-end coverage #9 — Driver version
compatibility.

`status.driverMajor` is stamped at first reconcile and is
sticky. Patch and minor bumps update `status.driverBuildVersion`
in place and proceed. Major bumps surface
`DriverVersionIncompatible` and pause reconcile until the
maintainer pins a compatible binary or migrates the resource.

**Demonstrates:**

- Apply a Buckety against a controller built with driver
  version `X.Y.Z`. `status.driverMajor` is stamped to `X`;
  `status.driverBuildVersion` is `X.Y.Z`.
- Rotate to a controller built with `X.Y.Z+1` (patch bump):
  auto-applied; `status.driverBuildVersion` advances;
  `status.driverMajor` unchanged; `Ready=True`.
- Rotate to a controller built with `(X+1).0.0` (major bump):
  `DriverVersionIncompatible=True`; reconcile pauses;
  `status.driverMajor` unchanged.
- Rotate back to the original binary; the resource resumes
  reconciliation.

**Harness requirements:**

The harness must build (or sideload) three controller images:
the base, and for every driver at version `X.Y.Z` a patch bump
`X.Y.Z+1` and a major bump `(X+1).0.0`
(`test/e2e/build-rotation-images.sh` derives these from
`buckety --version`). The image refs come in via
`E2E_IMAGE_BASE`, `E2E_IMAGE_PATCH`, `E2E_IMAGE_MAJOR`; the
scenario derives its expected versions from the
`status.driverBuildVersion` the base image stamps, so nothing in
the harness needs to know a driver's current version.

**Assertions** (`assert.sh`):

1. After initial apply: `status.driverMajor` matches the
   major of `status.driverBuildVersion`.
2. After patch-rotate: `status.driverBuildVersion` is the base
   with its patch number incremented; `driverMajor` unchanged;
   Ready=True.
3. After major-rotate: `DriverVersionIncompatible=True`;
   `driverMajor` unchanged; Ready=False.
4. After restore: Ready=True again.
