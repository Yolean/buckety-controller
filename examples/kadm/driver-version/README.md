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

The harness must build (or sideload) three versions of the
controller image, stamped via `-ldflags '-X
main.driverVersion=...'` per `contain.yaml`. The version
strings to use are conveyed to this scenario via the
`E2E_VERSION_BASE`, `E2E_VERSION_PATCH`, and
`E2E_VERSION_MAJOR` env vars (e.g. `0.1.0`, `0.1.1`, `1.0.0`).
The image refs that contain those versions come in via
`E2E_IMAGE_BASE`, `E2E_IMAGE_PATCH`, `E2E_IMAGE_MAJOR`.

**Assertions** (`assert.sh`):

1. After initial apply: `status.driverMajor` matches the
   major of `E2E_VERSION_BASE`.
2. After patch-rotate: `status.driverBuildVersion` ==
   `E2E_VERSION_PATCH`; `driverMajor` unchanged; Ready=True.
3. After major-rotate: `DriverVersionIncompatible=True`;
   `driverMajor` unchanged; Ready=False.
4. After restore: Ready=True again.
