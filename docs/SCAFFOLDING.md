# Scaffolding strategy

Companion to `SPEC.md`. Locks in the implementation-side
choices the SPEC explicitly leaves to the maintainer
(SPEC.md §"Open implementation choices"), so the impl branch
can start without a second design round.

## Framework: hand-rolled controller-runtime

`sigs.k8s.io/controller-runtime` directly, no kubebuilder or
operator-sdk scaffolding. Matches y-cluster's lean-toolchain
posture: the binary, the Dockerfile, the kustomize base, and
the CRDs all live in this repo, owned by the people working
on it, and not in a generated `config/` tree the next person
has to learn before they can edit anything.

In practice that means:

- `cmd/buckety/main.go` constructs a `manager.Manager` with
  metrics on `:8080`, healthz/readyz on `:8081`, webhook server
  on `:9443`. Leader election on by default (single replica is
  fine; this stops two Pods from doubling up during a rollout).
- `pkg/controller/buckety/` and `pkg/controller/bucketyaccess/`
  hold the two `Reconciler`s. Each is a thin loop that resolves
  `(backend, driver)` from `pkg/config`, delegates the
  backend-side work to the `Driver` interface, and writes
  status. Drift detection is one re-check per `RequeueAfter`
  (default 5 min, env-tunable).
- `pkg/webhook/` is one `ValidatingWebhook` for both kinds,
  routed by request `kind`. It reads the loaded controller
  config from a package-level `*config.Registry` (set once at
  startup, never mutated; hot-reload is a non-goal).

The `Driver` interface (per SPEC §Driver interface): one
package per driver under `pkg/drivers/<driver>/`, each
implementing `EnsureBuckety`, `DeleteBuckety`, `GrantAccess`,
`RevokeAccess`, `ValidateParameters`, `Version()`. Drivers
register themselves into `pkg/drivers/registry` via a
`func init()`; the binary's compiled-in driver set is whatever
imports cmd/buckety pulls in. Tests instantiate drivers via
the registry, not directly.

## Config loading

`pkg/config.Load(dir)` is a thin wrapper around
`github.com/Yolean/y-cluster/pkg/configfile.Load[Config]`. The
`Config` struct implements `Validate()` (unique backend names,
registered-driver lookup, per-driver required-field check via
`Driver.ValidateConfig`) and `ApplyDefaults()` is the no-op
identity. The lifecycle order from
`y-cluster/pkg/configfile.go:74-92` (Unmarshal → envsubst.Apply
→ SetDir → ApplyDefaults → Validate) is the surface the impl
branch sits on.

Per-driver `config:` blocks decode into the driver-package's
options struct via a two-step: outer YAML decode keeps
`config:` as `json.RawMessage`; the registry's per-driver
`DecodeConfig(json.RawMessage) (DriverInstance, error)`
finishes the decode strictly.

## Driver version stamping

One package-level `var version string` per driver, set via
`-ldflags '-X github.com/Yolean/buckety-controller/pkg/drivers/kadm.version=0.1.0'`.
`contain.yaml` already documents the kadm/s3 build commands;
the driver-version e2e (SPEC scenario #9) builds three images
with different `-X` values and switches the Deployment image
between them. No git-tag-per-test infrastructure required.

`Driver.Version()` returns the package var; the reconciler
stamps `status.driverMajor` (sticky) and
`status.driverBuildVersion` (running) per SPEC §Driver
versioning.

## Webhook TLS

cert-manager. The kustomize base annotates the
`ValidatingWebhookConfiguration` with
`cert-manager.io/inject-ca-from-secret: buckety/buckety-controller-webhook-tls`;
the overlay (or release base) creates a `Certificate` whose
secret is `buckety-controller-webhook-tls`. Self-signed
fallback is not in v1alpha1; the deploy-time guidance is
"cert-manager is a prerequisite". This matches what ystack
already deploys cluster-wide.

## Re-check cadence

Default 5 min `RequeueAfter`. Knob via `--periodic-recheck`
on the controller binary; documented as deploy-time tuneable,
not a contract.

## CI matrix

| Driver | Implementations |
| --- | --- |
| `kadm` | redpanda |
| `s3`   | versitygw, minio |

Per SPEC.md §E2E harness. Adding R2 to CI requires Cloudflare
credentials (R2-specific scenario today asserts admission only;
real-R2 provisioning lives in a manual recipe under
`examples/s3/r2/manual/` once a follow-up adds it).

## What lives where

```
cmd/buckety/                 main: flags, manager wiring
pkg/config/                  controller-config loading (configfile wrapper)
pkg/controller/buckety/      Buckety reconciler
pkg/controller/bucketyaccess/ BucketyAccess reconciler
pkg/webhook/                 single validating webhook, both kinds
pkg/drivers/registry/        driver name -> factory map
pkg/drivers/kadm/            kadm driver impl + schema/
pkg/drivers/s3/              s3 driver impl + schema/
pkg/template/                spec.name template resolver
deploy/kustomize/base/       CRDs, RBAC, Deployment, Service, webhook
deploy/kustomize/release/    generated at tag time; vendored downstream
test/e2e/                    harness (one script, both callers)
examples/                    runnable scenario YAML + assert.sh
docs/                        this file
```

## Non-goals on this branch

`initial-impl` (the next branch) writes the Go code that
makes the e2e suite green. Nothing in this scaffolding doc
forecloses on the operator-SDK choice being revisited later
if controller-runtime's API drifts; the surface area we
expose to drivers and to consumers is the contract, the
framework underneath is not.
