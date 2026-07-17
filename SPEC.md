# Buckety controller — design specification (v1alpha1)

This document is the contract for the appointed maintainer of the
buckety-controller. It freezes the API shape, the driver model,
and the non-negotiable runtime properties. Implementation choices
not fixed here (operator SDK, reconciler queueing, periodic
re-check cadence, build pipeline within the standard `contain` +
ghcr.io shape) are the maintainer's to make.

The audience-facing companion is `README.md`, which documents the
v1alpha1 contract from a cluster maintainer's perspective. Where
the two disagree, this file wins until both are revised together.

## Context

Buckety provisions named, individually-configurable resources on
backing services (Kafka topics, S3 buckets, future: MySQL
databases) and mints per-consumer credentials.

The design is informed by a Yolean exploration of the Container
Object Storage Interface (COSI). COSI's two-resource split
(compartment + credential binding) and its data-path-independence
property are both inherited here. Four COSI properties were
disqualifying for our needs and are explicitly fixed:

1. COSI uses opaque backend names derived from `BucketClaim`
   UIDs. Both v1alpha1 and the v1alpha2 currently on `main`
   produce the storage-side identity as `bc-<UID>`; there is no
   mechanism to make the actual Kafka topic name or S3 bucket
   name match a friendly, operator-chosen identity. Buckety lets
   the operator choose, with optional templating for namespace
   / region / generation substitution (see *Naming templates*
   below).
2. COSI's `BucketClaim` carries no per-claim parameters —
   `bucketClassName`, `protocols`, `existingBucketName` and
   nothing else. Every claim of the same class gets identical
   config. Buckety puts parameters on the per-resource Buckety,
   not on a shared class.
3. COSI's `BucketClass` spec is immutable in v1alpha2;
   reconfiguring a class without recreating all its bound buckets
   is impossible. Buckety's per-resource parameters are mutable,
   and drift is reconciled to the backend by the controller.
4. COSI emits a single `BucketInfo` JSON-blob Secret — every
   consumer needs an init-container shim to parse it. Buckety
   emits flat, `secretKeyRef`-friendly keys per driver.

## API group and kinds

- Group: `buckety.yolean.se`
- Version: `v1alpha1`
- Kinds: `Buckety`, `BucketyAccess` (both namespaced)

Cross-namespace `bucketyRef` is out of scope for v1alpha1.

## Backends and drivers

A **backend** is a named connection to a backing-service instance,
defined by the cluster maintainer in the controller's config file.
A **driver** is the client library that the backend uses to talk
to that service. A `Buckety` resource selects a backend by name;
the backend's definition determines the driver and how it's
configured.

This is the COSI `BucketClass` slot done as plain controller
config rather than as a cluster-scoped CRD: maintainers don't
commit a `BucketClass` per knob, and consumers don't reason about
classes — they pick the named thing their cluster offers.

### Drivers in v1alpha1

| Driver name | Backing services | Library | e2e |
| --- | --- | --- | --- |
| `kadm` | Kafka-protocol topics (Redpanda, Apache Kafka, Confluent) | `github.com/twmb/franz-go/pkg/kadm` | Redpanda |
| `s3` | S3 API (VersityGW, MinIO, AWS S3, R2, Hetzner, GCS interop) | `github.com/aws/aws-sdk-go-v2/service/s3` | VersityGW + MinIO |
| `gcs` | Google Cloud Storage, provisioned via the native JSON API | `cloud.google.com/go/storage` | fake-gcs-server |

Driver names are short, semantic, and stable; they MUST NOT
change after v1alpha1 ships. New drivers — `mysql` via
`go-sql-driver/mysql`, `minio-admin` for scoped MinIO IAM,
`versitygw-iam` for scoped VersityGW IAM — are follow-ups, not
v1alpha1. The `gcs` driver shipped as the first such follow-up
(same v1alpha1 CRDs, additive driver registration).

The `gcs` / `s3` boundary: the s3 driver's client-library bet
covers GCS on the DATA path (HMAC keys work with SigV4 against
`storage.googleapis.com`), but provisioning against GCS is not
S3-compatible — bucket creation needs the project, and the
native parameters (location, uniform bucket-level access,
versioning, lifecycle) have no S3-interop equivalent. The gcs
driver provisions through the native JSON API and mints Secrets
whose keys are S3-protocol compatible, so consumers are unchanged
whichever driver provisioned their bucket.

The single `s3` driver covers all S3-compatible backends. e2e on
VersityGW and MinIO is the warrant that workloads aimed at AWS S3,
R2, Hetzner, or GCS-interop will behave the same — the bet is that
client-library compatibility is the right abstraction layer and
that we won't need a second S3 driver. Per-consumer credential
minting (separate IAM users with scoped policies) is NOT in
v1alpha1 — all `BucketyAccess` instances for the same `Buckety`
receive identical credentials, sourced from the backend's
configured root keys. This is the same posture as the Kafka
driver's auth-mode=none for v1alpha1, and a known v1alpha2 task.

### Controller config file

Loaded via `github.com/Yolean/y-cluster/pkg/configfile.Load[T]`,
which is exported and importable from the y-cluster Go module.
The controller takes `-c <dir>` matching the y-cluster CLI
convention; the directory holds a single YAML file. Suggested
filename: `buckety-controller.yaml`.

The y-cluster load primitive provides:

- **Strict YAML decode** (`sigs.k8s.io/yaml.UnmarshalStrict`).
  Unknown keys are an immediate error.
- **Opt-in `${VAR}` interpolation** via the `envsubst:"true"`
  struct tag, provided by `github.com/Yolean/y-cluster/pkg/envsubst`.
  Fields not tagged that contain `${...}` are rejected at load —
  this is y-cluster's forward-compatibility guard. Supported
  forms: `${VAR}` (required, errors if unset) and
  `${VAR:-default}` (optional). `$$` escapes a literal `$`. Keys
  are never substituted.
- **Lifecycle hooks** `SetDir(abs)`, `ApplyDefaults()`,
  `Validate()`. Invoked in that order, after unmarshal and
  envsubst. The controller config type implements `Validate()`
  to enforce unique backend names, registered-driver lookup,
  and per-driver required fields. This ordering mirrors
  `y-cluster/pkg/configfile.Load`; the impl is trivially
  auditable against the upstream source.

Example:

```yaml
# buckety-controller.yaml
backends:

- name: cluster-kafka
  driver: kadm
  # `config:` is decoded into the kadm driver's options struct.
  # The schema for this block is generated from the kadm wrapper
  # types and published at a stable GitHub raw URL (see Schemas).
  config:
    seedBrokers:
    - y-bootstrap.kafka.svc.cluster.local:9092
    clientID: buckety-controller
  # Optional per-backend defaults map. Resolves `${backend.X}`
  # substitutions in `Buckety.spec.name` templates; see Naming
  # templates.
  defaults:
    zone: local

- name: cluster-objects
  driver: s3
  config:
    endpoint: http://y-s3-api.blobs.svc.cluster.local:9000
    region: us-east-1
    forcePathStyle: true
    # envsubst:"true" tagged in the s3 driver's config struct;
    # populated by the controller Deployment from a K8s Secret.
    accessKeyID:     ${VERSITYGW_ROOT_ACCESSKEY}
    secretAccessKey: ${VERSITYGW_ROOT_SECRETKEY}
  defaults:
    zone: local
  # Optional per-backend parameter defaults, merged under each
  # Buckety's spec.parameters (the CR wins per key). Keeps
  # driver-specific parameters out of portable CRs; see Driver
  # families. Validated against the driver at startup.
  parameters:
    versioning: "true"
```

Credentials are sourced from env vars the controller Deployment
populates from K8s Secrets — same pattern as y-cluster's
`registries.yaml` and its GCP auth-token field. Rotation requires
re-rolling the controller Pod; runtime credential rotation is a
non-goal in v1alpha1.

## `Buckety` shape

```yaml
apiVersion: buckety.yolean.se/v1alpha1
kind: Buckety
metadata:
  name: orders
  namespace: tenant1
  labels:
    yolean.se/generation: "003"   # source for ${label[...]} substitution
spec:
  # Required. Names a backend defined in the controller's config
  # file. Immutable after creation.
  backend: cluster-kafka

  # Optional. Template resolved to the backend identity at first
  # reconcile. Defaults to metadata.name (literal, no template).
  # Frozen in status.backendResourceName once resolved. See "Naming
  # templates" for the substitution grammar.
  name: "${namespace}.${name}.v${label['yolean.se/generation']}"

  # Optional. Driver-validated parameters. Schema is per-driver
  # (resolved via spec.backend -> driver). Mutable; controller
  # reconciles drift to the backend.
  parameters:
    partitions: "12"
    config.retention.ms: "604800000"
    config.cleanup.policy: "delete"

  # What happens when this Buckety is deleted. Default: Retain.
  retentionPolicy: Retain     # Retain | Delete

  # What happens when the resolved backend resource already exists
  # at first reconcile. Default: AdoptEmpty (adopt only when the
  # resource holds no content). See "Adoption".
  adoption: AdoptEmpty        # AdoptEmpty | Adopt

  # Optional. Single-consumer shortcut. The controller materialises
  # an implicit BucketyAccess of the same name in the same
  # namespace. See "Implicit access" below for lifecycle and
  # migration semantics.
  defaultAccess:
    role: ReadWrite
    credentialsSecretName: orders-topic

status:
  conditions: [...]
  backend: cluster-kafka            # sticky: resolved at first reconcile
  driver: kadm                      # sticky: driver of that backend
  driverMajor: 0                    # sticky: gates compatibility; see Driver versioning
  driverBuildVersion: "0.1.0"       # informational: full SemVer of the binary that last reconciled
  backendResourceName: tenant1.orders.v003  # sticky: resolved name template
  provenance: Created               # sticky: Created | Adopted; see Adoption
  observedGeneration: 3
```

`status.backend`, `status.driver`, `status.driverMajor`, and
`status.backendResourceName` are stamped on the first successful
reconcile and never changed. `status.driverBuildVersion` tracks
the currently running driver's full SemVer and updates on every
compatible reconcile; it is the field dashboards and
`kubectl describe` should read. Compatibility decisions read
`status.driverMajor`. If the maintainer renames or removes a
backend after consumers exist, the resources continue to be
reconciled against the original `(backend, driver, driverMajor)`
triple as long as a backend with that name, driver, and a
backward-compatible driver major still exists. If any element
is missing, the resource surfaces a `BackendUnavailable` or
`DriverVersionIncompatible` condition (see *Driver versioning*)
and reconciliation pauses until the maintainer either restores
compatibility or migrates the resource (manual; no
auto-migration in v1alpha1).

## `BucketyAccess` shape

```yaml
apiVersion: buckety.yolean.se/v1alpha1
kind: BucketyAccess
metadata:
  name: orders-reader
  namespace: tenant1
spec:
  bucketyRef:
    name: orders
  credentialsSecretName: orders-reader
  role: ReadOnly
  parameters:
    consumerGroupPrefix: "tenant1-orders-reporting-"

status:
  conditions: [...]
  principal: "user-tenant1-orders-reader-7a91"
  observedGeneration: 1
```

Each `BucketyAccess` mints exactly one Secret. Multiple
`BucketyAccess` instances can reference the same `Buckety`; in
v1alpha1 (no per-consumer scoping), they all receive identical
credentials drawn from the backend's root config. The CRD shape
already supports per-access scoping for v1alpha2 — drivers are
expected to ignore role/parameter values they don't yet
implement and surface a `ScopingNotImplemented` condition rather
than silently treating ReadOnly as ReadWrite.

## Implicit access (`defaultAccess`)

When `spec.defaultAccess` is set on a `Buckety`, the controller
materialises an implicit `BucketyAccess` of the same name in the
same namespace, labelled `buckety.yolean.se/implicit=true` with
an owner-ref to the `Buckety`. It is a real `BucketyAccess` for
all reconciler purposes: appearing in
`kubectl get bucketyaccess`, minting its Secret, contributing
to `Buckety` deletion blocking. The label and owner-ref govern
reclamation.

Once any explicit `BucketyAccess` targets the same `Buckety`, the
controller deletes the implicit one on the next reconcile. The
implicit Secret is garbage-collected via the owner-ref. Removing
`spec.defaultAccess` from the `Buckety` while no explicit
accesses exist has the same effect.

If an explicit `BucketyAccess` is authored with the same
`credentialsSecretName` the implicit one was using, consumers
see a brief gap as the implicit Secret is GC'd before the
explicit one materialises. Most migrations go from single
consumer to multiple with different roles and pick fresh Secret
names, so this is a corner case rather than the common path.
For zero-gap migration, remove `defaultAccess` in one apply,
wait for the implicit Secret to be reclaimed, then add the
explicit `BucketyAccess`.

## Naming templates

`spec.name` is a template, resolved once at first reconcile and
frozen in `status.backendResourceName`. When omitted, it defaults to the
literal `${name}` — i.e. `metadata.name` of the Buckety, with no
substitution.

The substitution grammar is intentionally close to the Kubernetes
Downward API surface so the mental model is shared:

| Variable | Source |
| --- | --- |
| `${name}` | `metadata.name` of the Buckety |
| `${namespace}` | `metadata.namespace` of the Buckety |
| `${label['x.example.net/my-label']}` | `metadata.labels[key]`; bracket syntax supports the full K8s label-key shape (including a `/` between prefix and name) |
| `${backend.X}` | `defaults.X` from the named backend's entry in the controller config (`zone`, `region`, free-form). Used for substitutions the cluster operator owns, not the tenant. |

Rules:

- Resolution is single-pass; substituted values are not
  re-scanned.
- Missing substitution source is an admission-time error (the
  controller's webhook rejects the resource). The pod will not be
  stamped with a half-resolved name.
- Zero-padding is the tenant's responsibility — author the
  padded form in the label value (`yolean.se/generation: "003"`).
  The driver does not pad.
- The resolved name must satisfy the driver's backend-name rules.
  Kafka topic-name validation, S3 bucket-name validation, etc.
  applies after substitution. A template that resolves to an
  invalid backend name is rejected at admission.

Each backend carries its own `defaults` map because a single
cluster can sit in front of backing services in different zones
(a Cloudflare R2 backend in `eu` alongside an in-cluster Kafka),
so `${backend.zone}` must reflect whichever backend a given
`Buckety` resolves to. Drivers do not inspect `defaults`; the
templating layer above does.

## Driver versioning

Each driver carries a SemVer (`major.minor.patch`) advertised by
the compiled binary (typically injected at build time via
`-ldflags '-X main.driverVersion=...'`; see *Build and
distribution*). At first reconcile the resource's
`status.driverMajor` is stamped from this version and is
**sticky** thereafter, exactly like `status.backend`. The full
running version is mirrored to `status.driverBuildVersion` on
every compatible reconcile. Compatibility decisions read
`status.driverMajor`; dashboards and operators read
`status.driverBuildVersion`.

Compatibility rules:

- **Patch** bumps (`0.1.0` → `0.1.1`) MUST be behaviour-preserving
  bug fixes. Auto-applied to existing resources on controller
  upgrade.
- **Minor** bumps (`0.1.x` → `0.2.0`) MAY add new optional
  parameters and new Secret keys (additively). MUST NOT remove
  or rename existing parameters or Secret keys. Auto-applied.
- **Major** bumps (`0.x` → `1.0.0`) are reserved for breaking
  changes (renamed/removed parameters, changed Secret key
  semantics, changed admission rules). NOT auto-applied;
  resources whose `status.driverMajor` no longer matches the
  running driver surface `DriverVersionIncompatible` until the
  maintainer either pins the binary to a compatible version or
  migrates the resource.

Schema URLs include the driver SemVer so consumers can pin
against a specific version:

```text
pkg/drivers/kadm/schema/v0.1/config.schema.json
pkg/drivers/kadm/schema/v0.1/parameters.schema.json
```

Major bumps rotate the URL path (`v0.1/` → `v1.0/`); minor and
patch bumps update the schema content in place at the same URL.
Consumers that want the absolute strictest pin can reference a
schema at a specific tagged commit instead of a branch.

The driver-version metadata flow:

1. Controller binary registers each driver with its compiled-in
   version (`major.minor.patch`).
2. On first reconcile of a resource, the resolved driver's
   current major is stamped into `status.driverMajor`; the full
   version is written to `status.driverBuildVersion`.
3. On subsequent reconciles, the running driver's major is
   compared to `status.driverMajor`. A mismatch surfaces
   `DriverVersionIncompatible` and skips the reconcile pass. A
   match updates `status.driverBuildVersion` to the running
   version and proceeds.

## Parameters: ownership, validation, defaults

`spec.parameters` is an opaque `map[string]string` at the CRD
level (`x-kubernetes-preserve-unknown-fields: true`). Validation
is per-driver via an admission webhook the controller runs.

Two categories of parameter keys:

1. **Driver-known keys.** Documented per driver. The driver
   declares which are *required* and which are *immutable*.
   Required-missing fails admission; immutable-changed fails
   admission on update.
2. **Pass-through `config.*` keys.** For drivers whose backend
   has its own config namespace (Kafka topic configs, S3
   bucket-policy fields). Passed through unmodified. The backend's
   own validation catches typos; the driver SHOULD document which
   `config.*` keys are immutable on the backend side and surface
   `ParameterDrift` conditions where it can't reconcile in place.

Naming conventions per driver:

- **Prefer the backend's own names.** If Kafka calls a thing
  `cleanup.policy`, the parameter key is `config.cleanup.policy`,
  not `cleanupPolicy` or `cleanup_policy`. Same posture for S3.
- **Avoid overlap and ambiguity.** Driver-known keys and
  `config.*` keys MUST NOT semantically duplicate (e.g. don't
  add a driver-known `retentionMs` if `config.retention.ms`
  exists). If the backend gains a new config key that would
  overlap with an existing driver-known key, the driver fails
  admission with a clear message rather than silently choosing
  precedence.

**Drivers MUST NOT have internal defaults.** A driver-side
default that ships in version N and changes in N+1 would silently
mutate every existing resource on operator upgrade. Omitted
parameters mean "pass nothing for this knob" — the backing
service's own defaults apply. Kafka's `replicationFactor` omitted
→ `-1` on the CreateTopics request → broker honours
`default.replication.factor`. S3's `region` omitted → backend
default.

### Implementation-specific capabilities

Some backend services expose features that aren't part of the
common shape. Cloudflare R2's data jurisdiction (`eu` for
EU-resident data, default global) is the Yolean must-have
example. The `s3` driver accepts a small set of
**capability-gated parameters** that are allowed only when the
backend's implementation supports them.

To enable capability gating, the backend's `config:` block
carries an optional `implementation:` discriminator:

```yaml
backends:
- name: r2-eu
  driver: s3
  config:
    implementation: r2
    endpoint: https://<account>.r2.cloudflarestorage.com
    region: auto
    accessKeyID:     ${R2_ACCESS_KEY_ID}
    secretAccessKey: ${R2_SECRET_ACCESS_KEY}
```

Known implementations for the `s3` driver: `aws`, `r2`, `minio`,
`versitygw`. Omitting `implementation:` is allowed and means
"no capability gating": only the common-shape parameters work,
and any capability-gated parameter on a Buckety against that
backend is rejected at admission with a clear message.

Capability-gated parameters in v1alpha1:

| Parameter | Allowed implementations | Semantics |
| --- | --- | --- |
| `jurisdiction` | `r2` | Maps to Cloudflare R2's jurisdiction (`eu` for EU-resident data; omit for the global default). Set at bucket creation, immutable post-create. Rejected at admission for any other implementation. |

A `Buckety` against `r2-eu` with `parameters.jurisdiction: "eu"`
is honoured. The same Buckety pointed at a `versitygw`,
`minio`, `aws`, or unmarked backend is rejected at admission.

Common-shape parameters (e.g. `region`) MUST NOT be
capability-gated — they either work everywhere the driver
claims support or they don't belong in the driver at all.
Adding a new capability-gated parameter is a minor driver
version bump (per *Driver versioning*); the mechanism is the
extension point for future per-backend features (MinIO ILM
rules, AWS Object Lock retention, etc.).

### Driver families

Drivers that provision the same kind of service form a family
and share parameter definitions, so one Buckety is portable
across backends: the CR names a use-case backend (say
`site-userdata-blobs`), carries only family-common parameters,
and provisions whether the cluster's backend entry is `gcs` or
`s3`. Families are per service kind; `kadm` shares nothing with
the bucket drivers and is deliberately not in a family.

The object-store family (`gcs`, `s3`) is defined in
`pkg/drivers/objectstore`. Its family-common parameters:

| Parameter | Semantics |
| --- | --- |
| `versioning` | `"true"` or `"false"`. Mutable, reconciled in place. |
| `lifecycle` | JSON document in the `gsutil lifecycle set` shape. Portable CRs stick to the portable subset: action `Delete` or `AbortIncompleteMultipartUpload`, condition `age` (required) plus at most one `matchesPrefix`. The gcs driver accepts the full GCS condition set beyond the subset; the s3 driver rejects anything outside it at admission, because silently dropping a condition would delete more than the operator asked. |

Three rules make portability hold:

1. **Family-common parameters are accepted by every driver in
   the family.** Rejecting one at admission is a driver bug, not
   a capability statement.
2. **Missing backend capability fails SAFE.** A backend that
   answers NotImplemented for a family parameter (versitygw has
   no versioning on its posix backend) is skipped: the bucket
   still provisions and turns Ready, and nothing is deleted that
   the operator did not ask deleted. Capability shortfalls are
   deployment knowledge, not CR knowledge.
3. **Driver-specific parameters live in the backend config, not
   the CR.** Each backend entry takes an optional `parameters:`
   map of per-cluster defaults (GCS `location`,
   `uniformBucketLevelAccess`, `softDeleteRetentionSeconds`; R2
   `jurisdiction`), merged under `spec.parameters` with the CR
   winning per key. The merged result is what admission
   validates and the driver receives, so immutability checks
   (e.g. `location`) apply to backend defaults exactly as to CR
   values. Backend defaults are validated against the driver at
   startup.

Backend `parameters:` defaults do not violate the "drivers MUST
NOT have internal defaults" rule above: they are operator-owned
cluster config, explicit and versioned with the deployment, not
code that changes meaning on operator upgrade.

The `examples/*/portable-blobs-cr/` scenario keeps the family
honest in CI: a byte-identical CR (enforced by the asserts) is
applied against minio, versitygw and the GCS emulator.

Subfamily hierarchies (definitions shared by a subset of a
family) are deferred until a third bucket driver forces the
question; today the family is flat and small.

## Schemas

Two schemas per driver, generated from Go types using y-cluster's
existing conventions (the same toolchain that produces
`pkg/provision/schema/*.schema.json` and the
`# yaml-language-server: $schema=...` annotations on the
y-cluster provision yamls):

1. **Backend config schema.** Describes the `config:` block
   under each entry in `buckety-controller.yaml`.
2. **Buckety parameters schema.** Describes `spec.parameters` for
   resources whose backend resolves to this driver. The admission
   webhook validates against it on apply.

Each schema lives in the operator repo under
`pkg/drivers/<driver>/schema/<major.minor>/*.schema.json` and is
published via the GitHub raw URL (this is the publication
mechanism — there's no separate registry). Versioning is the
driver SemVer (see *Driver versioning*). Example YAMLs (in this
repo and in consumer ystack) carry
`# yaml-language-server: $schema=...` annotations pointing at
the raw URL so editors validate before kubectl ever sees the
file.

## Mutability

| Field | Mutable? |
| --- | --- |
| `Buckety.spec.backend` | no |
| `Buckety.spec.name` | no |
| `Buckety.spec.retentionPolicy` | yes |
| `Buckety.spec.parameters` | yes, per-key per driver |
| `Buckety.spec.defaultAccess` | yes |
| `BucketyAccess.spec.bucketyRef` | no |
| `BucketyAccess.spec.credentialsSecretName` | no |
| `BucketyAccess.spec.role` | yes |
| `BucketyAccess.spec.parameters` | yes, per-key per driver |

Per-key immutability inside `parameters` is declared by each
driver's schema (CEL `XValidation` rules).

## Driver interface

The semantic contract — exact Go shapes are the maintainer's
call, guided by the chosen operator SDK.

- `EnsureBuckety` — idempotent create-or-update against the
  backend; reconciles drift on every call; returns the
  backend-side name and any driver-specific status hints.
- `DeleteBuckety` — drops the backend resource and its contents
  (see *`Delete` is recursive*), idempotent on NotFound; called
  only when `retentionPolicy == Delete`.
- `GrantAccess` — mints credentials, applies backend-side
  permissions (no-op in v1alpha1 for both shipped drivers),
  returns the Secret payload as a flat `map[string][]byte`.
  Idempotent for the same `(Buckety, BucketyAccess)` pair.
- `RevokeAccess` — removes the backend-side principal (no-op in
  v1alpha1), idempotent on NotFound.
- `ValidateParameters` — used by the admission webhook; returns
  a per-key error map.
- `Version()` — returns the driver's SemVer for
  `status.driverVersion` stamping.
- Driver also exposes its OpenAPI schemas separately (see
  Schemas).

Drivers MUST NOT have internal defaults. Drivers MUST declare
which `spec.parameters` keys are required, immutable, and
reconcilable-in-place.

## Reconciliation and drift

Reconciliation triggers, queueing strategy, and re-check cadence
are the maintainer's choice via the operator SDK. What's
specified:

- Reconciliation MUST detect drift introduced out-of-band (e.g.
  someone ran `rpk alter-config` directly). Where the backend
  supports it, drift is silently reapplied; where it can't be
  (Kafka partition count can grow but not shrink), the driver
  surfaces `ParameterDrift` and waits for human resolution.
- `status.observedGeneration` advances when `metadata.generation`
  has been fully reconciled, including downstream Secret state
  (not just the backend resource). Until then,
  `status.observedGeneration` lags `metadata.generation` and
  `Ready=False`.
- The controller MUST be honest in `status.conditions`:
  `Ready=False` until both the backend resource AND any
  access-side Secrets are in sync.

## Off the data path

The operator is required at provision, reconfigure, and revoke.
It is **not** required for workload data operations: once a
Secret exists, workloads connect to the backend directly.
Scaling the operator to zero MUST NOT affect running consumers.
This property was proven for the COSI versitygw exploration and
is non-negotiable here. The e2e suite includes an explicit
scenario for it.

## Secret output

Every `BucketyAccess` produces one `Secret` of type `Opaque` in
the same namespace, with flat keys stable per driver. Direct
`valueFrom.secretKeyRef` and `envFrom.secretRef` work without an
init-container shim.

Every minted Secret is labelled `buckety.yolean.se/owned: "true"`.
The label is controller bookkeeping: the manager's Secret informer
is scoped to it, so the controller's memory tracks buckety usage
rather than the cluster's total Secret count. Consumers MUST NOT
remove it (the Secret would fall out of the controller's watch
until the next periodic reconcile re-stamps it), and the
pre-existing-Secret safety check deliberately reads the apiserver
directly so unlabelled foreign Secrets still surface
`SecretConflict` instead of being adopted.

**Secret key naming convention.** Each driver's emitted Secret
includes a **resource-type key** holding the actual backend
identity:

- `kadm` driver → key `topic` carrying the resolved topic name
  on the broker.
- `s3` driver → key `bucket` carrying the resolved bucket name
  on the backend.
- Future drivers follow the same pattern: `database`,
  `namespace`, etc.

The resource-type key is the unambiguous handle a consumer uses
to operate on the resource. The other keys (`bootstrap`,
`endpoint`, `accessKeyID`, etc.) are connection metadata.

**Secret name convention.** The Secret name comes from
`spec.credentialsSecretName` and is consumer-chosen. The
convention recommended in user-facing examples is
`<buckety>-topic` for Kafka and `<buckety>-bucket` for S3 — the
suffix matches the resource-type key inside, which is helpful
when grepping. The key inside (e.g. `bootstrap`) shouldn't
duplicate the Secret name.

### `kadm` driver

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: orders-topic
  namespace: tenant1
type: Opaque
data:
  bootstrap: <base64>    # y-bootstrap.kafka.svc.cluster.local:9092
  topic:     <base64>    # tenant1.orders.v003  (resource-type key)
```

v1alpha2 SCRAM mode will additively introduce `username`,
`password`, `mechanism`, `securityProtocol`.

### `s3` driver

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: orders-bucket
  namespace: tenant1
type: Opaque
data:
  endpoint:        <base64>    # http://y-s3-api.blobs.svc.cluster.local:9000
  bucket:          <base64>    # tenant1-orders                (resource-type key)
  region:          <base64>    # us-east-1 (or absent)
  accessKeyID:     <base64>
  secretAccessKey: <base64>
```

In v1alpha1 the access keys are the backend's root keys (copied
identically to every `BucketyAccess`). v1alpha2 with per-access
IAM minting will replace them with scoped credentials; key names
stay the same.

### `gcs` driver

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: orders-bucket
  namespace: tenant1
type: Opaque
data:
  endpoint:        <base64>    # S3-interop BARE host (no scheme), derived per bucket: storage.<region>.rep.googleapis.com for regional locations, storage.googleapis.com for multi-regions
  bucket:          <base64>    # tenant1-orders                (resource-type key)
  project:         <base64>    # the backend's GCP project
  region:          <base64>    # SigV4 signing region, derived with the endpoint (absent for multi-regions)
  accessKeyID:     <base64>
  secretAccessKey: <base64>
```

Unlike the s3 driver, whose `endpoint` passes the configured URL
through verbatim, the gcs `endpoint` is a bare host: consumers
prepend the scheme. The regional derivation keeps the data path
in-region (residency) and gives SigV4 its signing region; backend
config can override both fields for emulators.

The access keys are the backend's static HMAC pair (minted out of
band via `gcloud storage hmac create`, copied identically to
every `BucketyAccess`). Driver-minted per-access credentials are
deliberately deferred to the v1alpha2 scoping design: GrantAccess
runs on every reconcile and its result rewrites the Secret, and a
GCS HMAC secret is only retrievable at creation, so per-access
minting cannot be idempotent until grant-once semantics exist.
Key names stay the same when scoped credentials land.

## Adoption

A resolved backend resource name can collide with a resource that
already exists: the default name is bare `metadata.name` (no
namespace), a Retain-deleted Buckety leaves its resource behind
for a recreate to find, and custom `spec.name` templates can
resolve onto anything. Before this section existed the drivers
treated "exists and reachable with our credentials" as "ours",
which silently adopted foreign data - and a subsequent
`retentionPolicy=Delete` recursive delete would destroy content
that predates the CR. Reachability proves nothing about intent,
so intent is tracked explicitly:

**Provenance.** At first reconcile, before `EnsureBuckety` runs,
the controller probes the backend via the driver's
`InspectBuckety` (exists? holds content?) and freezes the answer
into `status.provenance` together with the other sticky fields:

- `Created` - the resource did not exist; the controller made it.
- `Adopted` - the resource pre-existed and the controller claimed
  it.

Resources stamped by controllers predating provenance carry no
value and keep the old behavior end to end.

**The gate.** `spec.adoption` controls what first reconcile does
when the resource already exists:

- `AdoptEmpty` (default): adopt only when the resource holds no
  live content (no current objects for buckets; noncurrent
  versions and delete markers are not consulted. No retained
  records for topics; fully expired records count as none).
  A non-empty resource surfaces `Ready=False` with reason
  `BackendResourceExists`, mints no Secret, stamps nothing, and
  re-checks on the periodic cadence. The message names the
  resource and the opt-in.
- `Adopt`: claim the resource even when it holds content. An
  `Adopted` event records the takeover.

A backend that cannot verify emptiness (a provisioning credential
without list permission) reports non-empty: unknown content is
treated as content, and the operator sees the gate instead of a
silent adoption.

**Deletion never destroys adopted content.** `DeleteBuckety` runs
only for `provenance=Created`. On an adopted resource,
`retentionPolicy=Delete` degrades to Retain: the CR and its
Secrets go, the backend resource stays, and a `RetainedOnDelete`
event says why. This holds even for explicit `Adopt` - the
controller declines to be the tool that destroys data it did not
provision; content an operator truly wants gone is deleted
out-of-band with data-plane credentials. (An explicit override
may be added if a real need appears; none is known.)

Two accepted races, both narrow and both failing toward safety
or a trivially harmless mislabel: a resource created by a third
party between the inspect and the create call is adopted by the
create-conflict path but labelled `Created`; a controller crash
between the backend create and the status write re-runs the gate
and labels the (still empty) resource `Adopted`, which only makes
deletion more conservative.

## Lifecycle and deletion

- Both kinds carry a finalizer `buckety.yolean.se/cleanup`.
- `BucketyAccess` deletion blocks on `RevokeAccess` succeeding
  (in v1alpha1 the no-op revoke completes immediately).
- `Buckety` deletion blocks on (a) all referencing
  `BucketyAccess` being gone — controller does NOT cascade-delete
  them; it surfaces a `BlockedByAccesses` condition with the
  offending names — and (b) `DeleteBuckety` succeeding if
  `retentionPolicy == Delete` and `status.provenance` is
  `Created` (adopted resources are always retained, see
  *Adoption*).

### `Delete` is recursive

`retentionPolicy: Delete` removes the backend resource AND its
contents — the same promise as PersistentVolume
`reclaimPolicy: Delete`, and what the `kadm` driver has always
done (deleting a topic deletes its records). A Delete that blocks
on non-empty contents would instead wedge namespace teardown on
the resource's finalizer.

Semantics:

- Drivers empty contents in bounded slices, returning a typed
  in-progress signal between slices; the controller surfaces
  progress via a `Ready=False/DeletingContents` condition and
  requeues promptly. Deletion of large resources is resumable
  across controller restarts.
- Store-level protections the data plane placed on individual
  items (GCS object holds, retention policies) are honoured, not
  fought: deletion blocks with an error naming the protected
  items until they are released. Those protections — plus GCS
  soft delete, which keeps a recursively-deleted bucket
  restorable for its configured window — are the guardrails for
  the recursive semantics; there is no `DeleteIfEmpty` middle
  value and no extra admission ceremony for flipping
  `Retain -> Delete` (the flip alone deletes nothing).
- A resource under sustained concurrent writes is chased, not
  declared failed: each pass deletes what the backend listed.
- `Retain` (the default) never touches the backend on CR
  deletion, contents or not.

## End-to-end coverage — write this section first

The maintainer is asked to start here: **write the e2e example
set as runnable YAML before writing any operator code.**
Reviewing the examples is how we decide whether the impl
direction is right. Once the examples are signed off, the
operator implementation follows; the examples then form the CI
surface and the user docs.

Required scenarios per shipped driver, each as
`examples/<driver>/<scenario>/` containing kustomize-applyable
YAML, a short README explaining what it demonstrates, and an
`assert.sh` (the assertion contract is in *E2E harness and
parity* below):

1. **Single-consumer happy path.** `Buckety` with
   `defaultAccess`; verify the Secret materialises with the
   documented keys (including the resource-type key); verify a
   consumer Pod uses them.
2. **Multiple consumers, different roles.** `Buckety` + N
   explicit `BucketyAccess` with different roles. In v1alpha1 all
   Secrets carry identical credentials but exercise the
   multi-resource lifecycle correctly.
3. **Parameter mutation.** Apply, change a mutable parameter,
   verify drift is reconciled to the backend. Assert that an
   immutable parameter change is rejected at admission.
4. **Out-of-band drift.** Mutate the backend directly, observe
   the operator either reconcile silently or surface
   `ParameterDrift`, per driver semantics.
5. **Operator scaled to zero.** Apply, scale operator to zero,
   verify consumer Pod can still read/write through the issued
   Secret.
6. **Retention policy.** Delete a `Buckety` with `Retain`,
   verify backend persists. Delete one with `Delete`, verify
   it's gone.
7. **Backend stickiness.** Create a `Buckety` against backend A,
   rename A in controller config, verify the existing resource
   still references `status.backend=A` and surfaces
   `BackendUnavailable` once A is no longer present.
8. **Naming templates.** Author a `spec.name` with `${namespace}`
   + `${label[...]}` + `${backend.zone}`; verify
   `status.backendResourceName` resolves correctly; verify a template
   referencing a missing label is rejected at admission.
9. **Driver version compatibility.** Stamp a resource with a
   driver, run a controller built with the next patch version,
   verify auto-apply. Run a controller built with the next major
   version, verify `DriverVersionIncompatible` surfaces and
   reconcile pauses.
10. **Misconfigured controller startup.** Verify the controller
    exits non-zero with a useful message on: strict-decode
    failure, undefined `${VAR}` without a default, unknown driver
    name, duplicate backend names, missing required per-driver
    field. The pod restart loop is the operator's user-visible
    symptom — this e2e proves the log message is enough to
    diagnose.
11. **Adoption.** Pre-create a backend resource with content
    out-of-band; a Buckety resolving to that name surfaces
    `Ready=False/BackendResourceExists` and mints no Secret.
    `spec.adoption=Adopt` unblocks it with
    `status.provenance=Adopted`; deleting that Buckety with
    `retentionPolicy=Delete` retains the resource and its
    content. A pre-existing EMPTY resource adopts under the
    default policy; a fresh name gets `provenance=Created` and
    still deletes normally. See *Adoption*.

## E2E harness and parity

The e2e suite is the primary correctness gate for both
contributors and CI. The contract:

1. **One harness, two callers.** A single runnable script
   (suggested path `./test/e2e/run.sh`; the maintainer picks the
   exact CLI) executes every scenario. Its inputs are: a
   kubeconfig pointing at a cluster the harness may write to, an
   OCI dir or image reference for the controller binary, and an
   `IMPLEMENTATIONS` matrix (see below). CI calls this same
   script with the CI-built image; a contributor calls it with a
   locally-built `./oci/` dir. There is no second code path in
   GHA. If the workflow becomes more than a thin wrapper that
   builds the image, sets env, and invokes the harness, the
   harness has the wrong contract.

2. **Sideload over pull for local runs.** Local invocation runs
   `contain build --output ./oci --push=false` to produce an OCI
   layout, then `y-cluster images load ./oci` to import it into
   the cluster. CI also accepts a pre-pushed
   `ghcr.io/yolean/buckety-controller@sha256:...` reference and
   skips the sideload step. The harness reads `OCI_DIR` and
   `CONTROLLER_IMAGE` env vars in that priority order; setting
   `OCI_DIR` wins so contributors can override a stale registry
   image. This mirrors the pattern from
   `ystack/e2e/agents-clusterautomation-acceptance-linux-amd64.sh`
   (`KAFKATOPIC_DRIVER_OCI` default
   `$HOME/Yolean/kafkatopic-cosi-driver/oci`).

3. **Cluster reuse, not cluster provisioning.** The harness does
   not provision a cluster. Contributors run against a long-lived
   local k3d (`y-cluster-provision-yolean-local` or equivalent);
   CI starts the same k3d shape in a separate prelude step. The
   harness asserts a clean namespace per scenario and cleans up
   on success. On failure it leaves the namespace standing for
   `kubectl describe` / `kubectl logs` inspection. A
   `KEEP_FAILED=true` env var preserves resources even on
   intermediate-step failures.

4. **Per-scenario assertion contract.** Each
   `examples/<driver>/<scenario>/` directory contains:
   - `kustomization.yaml` that applies the resources under test
     (Buckety, BucketyAccess, consumer Pod where relevant).
   - `README.md` stating what the scenario demonstrates.
   - `assert.sh` that exits 0 on success and non-zero with a
     useful diagnostic on failure. The harness only invokes it;
     assertions live in-tree, not in the runner.
   Assertions are shell-based (`kubectl`, `jq`, `rpk`, `aws s3`)
   to stay contributor-debuggable; no Go test framework on top.
   `assert.sh` MUST run identically whether invoked by the
   harness or by a contributor running it directly while pointed
   at a still-standing failed scenario.

5. **GHA workflow shape.** The workflow's job body is, in this
   order: install y-cluster tooling, start k3d, sideload backing
   services (Redpanda, VersityGW, MinIO), build the controller
   image, invoke `./test/e2e/run.sh` with `IMPLEMENTATIONS`
   matching the CI matrix. The workflow has no scenario
   knowledge. If a scenario is added, the workflow file does not
   change.

### Multi-implementation reuse (same driver, different backends)

For drivers that support multiple backing-service implementations
(today: `s3` against versitygw and minio; future AWS S3 and R2),
most scenarios are implementation-agnostic. Reuse mechanism:

- Each `examples/<driver>/<scenario>/` references a backend by a
  **driver-typed name** (e.g. `s3`, `kafka`), not an
  implementation-typed name. The scenario YAML is unchanged
  between implementations.
- The harness applies a per-implementation kustomize overlay
  that defines `buckety-controller.yaml` with that backend name,
  the implementation's endpoint and credentials, and the
  `implementation:` discriminator where the driver supports
  capability gating. The Buckety and BucketyAccess YAMLs never
  change.
- For `IMPLEMENTATIONS=versitygw,minio` the harness runs every
  `s3` scenario twice, once per implementation, each in its own
  namespace. Failure of one implementation does not
  short-circuit the other; the harness reports per-implementation
  results.
- Implementation-specific scenarios (e.g. R2's `jurisdiction`
  capability) live under
  `examples/<driver>/<implementation>/<scenario>/` and run only
  when their implementation is in the matrix.

Required CI matrix for v1alpha1:

| Driver | Implementations exercised |
| --- | --- |
| `kadm` | redpanda |
| `s3`   | versitygw, minio |
| `gcs`  | fakegcs (fake-gcs-server; covers the JSON-API control plane — real-GCS-only behaviours like HMAC auth enforcement and the 90-day UBLA disable window are documented, not e2e-gated) |

Adding an implementation later (e.g. AWS S3 once the project has
credentials and a budget) requires no example or harness changes,
only a new controller-config overlay under `test/e2e/overlays/`
and the corresponding GHA secret.

## Build and distribution

- Repo: `Yolean/buckety-controller`.
- Single binary, all drivers compiled in. Suggested name
  `buckety` (binary) reading `buckety-controller.yaml` (config).
- **Reproducible build.** The image is built via `contain` from
  the Go binary on a tagged commit. Inputs: source tree at the
  tag + base image digest + `contain.yaml`. Given these, the
  output image digest is deterministic. This is what makes the
  next two artifacts trustworthy.
- **Generated kustomize "release" base.** On every tagged
  release, CI templates the image+digest into
  `deploy/kustomize/release/` and commits it back to the tag (or
  publishes it as a release artifact). Consumers vendor this
  generated base — they never copy the source `deploy/kustomize/base/`
  with a placeholder image. The generated base is the equivalent
  of the schema files: produced at build, published at a stable
  URL, copy-vendored by downstream platforms (e.g. ystack
  vendoring into `kafka/buckety/upstream/`, mirroring the
  `blobs-versitygw/cosi-driver/upstream/` pattern from the COSI
  exploration).
- Image pushed to `ghcr.io/yolean/buckety-controller` by digest
  in CI. Local PoC uses `y-cluster images load <oci-dir>` after
  a local `contain build --output ./oci --push=false`.

## Non-goals in v1alpha1

- MySQL driver.
- Per-consumer credential scoping (SASL/SCRAM for kafka, IAM
  users for S3). All `BucketyAccess` instances for the same
  `Buckety` receive identical credentials.
- Cross-namespace `bucketyRef`.
- Adopting backing resources that already exist outside Buckety.
- Quota enforcement.
- Hot-reload of `buckety-controller.yaml` (envsubst is
  startup-only; rotating credentials requires re-rolling the
  controller Pod).
- Runtime credential rotation in issued Secrets without
  `BucketyAccess` recreate.
- Multi-cluster federation.
- Admission webhook for cross-resource invariants. Per-resource
  parameter validation (against per-driver schemas) and
  naming-template resolution ARE in scope; the webhook reads the
  controller config to know driver registrations and
  capability-gated parameters.

## Open implementation choices (maintainer to lock in)

These are choices the maintainer makes that don't break the API
contract above:

1. **Operator SDK.** kubebuilder, operator-sdk, or hand-rolled
   with controller-runtime. Whichever is closest to what the
   maintainer has done before; the impl details (queueing, work
   scheduling, periodic re-check cadence) follow from the choice.
   The chosen direction is documented in the brief scaffolding
   strategy that accompanies the example set (see *What to do
   before writing code*).
2. **Re-check cadence default.** Suggested 5 min, configurable
   per driver via env or CLI. Document as a knob, not a contract.
3. **Webhook certificate provisioning.** cert-manager,
   controller-runtime's built-in webhook server with self-signed,
   or a sidecar — maintainer's call.
4. **`retentionPolicy` literal values.** Locked as `Retain` /
   `Delete` (short form). No verbose alternative.
5. **Status conditions catalog.** Minimum required: `Ready`,
   `Reconciling`, `BackendUnavailable`, `DriverVersionIncompatible`,
   `ParameterDrift`, `BlockedByAccesses`, `ScopingNotImplemented`.
   Maintainer can add more.

## What to do before writing code

1. Stand up the repo skeleton (`go.mod`, `cmd/buckety/main.go`,
   `pkg/drivers/`, `deploy/kustomize/base/`, `examples/`).
2. Write the e2e example set for both `kadm` and `s3`. The
   examples are the primary review surface: they are the API
   contract as runnable YAML. If a scenario reads awkwardly,
   the API needs another pass before any controller code is
   written.
3. Generate the schema skeletons under
   `pkg/drivers/<driver>/schema/<major.minor>/` from y-cluster's
   schema-generation tooling. Schemas need not be complete to
   start, but the publication URL and the directory layout
   should be in place so the schema URLs in the example YAMLs
   are stable from day one.
4. Write a brief scaffolding strategy — one or two paragraphs
   noting the operator-SDK direction (kubebuilder /
   operator-sdk / hand-rolled), where the reconcile loop lives,
   how drivers register, and how the admission webhook is
   wired up. This accompanies the example set as context; it
   is not a separate review.
5. Submit examples + schema skeletons + scaffolding strategy
   for review. Once approved, write a stub controller that
   loads the config file, registers driver placeholders, and
   answers admission webhook calls with "not implemented".
6. Implement `kadm` first; fewer moving parts than `s3` and the
   e2e against Redpanda is the highest-value first milestone.
7. Implement `s3`, with e2e against VersityGW and MinIO.
