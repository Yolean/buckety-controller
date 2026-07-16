# buckety-controller

A small in-cluster operator that provisions named resources on
backing services (Kafka topics, S3 buckets) and mints
`secretKeyRef`-friendly credentials Secrets for consumers.

> **Status: v1alpha1 shipped.** Both drivers (`kadm`, `s3`) are
> implemented and covered by the e2e suite in CI. Every commit on
> `main` pins a reproducible image build in
> [`deploy/kustomize/release/`](./deploy/kustomize/release), and
> the same digest is pushed to `ghcr.io/yolean/buckety-controller`
> on merge.

This README is for **cluster maintainers** — the person deploying
and configuring the controller. End-user examples for consumers
(workload teams writing `Buckety` resources) live under
[`examples/`](./examples). The internal design contract is in
[`SPEC.md`](./SPEC.md).

## Compared to COSI, from an SRE perspective

Buckety was designed informed by an exploration of the
Container Object Storage Interface (COSI). COSI's two-resource
split (compartment + credential binding) and the property that
the controller is **not on the data path** are both inherited
here. The deviations are operational, not architectural:

- **Diagnosability.** Backend identities are operator-chosen
  (e.g. `tenant1.orders.v1` for a Kafka topic, `tenant1-orders`
  for an S3 bucket), not opaque controller-generated UIDs. The
  name you wrote shows up in dashboards, in `rpk topic list`,
  in `aws s3 ls`, and in the operator's logs.
- **Blast radius of config changes.** Each `Buckety` carries
  its own mutable parameters. A retention or partition-count
  change touches one resource. COSI's immutable `BucketClass`
  forces "rebuild everything in this class" cycles when a
  class-level setting needs to change.
- **Failure surface.** One controller binary with all drivers
  compiled in. No sidecar / Unix-socket dance, no per-driver
  Deployment to monitor. Scaling the controller to zero is
  safe by design — running workloads hold their credentials
  Secret directly, with stock `secretKeyRef` keys, so they
  don't need a JSON-blob parser to keep working while the
  controller is down.
- **Recovery.** Standard Kubernetes status conditions
  (`Ready`, `Reconciling`, `BackendUnavailable`,
  `ParameterDrift`, `BlockedByAccesses`) cover the diagnostic
  surface. Out-of-band drift on the backend is surfaced
  explicitly rather than silently re-reconciled.
- **Portability.** Backend choice (VersityGW vs MinIO vs AWS S3)
  is a deploy-time cluster-maintainer decision, not part of the
  API. Consumer YAML moves between clusters with different
  backing services unchanged, as long as a backend by the same
  name exists.

## Status

v1alpha1. Three drivers shipped:

| Driver | Backing services | Notes |
| --- | --- | --- |
| `kadm` | Kafka-protocol brokers (Redpanda, Apache Kafka, Confluent) | Topic create/alter/delete. v1alpha1: no per-consumer SASL/SCRAM scoping. |
| `s3` | S3-compatible (VersityGW, MinIO, AWS S3, Cloudflare R2, Hetzner, GCS interop) | Bucket create/delete. v1alpha1: all consumers receive the backend's root keys. |
| `gcs` | Google Cloud Storage via the native JSON API | Bucket create/update/delete with location, uniform bucket-level access, versioning and lifecycle parameters. Access Secrets carry a static HMAC pair (S3-protocol data path); all consumers receive the same pair. |

e2e coverage in CI runs against Redpanda (`kadm`), VersityGW +
MinIO (`s3`) and fake-gcs-server (`gcs`). The other listed S3
backends share the same client library and the same e2e shape; if
you hit a compatibility issue with one of them, please file an
issue. For `gcs`, behaviours the emulator cannot exercise (HMAC
auth enforcement, the 90-day window for disabling uniform
bucket-level access) are documented rather than e2e-gated.

Use `gcs` (not `s3` interop) when the controller should provision
GCS buckets: creation needs the project, and location / uniform
bucket-level access / versioning / lifecycle only exist on the
native API. Consumers see S3-compatible Secrets either way.

## How it works

1. **You** (the cluster maintainer) deploy the controller and
   write a config file listing named *backends*. Each backend
   wires one driver up to one backing-service instance.
2. **A consumer team** writes a `Buckety` in their namespace
   selecting one of your named backends. The controller
   provisions the topic / bucket on the backing service.
3. The same consumer (or another) writes a `BucketyAccess` to
   mint a `Secret` with the bootstrap/endpoint/credentials. The
   Secret has flat keys; `valueFrom.secretKeyRef` and
   `envFrom.secretRef` work without any client-side parsing.
4. The controller is **only required at provision/reconfigure/revoke**.
   Once a Secret exists, workloads talk to the backend directly.
   Scaling the controller to zero does not affect running consumers.

## Install

The operator publishes a kustomize "release" base alongside its
container image. Every commit on `main` carries a
`deploy/kustomize/release/` directory with the image pinned by
digest — CI rebuilds from source and asserts the pinned digest
matches before the publish job pushes that exact image to
`ghcr.io/yolean/buckety-controller`. The build is reproducible:
image and base ship together, and the pin in the commit you
vendor is the digest that was e2e-tested.

Vendor the release base into your platform repo:

```text
# your-platform/buckety-controller/upstream/
# Copy from Yolean/buckety-controller@<commit>:deploy/kustomize/release/.
# Refresh by re-copying when you bump to a newer commit.
```

Then overlay it with namespace and your config:

```yaml
# your-platform/buckety-controller/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: buckety
resources:
- upstream
secretGenerator:
- name: buckety-controller-config
  files:
  - buckety-controller.yaml         # next file in this folder
patches:
- path: deployment-patch.yaml       # env vars for ${VAR} interpolation
```

`secretGenerator` is preferred over a hand-rolled `Secret`
manifest: it hashes the file content into the Secret name, so a
config change triggers a controller-Pod rollout on the next
`kubectl apply` automatically.

### Without cert-manager

The base includes a `ValidatingWebhookConfiguration` whose CA is
injected by cert-manager (`cert-manager.io/inject-ca-from`), and
the webhook server needs a TLS Secret named
`buckety-controller-webhook-tls`. On clusters without
cert-manager the controller cannot start in its default shape.
The cert-less recipe:

1. Vendor `deploy/kustomize/crd/` and `deploy/kustomize/controller/`
   separately (skip the composite base).
2. Drop `webhook.yaml` from your controller overlay (or patch the
   `ValidatingWebhookConfiguration` out).
3. Pass `--enable-webhook=false` to the controller via a
   Deployment args patch. No `Certificate`, no TLS Secret needed.

In webhook-disabled mode, per-driver parameter and resolved-name
validation runs in the reconcile loop and surfaces as
`Ready=False` with reason `InvalidParameters` or `NameInvalid` on
the resource's status (plus a Warning Event), instead of failing
the apply. CRD-level CEL still enforces `spec.backend` /
`spec.name` / `bucketyRef` / `credentialsSecretName` immutability
and the `role` / `retentionPolicy` enums regardless.

## Configure

The controller loads `buckety-controller.yaml` from a directory
passed via `-c <dir>` (same convention as `y-cluster -c <dir>`).
Strict YAML decode — typos in keys are a startup error.

### Minimal example

```yaml
# buckety-controller.yaml
backends:

- name: cluster-kafka
  driver: kadm
  config:
    seedBrokers:
    - y-bootstrap.kafka.svc.cluster.local:9092

- name: cluster-objects
  driver: s3
  config:
    endpoint: http://y-s3-api.blobs.svc.cluster.local:9000
    region: us-east-1
    forcePathStyle: true
    accessKeyID:     ${VERSITYGW_ROOT_ACCESSKEY}
    secretAccessKey: ${VERSITYGW_ROOT_SECRETKEY}

- name: gcs-objects
  driver: gcs
  config:
    project: my-gcp-project
    # Static S3-interop HMAC pair written to access Secrets;
    # mint out of band with: gcloud storage hmac create <sa-email>
    accessKeyID:     ${GCS_HMAC_ACCESS_ID}
    secretAccessKey: ${GCS_HMAC_SECRET}
```

The gcs control plane authenticates separately, via Application
Default Credentials: mount a service-account JSON key and set
`GOOGLE_APPLICATION_CREDENTIALS` on the controller Deployment
(Workload Identity also works where it exists). That service
account needs bucket create/get/update/delete on the project;
the HMAC pair above is data-plane only.

### Credentials via `${VAR}`

Fields tagged `envsubst:"true"` in each driver's config struct
support shell-style interpolation:

```text
${VAR}              required; controller exits non-zero if VAR is unset
${VAR:-default}     optional with default
$$                  literal $
```

Fields not tagged that contain `${...}` are also rejected at load.
This is the y-cluster `pkg/envsubst` forward-compatibility guard —
tagging a field is a commitment, anything else getting expansion
"for free" would surprise a later version.

For each driver, the documented config schema marks which fields
accept env substitution. Typically: credential fields only. Wire
them up via the controller Deployment's env:

```yaml
# deployment-patch.yaml
spec:
  template:
    spec:
      containers:
      - name: controller
        env:
        - name: VERSITYGW_ROOT_ACCESSKEY
          valueFrom:
            secretKeyRef: { name: versitygw-server, key: root-accesskey }
        - name: VERSITYGW_ROOT_SECRETKEY
          valueFrom:
            secretKeyRef: { name: versitygw-server, key: root-secretkey }
```

Rotating a credential means rotating the Secret AND re-rolling
the controller Pod. Hot-reload of the config file is not
supported in v1alpha1.

### Backend naming

You pick backend names; they're the consumer-facing surface.
Conventions that have worked:

- Purpose-based: `cluster-kafka`, `cluster-objects`, `tenant1-objects`.
- The same name across clusters that play the same role —
  consumer YAML is portable as long as a backend by that name
  exists on the target cluster. The driver behind the name can
  differ (a dev cluster might run MinIO; prod might run VersityGW).

### Schemas

Every driver publishes two JSON Schemas, generated from its Go
types using the y-cluster schema toolchain:

- The `config:` block (used in this file).
- The `spec.parameters` shape for `Buckety` resources (used by
  the controller's admission webhook).

Schemas are published at stable GitHub raw URLs under
`pkg/drivers/<driver>/schema/`. For the config file as a whole
(the `backends:` list shape), a third schema lives at
`pkg/config/schema/buckety-controller.schema.json`. Add a header
to your config file so your editor validates as you type:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/Yolean/buckety-controller/main/pkg/config/schema/buckety-controller.schema.json
```

The per-driver schemas describe each backend's `config:` block and
the `spec.parameters` shape; the controller enforces both at
startup and at admission regardless of editor support.

## Use (consumer view)

A workload team writes:

```yaml
apiVersion: buckety.yolean.se/v1alpha1
kind: Buckety
metadata:
  name: orders
  namespace: tenant1
spec:
  backend: cluster-kafka
  parameters:
    partitions: "12"
    config.retention.ms: "604800000"
  retentionPolicy: Retain
  # Optional: a single-consumer shortcut. Omit to author the
  # BucketyAccess resource(s) separately.
  defaultAccess:
    role: ReadWrite
    credentialsSecretName: orders-topic
```

The controller creates the Kafka topic and mints Secret
`orders-topic` in namespace `tenant1`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: orders-topic
  namespace: tenant1
type: Opaque
data:
  bootstrap: <base64>
  topic:     <base64>      # the actual topic name on the broker
```

The workload references it directly:

```yaml
env:
- name: KAFKA_BOOTSTRAP
  valueFrom:
    secretKeyRef: { name: orders-topic, key: bootstrap }
- name: KAFKA_TOPIC
  valueFrom:
    secretKeyRef: { name: orders-topic, key: topic }
```

For multi-consumer / different-role setups, drop `defaultAccess`
and author one `BucketyAccess` per consumer — see
[`examples/kadm/multi-consumer/`](./examples/kadm/multi-consumer).

When you remove `defaultAccess`, the controller deletes the
implicit `BucketyAccess` it materialised and its Secret is
garbage-collected via owner-ref. If your explicit
`BucketyAccess` reuses the same `credentialsSecretName`,
consumers see a brief gap during the swap as the implicit Secret
is removed before the explicit one materialises. Pick fresh
Secret names, or remove `defaultAccess` in one apply and add the
explicit `BucketyAccess` in a second apply, if zero-gap matters.
See [`SPEC.md`](./SPEC.md#implicit-access-defaultaccess) for the
full lifecycle.

> **`role` is advisory in v1alpha1.** `BucketyAccess.spec.role`
> accepts `Reader`, `Writer`, or `ReadWrite`, but the v1alpha1
> kadm and s3 drivers do not yet scope credentials per role.
> Every Secret minted for the same `Buckety` carries identical
> root credentials regardless of `role`. The controller surfaces
> a `ScopingNotImplemented=True` condition on each affected
> `BucketyAccess` (visible via `kubectl describe bucketyaccess`)
> so the gap is honest, not silent. Scoped credentials
> (SASL/SCRAM, IAM users) are v1alpha2 work. Until then, treat
> `role` as documentation of intent, not enforcement.

S3 is the same shape; see [`examples/s3/`](./examples/s3).

### Naming templates

For platform-conformant naming (region prefix, tenant
namespace, zero-padded generation), use a template in
`spec.name` instead of letting it default to `metadata.name`:

```yaml
spec:
  name: "${backend.zone}.${namespace}.${name}.v${label['yolean.se/generation']}"
  # resolves at first reconcile to e.g. eu.tenant1.orders.v003
```

The full set of substitution variables and the resolution rules
are in [`SPEC.md`](./SPEC.md#naming-templates).

## Resources

- `Buckety` — a topic / bucket / future-MySQL-database. Selects
  a backend by name. Carries mutable parameters; the controller
  reconciles drift to the backing service.
- `BucketyAccess` — a Secret request. Each one mints exactly one
  Secret in the same namespace as the `BucketyAccess`. Multiple
  accesses can target the same Buckety (in v1alpha1 they all
  receive identical credentials).

Both kinds are namespaced. Cross-namespace `bucketyRef` is not
supported in v1alpha1.

## Troubleshooting

Standard conditions you'll see on resources:

- **`Ready=True`** — backend resource is in sync with spec, all
  Secrets minted.
- **`Reconciling=True`** — work in progress; check the message.
- **`BackendUnavailable=True`** — the `status.backend` recorded
  on this resource no longer exists in
  `buckety-controller.yaml`, or its driver changed. Restore the
  backend in config or migrate the resource (see *Migration*
  below).
- **`ParameterDrift=True`** — out-of-band change on the backend
  the driver can't reconcile in place (e.g. someone shrunk a
  Kafka partition count). Inspect, decide, and either recreate
  the resource or adjust the spec to match reality.
- **`BlockedByAccesses=True`** — a `Buckety` deletion is waiting
  for its `BucketyAccess` children to be removed first. Delete
  them explicitly; the controller does not cascade.
- **`ScopingNotImplemented=True`** — a `BucketyAccess` requested
  a per-consumer role/scope the v1alpha1 driver does not yet
  honour. The Secret still mints with the backend's root creds;
  this condition warns you that the scope you asked for is not
  enforced.
- **`Ready=False` reason `InvalidParameters`** — the driver
  rejected `spec.parameters` (or `NameInvalid` for a `spec.name`
  template resolving to an illegal topic/bucket name). With the
  webhook enabled these are normally rejected at apply time; the
  reconcile-loop check is what you see in webhook-disabled
  deployments, or when a `BucketyAccess` was authored before its
  `Buckety` existed.
- **`Ready=False` reason `SecretConflict`** — the
  `credentialsSecretName` names a Secret this `BucketyAccess`
  does not own. The controller refuses to overwrite it (adopting
  it would clobber its data and later garbage-collect it). Delete
  the conflicting Secret or pick another name; the access recovers
  on the next periodic re-check.
- **Deletion hangs with `BackendUnavailable`** — a `Buckety` with
  `retentionPolicy: Delete` is being deleted while its backend is
  missing from `buckety-controller.yaml`. Removing the finalizer
  would silently orphan the backend resource, so the controller
  blocks. Restore the backend in config, or set
  `retentionPolicy: Retain` (mutable) to release the resource
  without backend cleanup.

Failure conditions are accompanied by Warning Events on
transition, so `kubectl describe buckety/<name>` (or
`bucketyaccess/<name>`) shows the history even after a condition
clears.

### Controller won't start

Strict YAML decode catches typos:

```text
parse /etc/buckety/buckety-controller.yaml: error unmarshalling JSON:
while decoding JSON: json: unknown field "seedBroker"
```

Means you wrote `seedBroker` instead of `seedBrokers`. Fix it.

Undefined `${VAR}` without a default also fails fast:

```text
/etc/buckety/buckety-controller.yaml: backends[1].config.accessKeyID:
undefined variable "VERSITYGW_ROOT_ACCESSKEY"
```

The Pod CrashLoopBackOff is the user-visible symptom; the log
message is the diagnostic. This path is covered by an e2e
scenario, so if the message ever stops being useful, file an
issue.

### Migration after backend rename

If you rename or replace a backend after consumers exist:

1. Existing resources keep reconciling against the old name as
   long as it's still in `buckety-controller.yaml`. Keep both
   names for a deprecation window.
2. Consumer teams update their `Buckety` resources to reference
   the new name. Since `spec.backend` is immutable, this means
   delete and recreate. Coordinate retention policy: with
   `retentionPolicy: Retain` the backing resource survives the
   recreate.
3. Once no resources reference the old name, remove it from
   config.

Auto-migration is not in v1alpha1.

## Non-goals (v1alpha1)

The full list is in [`SPEC.md`](./SPEC.md). Highlights:

- No per-consumer credential scoping. SASL/SCRAM and IAM-user
  minting are v1alpha2 work.
- No MySQL driver.
- No cross-namespace `bucketyRef`.
- No hot-reload of `buckety-controller.yaml`.
- No adoption of pre-existing backing resources.

## Filing issues

Issues that include the full controller log on startup, the
config file (credentials redacted), the affected resource's
`status.conditions`, and the backing-service version are
easiest to act on. The e2e suite in `examples/` is the
reproducer template — if your scenario doesn't match any of
those, that's useful information too.
