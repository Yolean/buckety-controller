// Package registry holds the Driver interface and a name -> Factory
// map populated by each driver package's init().
//
// The binary's compiled-in driver set is determined by which driver
// packages cmd/buckety/main.go imports for side effects.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Driver is what a backend-specific package implements. The
// controller talks only to this interface; drivers do not know
// about CRDs.
type Driver interface {
	// Name is the driver name as it appears in
	// buckety-controller.yaml (backends[].driver).
	Name() string

	// Version is the running driver SemVer (typically injected
	// via -ldflags '-X .../<driver>.version=...'). The reconciler
	// stamps status.driverMajor at first reconcile and updates
	// status.driverBuildVersion on every compatible reconcile.
	Version() string

	// InspectBuckety probes the backend resource without mutating
	// or claiming it: does it exist, and does it hold content?
	// Called once per Buckety at first reconcile, before
	// EnsureBuckety, to decide adoption (SPEC §Adoption). Empty
	// means no live content: no current objects for buckets
	// (noncurrent versions and delete markers are not consulted),
	// no retained records for topics. A backend that cannot verify
	// emptiness (e.g. a provisioning credential without list
	// permission) reports Empty=false - unknown content is treated
	// as content.
	InspectBuckety(ctx context.Context, name string) (Inspection, error)

	// EnsureBuckety creates or updates the backend resource to
	// match req. Idempotent. Drift the driver can't reconcile in
	// place (e.g. Kafka partition shrink) surfaces as
	// ErrParameterDrift carrying a human-readable reason.
	EnsureBuckety(ctx context.Context, req EnsureRequest) error

	// DeleteBuckety removes the backend resource AND its contents
	// - PersistentVolume reclaimPolicy=Delete semantics; the
	// operator opted into data loss when choosing the policy.
	// Idempotent on NotFound. Called only when
	// Buckety.spec.retentionPolicy is Delete.
	//
	// Contents deletion is bounded per call: a driver empties a
	// slice of the resource and returns ErrDeletionInProgress
	// carrying human-readable progress when more remains; the
	// controller requeues promptly. Store-level protections the
	// data plane placed on individual items (object holds,
	// retention) are honoured, not fought: deletion blocks with an
	// error naming the protected items until they are released.
	DeleteBuckety(ctx context.Context, req DeleteRequest) error

	// GrantAccess returns the Secret payload to mint for a
	// BucketyAccess. v1alpha1 drivers may return the backend's
	// root credentials with Scoped=false; the reconciler then
	// surfaces ScopingNotImplemented=True on the BucketyAccess.
	GrantAccess(ctx context.Context, req GrantRequest) (GrantResult, error)

	// RevokeAccess tears down a per-access principal. No-op +
	// idempotent in v1alpha1 (no per-consumer scoping).
	RevokeAccess(ctx context.Context, principal string) error

	// ValidateParameters reports whether spec.parameters on a
	// Buckety is acceptable for this driver. Called by the
	// admission webhook on CREATE.
	ValidateParameters(params map[string]string) error

	// ValidateUpdateParameters reports whether the transition
	// from old to new is acceptable. Drivers reject changes to
	// keys they have declared immutable post-create (kadm
	// rejects replicationFactor changes, for instance: brokers
	// cannot re-assign partitions in place). Called by the
	// admission webhook on UPDATE.
	ValidateUpdateParameters(old, new map[string]string) error

	// ValidateAccessParameters validates BucketyAccess.spec.parameters.
	// Most drivers accept empty parameters in v1alpha1. Called by
	// the BucketyAccess reconciler before GrantAccess; admission
	// cannot resolve the driver when the Buckety does not exist yet.
	ValidateAccessParameters(params map[string]string) error

	// ValidateResourceName reports whether a resolved spec.name
	// template result is a legal backend resource name for this
	// driver (Kafka topic name rules, S3 bucket name rules, ...).
	// Per SPEC "Naming templates", a template that resolves to an
	// invalid backend name is rejected at admission; the reconciler
	// re-checks before stamping for webhook-disabled deployments.
	ValidateResourceName(name string) error
}

// Inspection is InspectBuckety's report.
type Inspection struct {
	// Exists is whether the backend resource is present.
	Exists bool
	// Empty is whether it holds no live content. Meaningful only
	// when Exists; false when emptiness cannot be verified.
	Empty bool
}

// EnsureRequest carries the resolved spec the controller has
// finished computing (template resolved, defaults applied).
type EnsureRequest struct {
	// Name is the resolved backend resource name (Kafka topic
	// name, S3 bucket name, ...). Pinned in
	// status.backendResourceName.
	Name string
	// Parameters is the effective parameter view (backend defaults
	// merged under spec.parameters, declared templated keys
	// resolved).
	Parameters map[string]string
}

// DeleteRequest carries what DeleteBuckety needs to tear down the
// backend resource and any per-resource principals the driver
// provisioned for it.
type DeleteRequest struct {
	// Name is the resolved backend resource name, from
	// status.backendResourceName.
	Name string
	// Parameters is the same effective view EnsureBuckety received;
	// drivers that provisioned per-resource principals from
	// parameters (gcs serviceAccount) find them here at teardown.
	Parameters map[string]string
}

// GrantRequest carries the resolved access intent.
type GrantRequest struct {
	// BucketyName is the resolved backend resource name the
	// access is being minted for.
	BucketyName string
	// Role is BucketyAccess.spec.role. v1alpha1 drivers
	// typically ignore this and return Scoped=false.
	Role string
	// Parameters is BucketyAccess.spec.parameters.
	Parameters map[string]string
	// BucketyParameters is the referenced Buckety's effective
	// parameter view, identical to what EnsureBuckety received.
	// Drivers that mint per-resource principals read their knobs
	// from here (gcs serviceAccount); drivers without such
	// parameters ignore it.
	BucketyParameters map[string]string
	// ExistingSecretData is the current content of the access's
	// credentials Secret, nil when it does not exist yet. This is
	// what makes minting idempotent for credentials that are only
	// retrievable at creation (GCS SA keys, HMAC secrets):
	// GrantAccess runs on every reconcile and its result rewrites
	// the Secret, so such a driver MUST return the existing data
	// unchanged while it still verifies against the backend, and
	// mint only when it is absent or invalid (which doubles as
	// self-healing after out-of-band revocation).
	ExistingSecretData map[string][]byte
}

// GrantResult is what the driver hands back for a BucketyAccess.
type GrantResult struct {
	// SecretData is the flat key/value pairs the controller
	// writes to the minted Secret. Keys per SPEC §Secret output;
	// every driver MUST include the resource-type key (topic,
	// bucket, database, ...) carrying the backend identity.
	SecretData map[string][]byte
	// Principal is a stable identifier for the access-side
	// identity. v1alpha1 drivers may return the backend's root
	// principal name. Stamped into BucketyAccess.status.principal.
	Principal string
	// Scoped reports whether the credentials in SecretData are
	// actually scoped to Role. v1alpha1: false. The reconciler
	// surfaces ScopingNotImplemented=True when this is false and
	// Role != ReadWrite.
	Scoped bool
	// Revocable reports whether Principal names a credential
	// minted for THIS access that RevokeAccess must remove
	// backend-side (a gcs SA key), as opposed to a shared static
	// principal whose revoke is a no-op. Stamped into
	// status.principalRevocable; the reconciler blocks deletion
	// on a missing backend only for revocable principals, so
	// static-credential accesses keep v1alpha1's release
	// semantics.
	Revocable bool
}

// Factory builds a Driver from its raw `config:` block as it
// appears in buckety-controller.yaml. The block is delivered as
// json.RawMessage so each driver decodes strictly with its own
// types.
type Factory func(rawConfig json.RawMessage) (Driver, error)

// TemplatedParameters returns the Buckety parameter keys the
// driver declares as template-resolved, or nil for drivers
// without the optional capability. Declared keys' values are run
// through the restricted parameter-template grammar
// (template.ResolveParameters) by the controller and webhook
// before validation and before every driver call; backend
// parameter defaults for declared keys skip driver validation at
// startup, since they only resolve per resource.
func TemplatedParameters(d Driver) []string {
	if t, ok := d.(interface{ TemplatedParameters() []string }); ok {
		return t.TemplatedParameters()
	}
	return nil
}

// ErrParameterDrift is the typed error EnsureBuckety returns when
// it observes drift on the backend it cannot reconcile in place
// (e.g. Kafka partition shrink). The controller maps this to a
// ParameterDrift=True condition.
type ErrParameterDrift struct {
	Reason string
}

func (e *ErrParameterDrift) Error() string {
	return "parameter drift: " + e.Reason
}

// IsParameterDrift reports whether err is or wraps an
// ErrParameterDrift.
func IsParameterDrift(err error) bool {
	var d *ErrParameterDrift
	return errors.As(err, &d)
}

// ErrProvisioningInProgress is the typed error EnsureBuckety
// returns when the backend needs a moment rather than a fix: a
// freshly created GCS service account is not yet usable as an
// IAM member (propagation takes seconds, occasionally longer),
// so the first bucket binding is EXPECTED to be refused. Not a
// failure: the controller surfaces Progress with a Normal event
// and requeues promptly, instead of a Warning + backoff that
// misreports a merely-young resource as broken.
type ErrProvisioningInProgress struct {
	Progress string
}

func (e *ErrProvisioningInProgress) Error() string {
	return "provisioning in progress: " + e.Progress
}

// IsProvisioningInProgress reports whether err is or wraps an
// ErrProvisioningInProgress.
func IsProvisioningInProgress(err error) bool {
	var p *ErrProvisioningInProgress
	return errors.As(err, &p)
}

// ErrDeletionInProgress is the typed error DeleteBuckety returns
// when a bounded slice of recursive deletion completed but
// contents remain. Not a failure: the controller surfaces
// Progress on the resource and requeues promptly instead of
// backing off.
type ErrDeletionInProgress struct {
	Progress string
}

func (e *ErrDeletionInProgress) Error() string {
	return "deletion in progress: " + e.Progress
}

// IsDeletionInProgress reports whether err is or wraps an
// ErrDeletionInProgress.
func IsDeletionInProgress(err error) bool {
	var d *ErrDeletionInProgress
	return errors.As(err, &d)
}

// ---- registry ----

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
	versions  = map[string]string{}
)

// Register adds a driver factory under name, with the driver's
// running SemVer (the package version var, already ldflags-injected
// by init() time). Intended for init(). Panics on duplicate name --
// the binary's driver list is compile-time, so a duplicate is a
// programmer error.
func Register(name, version string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := factories[name]; dup {
		panic(fmt.Sprintf("driver %q already registered", name))
	}
	factories[name] = f
	versions[name] = version
}

// Lookup returns the factory for name, or false if unknown.
func Lookup(name string) (Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()
	f, ok := factories[name]
	return f, ok
}

// Names returns the set of registered driver names. Order is
// arbitrary; callers that want determinism should sort.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(factories))
	for k := range factories {
		out = append(out, k)
	}
	return out
}

// Versions returns registered driver names mapped to their running
// SemVer, for startup logging and --version output.
func Versions() map[string]string {
	mu.RLock()
	defer mu.RUnlock()
	out := make(map[string]string, len(versions))
	for k, v := range versions {
		out[k] = v
	}
	return out
}
