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

	// EnsureBuckety creates or updates the backend resource to
	// match req. Idempotent. Drift the driver can't reconcile in
	// place (e.g. Kafka partition shrink) surfaces as
	// ErrParameterDrift carrying a human-readable reason.
	EnsureBuckety(ctx context.Context, req EnsureRequest) error

	// DeleteBuckety removes the backend resource. Idempotent on
	// NotFound. Called only when Buckety.spec.retentionPolicy is
	// Delete.
	DeleteBuckety(ctx context.Context, name string) error

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
	// Most drivers accept empty parameters in v1alpha1.
	ValidateAccessParameters(params map[string]string) error
}

// EnsureRequest carries the resolved spec the controller has
// finished computing (template resolved, defaults applied).
type EnsureRequest struct {
	// Name is the resolved backend resource name (Kafka topic
	// name, S3 bucket name, ...). Pinned in
	// status.backendResourceName.
	Name string
	// Parameters is spec.parameters with no controller-side
	// transformation.
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
}

// Factory builds a Driver from its raw `config:` block as it
// appears in buckety-controller.yaml. The block is delivered as
// json.RawMessage so each driver decodes strictly with its own
// types.
type Factory func(rawConfig json.RawMessage) (Driver, error)

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

// ---- registry ----

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register adds a driver factory under name. Intended for init().
// Panics on duplicate name -- the binary's driver list is
// compile-time, so a duplicate is a programmer error.
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := factories[name]; dup {
		panic(fmt.Sprintf("driver %q already registered", name))
	}
	factories[name] = f
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
