// Package v1alpha1 holds the Buckety + BucketyAccess Go types.
// Source of truth for the CRD shape; the YAML in deploy/kustomize/crd/
// must stay in sync.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// GroupName is the API group hosting Buckety + BucketyAccess.
	GroupName = "buckety.yolean.se"
	// Version is the served+stored CRD version.
	Version = "v1alpha1"

	// FinalizerCleanup runs DeleteBuckety / RevokeAccess on the
	// driver side before the resource is removed.
	FinalizerCleanup = "buckety.yolean.se/cleanup"

	// LabelImplicit marks an implicit BucketyAccess materialised
	// from Buckety.spec.defaultAccess. The controller reclaims
	// the resource (and its Secret, via owner-ref) when an
	// explicit BucketyAccess targets the same Buckety or when
	// defaultAccess is removed.
	LabelImplicit = "buckety.yolean.se/implicit"
)

// RetentionPolicy controls what happens to the backend resource
// when the Buckety is deleted.
// +kubebuilder:validation:Enum=Retain;Delete
type RetentionPolicy string

const (
	RetentionRetain RetentionPolicy = "Retain"
	RetentionDelete RetentionPolicy = "Delete"
)

// Role is advisory in v1alpha1: drivers do not yet scope
// credentials per role. Drivers surface a ScopingNotImplemented
// condition rather than silently treating Reader as ReadWrite.
// +kubebuilder:validation:Enum=Reader;Writer;ReadWrite
type Role string

const (
	RoleReader    Role = "Reader"
	RoleWriter    Role = "Writer"
	RoleReadWrite Role = "ReadWrite"
)

// DefaultAccess is the single-consumer shortcut. The controller
// materialises an implicit BucketyAccess with this shape; the
// implicit resource is reclaimed when an explicit BucketyAccess
// arrives or when this field is removed. See SPEC.md §Implicit
// access.
type DefaultAccess struct {
	// +kubebuilder:default=ReadWrite
	Role                  Role   `json:"role,omitempty"`
	CredentialsSecretName string `json:"credentialsSecretName"`
}

// BucketySpec is the user-authored portion of a Buckety.
type BucketySpec struct {
	// Backend is a registered backend name from
	// buckety-controller.yaml. Immutable.
	Backend string `json:"backend"`

	// Name is an optional template resolved at first reconcile
	// and frozen in status.backendResourceName. Defaults to
	// metadata.name. Immutable.
	Name string `json:"name,omitempty"`

	// Parameters is opaque at the CRD level (preserve-unknown);
	// the controller's webhook validates against the resolved
	// driver's parameters schema.
	Parameters map[string]string `json:"parameters,omitempty"`

	// RetentionPolicy decides whether DeleteBuckety runs on the
	// backend when this Buckety is deleted. Defaults to Retain.
	// +kubebuilder:default=Retain
	RetentionPolicy RetentionPolicy `json:"retentionPolicy,omitempty"`

	// DefaultAccess materialises an implicit BucketyAccess. See
	// SPEC.md §Implicit access for the lifecycle and the
	// migration corner cases.
	DefaultAccess *DefaultAccess `json:"defaultAccess,omitempty"`
}

// BucketyStatus carries reconcile state. Several fields are
// sticky from first reconcile (see SPEC.md §Buckety shape).
type BucketyStatus struct {
	// ObservedGeneration advances when metadata.generation has
	// been fully reconciled (backend resource AND any access
	// Secrets are in sync). Until then it lags and Ready=False.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Backend is the name of the backend this resource reconciled
	// against at first reconcile. Sticky.
	Backend string `json:"backend,omitempty"`

	// Driver is the driver behind status.backend at first
	// reconcile. Sticky.
	Driver string `json:"driver,omitempty"`

	// DriverMajor is the major SemVer of the driver at first
	// reconcile. Gates compatibility. Sticky.
	DriverMajor int `json:"driverMajor,omitempty"`

	// DriverBuildVersion is the full SemVer of the binary that
	// most recently reconciled this resource. Informational.
	DriverBuildVersion string `json:"driverBuildVersion,omitempty"`

	// BackendResourceName is the resolved name template. Sticky.
	BackendResourceName string `json:"backendResourceName,omitempty"`

	// Conditions follow the standard meta/v1 shape. Required
	// conditions per SPEC.md §Open implementation choices:
	// Ready, Reconciling, BackendUnavailable,
	// DriverVersionIncompatible, ParameterDrift,
	// BlockedByAccesses, ScopingNotImplemented.
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=bky
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name=Backend,type=string,JSONPath=`.status.backend`
// +kubebuilder:printcolumn:name=Driver,type=string,JSONPath=`.status.driver`
// +kubebuilder:printcolumn:name=Resource,type=string,JSONPath=`.status.backendResourceName`
// +kubebuilder:printcolumn:name=Ready,type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
type Buckety struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BucketySpec   `json:"spec,omitempty"`
	Status BucketyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type BucketyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Buckety `json:"items"`
}

// BucketyRef references a Buckety by name in the same namespace.
// Cross-namespace refs are not in v1alpha1.
type BucketyRef struct {
	Name string `json:"name"`
}

// BucketyAccessSpec is the user-authored portion of a BucketyAccess.
type BucketyAccessSpec struct {
	// BucketyRef must point to a Buckety in the same namespace.
	// Immutable.
	BucketyRef BucketyRef `json:"bucketyRef"`

	// CredentialsSecretName is the Secret minted in this
	// namespace. Immutable.
	CredentialsSecretName string `json:"credentialsSecretName"`

	// Role is advisory in v1alpha1 (see Role docs). Mutable so
	// upgrades to a scope-aware driver can take effect without
	// recreating the resource.
	// +kubebuilder:default=ReadWrite
	Role Role `json:"role,omitempty"`

	// Parameters is opaque at the CRD level; validated by the
	// driver's parameters schema.
	Parameters map[string]string `json:"parameters,omitempty"`
}

// BucketyAccessStatus carries reconcile state.
type BucketyAccessStatus struct {
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Principal is the backend-side identity granted access.
	// v1alpha1: typically the backend's root principal because
	// per-consumer scoping is not implemented.
	Principal string `json:"principal,omitempty"`

	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=bka
// +kubebuilder:subresource:status
type BucketyAccess struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BucketyAccessSpec   `json:"spec,omitempty"`
	Status BucketyAccessStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type BucketyAccessList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BucketyAccess `json:"items"`
}
