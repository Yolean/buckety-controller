package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion identifies this CRD group at v1alpha1.
var GroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

// SchemeBuilder is the controller-runtime scheme builder for
// registering Buckety + BucketyAccess into a runtime.Scheme.
var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

// AddToScheme registers this group's types with the supplied
// scheme. Call from cmd/buckety/main.go.
var AddToScheme = SchemeBuilder.AddToScheme

func init() {
	SchemeBuilder.Register(
		&Buckety{}, &BucketyList{},
		&BucketyAccess{}, &BucketyAccessList{},
	)
}
