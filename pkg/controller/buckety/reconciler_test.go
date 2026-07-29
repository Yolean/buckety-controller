package buckety

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

func TestMajorOf(t *testing.T) {
	cases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"0.1.0", 0, false},
		{"1.0.0", 1, false},
		{"12.3.4", 12, false},
		{"7", 7, false},
		{"", 0, true},
		{"v1.0.0", 0, true},
		{"one.two", 0, true},
	}
	for _, c := range cases {
		got, err := majorOf(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("majorOf(%q): expected error, got %d", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("majorOf(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("majorOf(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// The adoption gate's decision table (SPEC "Adoption").
func TestDecideProvenance(t *testing.T) {
	cases := []struct {
		name   string
		insp   registry.Inspection
		policy bucketyv1.AdoptionPolicy
		want   bucketyv1.Provenance
	}{
		{"fresh name", registry.Inspection{}, "", bucketyv1.ProvenanceCreated},
		{"fresh name ignores Adopt", registry.Inspection{}, bucketyv1.AdoptionAdopt, bucketyv1.ProvenanceCreated},
		{"exists empty adopts by default", registry.Inspection{Exists: true, Empty: true}, "", bucketyv1.ProvenanceAdopted},
		{"exists empty under AdoptEmpty", registry.Inspection{Exists: true, Empty: true}, bucketyv1.AdoptionAdoptEmpty, bucketyv1.ProvenanceAdopted},
		{"content refused by default", registry.Inspection{Exists: true}, "", ""},
		{"content refused under AdoptEmpty", registry.Inspection{Exists: true}, bucketyv1.AdoptionAdoptEmpty, ""},
		{"content claimed with Adopt", registry.Inspection{Exists: true}, bucketyv1.AdoptionAdopt, bucketyv1.ProvenanceAdopted},
	}
	for _, c := range cases {
		if got := decideProvenance(c.insp, c.policy); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

// Buckety deletion must delete the implicit access and WAIT for
// its finalizer before releasing its own: owner-ref GC would
// otherwise remove the access after the Buckety, whose absence
// makes the access finalizer skip RevokeAccess - orphaning a live
// key on a Retain-surviving service account (checkit review
// finding 3, sharpened).
func TestDeleteWaitsForImplicitAccessRevocation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	bky := &bucketyv1.Buckety{
		ObjectMeta: metav1.ObjectMeta{
			Name: "orders", Namespace: "t1",
			Finalizers: []string{bucketyv1.FinalizerCleanup},
		},
		Spec: bucketyv1.BucketySpec{Backend: "be", RetentionPolicy: bucketyv1.RetentionRetain},
	}
	implicit := &bucketyv1.BucketyAccess{
		ObjectMeta: metav1.ObjectMeta{
			Name: "orders", Namespace: "t1",
			Labels:     map[string]string{bucketyv1.LabelImplicit: "true"},
			Finalizers: []string{bucketyv1.FinalizerCleanup},
		},
		Spec: bucketyv1.BucketyAccessSpec{
			BucketyRef:            bucketyv1.BucketyRef{Name: "orders"},
			CredentialsSecretName: "orders-bucket",
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bky, implicit).
		WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
		Build()
	r := &Reconciler{Client: cl, Scheme: scheme, Config: &config.Loaded{Backends: map[string]config.Backend{}}}
	if err := cl.Delete(ctx, bky); err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Namespace: "t1", Name: "orders"}
	req := reconcile.Request{NamespacedName: key}

	// First pass: implicit access gets deleted (terminating, held
	// by its finalizer), Buckety finalizer stays.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("first delete pass: %v", err)
	}
	var acc bucketyv1.BucketyAccess
	if err := cl.Get(ctx, key, &acc); err != nil {
		t.Fatalf("implicit access should still exist while revoking: %v", err)
	}
	if acc.DeletionTimestamp.IsZero() {
		t.Error("implicit access not deleted by the Buckety pass")
	}
	var stillHere bucketyv1.Buckety
	if err := cl.Get(ctx, key, &stillHere); err != nil {
		t.Fatalf("buckety released before implicit access was revoked: %v", err)
	}

	// Second pass with the access still terminating: keep waiting.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("waiting pass: %v", err)
	}
	if err := cl.Get(ctx, key, &stillHere); err != nil {
		t.Fatalf("buckety released while access still terminating: %v", err)
	}

	// Access finalizer completes (its own reconciler would do this
	// after RevokeAccess); the Buckety may then go.
	acc.Finalizers = nil
	if err := cl.Update(ctx, &acc); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("final delete pass: %v", err)
	}
	if err := cl.Get(ctx, key, &stillHere); !apierrors.IsNotFound(err) {
		t.Errorf("buckety not released after implicit access completed: %v", err)
	}
}
