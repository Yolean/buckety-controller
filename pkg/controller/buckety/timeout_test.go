package buckety

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// hangingDriver's EnsureBuckety answers only when its context ends,
// like a backend that accepted the connection and went quiet.
type hangingDriver struct{ provisioningDriver }

func (h *hangingDriver) EnsureBuckety(ctx context.Context, _ registry.EnsureRequest) error {
	<-ctx.Done()
	return ctx.Err()
}

// A pass cut short by the reconcile timeout (the deadline
// controller-runtime puts on the pass's context) fails with the
// context error, and its status write still lands: the timeout is
// visible on the Buckety, not only in the controller log.
func TestTimedOutPassRecordsWhy(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	bky := &bucketyv1.Buckety{
		ObjectMeta: metav1.ObjectMeta{
			Name: "orders", Namespace: "t1",
			Finalizers: []string{bucketyv1.FinalizerCleanup},
		},
		Spec: bucketyv1.BucketySpec{Backend: "be"},
		Status: bucketyv1.BucketyStatus{
			Backend: "be", Driver: "prov", DriverBuildVersion: "0.0.1",
			BackendResourceName: "t1-orders", Provenance: bucketyv1.ProvenanceCreated,
		},
	}
	// The fake store ignores contexts; a real client fails a
	// request whose context has ended, so this one does too.
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bky).
		WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				return c.SubResource(sub).Patch(ctx, obj, p, opts...)
			},
		}).
		Build()
	r := &Reconciler{Client: cl, Scheme: scheme, Config: &config.Loaded{
		Backends: map[string]config.Backend{"be": {Name: "be", Driver: &hangingDriver{}}},
	}}
	key := types.NamespacedName{Namespace: "t1", Name: "orders"}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the deadline error, got %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("pass took %v past its deadline", took)
	}
	var got bucketyv1.Buckety
	if err := cl.Get(context.Background(), key, &got); err != nil {
		t.Fatal(err)
	}
	c := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "EnsureFailed" {
		t.Errorf("timed-out pass left no EnsureFailed on status: %+v", got.Status.Conditions)
	}
}
