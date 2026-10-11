package buckety

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	"github.com/Yolean/buckety-controller/pkg/controller/cadence"
)

// Settled passes return the re-check cadence spread around its
// nominal value, so Bucketies that reconciled together do not stay
// in lockstep against their backends.
func TestSettledPassesSpreadTheRecheck(t *testing.T) {
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
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bky).
		WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
		Build()
	recheck := 5 * time.Minute
	r := &Reconciler{Client: cl, Scheme: scheme, Recheck: recheck, Config: &config.Loaded{
		Backends: map[string]config.Backend{"be": {Name: "be", Driver: &provisioningDriver{}}},
	}}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "t1", Name: "orders"}}

	spread := time.Duration(cadence.Spread * float64(recheck))
	seen := map[time.Duration]bool{}
	for range 20 {
		res, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if res.RequeueAfter < recheck-spread || res.RequeueAfter > recheck+spread {
			t.Fatalf("RequeueAfter %v outside %v ± %v", res.RequeueAfter, recheck, spread)
		}
		seen[res.RequeueAfter] = true
	}
	if len(seen) < 10 {
		t.Errorf("20 settled passes returned %d distinct re-check delays; want them spread", len(seen))
	}
}
