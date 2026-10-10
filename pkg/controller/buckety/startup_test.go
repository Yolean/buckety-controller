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
)

// After a controller start, a Buckety settled under the running
// driver build gets its first backend call within one re-check
// period instead of in the start-up burst; pending work does not
// wait.
func TestStartupSpreadsSettledBucketies(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	recheck := 5 * time.Minute
	settled := func(name string, gen int64, build string) *bucketyv1.Buckety {
		return &bucketyv1.Buckety{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "t1", Generation: gen,
				Finalizers: []string{bucketyv1.FinalizerCleanup},
			},
			Spec: bucketyv1.BucketySpec{Backend: "be"},
			Status: bucketyv1.BucketyStatus{
				Backend: "be", Driver: "prov", DriverBuildVersion: build,
				BackendResourceName: "t1-" + name, Provenance: bucketyv1.ProvenanceCreated,
				ObservedGeneration: 1,
				Conditions: []metav1.Condition{{
					Type: "Ready", Status: metav1.ConditionTrue, Reason: "EnsuredOnBackend",
					ObservedGeneration: 1, LastTransitionTime: metav1.Now(),
				}},
			},
		}
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			settled("steady", 1, "0.0.1"),
			settled("edited", 2, "0.0.1"),   // spec changed while the controller was down
			settled("upgraded", 1, "0.0.0"), // last reconciled by another driver build
		).
		WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
		Build()
	drv := &provisioningDriver{}
	r := &Reconciler{Client: cl, Scheme: scheme, Recheck: recheck, Config: &config.Loaded{
		Backends: map[string]config.Backend{"be": {Name: "be", Driver: drv}},
	}}
	req := func(name string) reconcile.Request {
		return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "t1", Name: name}}
	}

	res, err := r.Reconcile(context.Background(), req("steady"))
	if err != nil {
		t.Fatal(err)
	}
	if drv.ensures != 0 {
		t.Errorf("first pass over a settled Buckety called the backend %d times", drv.ensures)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > recheck+time.Second {
		t.Errorf("first pass deferred by %v, want within one re-check period", res.RequeueAfter)
	}
	if _, err := r.Reconcile(context.Background(), req("steady")); err != nil {
		t.Fatal(err)
	}
	if drv.ensures != 1 {
		t.Errorf("the deferred pass called the backend %d times, want 1", drv.ensures)
	}

	for _, name := range []string{"edited", "upgraded"} {
		before := drv.ensures
		if _, err := r.Reconcile(context.Background(), req(name)); err != nil {
			t.Fatal(err)
		}
		if drv.ensures != before+1 {
			t.Errorf("%s: pending work was deferred", name)
		}
	}
}
