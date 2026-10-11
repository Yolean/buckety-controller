package buckety

import (
	"context"
	"testing"
	"time"

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

// teardownDriver answers DeleteBuckety from a script: each entry
// is one pass, "progress", "stalled" or "done".
type teardownDriver struct {
	provisioningDriver
	script []string
}

func (d *teardownDriver) DeleteBuckety(context.Context, registry.DeleteRequest) error {
	step := d.script[0]
	d.script = d.script[1:]
	switch step {
	case "progress":
		return &registry.ErrDeletionInProgress{Progress: "deleted 1000 objects"}
	case "stalled":
		return &registry.ErrDeletionInProgress{Progress: "still not empty", Stalled: true}
	}
	return nil
}

// Deletion requeues promptly while passes make progress and backs
// off, doubling to a cap, while they remove nothing; progress
// resets the delay, and completion releases the finalizer.
func TestDeletionBacksOffWhileStalled(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := metav1.Now()
	bky := &bucketyv1.Buckety{
		ObjectMeta: metav1.ObjectMeta{
			Name: "orders", Namespace: "t1", UID: "u1",
			Finalizers:        []string{bucketyv1.FinalizerCleanup},
			DeletionTimestamp: &now,
		},
		Spec: bucketyv1.BucketySpec{Backend: "be", RetentionPolicy: bucketyv1.RetentionDelete},
		Status: bucketyv1.BucketyStatus{
			Backend: "be", Driver: "prov", BackendResourceName: "t1-orders",
			Provenance: bucketyv1.ProvenanceCreated,
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bky).
		WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
		Build()
	s := 2 * time.Second
	steps := []struct {
		pass string
		want time.Duration
	}{
		{"progress", s},
		{"progress", s},
		{"stalled", s},
		{"stalled", 2 * s},
		{"stalled", 4 * s},
		{"progress", s},
		{"stalled", s},
		{"stalled", 2 * s},
		{"stalled", 4 * s},
		{"stalled", 8 * s},
		{"stalled", 16 * s},
		{"stalled", 32 * s},
		{"stalled", 2 * time.Minute},
		{"stalled", 2 * time.Minute},
	}
	drv := &teardownDriver{}
	for _, st := range steps {
		drv.script = append(drv.script, st.pass)
	}
	drv.script = append(drv.script, "done")
	r := &Reconciler{Client: cl, Scheme: scheme, Config: &config.Loaded{
		Backends: map[string]config.Backend{"be": {Name: "be", Driver: drv}},
	}}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "t1", Name: "orders"}}

	for i, st := range steps {
		res, err := r.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("pass %d (%s): %v", i, st.pass, err)
		}
		if res.RequeueAfter != st.want {
			t.Errorf("pass %d (%s): RequeueAfter %v, want %v", i, st.pass, res.RequeueAfter, st.want)
		}
	}

	// Done: the finalizer goes, the object with it, and the
	// backoff state is dropped.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var got bucketyv1.Buckety
	if err := cl.Get(ctx, req.NamespacedName, &got); !apierrors.IsNotFound(err) {
		t.Errorf("Buckety still present after teardown: %v %+v", err, got.Finalizers)
	}
	if n := len(r.deletions.delay); n != 0 {
		t.Errorf("backoff state kept for %d Bucketies after teardown", n)
	}
}

func TestIsDeletionStalled(t *testing.T) {
	if registry.IsDeletionStalled(&registry.ErrDeletionInProgress{}) {
		t.Error("zero value reported stalled")
	}
	if !registry.IsDeletionStalled(&registry.ErrDeletionInProgress{Stalled: true}) {
		t.Error("stalled not reported")
	}
}
