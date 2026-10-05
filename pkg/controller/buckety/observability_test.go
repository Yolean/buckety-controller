package buckety

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// changingDriver is a provisioningDriver whose EnsureBuckety
// reports the given changes through OnChange, or answers with
// ParameterDrift while drift is set.
type changingDriver struct {
	provisioningDriver
	changes []registry.Change
	drift   bool
	gotReq  registry.EnsureRequest
}

func (c *changingDriver) EnsureBuckety(_ context.Context, req registry.EnsureRequest) error {
	c.gotReq = req
	if c.drift {
		return &registry.ErrParameterDrift{Reason: "partitions: current=6 requested=3"}
	}
	for _, ch := range c.changes {
		if req.OnChange != nil {
			req.OnChange(ch)
		}
	}
	return nil
}

// observedBuckety is a Buckety past first reconcile (sticky fields
// stamped) on backend "be", so Reconcile goes straight to Ensure.
func observedBuckety(ns, name string) *bucketyv1.Buckety {
	return &bucketyv1.Buckety{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: ns,
			Finalizers: []string{bucketyv1.FinalizerCleanup},
		},
		Spec: bucketyv1.BucketySpec{Backend: "be", RetentionPolicy: bucketyv1.RetentionRetain},
		Status: bucketyv1.BucketyStatus{
			Backend: "be", Driver: "prov", BackendResourceName: ns + "." + name,
		},
	}
}

func newObservabilityReconciler(t *testing.T, drv registry.Driver, objs ...client.Object) (*Reconciler, client.Client, *record.FakeRecorder) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
		Build()
	rec := record.NewFakeRecorder(32)
	r := &Reconciler{Client: cl, Scheme: scheme, Recorder: rec, Config: &config.Loaded{
		Backends: map[string]config.Backend{"be": {Name: "be", Driver: drv}},
	}}
	return r, cl, rec
}

// drainEvents returns the events recorded so far.
func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// Every change a driver reports becomes a Normal Event carrying the
// old and new values, and the request identifies the Buckety so
// drivers can label their own metrics.
func TestDriverChangesBecomeEvents(t *testing.T) {
	drv := &changingDriver{changes: []registry.Change{
		{Reason: "PartitionsAdded", Parameter: "partitions", Old: "3", New: "6"},
		{Reason: "TopicConfigChanged", Parameter: "config.retention.ms", Old: "604800000", New: "86400000"},
		{Reason: "TopicConfigChanged", Parameter: "config.cleanup.policy", New: "compact"},
	}}
	r, _, rec := newObservabilityReconciler(t, drv, observedBuckety("t1", "orders"))
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "t1", Name: "orders"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if drv.gotReq.Namespace != "t1" || drv.gotReq.BucketyName != "orders" {
		t.Errorf("EnsureRequest identity = %q/%q, want t1/orders", drv.gotReq.Namespace, drv.gotReq.BucketyName)
	}
	events := drainEvents(rec)
	for _, want := range []string{
		`Normal PartitionsAdded partitions: 3 -> 6 on "t1.orders"`,
		`Normal TopicConfigChanged config.retention.ms: 604800000 -> 86400000 on "t1.orders"`,
		`Normal TopicConfigChanged config.cleanup.policy: (unset) -> compact on "t1.orders"`,
	} {
		found := false
		for _, e := range events {
			if e == want {
				found = true
			}
		}
		if !found {
			t.Errorf("missing event %q in %q", want, events)
		}
	}

	// A no-op Ensure reports nothing and so records no change events.
	drv.changes = nil
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "PartitionsAdded") || strings.Contains(e, "TopicConfigChanged") {
			t.Errorf("no-op reconcile recorded change event %q", e)
		}
	}
}
