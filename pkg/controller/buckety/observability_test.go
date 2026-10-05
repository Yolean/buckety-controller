package buckety

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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

// driftSeries returns the drift gauge's value for ns/name, and
// whether such a series exists at all.
func driftSeries(t *testing.T, ns, name string) (float64, bool) {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	go func() { parameterDrift.Collect(ch); close(ch) }()
	val, found := 0.0, false
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatal(err)
		}
		labels := map[string]string{}
		for _, l := range d.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["namespace"] == ns && labels["buckety"] == name {
			if labels["backend"] != "be" {
				t.Errorf("drift series backend label = %q, want be", labels["backend"])
			}
			val, found = d.GetGauge().GetValue(), true
		}
	}
	return val, found
}

// The drift gauge follows the ParameterDrift condition: 1 while the
// driver answers ErrParameterDrift, 0 once Ensure succeeds again,
// and gone with the Buckety.
func TestParameterDriftGauge(t *testing.T) {
	const ns = "drift-gauge"
	ctx := context.Background()
	drv := &changingDriver{drift: true}
	r, cl, _ := newObservabilityReconciler(t, drv, observedBuckety(ns, "orders"))
	key := types.NamespacedName{Namespace: ns, Name: "orders"}
	req := reconcile.Request{NamespacedName: key}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if v, ok := driftSeries(t, ns, "orders"); !ok || v != 1 {
		t.Fatalf("drift gauge = %v (present %v), want 1", v, ok)
	}
	// Steady state while drifted.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if v, _ := driftSeries(t, ns, "orders"); v != 1 {
		t.Fatalf("drift gauge on repeat = %v, want 1", v)
	}

	// Resolved (spec adjusted to match the backend).
	drv.drift = false
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if v, ok := driftSeries(t, ns, "orders"); !ok || v != 0 {
		t.Fatalf("drift gauge after resolution = %v (present %v), want 0", v, ok)
	}

	// Drifted again, then deleted: the series goes with the Buckety.
	drv.drift = true
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var bky bucketyv1.Buckety
	if err := cl.Get(ctx, key, &bky); err != nil {
		t.Fatal(err)
	}
	if err := cl.Delete(ctx, &bky); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if v, ok := driftSeries(t, ns, "orders"); ok {
		t.Errorf("drift series still present after finalizer release: %v", v)
	}
	// A late NotFound pass for a Buckety this process never saw
	// drop its finalizer also clears any series.
	parameterDrift.WithLabelValues(ns, "gone", "be").Set(1)
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "gone"}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := driftSeries(t, ns, "gone"); ok {
		t.Error("drift series survived a NotFound reconcile")
	}
}

// inspectingDriver answers InspectBuckety with a fixed inspection.
type inspectingDriver struct {
	provisioningDriver
	inspection registry.Inspection
}

func (d *inspectingDriver) InspectBuckety(context.Context, string) (registry.Inspection, error) {
	return d.inspection, nil
}

// capturedLogs returns a context whose logger appends each info line,
// message and key-values, to the returned slice.
func capturedLogs() (context.Context, *[]string) {
	var lines []string
	l := funcr.New(func(prefix, args string) { lines = append(lines, args) }, funcr.Options{})
	return logr.NewContext(context.Background(), l), &lines
}

// First reconcile logs which backend resource the Buckety claimed
// and how, once; later reconciles that change nothing log nothing.
func TestClaimIsLoggedOnce(t *testing.T) {
	for _, tc := range []struct {
		name       string
		inspection registry.Inspection
		adoption   bucketyv1.AdoptionPolicy
		want       string
	}{
		{"created", registry.Inspection{}, "", `"provenance"="Created" "exists"=false "empty"=false`},
		{"adopted", registry.Inspection{Exists: true}, bucketyv1.AdoptionAdopt, `"provenance"="Adopted" "exists"=true "empty"=false`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bky := observedBuckety("claim-"+tc.name, "orders")
			bky.Status = bucketyv1.BucketyStatus{}
			bky.Spec.Adoption = tc.adoption
			r, _, _ := newObservabilityReconciler(t, &inspectingDriver{inspection: tc.inspection}, bky)
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: bky.Namespace, Name: "orders"}}
			ctx, lines := capturedLogs()
			for range 2 {
				if _, err := r.Reconcile(ctx, req); err != nil {
					t.Fatal(err)
				}
			}
			var claims []string
			for _, l := range *lines {
				if strings.Contains(l, `"msg"="backend resource claimed"`) {
					claims = append(claims, l)
				}
			}
			if len(claims) != 1 {
				t.Fatalf("claim log lines = %d, want 1: %q", len(claims), *lines)
			}
			if !strings.Contains(claims[0], tc.want) || !strings.Contains(claims[0], `"backend"="be"`) {
				t.Errorf("claim log = %s, want it to contain %s", claims[0], tc.want)
			}
			if len(*lines) != 1 {
				t.Errorf("info lines = %q, want only the claim", *lines)
			}
		})
	}
}
