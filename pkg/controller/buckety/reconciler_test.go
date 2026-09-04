package buckety

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	"github.com/Yolean/buckety-controller/pkg/controller/status"
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
// key on a Retain-surviving service account.
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

// Regression for ISSUE_status_message_reconcile_loop.md: a
// volatile provider error message (GCS mints a fresh errorId per
// 403) made every status patch a real change, whose watch event
// triggered an immediate reconcile - ~18/s with workqueue backoff
// bypassed, since watch events are Adds, not requeues. Condition
// messages must therefore never DRIVE a patch, only ride along
// with status/reason/field changes.
func TestPatchStatusIgnoresMessageOnlyChanges(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	bky := &bucketyv1.Buckety{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "t1"},
		Spec:       bucketyv1.BucketySpec{Backend: "be"},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bky).
		WithStatusSubresource(&bucketyv1.Buckety{}).
		Build()
	r := &Reconciler{Client: cl, Scheme: scheme}
	key := types.NamespacedName{Namespace: "t1", Name: "orders"}

	// Attempt #1: condition appears - patched.
	base := bky.DeepCopy()
	status.Set(&bky.Status.Conditions, "Ready", metav1.ConditionFalse, "InspectFailed", "403 errorId=aaa111", bky.Generation)
	if err := r.patchStatus(ctx, bky, base); err != nil {
		t.Fatal(err)
	}
	var got bucketyv1.Buckety
	if err := cl.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Message != "403 errorId=aaa111" {
		t.Fatalf("first attempt not persisted: %+v", got.Status.Conditions)
	}
	rvAfterFirst := got.ResourceVersion

	// Attempt #2: same status+reason, fresh errorId - the exact
	// loop driver. Must not write.
	current := got.DeepCopy()
	base = got.DeepCopy()
	status.Set(&current.Status.Conditions, "Ready", metav1.ConditionFalse, "InspectFailed", "403 errorId=bbb222", current.Generation)
	if err := r.patchStatus(ctx, current, base); err != nil {
		t.Fatal(err)
	}
	if err := cl.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	if got.ResourceVersion != rvAfterFirst {
		t.Errorf("message-only change bumped resourceVersion %s -> %s; this is the ~18/s loop", rvAfterFirst, got.ResourceVersion)
	}
	if got.Status.Conditions[0].Message != "403 errorId=aaa111" {
		t.Errorf("skipped patch mutated stored message: %q", got.Status.Conditions[0].Message)
	}

	// Attempt #3: reason changes - a real transition; the current
	// message rides along.
	current = got.DeepCopy()
	base = got.DeepCopy()
	status.Set(&current.Status.Conditions, "Ready", metav1.ConditionFalse, "EnsureFailed", "403 errorId=ccc333", current.Generation)
	if err := r.patchStatus(ctx, current, base); err != nil {
		t.Fatal(err)
	}
	if err := cl.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Conditions[0].Reason != "EnsureFailed" || got.Status.Conditions[0].Message != "403 errorId=ccc333" {
		t.Errorf("reason transition not persisted with its message: %+v", got.Status.Conditions[0])
	}

	// Non-condition status fields always drive a patch.
	current = got.DeepCopy()
	base = got.DeepCopy()
	current.Status.ObservedGeneration = 7
	if err := r.patchStatus(ctx, current, base); err != nil {
		t.Fatal(err)
	}
	if err := cl.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ObservedGeneration != 7 {
		t.Error("field change not patched")
	}
}

// provisioningDriver stubs registry.Driver with EnsureBuckety
// answering ErrProvisioningInProgress until released, counting
// the calls that reach the backend.
type provisioningDriver struct {
	inProgress       bool
	ensures, deletes int
}

func (p *provisioningDriver) Name() string    { return "prov" }
func (p *provisioningDriver) Version() string { return "0.0.1" }
func (p *provisioningDriver) InspectBuckety(context.Context, string) (registry.Inspection, error) {
	return registry.Inspection{}, nil
}
func (p *provisioningDriver) EnsureBuckety(context.Context, registry.EnsureRequest) error {
	p.ensures++
	if p.inProgress {
		return &registry.ErrProvisioningInProgress{Progress: "service account x is not yet bindable"}
	}
	return nil
}
func (p *provisioningDriver) DeleteBuckety(context.Context, registry.DeleteRequest) error {
	p.deletes++
	return nil
}
func (p *provisioningDriver) GrantAccess(context.Context, registry.GrantRequest) (registry.GrantResult, error) {
	return registry.GrantResult{}, nil
}
func (p *provisioningDriver) RevokeAccess(context.Context, string) error            { return nil }
func (p *provisioningDriver) ValidateParameters(map[string]string) error            { return nil }
func (p *provisioningDriver) ValidateUpdateParameters(_, _ map[string]string) error { return nil }
func (p *provisioningDriver) ValidateAccessParameters(map[string]string) error      { return nil }
func (p *provisioningDriver) ValidateResourceName(string) error                     { return nil }

// ISSUE_service_account_propagation_on_first_bind.md: an
// EnsureBuckety in-progress answer is a prompt requeue with a
// Provisioning condition, not an error - first provisioning of a
// young resource must not surface Warning + Ready=False backoff.
func TestProvisioningInProgressRequeuesWithoutError(t *testing.T) {
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
		Spec: bucketyv1.BucketySpec{Backend: "be"},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bky).
		WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
		Build()
	drv := &provisioningDriver{inProgress: true}
	r := &Reconciler{Client: cl, Scheme: scheme, Config: &config.Loaded{
		Backends: map[string]config.Backend{"be": {Name: "be", Driver: drv}},
	}}
	key := types.NamespacedName{Namespace: "t1", Name: "orders"}
	req := reconcile.Request{NamespacedName: key}

	res, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("in-progress surfaced as reconcile error: %v", err)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > 10e9 {
		t.Errorf("want prompt RequeueAfter, got %+v", res)
	}
	var got bucketyv1.Buckety
	if err := cl.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range got.Status.Conditions {
		if c.Type == "Ready" && c.Reason == "Provisioning" && c.Status == metav1.ConditionFalse {
			found = true
		}
	}
	if !found {
		t.Errorf("Provisioning condition missing: %+v", got.Status.Conditions)
	}

	// Propagation done: next reconcile goes Ready.
	drv.inProgress = false
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := cl.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	ready := false
	for _, c := range got.Status.Conditions {
		if c.Type == "Ready" && c.Status == metav1.ConditionTrue {
			ready = true
		}
	}
	if !ready {
		t.Errorf("not Ready after provisioning completed: %+v", got.Status.Conditions)
	}
}

// SPEC §Buckety shape: the (backend, driver, driverMajor) triple
// is sticky. A backend name re-pointed at another driver (both at
// major 0, so the major check alone passes) must pause the
// resource instead of reconciling a topic's name as a bucket, and
// must block a retentionPolicy=Delete teardown that would delete
// on the wrong backend type.
func TestDriverChangeIsBackendUnavailable(t *testing.T) {
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
		Spec: bucketyv1.BucketySpec{Backend: "be", RetentionPolicy: bucketyv1.RetentionDelete},
		Status: bucketyv1.BucketyStatus{
			Backend: "be", Driver: "kadm", BackendResourceName: "t1-orders",
			Provenance: bucketyv1.ProvenanceCreated,
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bky).
		WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
		Build()
	drv := &provisioningDriver{} // Name() is "prov", not "kadm"
	r := &Reconciler{Client: cl, Scheme: scheme, Config: &config.Loaded{
		Backends: map[string]config.Backend{"be": {Name: "be", Driver: drv}},
	}}
	key := types.NamespacedName{Namespace: "t1", Name: "orders"}
	req := reconcile.Request{NamespacedName: key}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if drv.ensures != 0 {
		t.Errorf("EnsureBuckety reached the wrong driver %d times", drv.ensures)
	}
	var got bucketyv1.Buckety
	if err := cl.Get(ctx, key, &got); err != nil {
		t.Fatal(err)
	}
	unavailable, ready := "", ""
	for _, c := range got.Status.Conditions {
		switch c.Type {
		case "BackendUnavailable":
			unavailable = c.Reason
		case "Ready":
			ready = string(c.Status)
		}
	}
	if unavailable != "DriverChanged" || ready != "False" {
		t.Errorf("conditions after driver change: %+v", got.Status.Conditions)
	}

	// Deletion under retentionPolicy=Delete blocks the same way a
	// missing backend does; the finalizer stays.
	if err := cl.Delete(ctx, &got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("delete reconcile: %v", err)
	}
	if drv.deletes != 0 {
		t.Errorf("DeleteBuckety reached the wrong driver %d times", drv.deletes)
	}
	if err := cl.Get(ctx, key, &got); err != nil {
		t.Fatalf("resource released to the wrong driver's teardown: %v", err)
	}
}

// staleLister is a client whose cached List never sees
// BucketyAccess objects, the informer lag after the controller's
// own write. Get/Create/Delete go to the real store.
type staleLister struct{ client.Client }

func (s staleLister) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*bucketyv1.BucketyAccessList); ok {
		return nil
	}
	return s.Client.List(ctx, list, opts...)
}

// The reconcile that follows the implicit access's creation is
// triggered by this reconciler's own status patch and can run
// before the informer lists the new access; a second Create must
// be a no-op, not an AlreadyExists error with workqueue backoff.
// Deletion of the Buckety, which gates the irreversible
// DeleteBuckety on "no explicit access exists", must not trust
// that lagging cache at all.
func TestStaleAccessCacheIsHarmless(t *testing.T) {
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
			Name: "orders", Namespace: "t1", UID: "uid-b",
			Finalizers: []string{bucketyv1.FinalizerCleanup},
		},
		Spec: bucketyv1.BucketySpec{
			Backend:         "be",
			RetentionPolicy: bucketyv1.RetentionDelete,
			DefaultAccess:   &bucketyv1.DefaultAccess{CredentialsSecretName: "orders-creds"},
		},
	}
	store := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bky).
		WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
		Build()
	drv := &provisioningDriver{}
	r := &Reconciler{Client: staleLister{store}, Live: store, Scheme: scheme, Config: &config.Loaded{
		Backends: map[string]config.Backend{"be": {Name: "be", Driver: drv}},
	}}
	key := types.NamespacedName{Namespace: "t1", Name: "orders"}
	req := reconcile.Request{NamespacedName: key}

	// Two passes with the cache never showing the implicit access
	// the first one created: the second must not error.
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatalf("pass %d: %v", i+1, err)
		}
	}
	var implicit bucketyv1.BucketyAccess
	if err := store.Get(ctx, key, &implicit); err != nil {
		t.Fatalf("implicit access not created: %v", err)
	}

	// An explicit access exists (live) while the cache says none:
	// deletion must block instead of running DeleteBuckety.
	explicit := &bucketyv1.BucketyAccess{
		ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "t1"},
		Spec: bucketyv1.BucketyAccessSpec{
			BucketyRef:            bucketyv1.BucketyRef{Name: "orders"},
			CredentialsSecretName: "reader-creds",
		},
	}
	if err := store.Create(ctx, explicit); err != nil {
		t.Fatal(err)
	}
	var cur bucketyv1.Buckety
	if err := store.Get(ctx, key, &cur); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, &cur); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("delete pass: %v", err)
	}
	if drv.deletes != 0 {
		t.Errorf("DeleteBuckety ran %d times with a live explicit access present", drv.deletes)
	}
	if err := store.Get(ctx, key, &cur); err != nil {
		t.Fatalf("buckety released past a live explicit access: %v", err)
	}
	blocked := false
	for _, c := range cur.Status.Conditions {
		if c.Type == "BlockedByAccesses" && c.Status == metav1.ConditionTrue {
			blocked = true
		}
	}
	if !blocked {
		t.Errorf("BlockedByAccesses missing: %+v", cur.Status.Conditions)
	}
}

// An in-progress answer is trusted for provisioningPatience and
// no longer: a resource still "provisioning" minutes later is
// stuck, and must get the failure posture (error with backoff,
// Warning event) instead of a Normal event every 2s forever.
func TestProvisioningOverdueBecomesFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, c := range []struct {
		name  string
		since time.Duration
		fail  bool
	}{
		{"fresh", 10 * time.Second, false},
		{"overdue", provisioningPatience + time.Minute, true},
	} {
		bky := &bucketyv1.Buckety{
			ObjectMeta: metav1.ObjectMeta{
				Name: "orders", Namespace: "t1",
				Finalizers: []string{bucketyv1.FinalizerCleanup},
			},
			Spec: bucketyv1.BucketySpec{Backend: "be"},
			Status: bucketyv1.BucketyStatus{
				Backend: "be", Driver: "prov", BackendResourceName: "orders",
				Conditions: []metav1.Condition{{
					Type: "Ready", Status: metav1.ConditionFalse, Reason: "Provisioning",
					LastTransitionTime: metav1.NewTime(time.Now().Add(-c.since)),
				}},
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(bky).
			WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
			Build()
		r := &Reconciler{Client: cl, Scheme: scheme, Config: &config.Loaded{
			Backends: map[string]config.Backend{"be": {Name: "be", Driver: &provisioningDriver{inProgress: true}}},
		}}
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "t1", Name: "orders"}})
		if c.fail && err == nil {
			t.Errorf("%s: overdue provisioning did not surface as an error", c.name)
		}
		if !c.fail && (err != nil || res.RequeueAfter <= 0) {
			t.Errorf("%s: err=%v res=%+v, want prompt requeue without error", c.name, err, res)
		}
		var got bucketyv1.Buckety
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "t1", Name: "orders"}, &got); err != nil {
			t.Fatal(err)
		}
		want := "Provisioning"
		if c.fail {
			want = "EnsureFailed"
		}
		if r := meta.FindStatusCondition(got.Status.Conditions, "Ready"); r == nil || r.Reason != want {
			t.Errorf("%s: Ready condition %+v, want reason %s", c.name, r, want)
		}
	}
}
