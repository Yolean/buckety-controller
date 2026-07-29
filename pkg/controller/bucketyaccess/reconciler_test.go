package bucketyaccess

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
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

// The gate must treat a reason change within the same status as a
// transition: Ready staying False while moving WaitingForBuckety ->
// SecretConflict is exactly the moment users need an Event (this
// suppression shipped once and was caught by e2e).
func TestEventIfTransition(t *testing.T) {
	obj := &bucketyv1.BucketyAccess{}
	base := []metav1.Condition{{
		Type: "Ready", Status: metav1.ConditionFalse, Reason: "WaitingForBuckety",
	}}

	drain := func(rec *record.FakeRecorder) []string {
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

	rec := record.NewFakeRecorder(10)
	r := &Reconciler{Recorder: rec}

	// Same status, same reason: suppressed.
	r.eventIfTransition(obj, base, "Ready", metav1.ConditionFalse, "WaitingForBuckety",
		corev1.EventTypeWarning, "WaitingForBuckety", "x")
	if got := drain(rec); len(got) != 0 {
		t.Fatalf("steady state emitted %v", got)
	}

	// Same status, new reason: emitted.
	r.eventIfTransition(obj, base, "Ready", metav1.ConditionFalse, "SecretConflict",
		corev1.EventTypeWarning, "SecretConflict", "x")
	if got := drain(rec); len(got) != 1 {
		t.Fatalf("reason change emitted %v", got)
	}

	// Status flip: emitted.
	r.eventIfTransition(obj, base, "Ready", metav1.ConditionTrue, "SecretMinted",
		corev1.EventTypeNormal, "SecretMinted", "x")
	if got := drain(rec); len(got) != 1 {
		t.Fatalf("status flip emitted %v", got)
	}

	// Condition absent from base (first reconcile): emitted.
	r.eventIfTransition(obj, nil, "Ready", metav1.ConditionFalse, "GrantFailed",
		corev1.EventTypeWarning, "GrantFailed", "x")
	if got := drain(rec); len(got) != 1 {
		t.Fatalf("first-seen condition emitted %v", got)
	}

	// Nil recorder: no panic.
	(&Reconciler{}).eventIfTransition(obj, base, "Ready", metav1.ConditionTrue, "SecretMinted",
		corev1.EventTypeNormal, "SecretMinted", "x")
}

// The manager cache only carries Secrets labelled LabelOwnedSecret
// (issue #10), so writeSecret works from a live read and must (a)
// stamp the label on creation, (b) stamp it onto owned Secrets
// minted before the label existed - that update is the upgrade
// migration - and (c) skip no-op updates so steady-state requeues
// do not churn resourceVersion.
func TestWriteSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	access := &bucketyv1.BucketyAccess{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "t1", UID: "uid-1"},
		Spec: bucketyv1.BucketyAccessSpec{
			BucketyRef:            bucketyv1.BucketyRef{Name: "orders"},
			CredentialsSecretName: "orders-bucket",
		},
	}
	data := map[string][]byte{"bucket": []byte("t1-orders")}
	ctx := context.Background()
	key := types.NamespacedName{Namespace: "t1", Name: "orders-bucket"}

	// (a) create: label + controller owner-ref stamped.
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: cl, Scheme: scheme}
	if err := r.writeSecret(ctx, access, nil, true, data); err != nil {
		t.Fatalf("create: %v", err)
	}
	var got corev1.Secret
	if err := cl.Get(ctx, key, &got); err != nil {
		t.Fatalf("get after create: %v", err)
	}
	if got.Labels[bucketyv1.LabelOwnedSecret] != "true" {
		t.Errorf("created Secret missing %s label: %v", bucketyv1.LabelOwnedSecret, got.Labels)
	}
	if !metav1.IsControlledBy(&got, access) {
		t.Error("created Secret not controlled by the access")
	}

	// (b) migration: owned pre-label Secret gains the label and
	// fresh data on update.
	preLabel := got.DeepCopy()
	delete(preLabel.Labels, bucketyv1.LabelOwnedSecret)
	preLabel.Data = map[string][]byte{"bucket": []byte("stale")}
	cl = fake.NewClientBuilder().WithScheme(scheme).WithObjects(preLabel).Build()
	r = &Reconciler{Client: cl, Scheme: scheme}
	if err := r.writeSecret(ctx, access, preLabel, false, data); err != nil {
		t.Fatalf("migration update: %v", err)
	}
	if err := cl.Get(ctx, key, &got); err != nil {
		t.Fatalf("get after migration: %v", err)
	}
	if got.Labels[bucketyv1.LabelOwnedSecret] != "true" {
		t.Errorf("pre-label Secret did not gain the label: %v", got.Labels)
	}
	if string(got.Data["bucket"]) != "t1-orders" {
		t.Errorf("data not converged: %q", got.Data["bucket"])
	}

	// (c) steady state: identical desired state writes nothing.
	before := got.ResourceVersion
	if err := r.writeSecret(ctx, access, &got, false, data); err != nil {
		t.Fatalf("no-op update: %v", err)
	}
	var after corev1.Secret
	if err := cl.Get(ctx, key, &after); err != nil {
		t.Fatal(err)
	}
	if after.ResourceVersion != before {
		t.Errorf("no-op reconcile bumped resourceVersion %s -> %s", before, after.ResourceVersion)
	}
}

// grantRecorder is a minimal registry.Driver that records what
// GrantAccess received.
type grantRecorder struct {
	req  *registry.GrantRequest
	data map[string][]byte
}

func (g *grantRecorder) Name() string    { return "rec" }
func (g *grantRecorder) Version() string { return "0.0.1" }
func (g *grantRecorder) InspectBuckety(context.Context, string) (registry.Inspection, error) {
	return registry.Inspection{}, nil
}
func (g *grantRecorder) EnsureBuckety(context.Context, registry.EnsureRequest) error { return nil }
func (g *grantRecorder) DeleteBuckety(context.Context, registry.DeleteRequest) error { return nil }
func (g *grantRecorder) GrantAccess(_ context.Context, req registry.GrantRequest) (registry.GrantResult, error) {
	g.req = &req
	return registry.GrantResult{SecretData: g.data, Principal: "rec-principal", Scoped: true}, nil
}
func (g *grantRecorder) RevokeAccess(context.Context, string) error            { return nil }
func (g *grantRecorder) ValidateParameters(map[string]string) error            { return nil }
func (g *grantRecorder) ValidateUpdateParameters(_, _ map[string]string) error { return nil }
func (g *grantRecorder) ValidateAccessParameters(map[string]string) error      { return nil }
func (g *grantRecorder) ValidateResourceName(string) error                     { return nil }

// The Secret gate runs BEFORE GrantAccess: drivers may mint live
// credentials there (a GCS SA key), so a conflicting Secret must
// pre-empt minting entirely, and the existing owned Secret's data
// must ride along so create-only-retrievable credentials can be
// returned unchanged (registry.GrantRequest.ExistingSecretData).
func TestGrantGatedOnSecretAndFedExistingData(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	bky := &bucketyv1.Buckety{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "t1"},
		Spec:       bucketyv1.BucketySpec{Backend: "be", Parameters: map[string]string{"serviceAccount": "orders-t1"}},
		Status: bucketyv1.BucketyStatus{
			Backend:             "be",
			BackendResourceName: "t1-orders",
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionTrue, Reason: "EnsuredOnBackend", LastTransitionTime: now,
			}},
		},
	}
	newAccess := func() *bucketyv1.BucketyAccess {
		return &bucketyv1.BucketyAccess{
			ObjectMeta: metav1.ObjectMeta{
				Name: "reader", Namespace: "t1", UID: "uid-a",
				Finalizers: []string{bucketyv1.FinalizerCleanup},
			},
			Spec: bucketyv1.BucketyAccessSpec{
				BucketyRef:            bucketyv1.BucketyRef{Name: "orders"},
				CredentialsSecretName: "reader-creds",
			},
		}
	}
	reconcile := func(t *testing.T, rec *grantRecorder, extra ...client.Object) (client.Client, *bucketyv1.BucketyAccess) {
		t.Helper()
		access := newAccess()
		objs := append([]client.Object{bky.DeepCopy(), access}, extra...)
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(objs...).
			WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
			Build()
		r := &Reconciler{Client: cl, Scheme: scheme, Config: &config.Loaded{
			Backends: map[string]config.Backend{"be": {Name: "be", Driver: rec}},
		}}
		if _, err := r.Reconcile(context.Background(), reconcilerRequest("t1", "reader")); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		var got bucketyv1.BucketyAccess
		if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "t1", Name: "reader"}, &got); err != nil {
			t.Fatal(err)
		}
		return cl, &got
	}

	// (a) No Secret: grant runs with nil ExistingSecretData and
	// the Buckety's parameters, Secret gets minted.
	rec := &grantRecorder{data: map[string][]byte{"bucket": []byte("t1-orders")}}
	cl, got := reconcile(t, rec)
	if rec.req == nil {
		t.Fatal("grant not called")
	}
	if rec.req.ExistingSecretData != nil {
		t.Errorf("ExistingSecretData on first mint: %v", rec.req.ExistingSecretData)
	}
	if rec.req.BucketyParameters["serviceAccount"] != "orders-t1" {
		t.Errorf("BucketyParameters: %v", rec.req.BucketyParameters)
	}
	if rec.req.BucketyName != "t1-orders" {
		t.Errorf("BucketyName: %q", rec.req.BucketyName)
	}
	var secret corev1.Secret
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "t1", Name: "reader-creds"}, &secret); err != nil {
		t.Fatalf("minted secret: %v", err)
	}
	if got.Status.Principal != "rec-principal" {
		t.Errorf("principal: %q", got.Status.Principal)
	}

	// (b) Foreign Secret: SecretConflict pre-empts the grant -
	// nothing is minted for a Secret we refuse to write.
	rec = &grantRecorder{data: map[string][]byte{"bucket": []byte("x")}}
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "reader-creds", Namespace: "t1"}}
	_, got = reconcile(t, rec, foreign)
	if rec.req != nil {
		t.Error("grant called despite SecretConflict")
	}
	conflicted := false
	for _, c := range got.Status.Conditions {
		if c.Type == "Ready" && c.Reason == "SecretConflict" {
			conflicted = true
		}
	}
	if !conflicted {
		t.Errorf("SecretConflict not surfaced: %+v", got.Status.Conditions)
	}

	// (c) Owned Secret: its current data feeds the grant.
	rec = &grantRecorder{data: map[string][]byte{"bucket": []byte("t1-orders")}}
	ctrl := true
	owned := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "reader-creds", Namespace: "t1",
			Labels: map[string]string{bucketyv1.LabelOwnedSecret: "true"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: bucketyv1.GroupVersion.String(), Kind: "BucketyAccess",
				Name: "reader", UID: "uid-a", Controller: &ctrl,
			}},
		},
		Data: map[string][]byte{"serviceAccountKey": []byte("previously-minted")},
	}
	_, _ = reconcile(t, rec, owned)
	if rec.req == nil {
		t.Fatal("grant not called for owned secret")
	}
	if string(rec.req.ExistingSecretData["serviceAccountKey"]) != "previously-minted" {
		t.Errorf("ExistingSecretData: %v", rec.req.ExistingSecretData)
	}
}

func reconcilerRequest(ns, name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
}
