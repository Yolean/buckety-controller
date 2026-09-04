package bucketyaccess

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
// GrantAccess received and which principals were revoked.
type grantRecorder struct {
	req       *registry.GrantRequest
	data      map[string][]byte
	principal string
	minted    bool
	revoked   []string
	revokeErr error
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
	p := g.principal
	if p == "" {
		p = "rec-principal"
	}
	return registry.GrantResult{SecretData: g.data, Principal: p, Scoped: true, Revocable: g.minted, Minted: g.minted}, nil
}
func (g *grantRecorder) RevokeAccess(_ context.Context, principal string) error {
	g.revoked = append(g.revoked, principal)
	return g.revokeErr
}
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

// A re-mint that changes the principal must revoke the replaced
// one AFTER the Secret write, or every lost/hand-edited Secret
// orphans a live key until the SA's 10-key cap wedges Keys.Create
// (checkit review finding 1). status.principal advances only once
// revocation succeeds, so failures retry.
func TestReplacedPrincipalRevoked(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	newObjs := func() (*bucketyv1.Buckety, *bucketyv1.BucketyAccess) {
		bky := &bucketyv1.Buckety{
			ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "t1"},
			Spec:       bucketyv1.BucketySpec{Backend: "be"},
			Status: bucketyv1.BucketyStatus{
				Backend: "be", BackendResourceName: "t1-orders",
				Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "EnsuredOnBackend", LastTransitionTime: now}},
			},
		}
		access := &bucketyv1.BucketyAccess{
			ObjectMeta: metav1.ObjectMeta{
				Name: "reader", Namespace: "t1", UID: "uid-a",
				Finalizers: []string{bucketyv1.FinalizerCleanup},
			},
			Spec: bucketyv1.BucketyAccessSpec{
				BucketyRef:            bucketyv1.BucketyRef{Name: "orders"},
				CredentialsSecretName: "reader-creds",
			},
			Status: bucketyv1.BucketyAccessStatus{Principal: "projects/p/serviceAccounts/x/keys/old"},
		}
		return bky, access
	}
	events := record.NewFakeRecorder(10)
	run := func(t *testing.T, rec *grantRecorder) (client.Client, *bucketyv1.BucketyAccess, error) {
		t.Helper()
		bky, access := newObjs()
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(bky, access).
			WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
			Build()
		r := &Reconciler{Client: cl, Scheme: scheme, Recorder: events, Config: &config.Loaded{
			Backends: map[string]config.Backend{"be": {Name: "be", Driver: rec}},
		}}
		_, err := r.Reconcile(context.Background(), reconcilerRequest("t1", "reader"))
		var got bucketyv1.BucketyAccess
		if gerr := cl.Get(context.Background(), types.NamespacedName{Namespace: "t1", Name: "reader"}, &got); gerr != nil {
			t.Fatal(gerr)
		}
		return cl, &got, err
	}
	drainEvents := func() []string {
		var out []string
		for {
			select {
			case e := <-events.Events:
				out = append(out, e)
			default:
				return out
			}
		}
	}
	hasEvent := func(reason string) bool {
		for _, e := range drainEvents() {
			if strings.Contains(e, " "+reason+" ") {
				return true
			}
		}
		return false
	}

	// Principal change: old revoked, status advances, and the
	// replacement is announced - it is otherwise invisible on a
	// resource that stays Ready=True while consumers holding the
	// old key are broken.
	rec := &grantRecorder{
		data:      map[string][]byte{"bucket": []byte("t1-orders")},
		principal: "projects/p/serviceAccounts/x/keys/new",
	}
	_, got, err := run(t, rec)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(rec.revoked) != 1 || rec.revoked[0] != "projects/p/serviceAccounts/x/keys/old" {
		t.Errorf("revoked: %v", rec.revoked)
	}
	if got.Status.Principal != "projects/p/serviceAccounts/x/keys/new" {
		t.Errorf("principal: %q", got.Status.Principal)
	}
	if !hasEvent("PrincipalReplaced") {
		t.Error("principal replacement emitted no PrincipalReplaced event")
	}

	// Unchanged principal: no revocation, no announcement.
	rec = &grantRecorder{
		data:      map[string][]byte{"bucket": []byte("t1-orders")},
		principal: "projects/p/serviceAccounts/x/keys/old",
	}
	if _, _, err := run(t, rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.revoked) != 0 {
		t.Errorf("steady state revoked: %v", rec.revoked)
	}
	if hasEvent("PrincipalReplaced") {
		t.Error("steady state emitted PrincipalReplaced")
	}

	// Revocation failure: reconcile errors and status.principal
	// still names the old key so the retry revokes it again.
	rec = &grantRecorder{
		data:      map[string][]byte{"bucket": []byte("t1-orders")},
		principal: "projects/p/serviceAccounts/x/keys/new",
		revokeErr: context.DeadlineExceeded,
	}
	_, got, err = run(t, rec)
	if err == nil {
		t.Fatal("revoke failure swallowed")
	}
	if got.Status.Principal != "projects/p/serviceAccounts/x/keys/old" {
		t.Errorf("principal advanced past failed revoke: %q", got.Status.Principal)
	}
}

// Deleting an access whose backend is missing from config must
// BLOCK while a REVOCABLE principal exists (releasing the
// finalizer would orphan the credential), and proceed for static
// shared principals - whose revoke is a no-op - exactly as in
// v1alpha1, or backend renames wedge every access teardown (seen
// as backend-stickiness e2e failures across all drivers).
func TestDeletionBlocksWithoutBackend(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	newAccess := func(principal string, revocable bool) (*bucketyv1.Buckety, *bucketyv1.BucketyAccess) {
		bky := &bucketyv1.Buckety{
			ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "t1"},
			Spec:       bucketyv1.BucketySpec{Backend: "gone"},
		}
		return bky, &bucketyv1.BucketyAccess{
			ObjectMeta: metav1.ObjectMeta{
				Name: "reader", Namespace: "t1",
				Finalizers: []string{bucketyv1.FinalizerCleanup},
			},
			Spec: bucketyv1.BucketyAccessSpec{
				BucketyRef:            bucketyv1.BucketyRef{Name: "orders"},
				CredentialsSecretName: "reader-creds",
			},
			Status: bucketyv1.BucketyAccessStatus{Principal: principal, PrincipalRevocable: revocable},
		}
	}

	// Revocable principal: blocked with a condition.
	bky, access := newAccess("projects/p/serviceAccounts/x/keys/k1", true)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(bky, access).
		WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
		Build()
	r := &Reconciler{Client: cl, Scheme: scheme, Config: &config.Loaded{Backends: map[string]config.Backend{}}}
	if err := cl.Delete(ctx, access); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reconcilerRequest("t1", "reader")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got bucketyv1.BucketyAccess
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "t1", Name: "reader"}, &got); err != nil {
		t.Fatalf("access should still exist (blocked): %v", err)
	}
	blocked := false
	for _, c := range got.Status.Conditions {
		if c.Type == "Ready" && c.Reason == "BackendUnavailable" {
			blocked = true
		}
	}
	if !blocked {
		t.Errorf("no blocking condition: %+v", got.Status.Conditions)
	}

	// Static shared principal (revoke is a no-op): released, as in
	// v1alpha1 - this is what backend-stickiness scenarios do.
	for _, principal := range []string{"gcs-static", ""} {
		bky, access = newAccess(principal, false)
		cl = fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(bky, access).
			WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
			Build()
		r = &Reconciler{Client: cl, Scheme: scheme, Config: &config.Loaded{Backends: map[string]config.Backend{}}}
		if err := cl.Delete(ctx, access); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(ctx, reconcilerRequest("t1", "reader")); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if err := cl.Get(ctx, types.NamespacedName{Namespace: "t1", Name: "reader"}, &got); !apierrors.IsNotFound(err) {
			t.Errorf("access with principal %q not released: %v", principal, err)
		}
	}
}

// secretWriteFails is a client whose Secret creates fail with the
// given error; everything else reaches the store.
type secretWriteFails struct {
	client.Client
	err error
}

func (s secretWriteFails) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*corev1.Secret); ok {
		return s.err
	}
	return s.Client.Create(ctx, obj, opts...)
}

// A credential minted in a pass whose Secret write fails is held
// by nobody and recorded nowhere (status.principal still names
// the predecessor), so it must be revoked in that same pass -
// including when the namespace is terminating, where no retry
// follows and the access finalizer would revoke only the
// recorded principal.
func TestMintedCredentialRevokedWhenSecretWriteFails(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	terminating := &apierrors.StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusFailure, Code: 403, Reason: metav1.StatusReasonForbidden,
		Details: &metav1.StatusDetails{Causes: []metav1.StatusCause{{Type: corev1.NamespaceTerminatingCause}}},
	}}
	for name, werr := range map[string]error{
		"transient":   context.DeadlineExceeded,
		"terminating": terminating,
	} {
		bky := &bucketyv1.Buckety{
			ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "t1"},
			Spec:       bucketyv1.BucketySpec{Backend: "be"},
			Status: bucketyv1.BucketyStatus{
				Backend: "be", BackendResourceName: "t1-orders",
				Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "EnsuredOnBackend", LastTransitionTime: now}},
			},
		}
		access := &bucketyv1.BucketyAccess{
			ObjectMeta: metav1.ObjectMeta{
				Name: "reader", Namespace: "t1", UID: "uid-a",
				Finalizers: []string{bucketyv1.FinalizerCleanup},
			},
			Spec: bucketyv1.BucketyAccessSpec{
				BucketyRef:            bucketyv1.BucketyRef{Name: "orders"},
				CredentialsSecretName: "reader-creds",
			},
		}
		store := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(bky, access).
			WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
			Build()
		rec := &grantRecorder{
			data:      map[string][]byte{"bucket": []byte("t1-orders")},
			principal: "projects/p/serviceAccounts/x/keys/fresh",
			minted:    true,
		}
		r := &Reconciler{Client: secretWriteFails{store, werr}, Scheme: scheme, Config: &config.Loaded{
			Backends: map[string]config.Backend{"be": {Name: "be", Driver: rec}},
		}}
		_, _ = r.Reconcile(context.Background(), reconcilerRequest("t1", "reader"))
		if len(rec.revoked) != 1 || rec.revoked[0] != "projects/p/serviceAccounts/x/keys/fresh" {
			t.Errorf("%s write failure: revoked %v, want the fresh principal", name, rec.revoked)
		}
		var got bucketyv1.BucketyAccess
		if err := store.Get(context.Background(), types.NamespacedName{Namespace: "t1", Name: "reader"}, &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Principal != "" {
			t.Errorf("%s write failure: status recorded the unwritten principal %q", name, got.Status.Principal)
		}
	}
}
