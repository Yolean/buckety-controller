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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
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
