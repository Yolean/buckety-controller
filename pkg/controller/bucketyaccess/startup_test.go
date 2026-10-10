package bucketyaccess

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
)

// After a controller start, a Ready access whose Secret exists gets
// its first grant check within one re-check period instead of in
// the start-up burst; an access whose Secret is gone is re-minted
// at once.
func TestStartupSpreadsSettledAccesses(t *testing.T) {
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
		Spec:       bucketyv1.BucketySpec{Backend: "be"},
		Status: bucketyv1.BucketyStatus{
			Backend: "be", BackendResourceName: "t1-orders",
			Conditions: []metav1.Condition{{
				Type: "Ready", Status: metav1.ConditionTrue, Reason: "EnsuredOnBackend", LastTransitionTime: now,
			}},
		},
	}
	access := func(name, uid string) *bucketyv1.BucketyAccess {
		return &bucketyv1.BucketyAccess{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "t1", UID: types.UID(uid),
				Finalizers: []string{bucketyv1.FinalizerCleanup},
			},
			Spec: bucketyv1.BucketyAccessSpec{
				BucketyRef:            bucketyv1.BucketyRef{Name: "orders"},
				CredentialsSecretName: name + "-creds",
			},
			Status: bucketyv1.BucketyAccessStatus{
				Principal: "rec-principal",
				Conditions: []metav1.Condition{{
					Type: "Ready", Status: metav1.ConditionTrue, Reason: "SecretMinted", LastTransitionTime: now,
				}},
			},
		}
	}
	withSecret := access("reader", "uid-r")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "reader-creds", Namespace: "t1",
			Labels: map[string]string{bucketyv1.LabelOwnedSecret: "true"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: bucketyv1.GroupVersion.String(), Kind: "BucketyAccess",
				Name: "reader", UID: withSecret.UID, Controller: ptr.To(true),
			}},
		},
		Data: map[string][]byte{"bucket": []byte("t1-orders")},
	}
	lost := access("writer", "uid-w")
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects([]client.Object{bky, withSecret, secret, lost}...).
		WithStatusSubresource(&bucketyv1.Buckety{}, &bucketyv1.BucketyAccess{}).
		Build()
	rec := &grantRecorder{data: map[string][]byte{"bucket": []byte("t1-orders")}}
	recheck := 5 * time.Minute
	r := &Reconciler{Client: cl, Scheme: scheme, Recheck: recheck, Config: &config.Loaded{
		Backends: map[string]config.Backend{"be": {Name: "be", Driver: rec}},
	}}

	res, err := r.Reconcile(context.Background(), reconcilerRequest("t1", "reader"))
	if err != nil {
		t.Fatal(err)
	}
	if rec.req != nil {
		t.Error("first pass over a settled access called GrantAccess")
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > recheck+time.Second {
		t.Errorf("first pass deferred by %v, want within one re-check period", res.RequeueAfter)
	}
	if _, err := r.Reconcile(context.Background(), reconcilerRequest("t1", "reader")); err != nil {
		t.Fatal(err)
	}
	if rec.req == nil {
		t.Error("the deferred pass did not call GrantAccess")
	}

	rec.req = nil
	if _, err := r.Reconcile(context.Background(), reconcilerRequest("t1", "writer")); err != nil {
		t.Fatal(err)
	}
	if rec.req == nil {
		t.Error("an access whose Secret is missing was deferred")
	}
}
