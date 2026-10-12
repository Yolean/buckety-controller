package buckety

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
)

// forgettingDriver records ForgetBuckety calls.
type forgettingDriver struct {
	provisioningDriver
	forgot []string
}

func (f *forgettingDriver) ForgetBuckety(namespace, name string) {
	f.forgot = append(f.forgot, namespace+"/"+name)
}

// A Buckety that no longer exists is forgotten by every backend's
// driver: its backend can no longer be read from it, and drivers
// export per-Buckety series whatever its retentionPolicy was.
func TestGoneBucketyIsForgottenByDrivers(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := bucketyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	a, b := &forgettingDriver{}, &forgettingDriver{}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), Scheme: scheme, Config: &config.Loaded{
		Backends: map[string]config.Backend{"a": {Name: "a", Driver: a}, "b": {Name: "b", Driver: b}, "c": {Name: "c", Driver: &provisioningDriver{}}},
	}}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "t1", Name: "gone"}}); err != nil {
		t.Fatal(err)
	}
	for _, d := range []*forgettingDriver{a, b} {
		if len(d.forgot) != 1 || d.forgot[0] != "t1/gone" {
			t.Errorf("forgot %q, want [t1/gone]", d.forgot)
		}
	}
}
