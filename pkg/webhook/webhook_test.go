package webhook

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	bucketyv1 "github.com/Yolean/buckety-controller/pkg/api/v1alpha1"
	"github.com/Yolean/buckety-controller/pkg/config"
	_ "github.com/Yolean/buckety-controller/pkg/drivers/kadm"
	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// kadmValidator validates against a kadm backend whose defaults
// include config.cleanup.policy. The driver never dials during
// admission, so the seed broker need not exist.
func kadmValidator(t *testing.T) *Validator {
	t.Helper()
	f, ok := registry.Lookup("kadm")
	if !ok {
		t.Fatal("kadm driver not registered")
	}
	d, err := f(json.RawMessage(`{"seedBrokers":["127.0.0.1:1"]}`))
	if err != nil {
		t.Fatal(err)
	}
	return &Validator{Config: &config.Loaded{Backends: map[string]config.Backend{
		"stream": {Name: "stream", Driver: d, Parameters: map[string]string{"config.cleanup.policy": "delete"}},
	}}}
}

func topicUpdate(t *testing.T, v *Validator, oldParams, newParams map[string]string, dryRun bool) admission.Response {
	t.Helper()
	obj := func(p map[string]string) runtime.RawExtension {
		raw, err := json.Marshal(&bucketyv1.Buckety{
			TypeMeta:   metav1.TypeMeta{APIVersion: "buckety.yolean.se/v1alpha1", Kind: "Buckety"},
			ObjectMeta: metav1.ObjectMeta{Name: "topic-ops", Namespace: "t1"},
			Spec:       bucketyv1.BucketySpec{Backend: "stream", Parameters: p},
			Status:     bucketyv1.BucketyStatus{BackendResourceName: "t1.ops"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return runtime.RawExtension{Raw: raw}
	}
	return v.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Kind:      metav1.GroupVersionKind{Kind: "Buckety"},
		Operation: admissionv1.Update,
		Object:    obj(newParams),
		OldObject: obj(oldParams),
		DryRun:    &dryRun,
	}})
}

// An apply that drops a pass-through config.* key is allowed, with
// a warning that the broker keeps the value last applied. Keys still
// present, changed, or falling back to a backend default are not
// removed.
func TestRemovedConfigKeyWarns(t *testing.T) {
	v := kadmValidator(t)
	base := map[string]string{"partitions": "1", "config.max.message.bytes": "500006"}
	with := func(kv ...string) map[string]string {
		m := map[string]string{}
		for k, val := range base {
			m[k] = val
		}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}

	res := topicUpdate(t, v, with("config.retention.ms", "-1", "config.retention.bytes", "-1"), base, false)
	if !res.Allowed {
		t.Fatalf("removal denied: %+v", res.Result)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "config.retention.bytes, config.retention.ms removed") {
		t.Errorf("warnings %q", res.Warnings)
	}
	if res := topicUpdate(t, v, with("config.retention.ms", "-1"), base, true); !res.Allowed || len(res.Warnings) != 1 {
		t.Errorf("dry run: allowed=%v warnings %q", res.Allowed, res.Warnings)
	}

	for name, c := range map[string][2]map[string]string{
		"unchanged":           {with("config.retention.ms", "-1"), with("config.retention.ms", "-1")},
		"changed":             {with("config.retention.ms", "-1"), with("config.retention.ms", "1000")},
		"added":               {base, with("config.retention.ms", "-1")},
		"driver-known key":    {with("partitions", "3"), map[string]string{"config.max.message.bytes": "500006"}},
		"back to the default": {with("config.cleanup.policy", "compact"), base},
	} {
		res := topicUpdate(t, v, c[0], c[1], false)
		if !res.Allowed || len(res.Warnings) != 0 {
			t.Errorf("%s: allowed=%v warnings %q", name, res.Allowed, res.Warnings)
		}
	}
}
