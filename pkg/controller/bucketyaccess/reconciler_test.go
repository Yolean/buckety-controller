package bucketyaccess

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

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
