package status

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
func TestEventOnlyOnTransition(t *testing.T) {
	obj := &bucketyv1.BucketyAccess{}
	base := []metav1.Condition{{
		Type: "Ready", Status: metav1.ConditionFalse, Reason: "WaitingForBuckety",
	}}
	rec := record.NewFakeRecorder(10)
	drain := func() int {
		n := 0
		for {
			select {
			case <-rec.Events:
				n++
			default:
				return n
			}
		}
	}

	Event(rec, obj, base, "Ready", metav1.ConditionFalse, "WaitingForBuckety", corev1.EventTypeWarning, "WaitingForBuckety", "x")
	if n := drain(); n != 0 {
		t.Fatalf("steady state emitted %d events", n)
	}
	Event(rec, obj, base, "Ready", metav1.ConditionFalse, "SecretConflict", corev1.EventTypeWarning, "SecretConflict", "x")
	if n := drain(); n != 1 {
		t.Fatalf("reason change emitted %d events", n)
	}
	Event(rec, obj, base, "Ready", metav1.ConditionTrue, "SecretMinted", corev1.EventTypeNormal, "SecretMinted", "x")
	if n := drain(); n != 1 {
		t.Fatalf("status flip emitted %d events", n)
	}
	Event(rec, obj, nil, "Ready", metav1.ConditionFalse, "GrantFailed", corev1.EventTypeWarning, "GrantFailed", "x")
	if n := drain(); n != 1 {
		t.Fatalf("first-seen condition emitted %d events", n)
	}
	// Nil recorder: no panic.
	Event(nil, obj, base, "Ready", metav1.ConditionTrue, "SecretMinted", corev1.EventTypeNormal, "SecretMinted", "x")
}

func TestChangedBeyondMessages(t *testing.T) {
	conds := func(s *bucketyv1.BucketyAccessStatus) *[]metav1.Condition { return &s.Conditions }
	base := &bucketyv1.BucketyAccessStatus{Principal: "p"}
	Set(&base.Conditions, "Ready", metav1.ConditionFalse, "GrantFailed", "403 errorId=aaa", 1)

	same := *base
	same.Conditions = append([]metav1.Condition(nil), base.Conditions...)
	Set(&same.Conditions, "Ready", metav1.ConditionFalse, "GrantFailed", "403 errorId=bbb", 1)
	if ChangedBeyondMessages(base, &same, conds) {
		t.Error("message-only change reported as a change")
	}
	if base.Conditions[0].Message != "403 errorId=aaa" {
		t.Error("comparison mutated its input")
	}

	reason := *base
	reason.Conditions = nil
	Set(&reason.Conditions, "Ready", metav1.ConditionFalse, "SecretConflict", "403 errorId=aaa", 1)
	if !ChangedBeyondMessages(base, &reason, conds) {
		t.Error("reason change not reported")
	}

	field := *base
	field.Principal = "q"
	if !ChangedBeyondMessages(base, &field, conds) {
		t.Error("field change not reported")
	}
}
