// Package status holds the condition and event helpers the two
// reconcilers share: setting a condition with its observed
// generation, emitting an Event only on a transition, and
// deciding whether a status changed in anything but condition
// messages (the check that keeps volatile provider error text
// from driving a patch loop).
package status

import (
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

// Set sets a condition, stamping ObservedGeneration so consumers
// can tell which spec produced it.
func Set(conds *[]metav1.Condition, typ string, st metav1.ConditionStatus, reason, msg string, gen int64) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               typ,
		Status:             st,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: gen,
	})
}

// Event emits an Event on rec (nil disables, for tests) unless
// base already carried the condition with the same (status,
// reason) pair, so steady-state requeues do not spam the stream.
// Reason is part of the comparison: Ready staying False while its
// reason moves (WaitingForBuckety to SecretConflict, say) is a
// transition users need to see.
func Event(rec record.EventRecorder, obj runtime.Object, base []metav1.Condition, typ string, st metav1.ConditionStatus, reason, eventType, eventReason, msg string) {
	if rec == nil {
		return
	}
	if c := meta.FindStatusCondition(base, typ); c != nil && c.Status == st && c.Reason == reason {
		return
	}
	rec.Event(obj, eventType, eventReason, msg)
}

// ChangedBeyondMessages reports whether two statuses differ in
// anything except condition messages. conds selects the
// conditions slice of the status struct S, whose other fields
// must be plain values (they are compared through a shallow
// copy).
//
// Message text is allowed to be volatile - provider errors embed
// per-attempt tokens (a GCS 403 mints a fresh troubleshooter
// errorId on every call) - and a message-only patch feeds the
// controller's own watch: patch -> event -> immediate reconcile
// -> fresh provider error -> new message -> patch, at whatever
// rate the provider answers, with workqueue backoff never
// engaging because watch events are Adds, not requeues. Skipping
// the write breaks that cycle structurally, whatever the next
// provider embeds: the message rides along with the next real
// transition, which says the same thing attempt #1 said. Cost:
// progress-style message refreshes (recursive-deletion counts)
// lag while their condition is otherwise unchanged.
func ChangedBeyondMessages[S any](base, cur *S, conds func(*S) *[]metav1.Condition) bool {
	b, c := *base, *cur
	*conds(&b) = withoutMessages(*conds(&b))
	*conds(&c) = withoutMessages(*conds(&c))
	return !equality.Semantic.DeepEqual(b, c)
}

func withoutMessages(conds []metav1.Condition) []metav1.Condition {
	out := make([]metav1.Condition, len(conds))
	for i, c := range conds {
		c.Message = ""
		out[i] = c
	}
	return out
}
