package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

// Every controller of the manager runs its passes under the
// reconcile timeout, so one unresponsive backend cannot hold the
// (single) worker of a controller indefinitely.
func TestManagerOptionsBoundEveryPass(t *testing.T) {
	opts := managerOptions(runtime.NewScheme(), ":0", ":0", false, defaultReconcileTimeout)
	if got := opts.Controller.ReconciliationTimeout; got != defaultReconcileTimeout || got <= 0 {
		t.Errorf("ReconciliationTimeout %v, want %v", got, defaultReconcileTimeout)
	}
	if opts.Cache.ByObject == nil {
		t.Error("the scoped Secret cache got lost")
	}
}
