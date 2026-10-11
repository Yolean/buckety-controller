package main

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
)

// Every controller of the manager runs its passes under the
// reconcile timeout, so one unresponsive backend cannot hold the
// (single) worker of a controller indefinitely.
func TestManagerOptionsBoundEveryPass(t *testing.T) {
	opts := managerOptions(runtime.NewScheme(), ":0", ":0", false, defaultLease, defaultReconcileTimeout)
	if got := opts.Controller.ReconciliationTimeout; got != defaultReconcileTimeout || got <= 0 {
		t.Errorf("ReconciliationTimeout %v, want %v", got, defaultReconcileTimeout)
	}
	if opts.Cache.ByObject == nil {
		t.Error("the scoped Secret cache got lost")
	}
}

// The lease survives an apiserver that answers a renewal slowly:
// controller-runtime's 10s renew deadline (5s per request) cost a
// restart per slow minute. A stopped leader releases the lease so
// its successor does not wait it out.
func TestManagerOptionsLeaseToleratesSlowRenewals(t *testing.T) {
	opts := managerOptions(runtime.NewScheme(), ":0", ":0", true, defaultLease, defaultReconcileTimeout)
	if opts.LeaseDuration == nil || opts.RenewDeadline == nil || opts.RetryPeriod == nil {
		t.Fatal("lease timing left to controller-runtime's defaults")
	}
	ld, rd, rp := *opts.LeaseDuration, *opts.RenewDeadline, *opts.RetryPeriod
	if rd/2 < 15*time.Second {
		t.Errorf("renew deadline %v gives each renewal request %v; want at least 15s", rd, rd/2)
	}
	if !(rp < rd && rd < ld) {
		t.Errorf("want retry period < renew deadline < lease duration, got %v, %v, %v", rp, rd, ld)
	}
	if !opts.LeaderElectionReleaseOnCancel {
		t.Error("a stopped leader keeps its lease until it expires")
	}
}
