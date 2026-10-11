package kadm

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

var errInvalidConfigForTest = kerr.InvalidConfig

// unreportedPass runs one alignTopic pass on drv for topic, with
// the broker reporting reported as the topic's configs, and
// returns how many keys it altered and the changes it reported.
func unreportedPass(t *testing.T, drv *Driver, m *fakeMutator, topic string, reported map[string]string, want map[string]*string) (int, []registry.Change) {
	t.Helper()
	before := 0
	for _, a := range m.alters {
		before += len(a)
	}
	var got []registry.Change
	err := drv.alignTopic(context.Background(), registry.EnsureRequest{
		Name: topic, Namespace: "unreported", BucketyName: "orders",
		OnChange: func(c registry.Change) { got = append(got, c) },
	}, &topicView{partitions: 1, rf: 1, configs: reported}, 0, 0, want)
	if err != nil {
		t.Fatal(err)
	}
	after := 0
	for _, a := range m.alters {
		after += len(a)
	}
	return after - before, got
}

// A broker that acknowledges an alter of a key but never reports
// the key back (seen with min.compaction.lag.ms) must not make
// every pass alter it again: one alter, then nothing while the
// requested value is unchanged.
func TestUnreportedConfigIsNotReapplied(t *testing.T) {
	const topic = "unreported.orders"
	m := &fakeMutator{}
	drv := &Driver{mutator: m}
	want := map[string]*string{"min.compaction.lag.ms": strp("43200000"), "retention.ms": strp("1000")}
	reported := map[string]string{"retention.ms": "1000"} // the other key never shows up

	if n, changes := unreportedPass(t, drv, m, topic, reported, want); n != 1 || len(changes) != 1 {
		t.Fatalf("first pass: %d alters, %d changes; want 1 and 1", n, len(changes))
	}
	for pass := 2; pass <= 3; pass++ {
		if n, changes := unreportedPass(t, drv, m, topic, reported, want); n != 0 || len(changes) != 0 {
			t.Fatalf("pass %d re-applied an acknowledged, unreported key: %d alters, %+v", pass, n, changes)
		}
	}

	// The requested value changes: altered again, once.
	want["min.compaction.lag.ms"] = strp("60000")
	if n, _ := unreportedPass(t, drv, m, topic, reported, want); n != 1 {
		t.Fatalf("spec change: %d alters, want 1", n)
	}
	if n, _ := unreportedPass(t, drv, m, topic, reported, want); n != 0 {
		t.Fatalf("after spec change: %d alters, want 0", n)
	}
}

// The memory only covers keys the broker does not report. A key
// that is reported with another value is drift and is altered on
// every pass that finds it, as before.
func TestReportedDriftIsStillReapplied(t *testing.T) {
	const topic = "reported.orders"
	m := &fakeMutator{}
	drv := &Driver{mutator: m}
	want := map[string]*string{"retention.ms": strp("1000")}

	if n, _ := unreportedPass(t, drv, m, topic, map[string]string{"retention.ms": "5"}, want); n != 1 {
		t.Fatalf("drift: %d alters, want 1", n)
	}
	if n, _ := unreportedPass(t, drv, m, topic, map[string]string{"retention.ms": "1000"}, want); n != 0 {
		t.Fatalf("in shape: %d alters, want 0", n)
	}
	for pass := 1; pass <= 2; pass++ {
		if n, changes := unreportedPass(t, drv, m, topic, map[string]string{"retention.ms": "7"}, want); n != 1 || len(changes) != 1 {
			t.Fatalf("drift again (%d): %d alters, %d changes; want 1 and 1", pass, n, len(changes))
		}
	}
}

// A rejected alter is not remembered: the next pass tries again.
func TestRejectedAlterIsNotRemembered(t *testing.T) {
	const topic = "rejected.orders"
	m := &fakeMutator{alterErr: errInvalidConfigForTest}
	drv := &Driver{mutator: m}
	want := map[string]*string{"min.compaction.lag.ms": strp("1")}
	for pass := 1; pass <= 2; pass++ {
		err := drv.alignTopic(context.Background(), registry.EnsureRequest{Name: topic, Namespace: "unreported", BucketyName: "orders"},
			&topicView{partitions: 1, rf: 1, configs: map[string]string{}}, 0, 0, want)
		if err == nil {
			t.Fatalf("pass %d: rejected alter returned no error", pass)
		}
	}
	if len(m.alters) != 2 {
		t.Fatalf("rejected alter attempted %d times in 2 passes, want 2", len(m.alters))
	}
}

// Deleting (or re-creating) the topic forgets what was accepted:
// a topic of the same name starts from what its broker reports.
func TestForgetAcceptedOnDelete(t *testing.T) {
	const topic = "forget.orders"
	m := &fakeMutator{}
	drv := &Driver{mutator: m}
	want := map[string]*string{"min.compaction.lag.ms": strp("1")}
	reported := map[string]string{}
	if n, _ := unreportedPass(t, drv, m, topic, reported, want); n != 1 {
		t.Fatalf("first pass: %d alters", n)
	}
	// DeleteBuckety forgets before it asks the broker; the broker
	// here is unreachable, so the call itself fails fast.
	kc, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer kc.Close()
	drv.kclient, drv.aclient = kc, kadm.NewClient(kc)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = drv.DeleteBuckety(ctx, registry.DeleteRequest{Name: topic})
	if n, _ := unreportedPass(t, drv, m, topic, reported, want); n != 1 {
		t.Fatalf("after DeleteBuckety: %d alters, want 1", n)
	}
}
