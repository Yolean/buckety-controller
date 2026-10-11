package cadence

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func TestStartupDefersOnlyTheFirstSettledPass(t *testing.T) {
	var s Startup
	w := 5 * time.Minute
	a := types.NamespacedName{Namespace: "n", Name: "a"}
	if d := s.Defer(a, true, w); d < time.Second || d > w+time.Second {
		t.Fatalf("first settled pass deferred %v, want within (1s, %v]", d, w+time.Second)
	}
	if d := s.Defer(a, true, w); d != 0 {
		t.Errorf("second pass deferred %v", d)
	}

	b := types.NamespacedName{Namespace: "n", Name: "b"}
	if d := s.Defer(b, false, w); d != 0 {
		t.Errorf("unsettled first pass deferred %v", d)
	}
	if d := s.Defer(b, true, w); d != 0 {
		t.Errorf("pass after an unsettled first pass deferred %v", d)
	}

	c := types.NamespacedName{Namespace: "n", Name: "c"}
	if d := s.Defer(c, true, 0); d != 0 {
		t.Errorf("zero window deferred %v", d)
	}

	s.Forget(a)
	if d := s.Defer(a, true, w); d == 0 {
		t.Error("a forgotten key is new again")
	}

	seen := map[time.Duration]bool{}
	for i := range 50 {
		seen[s.Defer(types.NamespacedName{Name: string(rune('A' + i))}, true, w)] = true
	}
	if len(seen) < 40 {
		t.Errorf("50 first passes got %d distinct delays; want them spread", len(seen))
	}
}
