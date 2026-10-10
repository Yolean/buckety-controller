package cadence

import (
	"testing"
	"time"
)

func TestRecheckSpreads(t *testing.T) {
	if got := Recheck(0); got != 0 {
		t.Errorf("disabled re-check became %v", got)
	}
	d := 5 * time.Minute
	lo, hi := d-time.Duration(Spread*float64(d)), d+time.Duration(Spread*float64(d))
	seen := map[time.Duration]bool{}
	var sum time.Duration
	const n = 2000
	for range n {
		got := Recheck(d)
		if got < lo || got > hi {
			t.Fatalf("Recheck(%v) = %v outside [%v, %v]", d, got, lo, hi)
		}
		seen[got] = true
		sum += got
	}
	if len(seen) < n/2 {
		t.Errorf("only %d distinct delays in %d draws: not spread", len(seen), n)
	}
	if mean := sum / n; mean < d-10*time.Second || mean > d+10*time.Second {
		t.Errorf("mean %v drifted from %v", mean, d)
	}
}
