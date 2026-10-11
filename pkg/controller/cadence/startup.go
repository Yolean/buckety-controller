package cadence

import (
	"math/rand/v2"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// Startup spreads the first pass over settled objects after the
// controller starts. The informers' initial list enqueues every
// object at once, and each pass calls its backend, so without it a
// controller restart sends the whole fleet's backend calls in one
// burst (one per object per controller, back to back) and, with a
// fixed re-check cadence, keeps them together afterwards. An object
// that is settled (the caller decides: Ready for its current
// generation, nothing pending) can wait for its first pass like it
// would for a periodic re-check; anything else is reconciled at
// once. The zero value is ready to use.
type Startup struct {
	seen sync.Map
}

// Defer reports how long to postpone this pass: a random delay
// within window (at least a second) the first time key is seen by
// this process while settled, zero otherwise. Every call marks key
// as seen, so only the first pass after a start is ever deferred,
// and a zero window disables deferral.
func (s *Startup) Defer(key types.NamespacedName, settled bool, window time.Duration) time.Duration {
	_, seen := s.seen.LoadOrStore(key, struct{}{})
	if seen || !settled || window <= 0 {
		return 0
	}
	return time.Second + time.Duration(rand.Int64N(int64(window)))
}

// Forget drops key, for objects that are gone.
func (s *Startup) Forget(key types.NamespacedName) {
	s.seen.Delete(key)
}
