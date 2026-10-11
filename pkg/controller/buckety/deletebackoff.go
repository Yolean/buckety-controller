package buckety

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// Requeue delays while recursive deletion is in progress: prompt
// while slices make progress, doubling up to the cap while passes
// remove nothing (a writer racing the teardown, contents the
// listing does not show), so a stuck teardown costs a list and a
// delete call every two minutes instead of every two seconds.
const (
	deletionRequeue    = 2 * time.Second
	deletionRequeueCap = 2 * time.Minute
)

// deletionBackoff tracks the next requeue delay per Buckety in
// teardown. The zero value is ready to use.
type deletionBackoff struct {
	mu    sync.Mutex
	delay map[types.UID]time.Duration
}

// next returns the requeue delay after a deletion pass. A pass
// that made progress resets the delay; a stalled pass doubles it.
func (b *deletionBackoff) next(uid types.UID, stalled bool) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !stalled {
		delete(b.delay, uid)
		return deletionRequeue
	}
	if b.delay == nil {
		b.delay = map[types.UID]time.Duration{}
	}
	d, ok := b.delay[uid]
	if !ok {
		d = deletionRequeue
	} else {
		d = min(2*d, deletionRequeueCap)
	}
	b.delay[uid] = d
	return d
}

// forget drops the Buckety's state once its teardown completed.
func (b *deletionBackoff) forget(uid types.UID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.delay, uid)
}
