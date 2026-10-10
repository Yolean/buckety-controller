// Package cadence spreads the periodic re-checks the two
// reconcilers return from every settled pass.
package cadence

import (
	"math/rand/v2"
	"time"
)

// Spread is the fraction a re-check may land early or late.
const Spread = 0.1

// Recheck returns d moved by a random amount within ±Spread of d;
// zero (re-checks disabled) stays zero. Without it every object
// re-checks exactly d after its previous pass, so objects that
// reconciled together (all of them, after a controller start)
// call their backends together on every period, indefinitely.
// The random walk lets such a group drift apart over a few
// periods while the mean cadence stays d.
func Recheck(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d + time.Duration((rand.Float64()*2-1)*Spread*float64(d))
}
