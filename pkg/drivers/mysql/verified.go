package mysql

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"
)

// defaultVerifyInterval is the default of the backend config key
// accountCheckInterval: how long an access whose login check passed
// is taken as converged while nothing about it changes.
//
// The check logs in as the access's user: one new authenticated
// connection per access per re-check, the only way to see that the
// user exists with the Secret's password and to read its grants
// (the admin account cannot read other users' grants). Within the
// interval a pass with the same account, grant pattern, role and
// password skips it. The trade-off: a user dropped, re-keyed or
// granted more out of band is repaired within the interval instead
// of within one re-check. Changes made through the API (Secret
// password, role, userHost, the database) change the key and are
// applied on the next pass, and EnsureBuckety still checks the
// database itself on every pass.
const defaultVerifyInterval = 30 * time.Minute

// verifiedAccounts remembers, per account, the last check that
// passed. Only passes that logged in and found (or left) the grants
// aligned are recorded; errors and unverified passes never are.
type verifiedAccounts struct {
	mu   sync.Mutex
	seen map[string]verification
}

type verification struct {
	key string
	at  time.Time
}

// verifyKey identifies what a check verified. The password is
// hashed so the map holds no credential.
func verifyKey(acct account, pattern string, want []string, password string) string {
	h := sha256.Sum256([]byte(password))
	return strings.Join([]string{acct.String(), pattern, strings.Join(want, ","), hex.EncodeToString(h[:])}, "\x00")
}

// fresh reports whether key was verified for acct within interval
// before now.
func (v *verifiedAccounts) fresh(acct account, key string, now time.Time, interval time.Duration) bool {
	if interval <= 0 {
		return false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	e, ok := v.seen[acct.String()]
	return ok && e.key == key && now.Sub(e.at) < interval
}

func (v *verifiedAccounts) record(acct account, key string, now time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.seen == nil {
		v.seen = map[string]verification{}
	}
	v.seen[acct.String()] = verification{key: key, at: now}
}

func (v *verifiedAccounts) forget(acct account) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.seen, acct.String())
}
