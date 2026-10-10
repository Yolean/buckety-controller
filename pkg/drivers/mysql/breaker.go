package mysql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
)

// errBackendUnreachable is what operations return while the breaker
// holds them back after connection failures.
var errBackendUnreachable = errors.New("backend unreachable")

// Breaker cooldowns: the first connection failure holds operations
// back for breakerMin, each consecutive one doubles it up to
// breakerMax.
const (
	breakerMin = 2 * time.Second
	breakerMax = 5 * time.Minute
)

// breakerConn fails fast after connection-level failures, so that a
// server that is down, or a network or TLS path that is broken, gets
// one connection attempt per cooldown from this controller instead
// of one per operation of every reconcile.
//
// The servers count failed handshakes per client host and block the
// host after max_connect_errors (default 100) of them, until
// FLUSH HOSTS: every consumer behind the same address then loses the
// database. A few dozen accesses re-checked in a loop of failures
// reach that quickly; a cooldown that grows per consecutive failure
// keeps the count low. Server answers (access denied, a refused
// statement) mean the connection worked and never trip it.
type breakerConn struct {
	inner conn
	now   func() time.Time

	mu       sync.Mutex
	failures int
	until    time.Time
	last     error
}

func newBreakerConn(inner conn) *breakerConn {
	return &breakerConn{inner: inner, now: time.Now}
}

// admit returns an error while the breaker is open. Once a cooldown
// has passed it lets one operation through and holds the others
// back for another cooldown until that one reports.
func (b *breakerConn) admit() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures == 0 {
		return nil
	}
	now := b.now()
	if now.Before(b.until) {
		return fmt.Errorf("%w after %d consecutive connection failures (last: %v); next attempt after %s",
			errBackendUnreachable, b.failures, b.last, b.until.UTC().Format(time.RFC3339))
	}
	b.until = now.Add(b.cooldown())
	return nil
}

func (b *breakerConn) cooldown() time.Duration {
	d := breakerMin
	for i := 1; i < b.failures && d < breakerMax; i++ {
		d *= 2
	}
	return min(d, breakerMax)
}

// report records the outcome of an admitted operation.
func (b *breakerConn) report(ctx context.Context, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case err == nil || serverAnswered(err):
		b.failures, b.last = 0, nil
	case ctx.Err() != nil:
		// The caller's context ended; that says nothing about the
		// server.
	default:
		b.failures++
		b.last = err
		b.until = b.now().Add(b.cooldown())
	}
}

// serverAnswered reports whether err came from the server, over a
// working connection.
func serverAnswered(err error) bool {
	var me *gomysql.MySQLError
	return errors.As(err, &me) || errors.Is(err, errLoginDenied) || errors.Is(err, errShadowed)
}

func (b *breakerConn) exec(ctx context.Context, query string, args ...any) error {
	if err := b.admit(); err != nil {
		return err
	}
	err := b.inner.exec(ctx, query, args...)
	b.report(ctx, err)
	return err
}

func (b *breakerConn) lookupSchema(ctx context.Context, name string) (*schemaInfo, error) {
	if err := b.admit(); err != nil {
		return nil, err
	}
	s, err := b.inner.lookupSchema(ctx, name)
	b.report(ctx, err)
	return s, err
}

func (b *breakerConn) countContent(ctx context.Context, name string) (int, error) {
	if err := b.admit(); err != nil {
		return 0, err
	}
	n, err := b.inner.countContent(ctx, name)
	b.report(ctx, err)
	return n, err
}

func (b *breakerConn) login(ctx context.Context, acct account, password, pattern string) (*grants, error) {
	if err := b.admit(); err != nil {
		return nil, err
	}
	g, err := b.inner.login(ctx, acct, password, pattern)
	b.report(ctx, err)
	return g, err
}
