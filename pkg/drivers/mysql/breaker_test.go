package mysql

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
)

// scriptedConn answers every operation with err and counts calls.
type scriptedConn struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (s *scriptedConn) answer() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.err
}

func (s *scriptedConn) set(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *scriptedConn) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *scriptedConn) exec(context.Context, string, ...any) error { return s.answer() }
func (s *scriptedConn) lookupSchema(context.Context, string) (*schemaInfo, error) {
	return nil, s.answer()
}
func (s *scriptedConn) countContent(context.Context, string) (int, error) { return 0, s.answer() }
func (s *scriptedConn) login(context.Context, account, string, string) (*grants, error) {
	return nil, s.answer()
}

func testBreaker(inner conn) (*breakerConn, *time.Time) {
	b := newBreakerConn(inner)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b.now = func() time.Time { return now }
	return b, &now
}

var errDial = errors.New("dial tcp 10.0.0.1:3306: connect: connection refused")

// After a connection failure, operations from every reconcile fail
// fast; one reaches the server per cooldown.
func TestBreakerHoldsBackAfterConnectionFailure(t *testing.T) {
	ctx := context.Background()
	in := &scriptedConn{err: errDial}
	b, now := testBreaker(in)
	if err := b.exec(ctx, "SELECT 1"); !errors.Is(err, errDial) {
		t.Fatalf("first failure: %v", err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := b.lookupSchema(ctx, "b_x")
			if !errors.Is(err, errBackendUnreachable) {
				t.Errorf("held-back operation: %v", err)
			}
		}()
	}
	wg.Wait()
	if in.count() != 1 {
		t.Fatalf("operations reaching the server within the cooldown: %d, want 1", in.count())
	}

	// Cooldown over: one attempt, which fails again, doubling it.
	*now = now.Add(breakerMin)
	_, _ = b.login(ctx, account{"u", "%"}, "p", "x")
	_, _ = b.countContent(ctx, "b_x")
	if in.count() != 2 {
		t.Fatalf("after the first cooldown: %d calls, want 2", in.count())
	}
	*now = now.Add(breakerMin)
	_ = b.exec(ctx, "SELECT 1")
	if in.count() != 2 {
		t.Fatalf("second cooldown is not doubled: %d calls", in.count())
	}

	// Recovery: the next admitted operation succeeds and resets.
	in.set(nil)
	*now = now.Add(breakerMin)
	if err := b.exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
	for range 3 {
		if err := b.exec(ctx, "SELECT 1"); err != nil {
			t.Fatalf("closed breaker: %v", err)
		}
	}
	if in.count() != 6 {
		t.Fatalf("calls after recovery: %d, want 6", in.count())
	}
}

// Server answers mean the connection works.
func TestBreakerIgnoresServerAnswers(t *testing.T) {
	ctx := context.Background()
	for _, err := range []error{
		errLoginDenied,
		errShadowed,
		&gomysql.MySQLError{Number: erAccessDenied, Message: "Access denied"},
		&gomysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"},
	} {
		in := &scriptedConn{err: err}
		b, _ := testBreaker(in)
		for range 5 {
			_ = b.exec(ctx, "SELECT 1")
		}
		if in.count() != 5 {
			t.Errorf("%v tripped the breaker: %d of 5 calls reached the server", err, in.count())
		}
	}
}

// The caller's own deadline or cancellation says nothing about the
// server.
func TestBreakerIgnoresCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	in := &scriptedConn{err: context.Canceled}
	b, _ := testBreaker(in)
	_ = b.exec(ctx, "SELECT 1")
	_ = b.exec(context.Background(), "SELECT 1")
	if in.count() != 2 {
		t.Fatalf("a cancelled caller tripped the breaker: %d calls", in.count())
	}
}

func TestBreakerCooldownCaps(t *testing.T) {
	b, _ := testBreaker(&scriptedConn{})
	for f, want := range map[int]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 5: 32 * time.Second, 8: 256 * time.Second, 9: breakerMax, 50: breakerMax} {
		b.failures = f
		if got := b.cooldown(); got != want {
			t.Errorf("failures=%d: cooldown %v, want %v", f, got, want)
		}
	}
}

// newDriver puts the breaker in front of the server connection.
func TestNewDriverHasBreaker(t *testing.T) {
	d, err := newDriver(&Config{Host: "db", Port: 3306, AdminUser: "buckety", AdminPassword: "x", NamePrefix: "b_", UserHost: "%"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.conn.(*breakerConn); !ok {
		t.Fatalf("conn is %T", d.conn)
	}
}
