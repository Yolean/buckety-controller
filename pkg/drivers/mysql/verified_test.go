package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// cachingDriver is testDriver with the passed-check memory on and a
// clock the test moves.
func cachingDriver(fc *fakeConn) (*Driver, *time.Time) {
	d := testDriver(fc)
	d.verifyInterval = 30 * time.Minute
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d.now = func() time.Time { return now }
	return d, &now
}

// Steady state: one login per access per interval, not per pass.
func TestGrantVerifiedSkipsLogin(t *testing.T) {
	ctx := context.Background()
	fc := &fakeConn{logins: []loginResult{{grants: allRW()}}}
	d, now := cachingDriver(fc)
	req := grantReq("ReadWrite", withPassword(secretPassword))
	for range 3 {
		res, err := d.GrantAccess(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if string(res.SecretData["password"]) != secretPassword || res.Principal != "b_t1_app@%" || res.Minted {
			t.Fatalf("result from memory: principal=%q minted=%v", res.Principal, res.Minted)
		}
		*now = now.Add(5 * time.Minute)
	}
	if len(fc.loginCalls) != 1 || len(fc.execs) != 0 {
		t.Fatalf("3 passes within the interval: %d logins, statements %q", len(fc.loginCalls), fc.queries())
	}

	// Past the interval: checked again.
	*now = now.Add(30 * time.Minute)
	if _, err := d.GrantAccess(ctx, req); err != nil {
		t.Fatal(err)
	}
	if len(fc.loginCalls) != 2 {
		t.Errorf("after the interval: %d logins, want 2", len(fc.loginCalls))
	}
}

// Anything the access's spec or Secret changes is checked on the
// next pass.
func TestGrantVerifiedKeyChanges(t *testing.T) {
	ctx := context.Background()
	fc := &fakeConn{logins: []loginResult{{grants: allRW()}}}
	d, _ := cachingDriver(fc)
	if _, err := d.GrantAccess(ctx, grantReq("ReadWrite", withPassword(secretPassword))); err != nil {
		t.Fatal(err)
	}

	// Password changed in the Secret.
	if _, err := d.GrantAccess(ctx, grantReq("ReadWrite", withPassword(secretPassword+"x"))); err != nil {
		t.Fatal(err)
	}
	if len(fc.loginCalls) != 2 {
		t.Fatalf("password change: %d logins, want 2", len(fc.loginCalls))
	}

	// Role changed: logs in and revokes.
	if _, err := d.GrantAccess(ctx, grantReq("Reader", withPassword(secretPassword+"x"))); err != nil {
		t.Fatal(err)
	}
	if len(fc.loginCalls) != 3 || len(fc.execs) != 1 {
		t.Fatalf("role change: %d logins, statements %q", len(fc.loginCalls), fc.queries())
	}

	// userHost changed: another account.
	d.cfg.UserHost = "10.0.0.0/255.0.0.0"
	if _, err := d.GrantAccess(ctx, grantReq("Reader", withPassword(secretPassword+"x"))); err != nil {
		t.Fatal(err)
	}
	if len(fc.loginCalls) != 4 {
		t.Fatalf("userHost change: %d logins, want 4", len(fc.loginCalls))
	}
}

// A pass that failed, or that did not verify, is not remembered.
func TestGrantVerifiedOnlyAfterSuccess(t *testing.T) {
	ctx := context.Background()
	fc := &fakeConn{logins: []loginResult{{err: errors.New("dial tcp: connection refused")}, {grants: allRW()}}}
	d, _ := cachingDriver(fc)
	req := grantReq("ReadWrite", withPassword(secretPassword))
	if _, err := d.GrantAccess(ctx, req); err == nil {
		t.Fatal("login error swallowed")
	}
	if _, err := d.GrantAccess(ctx, req); err != nil {
		t.Fatal(err)
	}
	if len(fc.loginCalls) != 2 {
		t.Fatalf("pass after a failure: %d logins, want 2", len(fc.loginCalls))
	}

	// A generated password is not verified in its own pass.
	fc2 := &fakeConn{logins: []loginResult{{grants: allRW()}}}
	d2, _ := cachingDriver(fc2)
	res, err := d2.GrantAccess(ctx, grantReq("ReadWrite", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d2.GrantAccess(ctx, grantReq("ReadWrite", res.SecretData)); err != nil {
		t.Fatal(err)
	}
	if len(fc2.loginCalls) != 1 {
		t.Fatalf("first pass with the minted password: %d logins, want 1", len(fc2.loginCalls))
	}
}

// RevokeAccess forgets: a re-created access is checked again.
func TestGrantVerifiedForgottenOnRevoke(t *testing.T) {
	ctx := context.Background()
	fc := &fakeConn{logins: []loginResult{{grants: allRW()}}}
	d, _ := cachingDriver(fc)
	req := grantReq("ReadWrite", withPassword(secretPassword))
	res, err := d.GrantAccess(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.RevokeAccess(ctx, res.Principal); err != nil {
		t.Fatal(err)
	}
	if _, err := d.GrantAccess(ctx, req); err != nil {
		t.Fatal(err)
	}
	if len(fc.loginCalls) != 2 {
		t.Fatalf("after revoke: %d logins, want 2", len(fc.loginCalls))
	}
}

// The memory holds no password.
func TestVerifyKeyHashesPassword(t *testing.T) {
	k := verifyKey(account{"u", "%"}, "p", []string{"SELECT"}, secretPassword)
	if strings.Contains(k, secretPassword) {
		t.Fatal("key holds the password")
	}
}
