package mysql

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// fakeConn records statements and plays scripted answers. It never
// prints what it records: tests compare passwords, they don't log
// them.
type fakeConn struct {
	execs   []execCall
	onExec  func(query string, args []any) error
	schemas map[string]*schemaInfo
	content map[string]int
	// logins are consumed in order; the last one repeats.
	logins     []loginResult
	loginCalls []loginCall
	calls      int
}

type execCall struct {
	query string
	args  []any
}

type loginResult struct {
	grants *grants
	err    error
}

type loginCall struct {
	acct     account
	password string
	pattern  string
}

func (f *fakeConn) exec(_ context.Context, query string, args ...any) error {
	f.calls++
	f.execs = append(f.execs, execCall{query, args})
	if f.onExec != nil {
		return f.onExec(query, args)
	}
	return nil
}

func (f *fakeConn) lookupSchema(_ context.Context, name string) (*schemaInfo, error) {
	f.calls++
	return f.schemas[name], nil
}

func (f *fakeConn) countContent(_ context.Context, name string) (int, error) {
	f.calls++
	return f.content[name], nil
}

func (f *fakeConn) login(_ context.Context, acct account, password, pattern string) (*grants, error) {
	f.calls++
	f.loginCalls = append(f.loginCalls, loginCall{acct, password, pattern})
	if len(f.logins) == 0 {
		return nil, errLoginDenied
	}
	r := f.logins[0]
	if len(f.logins) > 1 {
		f.logins = f.logins[1:]
	}
	return r.grants, r.err
}

func (f *fakeConn) queries() []string {
	out := make([]string, len(f.execs))
	for i, e := range f.execs {
		out[i] = e.query
	}
	return out
}

func allRW() *grants { return &grants{privileges: slices.Clone(readWritePrivileges)} }

func grantReq(role string, secret map[string][]byte) registry.GrantRequest {
	return registry.GrantRequest{
		BucketyName: "b_t1_orders", AccessNamespace: "t1", AccessName: "app",
		Role: role, ExistingSecretData: secret,
	}
}

const secretPassword = "Secret-Pw-0123456789"

func withPassword(p string) map[string][]byte {
	return map[string][]byte{"password": []byte(p), "username": []byte("ignored")}
}

func TestValidateParameters(t *testing.T) {
	d := testDriver(nil)
	if err := d.ValidateParameters(map[string]string{"characterSet": "utf8mb4", "collation": "utf8mb4_unicode_ci"}); err != nil {
		t.Fatal(err)
	}
	if err := d.ValidateParameters(nil); err != nil {
		t.Fatal(err)
	}
	for _, p := range []map[string]string{
		{"charset": "utf8mb4"},
		{"characterSet": "utf8"},
		{"characterSet": "UTF8MB4"},
		{"characterSet": "utf8mb4; DROP DATABASE mysql"},
		{"characterSet": "utf8mb4`"},
		{"collation": "utf8_general_ci"},
		{"collation": "utf8mb4 COLLATE x"},
		{"collation": ""},
	} {
		if err := d.ValidateParameters(p); err == nil {
			t.Errorf("parameters %v accepted", p)
		}
	}
}

func TestValidateUpdateParameters(t *testing.T) {
	d := testDriver(nil)
	same := map[string]string{"characterSet": "utf8mb4"}
	if err := d.ValidateUpdateParameters(same, same); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][2]map[string]string{
		{{"characterSet": "utf8mb4"}, {"characterSet": "latin1"}},
		{{}, {"collation": "utf8mb4_bin"}},
		{{"collation": "utf8mb4_bin"}, {}},
	} {
		if err := d.ValidateUpdateParameters(c[0], c[1]); err == nil {
			t.Errorf("%v -> %v accepted", c[0], c[1])
		}
	}
}

func TestValidateAccessParameters(t *testing.T) {
	d := testDriver(nil)
	if err := d.ValidateAccessParameters(nil); err != nil {
		t.Fatal(err)
	}
	if err := d.ValidateAccessParameters(map[string]string{"host": "x"}); err == nil {
		t.Fatal("access parameters accepted")
	}
}

func TestInspectBuckety(t *testing.T) {
	fc := &fakeConn{
		schemas: map[string]*schemaInfo{"b_full": {}, "b_empty": {}},
		content: map[string]int{"b_full": 3},
	}
	d := testDriver(fc)
	for name, want := range map[string]registry.Inspection{
		"b_missing": {},
		"b_empty":   {Exists: true, Empty: true},
		"b_full":    {Exists: true, Empty: false},
	} {
		got, err := d.InspectBuckety(context.Background(), name)
		if err != nil || got != want {
			t.Errorf("InspectBuckety(%s) = %+v, %v", name, got, err)
		}
	}
	fc.calls = 0
	if _, err := d.InspectBuckety(context.Background(), "mysql"); err == nil || fc.calls != 0 {
		t.Errorf("outside the prefix: err=%v calls=%d", err, fc.calls)
	}
}

func TestEnsureBucketyCreates(t *testing.T) {
	fc := &fakeConn{schemas: map[string]*schemaInfo{}}
	fc.onExec = func(q string, _ []any) error {
		fc.schemas["b_t1_orders"] = &schemaInfo{charset: "utf8mb4", collation: "utf8mb4_unicode_ci"}
		return nil
	}
	err := testDriver(fc).EnsureBuckety(context.Background(), registry.EnsureRequest{
		Name:       "b_t1_orders",
		Parameters: map[string]string{"characterSet": "utf8mb4", "collation": "utf8mb4_unicode_ci"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"CREATE DATABASE IF NOT EXISTS `b_t1_orders` CHARACTER SET `utf8mb4` COLLATE `utf8mb4_unicode_ci`"}
	if !slices.Equal(fc.queries(), want) {
		t.Fatalf("statements: %q", fc.queries())
	}
}

func TestEnsureBucketyExisting(t *testing.T) {
	fc := &fakeConn{schemas: map[string]*schemaInfo{"b_x": {charset: "utf8mb4", collation: "utf8mb4_unicode_ci"}}}
	d := testDriver(fc)
	ctx := context.Background()
	if err := d.EnsureBuckety(ctx, registry.EnsureRequest{Name: "b_x", Parameters: map[string]string{"characterSet": "utf8mb4", "collation": "utf8mb4_unicode_ci"}}); err != nil {
		t.Fatal(err)
	}
	// Server spelling may differ in case.
	fc.schemas["b_x"] = &schemaInfo{charset: "UTF8MB4", collation: "UTF8MB4_UNICODE_CI"}
	if err := d.EnsureBuckety(ctx, registry.EnsureRequest{Name: "b_x", Parameters: map[string]string{"characterSet": "utf8mb4"}}); err != nil {
		t.Fatal(err)
	}
	if len(fc.execs) != 0 {
		t.Fatalf("existing database altered: %q", fc.queries())
	}
}

func TestEnsureBucketyDrift(t *testing.T) {
	fc := &fakeConn{schemas: map[string]*schemaInfo{"b_x": {charset: "latin1", collation: "latin1_swedish_ci"}}}
	d := testDriver(fc)
	for _, p := range []map[string]string{{"characterSet": "utf8mb4"}, {"collation": "latin1_bin"}} {
		err := d.EnsureBuckety(context.Background(), registry.EnsureRequest{Name: "b_x", Parameters: p})
		if !registry.IsParameterDrift(err) {
			t.Errorf("%v: want ParameterDrift, got %v", p, err)
		}
	}
	if len(fc.execs) != 0 {
		t.Fatalf("drift was 'repaired': %q", fc.queries())
	}
	// Parameters absent: nothing managed, nothing drifts.
	if err := d.EnsureBuckety(context.Background(), registry.EnsureRequest{Name: "b_x"}); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureBucketyRefusals(t *testing.T) {
	fc := &fakeConn{schemas: map[string]*schemaInfo{}}
	d := testDriver(fc)
	for _, req := range []registry.EnsureRequest{
		{Name: "mysql"},
		{Name: "keycloak"},
		{Name: "b_x`; DROP DATABASE mysql; --"},
		{Name: "b_x", Parameters: map[string]string{"characterSet": "utf8mb4 COLLATE x"}},
	} {
		if err := d.EnsureBuckety(context.Background(), req); err == nil {
			t.Errorf("%+v accepted", req)
		}
	}
	if fc.calls != 0 {
		t.Fatalf("refused requests reached the server: %d calls", fc.calls)
	}
	// Created but invisible: the admin grants do not cover it.
	if err := d.EnsureBuckety(context.Background(), registry.EnsureRequest{Name: "b_x"}); err == nil || !strings.Contains(err.Error(), "not visible") {
		t.Fatalf("invisible database: %v", err)
	}
}

func TestDeleteBuckety(t *testing.T) {
	fc := &fakeConn{}
	d := testDriver(fc)
	if err := d.DeleteBuckety(context.Background(), registry.DeleteRequest{Name: "b_t1_orders"}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fc.queries(), []string{"DROP DATABASE IF EXISTS `b_t1_orders`"}) {
		t.Fatalf("statements: %q", fc.queries())
	}
	for _, n := range []string{"mysql", "sys", "keycloak", ""} {
		if err := d.DeleteBuckety(context.Background(), registry.DeleteRequest{Name: n}); err == nil {
			t.Errorf("DeleteBuckety(%q) accepted", n)
		}
	}
	if len(fc.execs) != 1 {
		t.Fatalf("refused deletes reached the server: %q", fc.queries())
	}
}

func TestGrantFirstMint(t *testing.T) {
	fc := &fakeConn{}
	res, err := testDriver(fc).GrantAccess(context.Background(), grantReq("ReadWrite", nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(fc.loginCalls) != 0 {
		t.Errorf("first mint logged in %d times; nothing to verify yet", len(fc.loginCalls))
	}
	if len(fc.execs) != 2 || fc.execs[0].query != createUserSQL || !strings.HasPrefix(fc.execs[1].query, "GRANT SELECT, INSERT") {
		t.Fatalf("statements: %q", fc.queries())
	}
	pw := string(res.SecretData["password"])
	if !slices.Equal(fc.execs[0].args, []any{"b_t1_app", "%", pw}) {
		t.Error("CREATE USER arguments are not (user, host, the Secret's password)")
	}
	if !slices.Equal(fc.execs[1].args, []any{"b_t1_app", "%"}) {
		t.Errorf("GRANT arguments: %v", fc.execs[1].args)
	}
	if !strings.Contains(fc.execs[1].query, "ON `b\\_t1\\_orders`.* TO ?@?") {
		t.Errorf("GRANT is not scoped to the escaped database: %s", fc.execs[1].query)
	}
	if err := validatePassword(pw); err != nil || len(pw) != passwordLen {
		t.Errorf("generated password: %v", err)
	}
	if res.Principal != "b_t1_app@%" || !res.Scoped || !res.Revocable || !res.Minted {
		t.Errorf("result: principal=%q scoped=%v revocable=%v minted=%v", res.Principal, res.Scoped, res.Revocable, res.Minted)
	}
}

func TestGrantSecretContents(t *testing.T) {
	fc := &fakeConn{logins: []loginResult{{grants: allRW()}}}
	res, err := testDriver(fc).GrantAccess(context.Background(), grantReq("ReadWrite", withPassword(secretPassword)))
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(res.SecretData))
	for k := range res.SecretData {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if !slices.Equal(keys, []string{"database", "host", "jdbcUrl", "password", "port", "url", "username"}) {
		t.Fatalf("keys: %v", keys)
	}
	want := map[string]string{
		"host":     "mysql.mysql.svc.cluster.local",
		"port":     "3306",
		"database": "b_t1_orders",
		"username": "b_t1_app",
		"jdbcUrl":  "jdbc:mariadb://mysql.mysql.svc.cluster.local:3306/b_t1_orders",
	}
	for k, v := range want {
		if got := string(res.SecretData[k]); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if string(res.SecretData["password"]) != secretPassword {
		t.Error("password is not the Secret's")
	}
	u, err := url.Parse(string(res.SecretData["url"]))
	if err != nil {
		t.Fatal("url does not parse")
	}
	p, _ := u.User.Password()
	if u.Scheme != "mysql" || u.User.Username() != "b_t1_app" || p != secretPassword || u.Host != "mysql.mysql.svc.cluster.local:3306" || u.Path != "/b_t1_orders" {
		t.Error("url does not carry user, password, host:port and database")
	}
	if res.Minted {
		t.Error("existing user reported as minted")
	}
}

func TestGrantSteadyStateWritesNothing(t *testing.T) {
	fc := &fakeConn{logins: []loginResult{{grants: allRW()}}}
	if _, err := testDriver(fc).GrantAccess(context.Background(), grantReq("", withPassword(secretPassword))); err != nil {
		t.Fatal(err)
	}
	if len(fc.execs) != 0 {
		t.Fatalf("steady state wrote: %q", fc.queries())
	}
	lc := fc.loginCalls[0]
	if lc.acct != (account{"b_t1_app", "%"}) || lc.password != secretPassword || lc.pattern != `b\_t1\_orders` {
		t.Errorf("login: acct=%v pattern=%q secretPassword=%v", lc.acct, lc.pattern, lc.password == secretPassword)
	}
}

func TestGrantRepairsGrants(t *testing.T) {
	cases := []struct {
		name string
		role string
		have *grants
		want []string
	}{
		{"missing INSERT", "ReadWrite", &grants{privileges: without(readWritePrivileges, []string{"INSERT"})},
			[]string{"GRANT INSERT ON `b\\_t1\\_orders`.* TO ?@?"}},
		{"no grants at all", "Reader", &grants{},
			[]string{"GRANT SELECT ON `b\\_t1\\_orders`.* TO ?@?"}},
		{"extra privileges and grant option", "ReadWrite", &grants{privileges: append(slices.Clone(readWritePrivileges), "EXECUTE", "CREATE VIEW"), grantOption: true},
			[]string{"REVOKE EXECUTE, CREATE VIEW ON `b\\_t1\\_orders`.* FROM ?@?", "REVOKE GRANT OPTION ON `b\\_t1\\_orders`.* FROM ?@?"}},
		{"role downgraded to Reader", "Reader", allRW(),
			[]string{"REVOKE INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES, CREATE TEMPORARY TABLES, LOCK TABLES ON `b\\_t1\\_orders`.* FROM ?@?"}},
	}
	for _, c := range cases {
		fc := &fakeConn{logins: []loginResult{{grants: c.have}}}
		if _, err := testDriver(fc).GrantAccess(context.Background(), grantReq(c.role, withPassword(secretPassword))); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !slices.Equal(fc.queries(), c.want) {
			t.Errorf("%s:\n got %q\nwant %q", c.name, fc.queries(), c.want)
		}
	}
}

func TestGrantRefusesUnexpectedPrivilegeNames(t *testing.T) {
	fc := &fakeConn{logins: []loginResult{{grants: &grants{privileges: append(slices.Clone(readWritePrivileges), "EXECUTE ON *.* TO x; --")}}}}
	if _, err := testDriver(fc).GrantAccess(context.Background(), grantReq("ReadWrite", withPassword(secretPassword))); err == nil {
		t.Fatal("privilege name from the server spliced unchecked")
	}
	if len(fc.execs) != 0 {
		t.Fatalf("statements: %q", fc.queries())
	}
}

func TestGrantRecreatesMissingUser(t *testing.T) {
	fc := &fakeConn{logins: []loginResult{{err: errLoginDenied}, {grants: allRW()}}}
	res, err := testDriver(fc).GrantAccess(context.Background(), grantReq("ReadWrite", withPassword(secretPassword)))
	if err != nil {
		t.Fatal(err)
	}
	if len(fc.execs) != 2 || fc.execs[0].query != createUserSQL || fc.execs[0].args[2] != secretPassword {
		t.Fatalf("statements: %q", fc.queries())
	}
	if len(fc.loginCalls) != 2 {
		t.Errorf("login calls: %d (check + verify)", len(fc.loginCalls))
	}
	if !res.Minted || string(res.SecretData["password"]) != secretPassword {
		t.Errorf("minted=%v, password kept=%v", res.Minted, string(res.SecretData["password"]) == secretPassword)
	}
}

func TestGrantAppliesChangedPassword(t *testing.T) {
	fc := &fakeConn{logins: []loginResult{{err: errLoginDenied}, {grants: allRW()}}}
	fc.onExec = func(q string, _ []any) error {
		if q == createUserSQL {
			return &gomysql.MySQLError{Number: erCannotUser, Message: "Operation CREATE USER failed for 'b_t1_app'@'%'"}
		}
		return nil
	}
	res, err := testDriver(fc).GrantAccess(context.Background(), grantReq("ReadWrite", withPassword(secretPassword)))
	if err != nil {
		t.Fatal(err)
	}
	q := fc.queries()
	if len(q) < 2 || q[0] != createUserSQL || q[1] != alterUserSQL || fc.execs[1].args[2] != secretPassword {
		t.Fatalf("statements: %q", q)
	}
	if res.Minted {
		t.Error("re-keyed user reported as minted")
	}
}

// A user created in this pass holds nothing: no REVOKE, which
// MySQL 8.4 refuses for privileges that are not held.
func TestGrantFirstMintReaderRevokesNothing(t *testing.T) {
	fc := &fakeConn{}
	if _, err := testDriver(fc).GrantAccess(context.Background(), grantReq("Reader", nil)); err != nil {
		t.Fatal(err)
	}
	q := fc.queries()
	if len(q) != 2 || q[0] != createUserSQL || !strings.HasPrefix(q[1], "GRANT SELECT ON ") {
		t.Fatalf("statements: %q", q)
	}
}

// A re-keyed user's grants are unknown: the rest of the managed set
// is revoked one privilege at a time, and "no such grant" is the
// expected answer for each privilege it does not hold.
func TestGrantRekeyedReaderRevokesEachPrivilege(t *testing.T) {
	fc := &fakeConn{}
	fc.onExec = func(q string, _ []any) error {
		switch {
		case q == createUserSQL:
			return &gomysql.MySQLError{Number: erCannotUser}
		case strings.HasPrefix(q, "REVOKE INSERT "):
			return nil
		case strings.HasPrefix(q, "REVOKE "):
			return &gomysql.MySQLError{Number: erNoSuchGrant}
		}
		return nil
	}
	// No password in the Secret: deleting it rotates the password.
	if _, err := testDriver(fc).GrantAccess(context.Background(), grantReq("Reader", nil)); err != nil {
		t.Fatal(err)
	}
	var revokes []string
	for _, q := range fc.queries() {
		if strings.HasPrefix(q, "REVOKE ") {
			revokes = append(revokes, q)
		}
	}
	if len(revokes) != len(readWritePrivileges)-1 {
		t.Fatalf("want one REVOKE per non-Reader privilege, got %q", revokes)
	}

	fc.onExec = func(q string, _ []any) error {
		switch {
		case q == createUserSQL:
			return &gomysql.MySQLError{Number: erCannotUser}
		case strings.HasPrefix(q, "REVOKE "):
			return &gomysql.MySQLError{Number: 1227}
		}
		return nil
	}
	if _, err := testDriver(fc).GrantAccess(context.Background(), grantReq("Reader", nil)); err == nil {
		t.Fatal("a REVOKE refused for another reason was swallowed")
	}
}

func TestGrantVerificationFailure(t *testing.T) {
	orig := verifyBackoff
	verifyBackoff = 0
	t.Cleanup(func() { verifyBackoff = orig })
	fc := &fakeConn{logins: []loginResult{{err: errLoginDenied}}}
	_, err := testDriver(fc).GrantAccess(context.Background(), grantReq("ReadWrite", withPassword(secretPassword)))
	if err == nil || !strings.Contains(err.Error(), "userHost") {
		t.Fatalf("want an error pointing at userHost, got %v", err)
	}
	if strings.Contains(err.Error(), secretPassword) {
		t.Fatal("error contains the password")
	}
	if len(fc.loginCalls) != 1+verifyAttempts {
		t.Errorf("login calls: %d", len(fc.loginCalls))
	}
	// The user this pass created is dropped again.
	last := fc.execs[len(fc.execs)-1]
	if last.query != dropUserSQL || !slices.Equal(last.args, []any{"b_t1_app", "%"}) {
		t.Errorf("last statement: %s %v", last.query, last.args)
	}
}

func TestGrantLoginErrorIsNotRepair(t *testing.T) {
	fc := &fakeConn{logins: []loginResult{{err: errors.New("dial tcp: connection refused")}}}
	if _, err := testDriver(fc).GrantAccess(context.Background(), grantReq("ReadWrite", withPassword(secretPassword))); err == nil {
		t.Fatal("login error swallowed")
	}
	if len(fc.execs) != 0 {
		t.Fatalf("an unreachable server is no reason to re-key: %q", fc.queries())
	}
}

// Server messages of statements that carry a password are dropped:
// a parse error quotes the statement, possibly cut mid-password.
func TestGrantKeepsPasswordOutOfErrors(t *testing.T) {
	for _, number := range []uint16{1064, 1819} {
		fc := &fakeConn{}
		fc.onExec = func(q string, args []any) error {
			pw := args[len(args)-1].(string)
			return &gomysql.MySQLError{Number: number, Message: fmt.Sprintf("near '%s' at line 1", pw[:10])}
		}
		_, err := testDriver(fc).GrantAccess(context.Background(), grantReq("ReadWrite", nil))
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("server error %d", number)) {
			t.Fatalf("want the error number, got %v", err)
		}
		if strings.Contains(err.Error(), fc.execs[0].args[2].(string)[:10]) || strings.Contains(err.Error(), "near") {
			t.Fatal("error carries the server message, and with it part of the password")
		}
	}
	// Client-side errors keep their text, minus the password.
	fc := &fakeConn{}
	fc.onExec = func(q string, args []any) error {
		return fmt.Errorf("write tcp: broken pipe after %v", args[len(args)-1])
	}
	_, err := testDriver(fc).GrantAccess(context.Background(), grantReq("ReadWrite", nil))
	if err == nil || !strings.Contains(err.Error(), "broken pipe") || !strings.Contains(err.Error(), "<redacted>") {
		t.Fatalf("client error: %v", err)
	}
}

func TestGrantRefusals(t *testing.T) {
	cases := map[string]registry.GrantRequest{
		"Writer role":      grantReq("Writer", nil),
		"unknown role":     grantReq("Admin", nil),
		"database outside": {BucketyName: "keycloak", AccessNamespace: "t1", AccessName: "app", Role: "ReadWrite"},
		"system database":  {BucketyName: "mysql", AccessNamespace: "t1", AccessName: "app", Role: "ReadWrite"},
		"no access name":   {BucketyName: "b_t1_orders", Role: "ReadWrite"},
		"unsafe password":  grantReq("ReadWrite", withPassword("Robert'); DROP USER root; --")),
		"short password":   grantReq("ReadWrite", withPassword("Short-1")),
	}
	for name, req := range cases {
		fc := &fakeConn{}
		_, err := testDriver(fc).GrantAccess(context.Background(), req)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if fc.calls != 0 {
			t.Errorf("%s: reached the server (%d calls)", name, fc.calls)
		}
		if strings.Contains(err.Error(), "DROP USER root") {
			t.Errorf("%s: error echoes the Secret's password", name)
		}
	}
}

func TestRevokeAccess(t *testing.T) {
	fc := &fakeConn{}
	d := testDriver(fc)
	ctx := context.Background()
	if err := d.RevokeAccess(ctx, ""); err != nil || fc.calls != 0 {
		t.Fatalf("empty principal: %v, %d calls", err, fc.calls)
	}
	if err := d.RevokeAccess(ctx, "b_t1_app@%"); err != nil {
		t.Fatal(err)
	}
	if len(fc.execs) != 1 || fc.execs[0].query != dropUserSQL || !slices.Equal(fc.execs[0].args, []any{"b_t1_app", "%"}) {
		t.Fatalf("statements: %v", fc.execs)
	}
	for _, p := range []string{"root@localhost", "buckety@%", "keycloak@%", "b_x@'%'", "nonsense"} {
		if err := d.RevokeAccess(ctx, p); err == nil {
			t.Errorf("RevokeAccess(%q) accepted", p)
		}
	}
	if len(fc.execs) != 1 {
		t.Fatalf("refused revokes reached the server: %v", fc.execs)
	}
}

// The principal is user@host with the host it was created with, so
// a userHost change revokes the old account, not a new one.
func TestPrincipalCarriesHost(t *testing.T) {
	fc := &fakeConn{}
	d := testDriver(fc)
	d.cfg.UserHost = "10.42.%"
	res, err := d.GrantAccess(context.Background(), grantReq("Reader", nil))
	if err != nil {
		t.Fatal(err)
	}
	if res.Principal != "b_t1_app@10.42.%" {
		t.Fatalf("principal %q", res.Principal)
	}
}

// After a userHost change the login matches the account for the
// previous host: repair creates the new account, and a verification
// that still lands on the old one succeeds without looping.
func TestGrantAcrossUserHostChange(t *testing.T) {
	shadowed := fmt.Errorf("%w: logged in as b_t1_app@%% instead of b_t1_app@10.42.%%", errShadowed)
	fc := &fakeConn{logins: []loginResult{{err: shadowed}}}
	d := testDriver(fc)
	d.cfg.UserHost = "10.42.%"
	res, err := d.GrantAccess(context.Background(), grantReq("ReadWrite", withPassword(secretPassword)))
	if err != nil {
		t.Fatal(err)
	}
	if fc.execs[0].query != createUserSQL || !slices.Equal(fc.execs[0].args[:2], []any{"b_t1_app", "10.42.%"}) {
		t.Fatalf("statements: %q %v", fc.queries(), fc.execs[0].args[:2])
	}
	if len(fc.loginCalls) != 2 {
		t.Errorf("login calls: %d (check + one verify)", len(fc.loginCalls))
	}
	if res.Principal != "b_t1_app@10.42.%" || !res.Minted {
		t.Errorf("principal %q minted=%v", res.Principal, res.Minted)
	}
}
