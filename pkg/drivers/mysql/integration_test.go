package mysql

// Integration test against a real MariaDB or MySQL server, skipped
// unless BUCKETY_MYSQL_TEST_DSN holds the controller account's DSN
// in go-sql-driver form, for an account set up as in docs/mysql.md.
// test/integration/mysql.sh runs it against every supported server
// in Docker, as CI does; against a server of your own:
//
//	BUCKETY_MYSQL_TEST_DSN="buckety:${PW}@tcp(127.0.0.1:3306)/" \
//	  go test ./pkg/drivers/mysql -run TestIntegration -v -count=1
//
// Optional: BUCKETY_MYSQL_TEST_PREFIX (default b_) must match the
// account's grant pattern; BUCKETY_MYSQL_TEST_USER_HOST (default %)
// must admit the address the test connects from;
// BUCKETY_MYSQL_TEST_ALT_USER_HOST (e.g. 127.0.0.1 through a
// port-forward, or auto for the address the server sees) enables
// the userHost-change step.
//
// The test creates one database and two users named after a random
// run id under the prefix, and drops them at the end. It never
// prints a password.

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"maps"
	"net"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestIntegration(t *testing.T) {
	dsn := os.Getenv("BUCKETY_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("BUCKETY_MYSQL_TEST_DSN not set; see the comment at the top of integration_test.go")
	}
	parsed, err := gomysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("BUCKETY_MYSQL_TEST_DSN does not parse as a go-sql-driver DSN")
	}
	host, portStr, err := net.SplitHostPort(parsed.Addr)
	if err != nil {
		t.Fatalf("DSN address %q: %v", parsed.Addr, err)
	}
	port, _ := strconv.Atoi(portStr)
	cfg := &Config{
		Host: host, Port: port,
		AdminUser: parsed.User, AdminPassword: parsed.Passwd,
		NamePrefix: envOr("BUCKETY_MYSQL_TEST_PREFIX", defaultNamePrefix),
		UserHost:   envOr("BUCKETY_MYSQL_TEST_USER_HOST", defaultUserHost),
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	d, err := newDriver(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sc := d.conn.(*sqlConn)
	admin := sc.admin
	ctx := context.Background()

	idb := make([]byte, 4)
	_, _ = rand.Read(idb)
	run := "it" + hex.EncodeToString(idb)
	db := cfg.NamePrefix + run + "_db"
	pattern := quoteIdent(grantPattern(db))
	params := map[string]string{"characterSet": "utf8mb4", "collation": "utf8mb4_unicode_ci"}
	rwReq := registry.GrantRequest{BucketyName: db, AccessNamespace: run, AccessName: "rw", Role: "ReadWrite"}
	// A long namespace exercises the hashed user name form.
	roReq := registry.GrantRequest{BucketyName: db, AccessNamespace: run + "-a-rather-long-namespace", AccessName: "ro", Role: "Reader"}

	var principals []string
	t.Cleanup(func() {
		for _, p := range principals {
			_ = d.RevokeAccess(ctx, p)
		}
		_ = d.DeleteBuckety(ctx, registry.DeleteRequest{Name: db})
	})

	// connect logs in with a Secret's coordinates.
	connect := func(t *testing.T, secret map[string][]byte) *sql.DB {
		t.Helper()
		c := sc.base.Clone()
		c.User = string(secret["username"])
		c.Passwd = string(secret["password"])
		c.Addr = net.JoinHostPort(string(secret["host"]), string(secret["port"]))
		c.DBName = string(secret["database"])
		conn, err := gomysql.NewConnector(c)
		if err != nil {
			t.Fatal(err)
		}
		u := sql.OpenDB(conn)
		u.SetMaxOpenConns(1)
		t.Cleanup(func() { u.Close() })
		return u
	}
	mustExec := func(t *testing.T, conn *sql.DB, q string, args ...any) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	denied := func(t *testing.T, conn *sql.DB, q string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, q)
		var me *gomysql.MySQLError
		if !errors.As(err, &me) || (me.Number != 1142 && me.Number != 1044 && me.Number != 1227) {
			t.Fatalf("%s: want a privilege error, got %v", q, err)
		}
	}
	loginDenied := func(t *testing.T, secret map[string][]byte) {
		t.Helper()
		err := connect(t, secret).PingContext(ctx)
		if !isMySQLError(err, erAccessDenied) {
			t.Fatalf("login as %s: want access denied, got %v", secret["username"], err)
		}
	}
	grant := func(t *testing.T, req registry.GrantRequest, existing map[string][]byte) registry.GrantResult {
		t.Helper()
		req.ExistingSecretData = existing
		res, err := d.GrantAccess(ctx, req)
		if err != nil {
			t.Fatalf("GrantAccess %s/%s: %v", req.AccessNamespace, req.AccessName, err)
		}
		if !slices.Contains(principals, res.Principal) {
			principals = append(principals, res.Principal)
		}
		return res
	}

	// Steps build on each other: stop at the first failure.
	step := func(name string, f func(t *testing.T)) {
		t.Helper()
		if !t.Run(name, f) {
			t.FailNow()
		}
	}

	step("database is created", func(t *testing.T) {
		if in, err := d.InspectBuckety(ctx, db); err != nil || in.Exists {
			t.Fatalf("before create: %+v, %v", in, err)
		}
		for i := 0; i < 2; i++ {
			if err := d.EnsureBuckety(ctx, registry.EnsureRequest{Name: db, Parameters: params}); err != nil {
				t.Fatalf("EnsureBuckety #%d: %v", i, err)
			}
		}
		if in, err := d.InspectBuckety(ctx, db); err != nil || !in.Exists || !in.Empty {
			t.Fatalf("after create: %+v, %v", in, err)
		}
	})

	rw := grant(t, rwReq, nil)
	step("ReadWrite user can migrate and write", func(t *testing.T) {
		if !rw.Minted || !rw.Scoped || !rw.Revocable {
			t.Fatalf("result flags: %+v", rw.Principal)
		}
		want := "jdbc:mariadb://" + net.JoinHostPort(host, portStr) + "/" + db
		if string(rw.SecretData["jdbcUrl"]) != want || string(rw.SecretData["database"]) != db {
			t.Fatalf("Secret coordinates: jdbcUrl=%s database=%s", rw.SecretData["jdbcUrl"], rw.SecretData["database"])
		}
		u := connect(t, rw.SecretData)
		mustExec(t, u, "CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(20))")
		mustExec(t, u, "INSERT INTO items VALUES (1, 'one')")
		mustExec(t, u, "ALTER TABLE items ADD COLUMN note VARCHAR(10)")
		mustExec(t, u, "CREATE INDEX items_name ON items (name)")
		mustExec(t, u, "CREATE TEMPORARY TABLE scratch (x INT)")
		var n int
		if err := u.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&n); err != nil || n != 1 {
			t.Fatalf("select: %d, %v", n, err)
		}
		// Scoped to its database: no other schema, no new
		// databases - including one an unescaped "_" in the grant
		// pattern would have matched.
		denied(t, u, "SELECT * FROM mysql.user")
		denied(t, u, "CREATE DATABASE "+quoteIdent(cfg.NamePrefix+run+"_other"))
		lookalike := []byte(db)
		for i := range lookalike {
			if lookalike[i] == '_' {
				lookalike[i] = '-'
			}
		}
		denied(t, u, "CREATE DATABASE "+quoteIdent(string(lookalike)))
		denied(t, u, "CREATE USER 'nope'@'%'")
	})

	step("database with tables is not empty", func(t *testing.T) {
		if in, err := d.InspectBuckety(ctx, db); err != nil || !in.Exists || in.Empty {
			t.Fatalf("%+v, %v", in, err)
		}
	})

	ro := grant(t, roReq, nil)
	step("Reader user can only read", func(t *testing.T) {
		if u := string(ro.SecretData["username"]); len(u) > maxUserNameLen {
			t.Fatalf("user name %q too long", u)
		}
		u := connect(t, ro.SecretData)
		var n int
		if err := u.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&n); err != nil || n != 1 {
			t.Fatalf("select: %d, %v", n, err)
		}
		denied(t, u, "INSERT INTO items (id, name) VALUES (2, 'two')")
		denied(t, u, "CREATE TABLE t2 (x INT)")
		denied(t, u, "DROP TABLE items")
	})

	step("steady state keeps the Secret", func(t *testing.T) {
		again := grant(t, rwReq, rw.SecretData)
		if again.Minted || !maps.EqualFunc(again.SecretData, rw.SecretData, func(a, b []byte) bool { return string(a) == string(b) }) {
			t.Fatal("a reconcile with an intact Secret changed it")
		}
	})

	step("missing grant is restored", func(t *testing.T) {
		mustExec(t, admin, "REVOKE INSERT ON "+pattern+".* FROM ?@?", string(rw.SecretData["username"]), cfg.UserHost)
		u := connect(t, rw.SecretData)
		denied(t, u, "INSERT INTO items (id, name) VALUES (3, 'three')")
		grant(t, rwReq, rw.SecretData)
		mustExec(t, connect(t, rw.SecretData), "INSERT INTO items (id, name) VALUES (3, 'three')")
	})

	step("extra privileges and grant option are revoked", func(t *testing.T) {
		mustExec(t, admin, "GRANT EXECUTE ON "+pattern+".* TO ?@?", string(rw.SecretData["username"]), cfg.UserHost)
		mustExec(t, admin, "GRANT SELECT ON "+pattern+".* TO ?@? WITH GRANT OPTION", string(rw.SecretData["username"]), cfg.UserHost)
		grant(t, rwReq, rw.SecretData)
		acct := account{User: string(rw.SecretData["username"]), Host: cfg.UserHost}
		g, err := sc.login(ctx, acct, string(rw.SecretData["password"]), grantPattern(db))
		if err != nil {
			t.Fatal(err)
		}
		if g.grantOption || slices.Contains(g.privileges, "EXECUTE") || len(without(readWritePrivileges, g.privileges)) != 0 {
			t.Fatalf("privileges after repair: %v grantOption=%v", g.privileges, g.grantOption)
		}
	})

	step("dropped user is recreated with the Secret's password", func(t *testing.T) {
		mustExec(t, admin, "DROP USER ?@?", string(rw.SecretData["username"]), cfg.UserHost)
		loginDenied(t, rw.SecretData)
		res := grant(t, rwReq, rw.SecretData)
		if !res.Minted || string(res.SecretData["password"]) != string(rw.SecretData["password"]) {
			t.Fatal("recreated user: want minted with the Secret's password")
		}
		var n int
		if err := connect(t, rw.SecretData).QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&n); err != nil || n != 2 {
			t.Fatalf("data after recreate: %d, %v", n, err)
		}
	})

	step("password changed in the Secret is applied", func(t *testing.T) {
		pw, err := generatePassword()
		if err != nil {
			t.Fatal(err)
		}
		edited := maps.Clone(rw.SecretData)
		edited["password"] = []byte(pw)
		res := grant(t, rwReq, edited)
		if string(res.SecretData["password"]) != pw || res.Minted {
			t.Fatal("the Secret's new password was not kept")
		}
		if err := connect(t, edited).PingContext(ctx); err != nil {
			t.Fatalf("login with the new password: %v", err)
		}
		loginDenied(t, rw.SecretData)
		rw = res
	})

	step("role change re-grants", func(t *testing.T) {
		toReader := rwReq
		toReader.Role = "Reader"
		grant(t, toReader, rw.SecretData)
		denied(t, connect(t, rw.SecretData), "INSERT INTO items (id, name) VALUES (4, 'four')")
		grant(t, rwReq, rw.SecretData)
		mustExec(t, connect(t, rw.SecretData), "INSERT INTO items (id, name) VALUES (4, 'four')")
	})

	step("a downgrade to Writer drops the user, and back restores it", func(t *testing.T) {
		toWriter := rwReq
		toWriter.Role, toWriter.ExistingSecretData = "Writer", rw.SecretData
		if _, err := d.GrantAccess(ctx, toWriter); err == nil {
			t.Fatal("Writer accepted")
		}
		loginDenied(t, rw.SecretData)
		back := grant(t, rwReq, rw.SecretData)
		if string(back.SecretData["password"]) != string(rw.SecretData["password"]) {
			t.Fatal("the Secret's password was not kept")
		}
		mustExec(t, connect(t, rw.SecretData), "INSERT INTO items (id, name) VALUES (6, 'six')")
		mustExec(t, connect(t, rw.SecretData), "DELETE FROM items WHERE id = 6")
	})

	step("the server enforces the lock wait timeout", func(t *testing.T) {
		var v int
		if err := admin.QueryRowContext(ctx, "SELECT @@SESSION.lock_wait_timeout").Scan(&v); err != nil || v != int(lockWaitTimeout/time.Second) {
			t.Fatalf("lock_wait_timeout = %d, %v", v, err)
		}
	})

	step("deleting the Secret rotates the password", func(t *testing.T) {
		// The re-keyed path: the user exists, its grants are unknown,
		// and the Reader must still be refused the ReadWrite set.
		rotated := grant(t, roReq, nil)
		if rotated.Minted || string(rotated.SecretData["password"]) == string(ro.SecretData["password"]) {
			t.Fatalf("minted=%v, password changed=%v", rotated.Minted, string(rotated.SecretData["password"]) != string(ro.SecretData["password"]))
		}
		loginDenied(t, ro.SecretData)
		u := connect(t, rotated.SecretData)
		var n int
		if err := u.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&n); err != nil {
			t.Fatalf("select after rotation: %v", err)
		}
		denied(t, u, "INSERT INTO items (id, name) VALUES (9, 'nine')")
		ro = rotated
	})

	step("character set drift is reported, not repaired", func(t *testing.T) {
		mustExec(t, admin, "ALTER DATABASE "+quoteIdent(db)+" CHARACTER SET latin1 COLLATE latin1_swedish_ci")
		err := d.EnsureBuckety(ctx, registry.EnsureRequest{Name: db, Parameters: params})
		if !registry.IsParameterDrift(err) {
			t.Fatalf("want ParameterDrift, got %v", err)
		}
		s, err := sc.lookupSchema(ctx, db)
		if err != nil || s.charset != "latin1" {
			t.Fatalf("driver altered the database: %+v, %v", s, err)
		}
		mustExec(t, admin, "ALTER DATABASE "+quoteIdent(db)+" CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
		if err := d.EnsureBuckety(ctx, registry.EnsureRequest{Name: db, Parameters: params}); err != nil {
			t.Fatal(err)
		}
	})

	step("names outside the prefix are refused", func(t *testing.T) {
		if err := d.EnsureBuckety(ctx, registry.EnsureRequest{Name: "mysql"}); err == nil {
			t.Error("EnsureBuckety(mysql) accepted")
		}
		if err := d.DeleteBuckety(ctx, registry.DeleteRequest{Name: "mysql"}); err == nil {
			t.Error("DeleteBuckety(mysql) accepted")
		}
		if err := d.RevokeAccess(ctx, "root@localhost"); err == nil {
			t.Error("RevokeAccess(root@localhost) accepted")
		}
		if err := d.RevokeAccess(ctx, cfg.AdminUser+"@%"); err == nil {
			t.Error("RevokeAccess of the admin account accepted")
		}
	})

	// Optional: a userHost other than the default that also admits
	// the test's address (127.0.0.1 through a port-forward). "auto"
	// takes the address the server sees this test connect from.
	alt := os.Getenv("BUCKETY_MYSQL_TEST_ALT_USER_HOST")
	if alt == "auto" {
		var self string
		if err := admin.QueryRowContext(ctx, "SELECT HOST FROM information_schema.PROCESSLIST WHERE ID = CONNECTION_ID()").Scan(&self); err != nil {
			t.Fatalf("own address for BUCKETY_MYSQL_TEST_ALT_USER_HOST=auto: %v", err)
		}
		if h, _, err := net.SplitHostPort(self); err == nil {
			self = h
		}
		alt = self
		if alt == cfg.UserHost || checkHost(alt) != nil {
			t.Logf("connecting from %q: no usable alternative userHost, skipping the userHost change", self)
			alt = ""
		}
	}
	if alt != "" {
		step("userHost change moves the account", func(t *testing.T) {
			altCfg := *cfg
			altCfg.UserHost = alt
			d2 := &Driver{cfg: &altCfg, conn: d.conn}
			moved, err := d2.GrantAccess(ctx, registry.GrantRequest{BucketyName: db, AccessNamespace: run, AccessName: "rw", Role: "ReadWrite", ExistingSecretData: rw.SecretData})
			if err != nil {
				t.Fatalf("grant under the new userHost: %v", err)
			}
			principals = append(principals, moved.Principal)
			if moved.Principal == rw.Principal || !moved.Minted {
				t.Fatalf("principal %q minted=%v", moved.Principal, moved.Minted)
			}
			// What the reconciler does next: revoke the replaced principal.
			if err := d.RevokeAccess(ctx, rw.Principal); err != nil {
				t.Fatal(err)
			}
			mustExec(t, connect(t, moved.SecretData), "INSERT INTO items (id, name) VALUES (5, 'five')")
			// And back, which passes through the shadowed case when
			// the alternative host is the more specific one.
			back := grant(t, rwReq, moved.SecretData)
			if err := d.RevokeAccess(ctx, moved.Principal); err != nil {
				t.Fatal(err)
			}
			if back.Principal != rw.Principal {
				t.Fatalf("principal after moving back: %q", back.Principal)
			}
			back = grant(t, rwReq, moved.SecretData)
			mustExec(t, connect(t, back.SecretData), "DELETE FROM items WHERE id = 5")
			rw = back
		})
	}

	step("revoke drops the users", func(t *testing.T) {
		for _, res := range []registry.GrantResult{rw, ro} {
			for i := 0; i < 2; i++ {
				if err := d.RevokeAccess(ctx, res.Principal); err != nil {
					t.Fatalf("RevokeAccess #%d: %v", i, err)
				}
			}
			loginDenied(t, res.SecretData)
		}
	})

	step("Retain keeps the database and its data", func(t *testing.T) {
		// Retain means the controller never calls DeleteBuckety;
		// the accesses are gone and the data must still be there.
		var n int
		if err := admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteIdent(db)+".items").Scan(&n); err != nil || n != 3 {
			t.Fatalf("retained rows: %d, %v", n, err)
		}
		if in, err := d.InspectBuckety(ctx, db); err != nil || !in.Exists || in.Empty {
			t.Fatalf("retained database: %+v, %v", in, err)
		}
	})

	step("Delete drops the database", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			if err := d.DeleteBuckety(ctx, registry.DeleteRequest{Name: db}); err != nil {
				t.Fatalf("DeleteBuckety #%d: %v", i, err)
			}
		}
		if in, err := d.InspectBuckety(ctx, db); err != nil || in.Exists {
			t.Fatalf("after delete: %+v, %v", in, err)
		}
	})
}
