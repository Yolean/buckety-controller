package mysql

import (
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"strings"
	"testing"
)

func testDriver(fc *fakeConn) *Driver {
	return &Driver{
		cfg: &Config{
			Host: "mysql.mysql.svc.cluster.local", Port: 3306,
			AdminUser: "buckety", AdminPassword: "not-used-by-fakes",
			NamePrefix: "b_", UserHost: "%",
		},
		conn: fc,
	}
}

func TestCheckPrefix(t *testing.T) {
	for _, p := range []string{"b_", "buckety_", "x1_", "abcdefghi_"} {
		if err := checkPrefix(p); err != nil {
			t.Errorf("checkPrefix(%q): %v", p, err)
		}
	}
	for _, p := range []string{
		"", "b", "_", "B_", "1_", "b-", "b__x", "b.", "abcdefghij_",
		"performance_", "information_", // system schemas start with these
	} {
		if err := checkPrefix(p); err == nil {
			t.Errorf("checkPrefix(%q) accepted", p)
		}
	}
}

func TestCheckDatabase(t *testing.T) {
	d := testDriver(nil)
	ok := []string{"b_t1_orders", "b_shop_orders", "b_x", "b_" + strings.Repeat("a", 62)}
	for _, n := range ok {
		if err := d.checkDatabase(n); err != nil {
			t.Errorf("checkDatabase(%q): %v", n, err)
		}
	}
	bad := []string{
		"orders", "mysql", "information_schema", "keycloak", "", "b_",
		"B_x", "b_X", "b_x;drop database mysql", "b_x`", "b_x'", "b_x y", `b_x\`, "b_x.y",
		"b_x%", "b_é", "b_x\n",
		"b_" + strings.Repeat("a", 63),
	}
	for _, n := range bad {
		if err := d.checkDatabase(n); err == nil {
			t.Errorf("checkDatabase(%q) accepted", n)
		}
	}
}

func TestValidateResourceNameHintsTemplate(t *testing.T) {
	err := testDriver(nil).ValidateResourceName("orders")
	if err == nil || !strings.Contains(err.Error(), `spec.name, e.g. "b_${namespace}_${name}"`) {
		t.Fatalf("missing-prefix error should suggest a template: %v", err)
	}
}

func TestUserNamePlain(t *testing.T) {
	d := testDriver(nil)
	u, err := d.userName("shop", "orders")
	if err != nil || u != "b_shop_orders" {
		t.Fatalf("userName = %q, %v", u, err)
	}
}

func TestUserNameHashed(t *testing.T) {
	d := testDriver(nil)
	cases := [][2]string{
		{"a-rather-long-namespace-name", "orders"}, // too long
		{"t1", "orders.v2"},                        // dot
		{strings.Repeat("n", 63), strings.Repeat("m", 253)},
	}
	for _, c := range cases {
		u, err := d.userName(c[0], c[1])
		if err != nil {
			t.Fatalf("userName(%q, %q): %v", c[0], c[1], err)
		}
		if len(u) > maxUserNameLen || !strings.HasPrefix(u, "b_") || strings.Count(u[2:], "_") != 2 || !strings.Contains(u, "__") {
			t.Errorf("userName(%q, %q) = %q: not the hashed form", c[0], c[1], u)
		}
		again, _ := d.userName(c[0], c[1])
		if again != u {
			t.Errorf("userName not deterministic: %q vs %q", u, again)
		}
	}
}

// An access named after another access's hash must not land on
// that access's user: the plain form has one underscore after the
// prefix, the hashed form two.
func TestUserNameFormsCannotCollide(t *testing.T) {
	d := testDriver(nil)
	victim, err := d.userName("a-rather-long-namespace-name", "orders")
	if err != nil {
		t.Fatal(err)
	}
	body := strings.TrimPrefix(victim, "b_")
	readable, hash, _ := strings.Cut(body, "__")
	attacker, err := d.userName(readable, hash)
	if err != nil {
		t.Fatal(err)
	}
	if attacker == victim {
		t.Fatalf("namespace %q name %q maps onto %q", readable, hash, victim)
	}
	// Same with a single underscore spliced in by a plain-form
	// lookalike.
	attacker2, _ := d.userName(readable+"-", "-"+hash)
	if attacker2 == victim {
		t.Fatalf("lookalike maps onto %q", victim)
	}
}

func TestUserNameUnique(t *testing.T) {
	d := testDriver(nil)
	seen := map[string]string{}
	for i := 0; i < 3000; i++ {
		for _, pair := range [][2]string{
			{fmt.Sprintf("ns%d", i%37), fmt.Sprintf("a%d", i)},
			{fmt.Sprintf("long-namespace-%d", i%41), fmt.Sprintf("access-%d", i)},
			{"t1", fmt.Sprintf("x.%d", i)},
			{"t1", fmt.Sprintf("x-%d", i)},
		} {
			u, err := d.userName(pair[0], pair[1])
			if err != nil {
				t.Fatal(err)
			}
			key := pair[0] + "/" + pair[1]
			if prev, dup := seen[u]; dup && prev != key {
				t.Fatalf("%q and %q both map to %q", prev, key, u)
			}
			seen[u] = key
		}
	}
}

func TestUserNameHashIsSHA256Prefix(t *testing.T) {
	d := testDriver(nil)
	u, _ := d.userName("a-rather-long-namespace-name", "orders")
	sum := sha256.Sum256([]byte("a-rather-long-namespace-name/orders"))
	want := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:]))[:userHashLen]
	if !strings.HasSuffix(u, "__"+want) {
		t.Fatalf("%q does not end with the sha256 prefix %q", u, want)
	}
}

func TestUserNameNeedsIdentity(t *testing.T) {
	if _, err := testDriver(nil).userName("", "x"); err == nil {
		t.Fatal("empty namespace accepted")
	}
}

func TestCheckUserRefusesAdmin(t *testing.T) {
	d := testDriver(nil)
	d.cfg.AdminUser = "b_admin" // config validation forbids this; the check holds regardless
	if err := d.checkUser("b_admin"); err == nil {
		t.Fatal("admin account accepted as a managed user")
	}
}

func TestParsePrincipal(t *testing.T) {
	d := testDriver(nil)
	a, err := d.parsePrincipal("b_t1_reader@%")
	if err != nil || a != (account{"b_t1_reader", "%"}) {
		t.Fatalf("parsePrincipal = %+v, %v", a, err)
	}
	if a.String() != "b_t1_reader@%" || a.grantee() != "'b_t1_reader'@'%'" {
		t.Fatalf("formats: %q %q", a.String(), a.grantee())
	}
	if _, err := d.parsePrincipal("b_t1_reader@10.42.0.0/255.255.0.0"); err != nil {
		t.Errorf("netmask host: %v", err)
	}
	for _, p := range []string{
		"root@localhost", "buckety@%", "keycloak@%", "b_x", "@%", "b_x@",
		"b_x@'%'", "b_x@%' OR 1=1", "b_x@% ", "B_x@%", "b_@%", "mysql.sys@localhost",
	} {
		if _, err := d.parsePrincipal(p); err == nil {
			t.Errorf("parsePrincipal(%q) accepted", p)
		}
	}
}

func TestQuoting(t *testing.T) {
	if got := quoteIdent("a`b"); got != "`a``b`" {
		t.Errorf("quoteIdent = %s", got)
	}
	if got := grantPattern("b_t1_orders"); got != `b\_t1\_orders` {
		t.Errorf("grantPattern = %s", got)
	}
	if got := grantPattern(`b_a%\`); got != `b\_a\%\\` {
		t.Errorf("grantPattern = %s", got)
	}
}

// The example in docs/mysql.md.
func TestUserNameDocExample(t *testing.T) {
	u, err := testDriver(nil).userName("e2e-mysql-mariadb-mysql-happy-path", "orders")
	if err != nil || u != "b_e2e-mysql-ma__eug6f3psticceco5" {
		t.Fatalf("userName = %q, %v", u, err)
	}
}
