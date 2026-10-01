package mysql

import (
	"slices"
	"strings"
	"testing"
)

func TestRolePrivileges(t *testing.T) {
	r, err := rolePrivileges("Reader")
	if err != nil || !slices.Equal(r, []string{"SELECT"}) {
		t.Fatalf("Reader = %v, %v", r, err)
	}
	wantRW := []string{
		"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "INDEX",
		"DROP", "REFERENCES", "CREATE TEMPORARY TABLES", "LOCK TABLES",
	}
	for _, role := range []string{"ReadWrite", ""} {
		rw, err := rolePrivileges(role)
		if err != nil || !slices.Equal(rw, wantRW) {
			t.Fatalf("%q = %v, %v", role, rw, err)
		}
	}
	for _, role := range []string{"Writer", "Owner", "readwrite"} {
		if _, err := rolePrivileges(role); err == nil {
			t.Errorf("role %q accepted", role)
		}
	}
}

func TestGrantAndRevokeSQL(t *testing.T) {
	q, err := grantSQL(readWritePrivileges, grantPattern("b_t1_orders"))
	if err != nil {
		t.Fatal(err)
	}
	want := "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES, CREATE TEMPORARY TABLES, LOCK TABLES ON `b\\_t1\\_orders`.* TO ?@?"
	if q != want {
		t.Errorf("grantSQL:\n got %s\nwant %s", q, want)
	}
	q, err = revokeSQL([]string{"INSERT", "EXECUTE"}, grantPattern("b_x"))
	if err != nil || q != "REVOKE INSERT, EXECUTE ON `b\\_x`.* FROM ?@?" {
		t.Errorf("revokeSQL = %s, %v", q, err)
	}
	if q := revokeGrantOptionSQL(grantPattern("b_x")); q != "REVOKE GRANT OPTION ON `b\\_x`.* FROM ?@?" {
		t.Errorf("revokeGrantOptionSQL = %s", q)
	}
	for _, bad := range [][]string{nil, {""}, {"select"}, {"SELECT; DROP DATABASE mysql"}, {"ALL PRIVILEGES ON *.* TO x"}, {"SELECT,INSERT"}} {
		if _, err := revokeSQL(bad, "b\\_x"); err == nil {
			t.Errorf("revokeSQL(%q) accepted", bad)
		}
	}
}

func TestDatabaseSQL(t *testing.T) {
	cases := map[string][3]string{
		"CREATE DATABASE IF NOT EXISTS `b_x`":                                                      {"b_x", "", ""},
		"CREATE DATABASE IF NOT EXISTS `b_x` CHARACTER SET `utf8mb4`":                              {"b_x", "utf8mb4", ""},
		"CREATE DATABASE IF NOT EXISTS `b_x` CHARACTER SET `utf8mb4` COLLATE `utf8mb4_unicode_ci`": {"b_x", "utf8mb4", "utf8mb4_unicode_ci"},
		"CREATE DATABASE IF NOT EXISTS `b_x-y` COLLATE `latin1_swedish_ci`":                        {"b_x-y", "", "latin1_swedish_ci"},
	}
	for want, in := range cases {
		if got := createDatabaseSQL(in[0], in[1], in[2]); got != want {
			t.Errorf("createDatabaseSQL%v = %s", in, got)
		}
	}
	if got := dropDatabaseSQL("b_x"); got != "DROP DATABASE IF EXISTS `b_x`" {
		t.Errorf("dropDatabaseSQL = %s", got)
	}
}

func TestAccountStatementsUsePlaceholders(t *testing.T) {
	for _, q := range []string{createUserSQL, alterUserSQL, dropUserSQL} {
		if !strings.Contains(q, "?@?") {
			t.Errorf("%q does not pass the account as arguments", q)
		}
	}
}

func TestWithout(t *testing.T) {
	got := without([]string{"a", "b", "c"}, []string{"b"})
	if !slices.Equal(got, []string{"a", "c"}) {
		t.Fatalf("without = %v", got)
	}
}
