package mysql

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Database-level privileges per BucketyAccess role, spelled as
// information_schema.SCHEMA_PRIVILEGES reports them.
var (
	readerPrivileges    = []string{"SELECT"}
	readWritePrivileges = []string{
		"SELECT", "INSERT", "UPDATE", "DELETE",
		"CREATE", "ALTER", "INDEX", "DROP", "REFERENCES",
		"CREATE TEMPORARY TABLES", "LOCK TABLES",
	}
)

// rolePrivileges maps BucketyAccess.spec.role to a grant set.
// Writer is refused rather than guessed: see docs/mysql.md.
func rolePrivileges(role string) ([]string, error) {
	switch role {
	case "Reader":
		return readerPrivileges, nil
	case "", "ReadWrite":
		return readWritePrivileges, nil
	case "Writer":
		return nil, fmt.Errorf("mysql: role Writer is not supported; use Reader (SELECT) or ReadWrite (data and schema changes)")
	default:
		return nil, fmt.Errorf("mysql: unknown role %q", role)
	}
}

// privilegeRE: privilege keywords only. Names read back from the
// server are spliced into REVOKE, so they are checked like input.
var privilegeRE = regexp.MustCompile(`^[A-Z]+( [A-Z]+)*$`)

// Account statements. ? arguments are interpolated client-side as
// escaped string literals (Config.clientConfig); the values are
// validated before they get here.
const (
	createUserSQL = "CREATE USER ?@? IDENTIFIED BY ?"
	alterUserSQL  = "ALTER USER ?@? IDENTIFIED BY ?"
	dropUserSQL   = "DROP USER IF EXISTS ?@?"
)

const (
	schemaSQL = "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?"
	// contentSQL counts what makes a database non-empty for the
	// adoption gate: tables, views, routines and events.
	contentSQL = "SELECT" +
		" (SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?)" +
		" + (SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ?)" +
		" + (SELECT COUNT(*) FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ?)"
	currentUserSQL = "SELECT CURRENT_USER()"
	// privilegesSQL runs as the created user, who sees its own rows.
	privilegesSQL = "SELECT PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.SCHEMA_PRIVILEGES WHERE GRANTEE = ? AND TABLE_SCHEMA = ?"
)

func createDatabaseSQL(name, charset, collation string) string {
	var b strings.Builder
	b.WriteString("CREATE DATABASE IF NOT EXISTS ")
	b.WriteString(quoteIdent(name))
	if charset != "" {
		b.WriteString(" CHARACTER SET ")
		b.WriteString(quoteIdent(charset))
	}
	if collation != "" {
		b.WriteString(" COLLATE ")
		b.WriteString(quoteIdent(collation))
	}
	return b.String()
}

func dropDatabaseSQL(name string) string {
	return "DROP DATABASE IF EXISTS " + quoteIdent(name)
}

// grantSQL and revokeSQL take the escaped grant pattern of one
// database; the account goes in as two ? arguments.
func grantSQL(privileges []string, pattern string) (string, error) {
	list, err := privilegeList(privileges)
	if err != nil {
		return "", err
	}
	return "GRANT " + list + " ON " + quoteIdent(pattern) + ".* TO ?@?", nil
}

func revokeSQL(privileges []string, pattern string) (string, error) {
	list, err := privilegeList(privileges)
	if err != nil {
		return "", err
	}
	return "REVOKE " + list + " ON " + quoteIdent(pattern) + ".* FROM ?@?", nil
}

func revokeGrantOptionSQL(pattern string) string {
	return "REVOKE GRANT OPTION ON " + quoteIdent(pattern) + ".* FROM ?@?"
}

func privilegeList(privileges []string) (string, error) {
	if len(privileges) == 0 {
		return "", fmt.Errorf("mysql: empty privilege list")
	}
	for _, p := range privileges {
		if !privilegeRE.MatchString(p) {
			return "", fmt.Errorf("mysql: unexpected privilege name %q", p)
		}
	}
	return strings.Join(privileges, ", "), nil
}

// without returns the elements of a not in b, in a's order.
func without(a, b []string) []string {
	var out []string
	for _, x := range a {
		if !slices.Contains(b, x) {
			out = append(out, x)
		}
	}
	return out
}
