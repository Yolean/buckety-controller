package mysql

import (
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Name limits. MariaDB allows 80-character user names but MySQL 8
// only 32; the driver keeps to the common limit so one naming
// scheme works on both.
const (
	maxDatabaseNameLen = 64
	maxUserNameLen     = 32
	maxHostLen         = 255
	maxPrefixLen       = 10
	userHashLen        = 16
)

var (
	// prefixRE: a letter, then letters/digits, ending in "_". The
	// trailing underscore keeps the prefix off every system schema
	// and account name.
	prefixRE = regexp.MustCompile(`^[a-z][a-z0-9]*_$`)
	// nameRE is the charset for databases and users. Lowercase
	// only, so lower_case_table_names cannot alias two names.
	nameRE = regexp.MustCompile(`^[a-z0-9_-]+$`)
	// hostRE covers host names, IPs, netmasks and the % and _
	// wildcards an operator may use in userHost.
	hostRE = regexp.MustCompile(`^[A-Za-z0-9.%_:/-]+$`)
	// k8sPartRE is a namespace or name usable verbatim in a user
	// name: Kubernetes characters without dots.
	k8sPartRE = regexp.MustCompile(`^[a-z0-9-]+$`)
)

// systemNames are schemas, accounts and roles the driver never
// touches, whatever the prefix.
var systemNames = []string{
	"mysql", "information_schema", "performance_schema", "sys",
	"root", "mariadb.sys", "mysql.sys", "mysql.session", "mysql.infoschema",
	"public", "healthcheck",
}

func checkPrefix(p string) error {
	if len(p) > maxPrefixLen || !prefixRE.MatchString(p) {
		return fmt.Errorf("namePrefix %q must be 2-%d characters: a lowercase letter, then lowercase letters or digits, ending with \"_\"", p, maxPrefixLen)
	}
	for _, s := range systemNames {
		if strings.HasPrefix(s, p) {
			return fmt.Errorf("namePrefix %q matches the system name %q", p, s)
		}
	}
	return nil
}

func checkHost(h string) error {
	if h == "" || len(h) > maxHostLen || !hostRE.MatchString(h) {
		return fmt.Errorf("host %q must be 1-%d characters of letters, digits and . %% _ : / -", h, maxHostLen)
	}
	return nil
}

// checkManaged is the prefix boundary: every database and user
// name reaches SQL only through here.
func (d *Driver) checkManaged(kind, name string, maxLen int) error {
	p := d.cfg.NamePrefix
	switch {
	case !strings.HasPrefix(name, p):
		return fmt.Errorf("%s name %q is outside this backend's namePrefix %q; the driver only touches names that start with it", kind, name, p)
	case len(name) == len(p):
		return fmt.Errorf("%s name %q is only the namePrefix", kind, name)
	case len(name) > maxLen:
		return fmt.Errorf("%s name %q is %d characters; the limit is %d", kind, name, len(name), maxLen)
	case !nameRE.MatchString(name):
		return fmt.Errorf("%s name %q may only contain lowercase letters, digits, \"_\" and \"-\"", kind, name)
	case slices.Contains(systemNames, name):
		return fmt.Errorf("%s name %q is reserved", kind, name)
	}
	return nil
}

func (d *Driver) checkDatabase(name string) error {
	return d.checkManaged("database", name, maxDatabaseNameLen)
}

func (d *Driver) checkUser(name string) error {
	if err := d.checkManaged("user", name, maxUserNameLen); err != nil {
		return err
	}
	if name == d.cfg.AdminUser {
		return fmt.Errorf("user name %q is the controller's own account", name)
	}
	return nil
}

// userName derives the account name for a BucketyAccess.
//
// Plain form: <prefix><namespace>_<name>, when both parts are
// dot-free and the result fits. Otherwise the hashed form:
// <prefix><readable>__<hash>, where readable is the dot-free,
// underscore-free start of namespace-name and hash is 16 base32
// characters (80 bits) of sha256(namespace/name).
//
// Kubernetes namespaces and names contain no "_", so the plain
// form has exactly one underscore after the prefix and the hashed
// form exactly two, adjacent: the forms cannot collide, and two
// accesses share a user only through a sha256 prefix collision.
func (d *Driver) userName(namespace, name string) (string, error) {
	if namespace == "" || name == "" {
		return "", fmt.Errorf("mysql: the grant request names no BucketyAccess namespace/name; the controller binary is older than this driver")
	}
	p := d.cfg.NamePrefix
	u := p + namespace + "_" + name
	if !k8sPartRE.MatchString(namespace) || !k8sPartRE.MatchString(name) || len(u) > maxUserNameLen {
		sum := sha256.Sum256([]byte(namespace + "/" + name))
		h := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:]))[:userHashLen]
		readable := strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
				return r
			}
			return '-'
		}, namespace+"-"+name)
		room := maxUserNameLen - len(p) - 2 - userHashLen
		if len(readable) > room {
			readable = readable[:room]
		}
		u = p + readable + "__" + h
	}
	if err := d.checkUser(u); err != nil {
		return "", fmt.Errorf("mysql: derived %w", err)
	}
	return u, nil
}

// account is a MySQL account: 'user'@'host'.
type account struct {
	User string
	Host string
}

// String is the principal form stamped into status.principal.
func (a account) String() string { return a.User + "@" + a.Host }

// grantee is the GRANTEE column format of information_schema.
func (a account) grantee() string { return "'" + a.User + "'@'" + a.Host + "'" }

// parsePrincipal turns a status.principal back into an account,
// refusing anything outside the prefix. Principals come from
// status, so they are validated like any other input.
func (d *Driver) parsePrincipal(principal string) (account, error) {
	user, host, ok := strings.Cut(principal, "@")
	if !ok {
		return account{}, fmt.Errorf("mysql: principal %q is not user@host; refusing to drop it", principal)
	}
	if err := d.checkUser(user); err != nil {
		return account{}, fmt.Errorf("mysql: refusing to drop principal %q: %w", principal, err)
	}
	if err := checkHost(host); err != nil {
		return account{}, fmt.Errorf("mysql: refusing to drop principal %q: %w", principal, err)
	}
	return account{User: user, Host: host}, nil
}

// quoteIdent backtick-quotes an identifier. Callers pass validated
// names; the doubling keeps the quoting correct regardless.
func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// grantPattern escapes the LIKE wildcards that GRANT ... ON db.*
// interprets in database names: an unescaped "_" in b_t1_orders
// would also grant on b_t1xorders. The escaped form is also what
// information_schema.SCHEMA_PRIVILEGES reports as TABLE_SCHEMA.
func grantPattern(db string) string {
	return strings.NewReplacer(`\`, `\\`, `_`, `\_`, `%`, `\%`).Replace(db)
}
