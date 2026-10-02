// Package mysql is the MariaDB/MySQL driver: a Buckety is a
// database, a BucketyAccess is a user with grants on that one
// database.
//
// The controller's account holds CREATE USER globally (MariaDB and
// MySQL cannot scope it by name) plus ALL on the prefix pattern.
// The driver therefore treats the backend's namePrefix as its
// security boundary: every database and account name passes
// checkManaged before it reaches SQL, and anything outside the
// prefix is refused (names.go). docs/mysql.md has the account
// setup, the naming rules and the decisions behind them.
//
// Credentials: each access gets its own user with a generated
// password, written to its Secret. GrantAccess reuses the Secret's
// password, so a password changed in the Secret is applied to the
// server, and deleting the Secret rotates it. Drift is detected by
// logging in as the user: a refused login means the user is
// missing or its password differs (both repaired by
// CREATE/ALTER USER), and a successful one reads the user's own
// grants, which the admin account cannot see without SELECT on the
// mysql schema.
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

// DriverName matches backends[].driver in buckety-controller.yaml.
const DriverName = "mysql"

// version is the driver SemVer. Injected at build time via
//
//	-ldflags '-X github.com/Yolean/buckety-controller/pkg/drivers/mysql.version=0.1.0'
//
// per SPEC §Driver versioning. Default keeps tests building.
var version = "0.1.0"

func init() {
	registry.Register(DriverName, version, factory)
}

func factory(raw json.RawMessage) (registry.Driver, error) {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return nil, err
	}
	return newDriver(cfg)
}

// newDriver connects lazily: a server that is down at controller
// start fails reconciles, not startup.
func newDriver(cfg *Config) (*Driver, error) {
	mc, err := cfg.clientConfig()
	if err != nil {
		return nil, fmt.Errorf("mysql config: %w", err)
	}
	c, err := newSQLConn(mc)
	if err != nil {
		return nil, fmt.Errorf("mysql config: %w", err)
	}
	return &Driver{cfg: cfg, conn: c}, nil
}

// Driver implements registry.Driver for MariaDB/MySQL.
type Driver struct {
	cfg  *Config
	conn conn
}

func (d *Driver) Name() string    { return DriverName }
func (d *Driver) Version() string { return version }

// InspectBuckety reports whether the database exists and whether
// it holds tables, views, routines or events.
func (d *Driver) InspectBuckety(ctx context.Context, name string) (registry.Inspection, error) {
	if err := d.checkDatabase(name); err != nil {
		return registry.Inspection{}, fmt.Errorf("mysql: %w", err)
	}
	s, err := d.conn.lookupSchema(ctx, name)
	if err != nil {
		return registry.Inspection{}, fmt.Errorf("mysql: look up database %q: %w", name, err)
	}
	if s == nil {
		return registry.Inspection{}, nil
	}
	n, err := d.conn.countContent(ctx, name)
	if err != nil {
		return registry.Inspection{}, fmt.Errorf("mysql: count objects in database %q: %w", name, err)
	}
	return registry.Inspection{Exists: true, Empty: n == 0}, nil
}

// EnsureBuckety creates the database when missing and reports a
// character set or collation that differs from the parameters as
// ParameterDrift. The driver never alters an existing database's
// defaults: ALTER DATABASE changes them for new tables only, so
// converging silently would hide that existing tables still differ.
func (d *Driver) EnsureBuckety(ctx context.Context, req registry.EnsureRequest) error {
	if err := d.checkDatabase(req.Name); err != nil {
		return fmt.Errorf("mysql: %w", err)
	}
	if err := d.ValidateParameters(req.Parameters); err != nil {
		return fmt.Errorf("mysql: %w", err)
	}
	charset, collation := req.Parameters["characterSet"], req.Parameters["collation"]
	s, err := d.conn.lookupSchema(ctx, req.Name)
	if err != nil {
		return fmt.Errorf("mysql: look up database %q: %w", req.Name, err)
	}
	if s == nil {
		if err := d.conn.exec(ctx, createDatabaseSQL(req.Name, charset, collation)); err != nil {
			return fmt.Errorf("mysql: create database %q: %w", req.Name, err)
		}
		if s, err = d.conn.lookupSchema(ctx, req.Name); err != nil {
			return fmt.Errorf("mysql: look up database %q: %w", req.Name, err)
		}
		if s == nil {
			return fmt.Errorf("mysql: database %q exists but is not visible to the controller account; its grants must cover the namePrefix %q", req.Name, d.cfg.NamePrefix)
		}
	}
	var drift []string
	if charset != "" && !strings.EqualFold(charset, s.charset) {
		drift = append(drift, fmt.Sprintf("characterSet: current=%q requested=%q", s.charset, charset))
	}
	if collation != "" && !strings.EqualFold(collation, s.collation) {
		drift = append(drift, fmt.Sprintf("collation: current=%q requested=%q", s.collation, collation))
	}
	if len(drift) > 0 {
		return &registry.ErrParameterDrift{Reason: fmt.Sprintf(
			"database %q %s; the driver does not change an existing database's defaults or convert its tables: ALTER DATABASE (and the tables) out of band, or recreate the Buckety",
			req.Name, strings.Join(drift, ", "))}
	}
	return nil
}

// DeleteBuckety drops the database and everything in it.
// Idempotent; one statement, so there is no in-progress state.
func (d *Driver) DeleteBuckety(ctx context.Context, req registry.DeleteRequest) error {
	if err := d.checkDatabase(req.Name); err != nil {
		return fmt.Errorf("mysql: %w", err)
	}
	if err := d.conn.exec(ctx, dropDatabaseSQL(req.Name)); err != nil {
		return fmt.Errorf("mysql: drop database %q: %w", req.Name, err)
	}
	return nil
}

// GrantAccess ensures the access's user exists with the Secret's
// password (or a generated one when the Secret has none) and holds
// exactly the role's privileges on the database, then returns the
// Secret payload.
//
// Secret keys: host, port, database (resource-type key), username,
// password, jdbcUrl, url.
func (d *Driver) GrantAccess(ctx context.Context, req registry.GrantRequest) (registry.GrantResult, error) {
	db := req.BucketyName
	if err := d.checkDatabase(db); err != nil {
		return registry.GrantResult{}, fmt.Errorf("mysql: %w", err)
	}
	user, err := d.userName(req.AccessNamespace, req.AccessName)
	if err != nil {
		return registry.GrantResult{}, err
	}
	acct := account{User: user, Host: d.cfg.UserHost}

	want, err := rolePrivileges(req.Role)
	if err != nil {
		// An access with no grant set holds nothing. Its user may
		// exist from an earlier role, and refusing without dropping
		// it would leave a downgrade to Writer with ReadWrite's
		// grants and a working Secret. Changing the role back
		// recreates the user with the Secret's password.
		if derr := d.conn.exec(ctx, dropUserSQL, acct.User, acct.Host); derr != nil {
			return registry.GrantResult{}, fmt.Errorf("%w; dropping the user %s it may hold from an earlier role failed: %v", err, acct, derr)
		}
		return registry.GrantResult{}, err
	}

	password, fromSecret := string(req.ExistingSecretData["password"]), true
	if password == "" {
		fromSecret = false
		if password, err = generatePassword(); err != nil {
			return registry.GrantResult{}, err
		}
	} else if err := validatePassword(password); err != nil {
		return registry.GrantResult{}, fmt.Errorf("mysql: the access Secret's password: %w", err)
	}

	created, err := d.ensureAccount(ctx, acct, password, fromSecret, grantPattern(db), want)
	if err != nil {
		if created {
			// Nothing records a user created in a failed pass: drop
			// it, the next pass creates it again. Detached from ctx,
			// which may be what failed.
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dialTimeout)
			_ = d.conn.exec(cctx, dropUserSQL, acct.User, acct.Host)
			cancel()
		}
		return registry.GrantResult{}, err
	}
	return registry.GrantResult{
		SecretData: d.secretData(db, user, password),
		Principal:  acct.String(),
		Scoped:     true,
		Revocable:  true,
		Minted:     created,
	}, nil
}

// Login verification after a password change: Galera applies
// account changes on the other nodes a moment after the statement
// returns on the one the controller wrote to.
const verifyAttempts = 5

var verifyBackoff = time.Second

// ensureAccount converges acct and reports whether it created the
// user. fromSecret says the password is already stored in the
// access Secret; only then is a failed verification an error,
// because a generated password must reach the Secret first.
func (d *Driver) ensureAccount(ctx context.Context, acct account, password string, fromSecret bool, pattern string, want []string) (created bool, err error) {
	if fromSecret {
		have, err := d.conn.login(ctx, acct, password, pattern)
		if err == nil {
			return false, d.alignGrants(ctx, acct, pattern, want, have)
		}
		if !errors.Is(err, errLoginDenied) && !errors.Is(err, errShadowed) {
			return false, fmt.Errorf("mysql: log in as %s to check it: %w", acct, err)
		}
		// Missing user, a password that differs from the Secret, or
		// an account for another userHost matched instead.
	}
	if created, err = d.setPassword(ctx, acct, password); err != nil {
		return false, err
	}
	// A user this pass created holds nothing yet; a re-keyed one
	// holds what it held, which no login has read.
	var have *grants
	if created {
		have = &grants{}
	}
	if err := d.alignGrants(ctx, acct, pattern, want, have); err != nil {
		return created, err
	}
	if !fromSecret {
		return created, nil
	}
	for attempt := 1; ; attempt++ {
		have, err := d.conn.login(ctx, acct, password, pattern)
		if err == nil {
			return created, d.alignGrants(ctx, acct, pattern, want, have)
		}
		if errors.Is(err, errShadowed) {
			// The account for the previous userHost is more specific
			// and still exists; the reconciler drops it once this
			// grant's principal is recorded, and the next pass
			// verifies.
			return created, nil
		}
		if !errors.Is(err, errLoginDenied) || attempt == verifyAttempts {
			return created, fmt.Errorf("mysql: set the password of %s from its Secret, but the controller cannot log in with it (userHost %q must admit the controller's address): %w", acct, d.cfg.UserHost, err)
		}
		select {
		case <-ctx.Done():
			return created, ctx.Err()
		case <-time.After(verifyBackoff):
		}
	}
}

// setPassword creates the user, or sets the password of an existing
// one. Errors are redacted: the statements carry the password.
func (d *Driver) setPassword(ctx context.Context, acct account, password string) (created bool, err error) {
	err = d.conn.exec(ctx, createUserSQL, acct.User, acct.Host, password)
	if err == nil {
		return true, nil
	}
	if !isMySQLError(err, erCannotUser) {
		return false, fmt.Errorf("mysql: create user %s: %w", acct, statementError(err, password))
	}
	if err := d.conn.exec(ctx, alterUserSQL, acct.User, acct.Host, password); err != nil {
		return false, fmt.Errorf("mysql: set password of %s: %w", acct, statementError(err, password))
	}
	return false, nil
}

// alignGrants grants what is missing from want and revokes what is
// beyond it, including GRANT OPTION. have is nil when the current
// grants are unknown (the user was re-keyed): then the whole set is
// granted and the rest of the managed set revoked one privilege at
// a time, and a later pass with a working login revokes anything
// else.
func (d *Driver) alignGrants(ctx context.Context, acct account, pattern string, want []string, have *grants) error {
	if have == nil {
		if err := d.grant(ctx, acct, pattern, want); err != nil {
			return err
		}
		return d.revokeIfHeld(ctx, acct, pattern, without(readWritePrivileges, want))
	}
	if err := d.grant(ctx, acct, pattern, without(want, have.privileges)); err != nil {
		return err
	}
	if extra := without(have.privileges, want); len(extra) > 0 {
		q, err := revokeSQL(extra, pattern)
		if err != nil {
			return err
		}
		if err := d.conn.exec(ctx, q, acct.User, acct.Host); err != nil {
			return fmt.Errorf("mysql: revoke %s from %s: %w", strings.Join(extra, ", "), acct, err)
		}
	}
	if have.grantOption {
		if err := d.conn.exec(ctx, revokeGrantOptionSQL(pattern), acct.User, acct.Host); err != nil {
			return fmt.Errorf("mysql: revoke GRANT OPTION from %s: %w", acct, err)
		}
	}
	return nil
}

func (d *Driver) grant(ctx context.Context, acct account, pattern string, privileges []string) error {
	if len(privileges) == 0 {
		return nil
	}
	q, err := grantSQL(privileges, pattern)
	if err != nil {
		return err
	}
	if err := d.conn.exec(ctx, q, acct.User, acct.Host); err != nil {
		return fmt.Errorf("mysql: grant %s to %s: %w", strings.Join(privileges, ", "), acct, err)
	}
	return nil
}

// revokeIfHeld revokes privileges that the account may or may not
// hold. One statement each: MySQL 8.4 refuses to revoke a privilege
// that is not held, and a REVOKE of several is atomic, so a single
// statement would also keep the ones that are.
func (d *Driver) revokeIfHeld(ctx context.Context, acct account, pattern string, privileges []string) error {
	for _, p := range privileges {
		q, err := revokeSQL([]string{p}, pattern)
		if err != nil {
			return err
		}
		if err := d.conn.exec(ctx, q, acct.User, acct.Host); err != nil && !isMySQLError(err, erNoSuchGrant) {
			return fmt.Errorf("mysql: revoke %s from %s: %w", p, acct, err)
		}
	}
	return nil
}

func (d *Driver) secretData(db, user, password string) map[string][]byte {
	port := strconv.Itoa(d.cfg.Port)
	hostPort := net.JoinHostPort(d.cfg.Host, port)
	u := url.URL{Scheme: "mysql", User: url.UserPassword(user, password), Host: hostPort, Path: "/" + db}
	return map[string][]byte{
		"host":     []byte(d.cfg.Host),
		"port":     []byte(port),
		"database": []byte(db),
		"username": []byte(user),
		"password": []byte(password),
		"jdbcUrl":  []byte("jdbc:mariadb://" + hostPort + "/" + db),
		"url":      []byte(u.String()),
	}
}

// RevokeAccess drops the access's user. Idempotent; an empty
// principal (never granted) has nothing to drop.
func (d *Driver) RevokeAccess(ctx context.Context, principal string) error {
	if principal == "" {
		return nil
	}
	acct, err := d.parsePrincipal(principal)
	if err != nil {
		return err
	}
	if err := d.conn.exec(ctx, dropUserSQL, acct.User, acct.Host); err != nil {
		return fmt.Errorf("mysql: drop user %s: %w", acct, err)
	}
	return nil
}

var (
	charsetRE   = regexp.MustCompile(`^[a-z][a-z0-9]{1,31}$`)
	collationRE = regexp.MustCompile(`^[a-z][a-z0-9_]{1,63}$`)
)

// ValidateParameters accepts characterSet and collation. "utf8" is
// refused: it means utf8mb3 today and is slated to mean utf8mb4,
// so the reported value would never match.
func (d *Driver) ValidateParameters(params map[string]string) error {
	for k, v := range params {
		switch k {
		case "characterSet":
			if !charsetRE.MatchString(v) {
				return fmt.Errorf("parameters.characterSet %q must be a lowercase character set name, e.g. utf8mb4", v)
			}
			if v == "utf8" {
				return fmt.Errorf("parameters.characterSet \"utf8\" is ambiguous; use utf8mb4 (or utf8mb3)")
			}
		case "collation":
			if !collationRE.MatchString(v) {
				return fmt.Errorf("parameters.collation %q must be a lowercase collation name, e.g. utf8mb4_unicode_ci", v)
			}
			if strings.HasPrefix(v, "utf8_") {
				return fmt.Errorf("parameters.collation %q is ambiguous; use the utf8mb4_ (or utf8mb3_) name", v)
			}
		default:
			return fmt.Errorf("unknown parameter %q (mysql v0.1 accepts: characterSet, collation)", k)
		}
	}
	return nil
}

// ValidateUpdateParameters: both parameters are set at creation and
// immutable, like a gcs bucket's location; adding or removing one
// counts as a change.
func (d *Driver) ValidateUpdateParameters(oldParams, newParams map[string]string) error {
	if err := d.ValidateParameters(newParams); err != nil {
		return err
	}
	for _, k := range []string{"characterSet", "collation"} {
		if o, n := oldParams[k], newParams[k]; o != n {
			return fmt.Errorf("parameters.%s is immutable post-create (current=%q, requested=%q); the driver does not convert existing tables", k, o, n)
		}
	}
	return nil
}

func (d *Driver) ValidateAccessParameters(params map[string]string) error {
	if len(params) == 0 {
		return nil
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return fmt.Errorf("mysql accepts no BucketyAccess parameters; got: %s", strings.Join(keys, ", "))
}

// ValidateResourceName enforces the prefix boundary and MySQL's
// limits on the resolved spec.name.
func (d *Driver) ValidateResourceName(name string) error {
	if err := d.checkDatabase(name); err != nil {
		if !strings.HasPrefix(name, d.cfg.NamePrefix) {
			return fmt.Errorf("%w (set spec.name, e.g. \"%s${namespace}_${name}\")", err, d.cfg.NamePrefix)
		}
		return err
	}
	return nil
}
