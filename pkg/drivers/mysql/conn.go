package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
)

// MySQL/MariaDB error numbers the driver branches on.
const (
	erAccessDenied = 1045 // login refused: unknown user or wrong password
	erCannotUser   = 1396 // CREATE USER of an existing account, and similar
)

var (
	// errLoginDenied is what conn.login returns for erAccessDenied.
	errLoginDenied = errors.New("login denied")
	// errShadowed: the login matched another account with the same
	// user name and a more specific host, so the managed account
	// could not be inspected. Seen while userHost changes.
	errShadowed = errors.New("login matched another account")
)

// schemaInfo is a database's defaults as SCHEMATA reports them.
type schemaInfo struct {
	charset   string
	collation string
}

// grants is what a created user holds on its database.
type grants struct {
	privileges  []string
	grantOption bool
}

// conn is the driver's view of the server; sqlConn implements it,
// tests substitute a fake.
type conn interface {
	// exec runs one statement as the admin account.
	exec(ctx context.Context, query string, args ...any) error
	// lookupSchema returns nil when the database does not exist or
	// is not visible to the admin account.
	lookupSchema(ctx context.Context, name string) (*schemaInfo, error)
	// countContent counts tables, views, routines and events.
	countContent(ctx context.Context, name string) (int, error)
	// login connects as acct and reads its grants on the escaped
	// database pattern. errLoginDenied when the server refuses.
	login(ctx context.Context, acct account, password, pattern string) (*grants, error)
}

type sqlConn struct {
	admin *sql.DB
	// base is the admin client config; logins clone it.
	base *gomysql.Config
}

func newSQLConn(base *gomysql.Config) (*sqlConn, error) {
	connector, err := gomysql.NewConnector(base)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)
	return &sqlConn{admin: db, base: base}, nil
}

func (c *sqlConn) exec(ctx context.Context, query string, args ...any) error {
	_, err := c.admin.ExecContext(ctx, query, args...)
	return err
}

func (c *sqlConn) lookupSchema(ctx context.Context, name string) (*schemaInfo, error) {
	var s schemaInfo
	err := c.admin.QueryRowContext(ctx, schemaSQL, name).Scan(&s.charset, &s.collation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (c *sqlConn) countContent(ctx context.Context, name string) (int, error) {
	var n int
	err := c.admin.QueryRowContext(ctx, contentSQL, name, name, name).Scan(&n)
	return n, err
}

func (c *sqlConn) login(ctx context.Context, acct account, password, pattern string) (*grants, error) {
	cfg := c.base.Clone()
	cfg.User = acct.User
	cfg.Passwd = password
	cfg.DBName = ""
	connector, err := gomysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)

	var current string
	if err := db.QueryRowContext(ctx, currentUserSQL).Scan(&current); err != nil {
		if isMySQLError(err, erAccessDenied) {
			return nil, errLoginDenied
		}
		return nil, err
	}
	if current != acct.String() {
		return nil, fmt.Errorf("%w: logged in as %s instead of %s", errShadowed, current, acct)
	}
	rows, err := db.QueryContext(ctx, privilegesSQL, acct.grantee(), pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	g := &grants{}
	for rows.Next() {
		var priv, grantable string
		if err := rows.Scan(&priv, &grantable); err != nil {
			return nil, err
		}
		g.privileges = append(g.privileges, priv)
		if grantable == "YES" {
			g.grantOption = true
		}
	}
	return g, rows.Err()
}

func isMySQLError(err error, number uint16) bool {
	var me *gomysql.MySQLError
	return errors.As(err, &me) && me.Number == number
}
