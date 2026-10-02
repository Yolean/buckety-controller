package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"github.com/Yolean/y-cluster/pkg/envsubst"
	yaml "sigs.k8s.io/yaml"
)

// Config defaults, applied when the key is absent.
const (
	defaultPort       = 3306
	defaultNamePrefix = "b_"
	defaultUserHost   = "%"
)

// Config is the typed shape of the `config:` block under a mysql
// backend. Mirrors schema/v0.1/config.schema.json.
type Config struct {
	// Host is the server address, used by the controller and
	// written to access Secrets.
	Host string `json:"host"`
	// Port defaults to 3306.
	Port int `json:"port,omitempty"`
	// AdminUser / AdminPassword are the controller's account; see
	// docs/mysql.md for the grants it needs.
	AdminUser     string `json:"adminUser" envsubst:"true"`
	AdminPassword string `json:"adminPassword" envsubst:"true"`
	// NamePrefix scopes every database and user the driver touches.
	// Defaults to "b_".
	NamePrefix string `json:"namePrefix,omitempty"`
	// UserHost is the host part of created accounts. Defaults to "%".
	UserHost string `json:"userHost,omitempty"`
	// TLS, when present, requires verified TLS for the controller's
	// connections (admin and login checks).
	TLS *TLSConfig `json:"tls,omitempty"`
}

// TLSConfig enables verified TLS towards the server.
type TLSConfig struct {
	// CAFile is a PEM bundle to verify the server with; system
	// roots when empty.
	CAFile string `json:"caFile,omitempty"`
	// ServerName overrides the name verified in the server
	// certificate; defaults to Host.
	ServerName string `json:"serverName,omitempty"`
}

func decodeConfig(raw json.RawMessage) (*Config, error) {
	var c Config
	if err := yaml.UnmarshalStrict(raw, &c); err != nil {
		return nil, fmt.Errorf("mysql config: %w", err)
	}
	if err := envsubst.Apply(&c, envsubst.OSEnv); err != nil {
		return nil, fmt.Errorf("mysql config: %w", err)
	}
	if c.Port == 0 {
		c.Port = defaultPort
	}
	if c.NamePrefix == "" {
		c.NamePrefix = defaultNamePrefix
	}
	if c.UserHost == "" {
		c.UserHost = defaultUserHost
	}
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("mysql config: %w", err)
	}
	return &c, nil
}

func (c *Config) validate() error {
	switch {
	case c.Host == "":
		return fmt.Errorf("missing required field %q", "host")
	case strings.ContainsAny(c.Host, "/@?# \t"),
		strings.Contains(c.Host, ":") && net.ParseIP(c.Host) == nil:
		return fmt.Errorf("host %q must be a bare host name or IP, without scheme, port or path", c.Host)
	case c.Port < 1 || c.Port > 65535:
		return fmt.Errorf("port %d is out of range", c.Port)
	case c.AdminUser == "":
		return fmt.Errorf("missing required field %q", "adminUser")
	case c.AdminPassword == "":
		return fmt.Errorf("missing required field %q", "adminPassword")
	case strings.ContainsAny(c.AdminPassword, "\r\n"):
		// The usual cause is a Secret created from a file that ends
		// in a newline; the bootstrap script refuses those too.
		return fmt.Errorf("adminPassword contains a line break; recreate its Secret without a trailing newline")
	}
	if err := checkPrefix(c.NamePrefix); err != nil {
		return err
	}
	if strings.HasPrefix(c.AdminUser, c.NamePrefix) {
		return fmt.Errorf("adminUser %q starts with namePrefix %q; the controller's own account must be outside the names it manages", c.AdminUser, c.NamePrefix)
	}
	if err := checkHost(c.UserHost); err != nil {
		return fmt.Errorf("userHost: %w", err)
	}
	return nil
}

// Connection timeouts. Reads are generous because DROP DATABASE on
// a large database returns only when the files are gone.
const (
	dialTimeout     = 10 * time.Second
	ioTimeout       = 60 * time.Second
	lockWaitTimeout = 50 * time.Second
)

// clientConfig builds the go-sql-driver config for the admin
// account. Logins as created users clone it. InterpolateParams
// makes the client escape ? arguments into string literals, which
// is what lets account names and passwords travel as arguments in
// statements (CREATE USER, GRANT) that the server-side prepared
// statement protocol does not parameterise.
func (c *Config) clientConfig() (*gomysql.Config, error) {
	mc := gomysql.NewConfig()
	mc.User = c.AdminUser
	mc.Passwd = c.AdminPassword
	mc.Net = "tcp"
	mc.Addr = net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	mc.Timeout = dialTimeout
	mc.ReadTimeout = ioTimeout
	mc.WriteTimeout = ioTimeout
	mc.InterpolateParams = true
	mc.Collation = "utf8mb4_general_ci"
	// The server gives up on a metadata lock before the client
	// gives up on the reply. DROP DATABASE waits for open
	// transactions on the database (consumer sessions outlive
	// DROP USER), by default for a day on MariaDB and a year on
	// MySQL; without this, each timed-out retry would queue
	// another DROP behind them, and the pending lock blocks new
	// queries on that database.
	mc.Params = map[string]string{"lock_wait_timeout": strconv.Itoa(int(lockWaitTimeout / time.Second))}
	if c.TLS != nil {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.TLS.ServerName}
		if tc.ServerName == "" {
			tc.ServerName = c.Host
		}
		if c.TLS.CAFile != "" {
			pem, err := os.ReadFile(c.TLS.CAFile)
			if err != nil {
				return nil, fmt.Errorf("tls.caFile: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("tls.caFile %q holds no PEM certificates", c.TLS.CAFile)
			}
			tc.RootCAs = pool
		}
		mc.TLS = tc
	}
	return mc, nil
}
