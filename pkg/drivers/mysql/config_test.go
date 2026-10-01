package mysql

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
	yaml "sigs.k8s.io/yaml"
)

// testAdminPassword stands in for the env-substituted secret; the
// tests assert it never shows up in an error.
const testAdminPassword = "Admin-Pw-From-Env-0123"

func rawConfig(t *testing.T, y string) json.RawMessage {
	t.Helper()
	j, err := yaml.YAMLToJSON([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestDecodeConfigDefaults(t *testing.T) {
	t.Setenv("MYSQL_BUCKETY_PASSWORD", testAdminPassword)
	c, err := decodeConfig(rawConfig(t, `
host: mysql.mysql.svc.cluster.local
adminUser: buckety
adminPassword: ${MYSQL_BUCKETY_PASSWORD}
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Port != 3306 || c.NamePrefix != "b_" || c.UserHost != "%" || c.TLS != nil {
		t.Errorf("defaults: %+v", *c)
	}
	if c.AdminPassword != testAdminPassword {
		t.Error("adminPassword not substituted from the environment")
	}
}

func TestDecodeConfigExplicit(t *testing.T) {
	t.Setenv("MYSQL_BUCKETY_PASSWORD", testAdminPassword)
	t.Setenv("MYSQL_BUCKETY_USER", "buckety")
	c, err := decodeConfig(rawConfig(t, `
host: 10.0.0.5
port: 3307
adminUser: ${MYSQL_BUCKETY_USER}
adminPassword: ${MYSQL_BUCKETY_PASSWORD}
namePrefix: app_
userHost: 10.42.%
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Port != 3307 || c.NamePrefix != "app_" || c.UserHost != "10.42.%" || c.AdminUser != "buckety" {
		t.Errorf("explicit values: %+v", *c)
	}
}

func TestDecodeConfigErrors(t *testing.T) {
	t.Setenv("MYSQL_BUCKETY_PASSWORD", testAdminPassword)
	base := "host: db\nadminUser: buckety\nadminPassword: ${MYSQL_BUCKETY_PASSWORD}\n"
	cases := map[string]string{
		"unknown field":         base + "adminPasword: x\n",
		"missing host":          "adminUser: buckety\nadminPassword: ${MYSQL_BUCKETY_PASSWORD}\n",
		"missing adminUser":     "host: db\nadminPassword: ${MYSQL_BUCKETY_PASSWORD}\n",
		"missing adminPassword": "host: db\nadminUser: buckety\n",
		"undefined variable":    "host: db\nadminUser: buckety\nadminPassword: ${NOT_SET_ANYWHERE_42}\n",
		"untagged substitution": "host: ${MYSQL_BUCKETY_PASSWORD}\nadminUser: buckety\nadminPassword: x\n",
		"host with path":        "host: db/x\nadminUser: buckety\nadminPassword: ${MYSQL_BUCKETY_PASSWORD}\n",
		"host with scheme":      "host: mysql://db\nadminUser: buckety\nadminPassword: ${MYSQL_BUCKETY_PASSWORD}\n",
		"host with port":        "host: db:3306\nadminUser: buckety\nadminPassword: ${MYSQL_BUCKETY_PASSWORD}\n",
		"port range":            base + "port: 70000\n",
		"prefix without _":      base + "namePrefix: b\n",
		"empty-ish prefix":      base + "namePrefix: _\n",
		"system prefix":         base + "namePrefix: performance_\n",
		"admin inside prefix":   "host: db\nadminUser: b_admin\nadminPassword: ${MYSQL_BUCKETY_PASSWORD}\n",
		"bad userHost":          base + "userHost: \"%' OR 1\"\n",
		"tls unknown field":     base + "tls:\n  insecure: true\n",
	}
	for name, y := range cases {
		_, err := decodeConfig(rawConfig(t, y))
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if strings.Contains(err.Error(), testAdminPassword) {
			t.Errorf("%s: error contains the admin password", name)
		}
	}
}

func TestFactoryRegisteredAndLazy(t *testing.T) {
	t.Setenv("MYSQL_BUCKETY_PASSWORD", testAdminPassword)
	f, ok := registry.Lookup(DriverName)
	if !ok {
		t.Fatal("mysql driver not registered")
	}
	// No server at this address: the factory must not connect.
	d, err := f(rawConfig(t, "host: 127.0.0.1\nport: 1\nadminUser: buckety\nadminPassword: ${MYSQL_BUCKETY_PASSWORD}\n"))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if d.Name() != "mysql" || d.Version() == "" {
		t.Fatalf("name/version: %q %q", d.Name(), d.Version())
	}
}

func TestClientConfig(t *testing.T) {
	c := &Config{Host: "db.example", Port: 3306, AdminUser: "buckety", AdminPassword: testAdminPassword, NamePrefix: "b_", UserHost: "%"}
	mc, err := c.clientConfig()
	if err != nil {
		t.Fatal(err)
	}
	if mc.Addr != "db.example:3306" || !mc.InterpolateParams || mc.MultiStatements || mc.AllowAllFiles || mc.TLS != nil || mc.AllowCleartextPasswords {
		t.Errorf("client config: addr=%s interpolate=%v multi=%v files=%v tls=%v cleartext=%v",
			mc.Addr, mc.InterpolateParams, mc.MultiStatements, mc.AllowAllFiles, mc.TLS != nil, mc.AllowCleartextPasswords)
	}
	c.Host = "fd00::1"
	if mc, _ := c.clientConfig(); mc.Addr != "[fd00::1]:3306" {
		t.Errorf("IPv6 addr = %s", mc.Addr)
	}
}

func TestClientConfigTLS(t *testing.T) {
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	writeTestCA(t, ca)
	c := &Config{Host: "db.example", Port: 3306, AdminUser: "buckety", AdminPassword: "x", TLS: &TLSConfig{CAFile: ca}}
	mc, err := c.clientConfig()
	if err != nil {
		t.Fatal(err)
	}
	if mc.TLS == nil || mc.TLS.RootCAs == nil || mc.TLS.ServerName != "db.example" || mc.TLS.InsecureSkipVerify {
		t.Fatalf("tls config: %+v", mc.TLS)
	}
	c.TLS.ServerName = "mariadb.mysql.svc"
	if mc, _ := c.clientConfig(); mc.TLS.ServerName != "mariadb.mysql.svc" {
		t.Errorf("serverName override: %s", mc.TLS.ServerName)
	}

	c.TLS = &TLSConfig{CAFile: filepath.Join(dir, "missing.crt")}
	if _, err := c.clientConfig(); err == nil {
		t.Error("missing caFile accepted")
	}
	notPEM := filepath.Join(dir, "not.pem")
	if err := os.WriteFile(notPEM, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	c.TLS = &TLSConfig{CAFile: notPEM}
	if _, err := c.clientConfig(); err == nil {
		t.Error("caFile without certificates accepted")
	}
	// System roots when no caFile.
	c.TLS = &TLSConfig{}
	if mc, err := c.clientConfig(); err != nil || mc.TLS == nil || mc.TLS.RootCAs != nil {
		t.Errorf("system roots: %v", err)
	}
}

func writeTestCA(t *testing.T, path string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}
