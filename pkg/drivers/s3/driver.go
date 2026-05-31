// Package s3 is the S3-protocol driver.
//
// v1alpha1 status: STUB. The driver registers, accepts the
// config block, validates parameters at admission - but
// EnsureBuckety/DeleteBuckety/GrantAccess return a clear error
// that surfaces as Ready=False on the resource. Full
// implementation lands in a follow-up branch alongside the s3
// e2e scenarios already in examples/s3/. This file exists so
// buckety-controller.yaml that mixes a kadm backend with an s3
// backend loads and validates without surprises.
package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
	"github.com/Yolean/y-cluster/pkg/envsubst"
	yaml "sigs.k8s.io/yaml"
)

// DriverName matches backends[].driver in buckety-controller.yaml.
const DriverName = "s3"

// version is the driver SemVer (see pkg/drivers/kadm for the
// ldflags pattern). The s3 STUB advertises 0.0.x to make it
// clear it has not reached v0.1.
var version = "0.0.1"

// ErrNotImplemented is the sentinel the stub returns. The
// Buckety reconciler maps this to a stable Ready=False /
// NotImplemented condition rather than retrying indefinitely.
var ErrNotImplemented = errors.New("s3 driver not implemented in this build (v1alpha1 stub)")

func init() {
	registry.Register(DriverName, factory)
}

// Config is the typed shape of the `config:` block under an s3
// backend. Mirrors pkg/drivers/s3/schema/v0.1/config.schema.json.
// Credential fields carry envsubst:"true" so ${VAR} interpolation
// works at controller startup.
type Config struct {
	Implementation  string `json:"implementation,omitempty"`
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region,omitempty"`
	ForcePathStyle  bool   `json:"forcePathStyle,omitempty"`
	AccessKeyID     string `json:"accessKeyID" envsubst:"true"`
	SecretAccessKey string `json:"secretAccessKey" envsubst:"true"`
}

func factory(raw json.RawMessage) (registry.Driver, error) {
	var c Config
	if err := yaml.UnmarshalStrict(raw, &c); err != nil {
		return nil, fmt.Errorf("s3 config: %w", err)
	}
	if err := envsubst.Apply(&c, envsubst.OSEnv); err != nil {
		return nil, fmt.Errorf("s3 config: %w", err)
	}
	if c.Endpoint == "" {
		return nil, fmt.Errorf("s3 config: missing required field %q", "endpoint")
	}
	if c.AccessKeyID == "" {
		return nil, fmt.Errorf("s3 config: missing required field %q", "accessKeyID")
	}
	if c.SecretAccessKey == "" {
		return nil, fmt.Errorf("s3 config: missing required field %q", "secretAccessKey")
	}
	switch c.Implementation {
	case "", "aws", "r2", "minio", "versitygw":
		// ok
	default:
		return nil, fmt.Errorf("s3 config: unknown implementation %q", c.Implementation)
	}
	return &Driver{cfg: &c}, nil
}

// Driver is the s3 STUB. See package docs.
type Driver struct {
	cfg *Config
}

func (d *Driver) Name() string    { return DriverName }
func (d *Driver) Version() string { return version }

func (d *Driver) EnsureBuckety(_ context.Context, _ registry.EnsureRequest) error {
	return ErrNotImplemented
}

func (d *Driver) DeleteBuckety(_ context.Context, _ string) error {
	return ErrNotImplemented
}

func (d *Driver) GrantAccess(_ context.Context, _ registry.GrantRequest) (registry.GrantResult, error) {
	return registry.GrantResult{}, ErrNotImplemented
}

func (d *Driver) RevokeAccess(_ context.Context, _ string) error { return nil }

// ValidateParameters honours the capability-gating contract even
// in the stub: jurisdiction is accepted only when implementation
// is r2; admission rejects mismatches so the SPEC behaviour
// matches once the EnsureBuckety side is implemented.
func (d *Driver) ValidateParameters(params map[string]string) error {
	for k, v := range params {
		switch k {
		case "jurisdiction":
			if d.cfg.Implementation != "r2" {
				return fmt.Errorf("parameters.jurisdiction is r2-only; this backend's implementation is %q", d.cfg.Implementation)
			}
			if v != "eu" {
				return fmt.Errorf("parameters.jurisdiction: only %q is supported in v0.1, got %q", "eu", v)
			}
		default:
			return fmt.Errorf("unknown parameter %q (s3 v0.1 accepts: jurisdiction when implementation=r2)", k)
		}
	}
	return nil
}

// ValidateUpdateParameters: jurisdiction is set-at-create and
// immutable in v1alpha1; any change is a rejection.
func (d *Driver) ValidateUpdateParameters(oldParams, newParams map[string]string) error {
	if err := d.ValidateParameters(newParams); err != nil {
		return err
	}
	if o, n := oldParams["jurisdiction"], newParams["jurisdiction"]; o != n {
		return fmt.Errorf("parameters.jurisdiction is immutable post-create (current=%q, requested=%q)", o, n)
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
	return fmt.Errorf("s3 v0.1 accepts no BucketyAccess parameters; got: %s",
		strings.Join(keys, ", "))
}
