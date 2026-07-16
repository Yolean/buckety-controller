// Package config loads buckety-controller.yaml.
//
// The outer file shape (backends[].name/driver/config/defaults) is
// known here. The `config:` block under each backend is delivered
// to the driver factory as json.RawMessage; each driver then
// performs its own strict decode + envsubst on its typed config
// struct (where envsubst:"true" tags live).
package config

import (
	"encoding/json"
	"fmt"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
	yclconfig "github.com/Yolean/y-cluster/pkg/configfile"
)

// Filename is the conventional name inside the directory passed
// to the controller via `-c <dir>`.
const Filename = "buckety-controller.yaml"

// rawConfig is the outer YAML shape pre-driver-decode.
type rawConfig struct {
	Backends []rawBackend `json:"backends"`
}

type rawBackend struct {
	Name     string            `json:"name"`
	Driver   string            `json:"driver"`
	Config   json.RawMessage   `json:"config"`
	Defaults map[string]string `json:"defaults,omitempty"`
	// Parameters are operator-declared per-backend defaults for
	// Buckety.spec.parameters, merged under each resource's own
	// parameters (the CR wins per key). This is what keeps
	// driver-specific knobs out of portable CRs (issue #17): a
	// gcs backend declares location/uniformBucketLevelAccess here
	// while the CR carries only family-common parameters.
	Parameters map[string]string `json:"parameters,omitempty"`
}

// Backend is a resolved backend after driver factory invocation.
type Backend struct {
	Name       string
	Driver     registry.Driver
	Defaults   map[string]string
	Parameters map[string]string
}

// EffectiveParameters merges the backend's parameter defaults
// under the resource's own parameters; the resource wins per key.
// Every validation and EnsureBuckety call operates on this merged
// view, so removing a key from a CR falls back to the backend
// default (and trips immutability checks if that changes an
// immutable knob).
func (b Backend) EffectiveParameters(crParams map[string]string) map[string]string {
	if len(b.Parameters) == 0 {
		return crParams
	}
	out := make(map[string]string, len(b.Parameters)+len(crParams))
	for k, v := range b.Parameters {
		out[k] = v
	}
	for k, v := range crParams {
		out[k] = v
	}
	return out
}

// Loaded is the result of a successful Load.
type Loaded struct {
	// Backends keyed by name for O(1) lookup from reconcilers.
	Backends map[string]Backend
}

// Lookup returns the backend by name, or false if unknown.
func (l *Loaded) Lookup(name string) (Backend, bool) {
	b, ok := l.Backends[name]
	return b, ok
}

// Load reads <dir>/buckety-controller.yaml, validates the outer
// shape, and invokes each backend's driver factory. The factory
// is where strict decode of the driver-specific `config:` block
// and per-driver envsubst happens.
func Load(dir string) (*Loaded, error) {
	var raw rawConfig
	if err := yclconfig.Load(dir, Filename, &raw); err != nil {
		return nil, err
	}
	if err := validateOuter(&raw); err != nil {
		return nil, fmt.Errorf("%s: %w", Filename, err)
	}
	out := &Loaded{Backends: make(map[string]Backend, len(raw.Backends))}
	for i, b := range raw.Backends {
		factory, ok := registry.Lookup(b.Driver)
		if !ok {
			return nil, fmt.Errorf("backends[%d]: unknown driver %q", i, b.Driver)
		}
		drv, err := factory(b.Config)
		if err != nil {
			return nil, fmt.Errorf("backends[%d] %q: %w", i, b.Name, err)
		}
		// Backend parameter defaults are validated at startup so a
		// typo crash-loops with a config diagnostic instead of
		// failing every resource at admission.
		if err := drv.ValidateParameters(b.Parameters); err != nil {
			return nil, fmt.Errorf("backends[%d] %q: parameters: %w", i, b.Name, err)
		}
		out.Backends[b.Name] = Backend{
			Name:       b.Name,
			Driver:     drv,
			Defaults:   b.Defaults,
			Parameters: b.Parameters,
		}
	}
	return out, nil
}

func validateOuter(c *rawConfig) error {
	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends configured")
	}
	seen := make(map[string]struct{}, len(c.Backends))
	for i, b := range c.Backends {
		if b.Name == "" {
			return fmt.Errorf("backends[%d]: missing required field %q", i, "name")
		}
		if b.Driver == "" {
			return fmt.Errorf("backends[%d] %q: missing required field %q", i, b.Name, "driver")
		}
		if _, dup := seen[b.Name]; dup {
			return fmt.Errorf("duplicate backend name %q", b.Name)
		}
		seen[b.Name] = struct{}{}
	}
	return nil
}
