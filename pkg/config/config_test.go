package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yolean/buckety-controller/pkg/drivers/registry"
)

func TestEffectiveParameters(t *testing.T) {
	b := Backend{Parameters: map[string]string{
		"location":   "EUROPE-WEST4",
		"versioning": "false",
	}}
	// CR wins per key; backend fills the rest.
	got := b.EffectiveParameters(map[string]string{"versioning": "true", "lifecycle": "{}"})
	if got["location"] != "EUROPE-WEST4" || got["versioning"] != "true" || got["lifecycle"] != "{}" {
		t.Errorf("merged: %v", got)
	}
	// No backend defaults: CR map passes through untouched.
	if out := (Backend{}).EffectiveParameters(map[string]string{"a": "1"}); out["a"] != "1" || len(out) != 1 {
		t.Errorf("passthrough: %v", out)
	}
	// Nil CR params with backend defaults still yields defaults.
	if out := b.EffectiveParameters(nil); out["location"] != "EUROPE-WEST4" {
		t.Errorf("defaults only: %v", out)
	}
}

// stubDriver is a minimal registry.Driver for exercising the
// config layer; templated declares TemplatedParameters, and
// ValidateParameters rejects any declared-templated key it is
// handed, proving Load stripped them before validating backend
// parameter defaults.
type stubDriver struct{ templated []string }

func (s stubDriver) Name() string    { return "stub" }
func (s stubDriver) Version() string { return "0.0.1" }
func (s stubDriver) InspectBuckety(context.Context, string) (registry.Inspection, error) {
	return registry.Inspection{}, nil
}
func (s stubDriver) EnsureBuckety(context.Context, registry.EnsureRequest) error { return nil }
func (s stubDriver) DeleteBuckety(context.Context, registry.DeleteRequest) error { return nil }
func (s stubDriver) GrantAccess(context.Context, registry.GrantRequest) (registry.GrantResult, error) {
	return registry.GrantResult{}, nil
}
func (s stubDriver) RevokeAccess(context.Context, string) error            { return nil }
func (s stubDriver) ValidateUpdateParameters(_, _ map[string]string) error { return nil }
func (s stubDriver) ValidateAccessParameters(map[string]string) error      { return nil }
func (s stubDriver) ValidateResourceName(string) error                     { return nil }
func (s stubDriver) TemplatedParameters() []string                         { return s.templated }
func (s stubDriver) ValidateParameters(params map[string]string) error {
	for _, k := range s.templated {
		if _, ok := params[k]; ok {
			return fmt.Errorf("declared-templated key %q reached driver validation unresolved", k)
		}
	}
	return nil
}

func TestResolvedParameters(t *testing.T) {
	b := Backend{
		Driver:   stubDriver{templated: []string{"serviceAccount"}},
		Defaults: map[string]string{"zone": "eu"},
		Parameters: map[string]string{
			"serviceAccount": "${name}-${namespace}",
			"location":       "EUROPE-WEST4",
		},
	}

	got, err := b.ResolvedParameters("orders", "tenant1", nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got["serviceAccount"] != "orders-tenant1" || got["location"] != "EUROPE-WEST4" {
		t.Errorf("resolved view: %v", got)
	}

	// CR wins per key before resolution, so a literal CR value
	// overrides a templated backend default.
	got, err = b.ResolvedParameters("orders", "tenant1", map[string]string{"serviceAccount": "fixed-name"})
	if err != nil || got["serviceAccount"] != "fixed-name" {
		t.Errorf("CR override: %v %v", got, err)
	}

	// Backend defaults feed ${backend.X} in parameter templates.
	b.Parameters["serviceAccount"] = "${backend.zone}-${name}"
	if got, err = b.ResolvedParameters("orders", "tenant1", nil); err != nil || got["serviceAccount"] != "eu-orders" {
		t.Errorf("backend ref: %v %v", got, err)
	}

	// Label references are rejected (restricted grammar).
	if _, err := b.ResolvedParameters("orders", "tenant1", map[string]string{"serviceAccount": "${label['site']}"}); err == nil {
		t.Error("label reference accepted in parameter template")
	}

	// A driver without templated keys passes the merged view
	// through untouched, templates and all.
	plain := Backend{Driver: stubDriver{}, Parameters: map[string]string{"serviceAccount": "${name}"}}
	if got, err := plain.ResolvedParameters("orders", "t1", nil); err != nil || got["serviceAccount"] != "${name}" {
		t.Errorf("no-capability passthrough: %v %v", got, err)
	}
}

// Load validates backend parameter defaults at startup, but
// driver-declared templated keys only resolve per resource: they
// get a template syntax check and skip driver value validation.
func TestLoadTemplatedParameterDefaults(t *testing.T) {
	registry.Register("cfgstub", "0.0.1", func(json.RawMessage) (registry.Driver, error) {
		return stubDriver{templated: []string{"serviceAccount"}}, nil
	})

	write := func(t *testing.T, yaml string) string {
		t.Helper()
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, Filename), []byte(yaml), 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}

	// Templated default passes startup despite the stub rejecting
	// any unresolved templated key it sees.
	dir := write(t, `
backends:
- name: be
  driver: cfgstub
  parameters:
    serviceAccount: ${name}-${namespace}
    location: EU
`)
	if _, err := Load(dir); err != nil {
		t.Fatalf("templated default rejected at startup: %v", err)
	}

	// Template syntax errors still crash-loop with a config
	// diagnostic instead of failing every resource at admission.
	dir = write(t, `
backends:
- name: be
  driver: cfgstub
  parameters:
    serviceAccount: ${label['site']}-x
`)
	if _, err := Load(dir); err == nil {
		t.Fatal("label reference in templated default accepted at startup")
	}
}

// The RawMessage detour that lets templated defaults through MUST
// NOT soften the loader's envsubst policy for everything else: a
// ${...} in a non-templated parameter default is still a startup
// error, not a value that travels to the backend verbatim.
func TestLoadRejectsRefsInNonTemplatedDefaults(t *testing.T) {
	registry.Register("cfgstub2", "0.0.1", func(json.RawMessage) (registry.Driver, error) {
		return stubDriver{templated: []string{"serviceAccount"}}, nil
	})
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, Filename), []byte(`
backends:
- name: be
  driver: cfgstub2
  parameters:
    location: ${REGION}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(dir)
	if err == nil || !strings.Contains(err.Error(), "parameters.location") {
		t.Fatalf("non-templated ${...} default: %v", err)
	}
}
