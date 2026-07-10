package registry

import (
	"encoding/json"
	"testing"
)

// This test binary imports no driver packages, so the registry
// starts empty and registrations here don't collide with the real
// drivers.

func noopFactory(json.RawMessage) (Driver, error) { return nil, nil }

func TestRegisterLookupVersions(t *testing.T) {
	Register("test-a", "0.2.1", noopFactory)
	if _, ok := Lookup("test-a"); !ok {
		t.Fatal("registered driver not found")
	}
	if _, ok := Lookup("nope"); ok {
		t.Fatal("unknown driver found")
	}
	if v := Versions()["test-a"]; v != "0.2.1" {
		t.Fatalf("version = %q", v)
	}
}

func TestDuplicateRegisterPanics(t *testing.T) {
	Register("test-dup", "0.1.0", noopFactory)
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate Register did not panic")
		}
	}()
	Register("test-dup", "0.1.0", noopFactory)
}

func TestIsParameterDrift(t *testing.T) {
	if !IsParameterDrift(&ErrParameterDrift{Reason: "x"}) {
		t.Fatal("direct ErrParameterDrift not detected")
	}
	if IsParameterDrift(json.Unmarshal([]byte("{"), &struct{}{})) {
		t.Fatal("unrelated error detected as drift")
	}
}
