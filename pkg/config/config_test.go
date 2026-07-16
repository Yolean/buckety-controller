package config

import "testing"

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
