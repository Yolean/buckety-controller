package kadm

import (
	"encoding/json"
	"fmt"

	"github.com/Yolean/y-cluster/pkg/envsubst"
	yaml "sigs.k8s.io/yaml"
)

// Config is the typed shape of the `config:` block under a kadm
// backend in buckety-controller.yaml. Matches
// pkg/drivers/kadm/schema/v0.1/config.schema.json.
type Config struct {
	// SeedBrokers is the bootstrap address list (host:port).
	SeedBrokers []string `json:"seedBrokers"`
	// ClientID is advertised to the broker; empty means use the
	// kgo default.
	ClientID string `json:"clientID,omitempty"`
}

// decodeConfig strictly decodes raw JSON into Config and applies
// envsubst to tagged fields. SeedBrokers/ClientID are not
// envsubst-tagged in v1alpha1; kadm has no credential fields
// that need it yet.
func decodeConfig(raw json.RawMessage) (*Config, error) {
	var c Config
	// sigs.k8s.io/yaml.UnmarshalStrict converts JSON->Go map then
	// strict-decodes into the typed struct, rejecting unknown keys.
	if err := yaml.UnmarshalStrict(raw, &c); err != nil {
		return nil, fmt.Errorf("kadm config: %w", err)
	}
	if err := envsubst.Apply(&c, envsubst.OSEnv); err != nil {
		return nil, fmt.Errorf("kadm config: %w", err)
	}
	if len(c.SeedBrokers) == 0 {
		return nil, fmt.Errorf("kadm config: missing required field %q", "seedBrokers")
	}
	return &c, nil
}
