package kadm

import (
	"strings"
	"testing"
)

// Validation methods never touch the broker clients, so a zero
// Driver is sufficient.
var d = &Driver{}

func TestValidateParameters(t *testing.T) {
	cases := []struct {
		name    string
		params  map[string]string
		wantErr string // substring; empty means accept
	}{
		{"empty", nil, ""},
		{"partitions ok", map[string]string{"partitions": "12"}, ""},
		{"partitions zero", map[string]string{"partitions": "0"}, "positive integer"},
		{"partitions negative", map[string]string{"partitions": "-3"}, "positive integer"},
		{"partitions garbage", map[string]string{"partitions": "many"}, "positive integer"},
		{"rf ok", map[string]string{"replicationFactor": "3"}, ""},
		{"rf broker default", map[string]string{"replicationFactor": "-1"}, ""},
		{"rf zero", map[string]string{"replicationFactor": "0"}, "non-zero"},
		{"config passthrough", map[string]string{"config.retention.ms": "not-even-a-number"}, ""},
		{"unknown key", map[string]string{"retentionMs": "1"}, "unknown parameter"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := d.ValidateParameters(c.params)
			checkErr(t, err, c.wantErr)
		})
	}
}

func TestValidateUpdateParametersRejectsRFChange(t *testing.T) {
	old := map[string]string{"replicationFactor": "1"}
	err := d.ValidateUpdateParameters(old, map[string]string{"replicationFactor": "3"})
	checkErr(t, err, "immutable")
	if err := d.ValidateUpdateParameters(old, old); err != nil {
		t.Fatalf("unchanged replicationFactor rejected: %v", err)
	}
	// Partition shrink is deliberately NOT rejected at admission;
	// it surfaces as ParameterDrift at reconcile time.
	if err := d.ValidateUpdateParameters(
		map[string]string{"partitions": "3"},
		map[string]string{"partitions": "2"}); err != nil {
		t.Fatalf("partition shrink should pass admission (handled as drift): %v", err)
	}
}

func TestValidateResourceName(t *testing.T) {
	cases := []struct {
		name    string
		topic   string
		wantErr string
	}{
		{"plain", "orders", ""},
		{"dotted", "tenant1.orders.v003", ""},
		{"mixed case ok", "Orders_2024-v1", ""},
		{"empty", "", "empty"},
		{"dot reserved", ".", "reserved"},
		{"dotdot reserved", "..", "reserved"},
		{"too long", strings.Repeat("a", 250), "249"},
		{"max length ok", strings.Repeat("a", 249), ""},
		{"bad char", "orders,v1", "characters outside"},
		{"space", "or ders", "characters outside"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			checkErr(t, d.ValidateResourceName(c.topic), c.wantErr)
		})
	}
}

func TestTranslateParameters(t *testing.T) {
	parts, rf, cfgs, err := translateParameters(map[string]string{
		"partitions":          "12",
		"replicationFactor":   "3",
		"config.retention.ms": " 604800000 ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if parts != 12 || rf != 3 {
		t.Fatalf("parts=%d rf=%d", parts, rf)
	}
	// config. prefix stripped, value trimmed.
	v, ok := cfgs["retention.ms"]
	if !ok || v == nil || *v != "604800000" {
		t.Fatalf("cfgs=%v", cfgs)
	}
	if _, _, _, err := translateParameters(map[string]string{"nope": "1"}); err == nil {
		t.Fatal("unknown key accepted")
	}
}

func checkErr(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not contain %q", err, want)
	}
}
