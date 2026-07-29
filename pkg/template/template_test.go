package template

import (
	"strings"
	"testing"
)

func TestResolve(t *testing.T) {
	cases := []struct {
		name  string
		tmpl  string
		in    Inputs
		want  string
		isErr bool
	}{
		{"empty", "", Inputs{}, "", false},
		{"literal", "orders", Inputs{}, "orders", false},
		{"name", "${name}", Inputs{Name: "orders"}, "orders", false},
		{"namespace.name", "${namespace}.${name}", Inputs{Name: "orders", Namespace: "tenant1"}, "tenant1.orders", false},
		{"label-bracket", "${label['yolean.se/generation']}", Inputs{Labels: map[string]string{"yolean.se/generation": "003"}}, "003", false},
		{"backend.zone", "${backend.zone}.${name}", Inputs{Name: "orders", BackendDefaults: map[string]string{"zone": "eu"}}, "eu.orders", false},
		{"dollar-dollar", "price$$cost", Inputs{}, "price$cost", false},
		{"missing-label", "${label['no.such']}", Inputs{Labels: map[string]string{"other": "x"}}, "", true},
		{"missing-backend-default", "${backend.zone}", Inputs{}, "", true},
		{"empty-name-via-name-ref", "${name}", Inputs{}, "", true},
		{"unterminated", "${name", Inputs{}, "", true},
		{"unsupported-ref", "${spec.foo}", Inputs{}, "", true},
		{"composite", "${backend.zone}.${namespace}.${name}.v${label['yolean.se/generation']}", Inputs{
			Name: "orders", Namespace: "tenant1",
			Labels:          map[string]string{"yolean.se/generation": "003"},
			BackendDefaults: map[string]string{"zone": "eu"},
		}, "eu.tenant1.orders.v003", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Resolve(c.tmpl, c.in)
			if c.isErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

// ResolveParameters is the parameter-template variant: declared
// keys only, restricted grammar (no labels - parameters re-resolve
// every reconcile, so mutable inputs would drift the value).
func TestResolveParameters(t *testing.T) {
	in := Inputs{Name: "orders", Namespace: "tenant1", BackendDefaults: map[string]string{"zone": "eu"}}

	params := map[string]string{
		"serviceAccount": "${name}-${namespace}",
		"lifecycle":      `{"rule": []}`,
	}
	got, err := ResolveParameters(params, []string{"serviceAccount"}, in)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got["serviceAccount"] != "orders-tenant1" {
		t.Errorf("serviceAccount: %q", got["serviceAccount"])
	}
	if got["lifecycle"] != `{"rule": []}` {
		t.Errorf("undeclared key touched: %q", got["lifecycle"])
	}
	// The input map is never mutated: callers pass the shared
	// effective view.
	if params["serviceAccount"] != "${name}-${namespace}" {
		t.Errorf("input mutated: %q", params["serviceAccount"])
	}

	// Undeclared keys keep template-looking values verbatim.
	if got, err := ResolveParameters(map[string]string{"lifecycle": "${name}"}, []string{"serviceAccount"}, in); err != nil || got["lifecycle"] != "${name}" {
		t.Errorf("undeclared passthrough: %v %v", got, err)
	}

	// Literal values: the same map comes back (no copy churn).
	lit := map[string]string{"serviceAccount": "orders-static"}
	if got, err := ResolveParameters(lit, []string{"serviceAccount"}, in); err != nil || got["serviceAccount"] != "orders-static" {
		t.Errorf("literal: %v %v", got, err)
	}

	// Backend defaults resolve; a missing default errors with the
	// parameter key named.
	if got, err := ResolveParameters(map[string]string{"serviceAccount": "${backend.zone}-${name}"}, []string{"serviceAccount"}, in); err != nil || got["serviceAccount"] != "eu-orders" {
		t.Errorf("backend default: %v %v", got, err)
	}
	if _, err := ResolveParameters(map[string]string{"serviceAccount": "${backend.nope}"}, []string{"serviceAccount"}, in); err == nil {
		t.Error("missing backend default accepted")
	}

	// Labels are rejected in parameter templates even when the
	// label exists - mutable inputs would drift the re-resolved
	// value.
	inWithLabels := in
	inWithLabels.Labels = map[string]string{"site": "a"}
	_, err = ResolveParameters(map[string]string{"serviceAccount": "${label['site']}-x"}, []string{"serviceAccount"}, inWithLabels)
	if err == nil || !strings.Contains(err.Error(), "parameter templates") {
		t.Errorf("label ref: %v", err)
	}

	// $$ escaping still applies.
	if got, _ := ResolveParameters(map[string]string{"serviceAccount": "a$$b"}, []string{"serviceAccount"}, in); got["serviceAccount"] != "a$b" {
		t.Errorf("escape: %q", got["serviceAccount"])
	}
}
