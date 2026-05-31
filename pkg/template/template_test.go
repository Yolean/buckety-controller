package template

import "testing"

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
