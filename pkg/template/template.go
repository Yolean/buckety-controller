// Package template implements the Buckety.spec.name resolver
// described in SPEC.md §Naming templates.
//
// Grammar (single-pass, no recursion into substituted values):
//
//	${name}                                  metadata.name of the Buckety
//	${namespace}                             metadata.namespace of the Buckety
//	${label['x.example.net/my-label']}       metadata.labels[key]
//	${backend.X}                             backend's defaults map (X)
//
// Resolution failure (missing label, missing backend default) is
// surfaced as an error so the admission webhook can reject the
// resource. A template that resolves successfully but produces a
// name the driver rejects is also caught at admission, by the
// driver's ValidateResourceName (pkg/drivers/registry.Driver);
// the reconciler re-checks it for webhook-disabled deployments.
package template

import (
	"fmt"
	"regexp"
	"strings"
)

// Inputs feeds resolution. All fields may be empty (a literal
// template like "orders" needs nothing).
type Inputs struct {
	Name            string
	Namespace       string
	Labels          map[string]string
	BackendDefaults map[string]string
	// noLabels rejects ${label[...]} references; set only by
	// ResolveParameters (see there for why).
	noLabels bool
}

// Resolve substitutes the supported variables in tmpl against
// inputs and returns the resolved string. Returns an error
// naming the first missing/malformed reference.
func Resolve(tmpl string, inputs Inputs) (string, error) {
	if tmpl == "" {
		// Caller decides the default (typically metadata.name).
		return "", nil
	}
	// Walk the template manually so error messages name the
	// exact reference that failed.
	var b strings.Builder
	for i := 0; i < len(tmpl); {
		// Literal $$ -> single $
		if i+1 < len(tmpl) && tmpl[i] == '$' && tmpl[i+1] == '$' {
			b.WriteByte('$')
			i += 2
			continue
		}
		// Variable starts with ${
		if i+1 < len(tmpl) && tmpl[i] == '$' && tmpl[i+1] == '{' {
			end := strings.IndexByte(tmpl[i:], '}')
			if end == -1 {
				return "", fmt.Errorf("unterminated reference at offset %d", i)
			}
			expr := tmpl[i+2 : i+end]
			val, err := resolveOne(expr, inputs)
			if err != nil {
				return "", err
			}
			b.WriteString(val)
			i += end + 1
			continue
		}
		b.WriteByte(tmpl[i])
		i++
	}
	return b.String(), nil
}

// ResolveParameters returns a copy of params with the values of
// the declared keys template-resolved; undeclared keys pass
// through untouched. Parameter templates use a restricted
// grammar: ${name}, ${namespace} and ${backend.X} only.
// ${label[...]} is rejected because parameters are re-resolved on
// every reconcile - a label edit would silently drift the
// resolved value (a service account name, say) - whereas
// spec.name gets away with label references only because its
// resolution is frozen into status.backendResourceName at first
// reconcile.
func ResolveParameters(params map[string]string, keys []string, in Inputs) (map[string]string, error) {
	if len(params) == 0 || len(keys) == 0 {
		return params, nil
	}
	in.Labels = nil
	in.noLabels = true
	var out map[string]string
	for _, k := range keys {
		tmpl, ok := params[k]
		if !ok {
			continue
		}
		resolved, err := Resolve(tmpl, in)
		if err != nil {
			return nil, fmt.Errorf("parameters.%s: %w", k, err)
		}
		if resolved == tmpl {
			continue
		}
		if out == nil {
			out = make(map[string]string, len(params))
			for pk, pv := range params {
				out[pk] = pv
			}
		}
		out[k] = resolved
	}
	if out == nil {
		return params, nil
	}
	return out, nil
}

// labelRE matches the label['key'] form. The key may contain
// the full K8s label-key shape (DNS-1123 names with an optional
// `<prefix>/` discriminator) but no closing bracket or quote.
var labelRE = regexp.MustCompile(`^label\['([^']+)'\]$`)

// backendRE matches backend.<word>.
var backendRE = regexp.MustCompile(`^backend\.([A-Za-z][A-Za-z0-9_-]*)$`)

func resolveOne(expr string, in Inputs) (string, error) {
	switch expr {
	case "name":
		if in.Name == "" {
			return "", fmt.Errorf("${name}: empty metadata.name")
		}
		return in.Name, nil
	case "namespace":
		if in.Namespace == "" {
			return "", fmt.Errorf("${namespace}: empty metadata.namespace")
		}
		return in.Namespace, nil
	}
	if m := labelRE.FindStringSubmatch(expr); m != nil {
		if in.noLabels {
			return "", fmt.Errorf("${%s}: parameter templates support ${name}, ${namespace} and ${backend.X} only; labels are mutable and would drift the re-resolved value", expr)
		}
		key := m[1]
		v, ok := in.Labels[key]
		if !ok {
			return "", fmt.Errorf("${label['%s']}: no such label on the resource", key)
		}
		return v, nil
	}
	if m := backendRE.FindStringSubmatch(expr); m != nil {
		key := m[1]
		v, ok := in.BackendDefaults[key]
		if !ok {
			return "", fmt.Errorf("${backend.%s}: no such key in the backend's defaults map", key)
		}
		return v, nil
	}
	return "", fmt.Errorf("unsupported reference ${%s}", expr)
}
