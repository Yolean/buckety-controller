// gen-cr-schemas emits standalone whole-document JSON schemas for
// Buckety and BucketyAccess into schema/, for yaml-language-server
// annotations on CR files (the kubernetes-json-schema pattern).
//
// The CRD yamls stay the source of truth for the CR shape; the
// per-driver and per-family parameters schemas stay the source of
// truth for spec.parameters. This generator only composes them, so
// the suffixed outputs form a specialize/generalize hierarchy a
// maintainer walks by switching the URL:
//
//	buckety.schema.json               any driver, parameters unconstrained
//	buckety-objectstore.schema.json   family-common parameters only (portable CR)
//	buckety-gcs.schema.json           full gcs driver parameters
//	buckety-s3.schema.json            full s3 driver parameters
//	buckety-kadm.schema.json          kadm driver parameters (no family)
//
// Run via `go run ./scripts/gen-cr-schemas` from the repo root; CI
// asserts the output is committed (git diff --exit-code -- schema/).
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	yaml "sigs.k8s.io/yaml"
)

const rawBase = "https://raw.githubusercontent.com/Yolean/buckety-controller/main/schema/"

type doc = map[string]interface{}

func main() {
	bucketySpec, bucketyStatus := crdSchema("deploy/kustomize/crd/buckety.yaml")
	accessSpec, accessStatus := crdSchema("deploy/kustomize/crd/bucketyaccess.yaml")

	variants := []struct {
		suffix     string
		paramsFile string
		desc       string
	}{
		{"", "",
			"Standalone editor schema for a Buckety against any backend. spec.parameters is unconstrained at this level; SPECIALIZE by switching the $schema URL suffix: buckety-objectstore (portable across bucket backends), buckety-gcs, buckety-s3, buckety-kadm."},
		{"objectstore", "pkg/drivers/objectstore/schema/v0.1/parameters.schema.json",
			"Standalone editor schema for a Buckety carrying only object-store family-common parameters, provisionable on any bucket backend (gcs, s3) - see SPEC.md \"Driver families\". SPECIALIZE to buckety-gcs or buckety-s3 for driver-specific parameters; GENERALIZE to buckety for no parameter constraints."},
		{"gcs", "pkg/drivers/gcs/schema/v0.1/parameters.schema.json",
			"Standalone editor schema for a Buckety whose backend resolves to the gcs driver. GENERALIZE to buckety-objectstore to keep the resource portable across bucket backends, or to buckety for no parameter constraints."},
		{"s3", "pkg/drivers/s3/schema/v0.1/parameters.schema.json",
			"Standalone editor schema for a Buckety whose backend resolves to the s3 driver. GENERALIZE to buckety-objectstore to keep the resource portable across bucket backends, or to buckety for no parameter constraints."},
		{"kadm", "pkg/drivers/kadm/schema/v0.1/parameters.schema.json",
			"Standalone editor schema for a Buckety whose backend resolves to the kadm driver. kadm is deliberately not in a driver family (families are per service kind); GENERALIZE to buckety for no parameter constraints."},
	}

	must(os.MkdirAll("schema", 0o755))
	for _, v := range variants {
		spec := deepCopy(bucketySpec)
		if v.paramsFile != "" {
			params := loadJSON(v.paramsFile)
			delete(params, "$schema")
			spec["properties"].(doc)["parameters"] = params
		}
		name := "buckety"
		title := "Buckety (any driver)"
		if v.suffix != "" {
			name += "-" + v.suffix
			title = "Buckety (" + v.suffix + ")"
		}
		write("schema/"+name+".schema.json",
			wrap(name, title, v.desc, "Buckety", spec, deepCopy(bucketyStatus)))
	}
	write("schema/bucketyaccess.schema.json",
		wrap("bucketyaccess", "BucketyAccess",
			"Standalone editor schema for a BucketyAccess. Driver-specific spec.parameters validation happens at admission; this schema leaves it unconstrained.",
			"BucketyAccess", deepCopy(accessSpec), deepCopy(accessStatus)))
}

// crdSchema extracts the served version's spec and status schemas
// from a CRD manifest, with Kubernetes-only extensions stripped.
func crdSchema(path string) (spec, status doc) {
	raw, err := os.ReadFile(path)
	must(err)
	var crd doc
	must(yaml.Unmarshal(raw, &crd))
	versions := crd["spec"].(doc)["versions"].([]interface{})
	root := versions[0].(doc)["schema"].(doc)["openAPIV3Schema"].(doc)
	stripKubernetesExtensions(root)
	props := root["properties"].(doc)
	return props["spec"].(doc), props["status"].(doc)
}

// stripKubernetesExtensions removes x-kubernetes-* keys (CEL
// validations, list-type hints, preserve-unknown-fields) that are
// not JSON Schema and would be dead weight in an editor schema.
func stripKubernetesExtensions(m doc) {
	for k, v := range m {
		if strings.HasPrefix(k, "x-kubernetes-") {
			delete(m, k)
			continue
		}
		switch t := v.(type) {
		case doc:
			stripKubernetesExtensions(t)
		case []interface{}:
			for _, e := range t {
				if em, ok := e.(doc); ok {
					stripKubernetesExtensions(em)
				}
			}
		}
	}
}

func wrap(name, title, description, kind string, spec, status doc) doc {
	return doc{
		"$schema":     "http://json-schema.org/draft-07/schema#",
		"$id":         rawBase + name + ".schema.json",
		"title":       title,
		"description": description,
		"type":        "object",
		"required":    []string{"apiVersion", "kind", "metadata", "spec"},
		"properties": doc{
			"apiVersion": doc{"type": "string", "enum": []string{"buckety.yolean.se/v1alpha1"}},
			"kind":       doc{"type": "string", "enum": []string{kind}},
			"metadata": doc{
				"type":     "object",
				"required": []string{"name"},
				"properties": doc{
					"name":        doc{"type": "string"},
					"namespace":   doc{"type": "string"},
					"labels":      doc{"type": "object", "additionalProperties": doc{"type": "string"}},
					"annotations": doc{"type": "object", "additionalProperties": doc{"type": "string"}},
				},
			},
			"spec":   spec,
			"status": status,
		},
	}
}

func loadJSON(path string) doc {
	raw, err := os.ReadFile(path)
	must(err)
	var m doc
	must(json.Unmarshal(raw, &m))
	return m
}

func deepCopy(m doc) doc {
	raw, err := json.Marshal(m)
	must(err)
	var out doc
	must(json.Unmarshal(raw, &out))
	return out
}

func write(path string, m doc) {
	raw, err := json.MarshalIndent(m, "", "  ")
	must(err)
	must(os.WriteFile(path, append(raw, '\n'), 0o644))
	fmt.Println("wrote", path)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
