# Standalone CR schemas

Whole-document JSON schemas for editor validation of Buckety and
BucketyAccess yamls, annotated the same way as
kubernetes-json-schema:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/Yolean/buckety-controller/main/schema/buckety-objectstore.schema.json
```

The suffix picks a rung on a specialize/generalize ladder; a
maintainer moves a resource between rungs by switching the URL,
nothing else:

| Schema | spec.parameters accepts |
| --- | --- |
| `buckety.schema.json` | anything (string values) - any driver |
| `buckety-objectstore.schema.json` | object-store family-common only (`versioning`, `lifecycle` portable subset) - the resource stays provisionable on any bucket backend, gcs or s3 |
| `buckety-gcs.schema.json` | the full gcs driver set |
| `buckety-s3.schema.json` | the full s3 driver set |
| `buckety-kadm.schema.json` | the kadm driver set (`partitions`, `replicationFactor`, `config.*`) - kadm is not in a family |
| `bucketyaccess.schema.json` | (BucketyAccess; parameters unconstrained) |

Editor validation is documentation and early feedback, not
enforcement: the admission webhook remains the authority, and it
validates against the backend the CR actually names, not the
schema the file claims. The two agree by construction - both
compose from the same per-driver definitions.

Pin a release by ref, e.g.
`.../buckety-controller/v0.1.1/schema/...` (schemas ship from
v0.1.1; the v0.1.0 tag predates this directory), or track `main`.

Generated - do not edit by hand. Source of truth is the CRD
yamls (CR shape) plus
`pkg/drivers/<driver-or-family>/schema/v0.1/parameters.schema.json`
(parameters). Regenerate with `go run ./scripts/gen-cr-schemas`;
CI fails if the output is not committed.
