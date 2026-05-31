# kadm / naming-templates

**Scenario:** SPEC.md §End-to-end coverage #8 — Naming
templates.

`spec.name` is a template resolved once at first reconcile and
frozen in `status.backendResourceName`. The grammar supports
`${name}`, `${namespace}`, `${label['key']}`, and
`${backend.X}` (resolved from the backend's per-entry
`defaults:` map).

**Demonstrates:**

- A Buckety with `spec.name` referencing `${namespace}`,
  `${label['yolean.se/generation']}`, and `${backend.zone}`.
  The harness's overlay sets the backend's `defaults.zone: e2e`
  so the resolved name is predictable.
- `status.backendResourceName` is the resolved string.
- Changing the label *after* first reconcile does NOT change
  the backend name (frozen).
- A Buckety with a template referencing a missing label is
  rejected at admission (webhook).

**Assertions** (`assert.sh`):

1. `Buckety/templated` reaches Ready. The resolved name
   matches the expected literal (computed from the spec and
   the harness's overlay zone).
2. Patch the label to a new value. After a reconcile the
   `backendResourceName` is unchanged.
3. Applying `bad-template.yaml` (references
   `${label['no.such/label']}`) is rejected by the webhook
   with a message that names the missing label.
