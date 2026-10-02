# mysql / parameter-mutation

**Scenario:** SPEC.md §End-to-end coverage, parameter validation,
for the `mysql` driver. Unlike bucket parameters, `characterSet`
and `collation` are set at creation and immutable: changing a
database's default does not convert its tables, so the driver
refuses to pretend it did.

The admission webhook refuses:

- a changed `characterSet`, or a removed `collation` (immutable);
- an unknown parameter key;
- `characterSet: utf8` on a new `Buckety` (ambiguous: utf8mb3 now,
  utf8mb4 later);
- a `Buckety` whose resolved name lacks the backend's `namePrefix`.

`BucketyAccess` parameters (the mysql driver accepts none) are
refused by the reconciler: `Ready=False`, reason
`InvalidParameters`, no Secret.

The database keeps the character set and collation it was created
with throughout.
