# mysql / adoption

SPEC scenario 11 ("Adoption") for the `mysql` driver: a `Buckety`
whose resolved name collides with an existing database must not
silently claim it, and deleting an adopted `Buckety` must never
drop data that predates the CR. A database is empty when it holds
no tables, views, routines or events.

- `adopt-pre` collides with a pre-created database holding a table:
  the default policy (AdoptEmpty) surfaces
  `Ready=False/BackendResourceExists` and mints no Secret;
  `spec.adoption=Adopt` unblocks it with `status.provenance=Adopted`,
  and the minted user reads the existing rows.
- `adopt-void` collides with a pre-created empty database and adopts
  under the default policy.
- `adopt-fresh` proves `provenance=Created` and that created
  databases still honour `retentionPolicy=Delete`.
- Deleting either adopted `Buckety` keeps its database despite
  `retentionPolicy=Delete`; assert.sh drops them at the end.

Only names inside the backend's `namePrefix` can be adopted; the
driver refuses every other name.
