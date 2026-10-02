# mysql / retention-policy

**Scenario:** SPEC.md §End-to-end coverage, retention, for the
`mysql` driver.

- `keep-me` (`Retain`): deleting the `Buckety` keeps the database
  and its rows; its user is dropped with the implicit access, so a
  retained database keeps no credentials.
- `drop-me` (`Delete`): the database is dropped with its tables,
  and its user with it.

assert.sh drops the retained database at the end.
