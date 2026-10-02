# mysql / multi-consumer

**Scenario:** SPEC.md §End-to-end coverage #2 - Multiple consumers,
different roles, for the `mysql` driver.

Unlike the v1alpha1 kadm/s3 drivers, the mysql driver gives every
`BucketyAccess` its own user with grants for its role, so the roles
are enforced by the server.

**Demonstrates:**

- One database, three `BucketyAccess`: `ReadWrite` (data and DDL),
  `Reader` (`SELECT` only) and `Writer`, which the mysql driver
  refuses (`Ready=False`, reason `GrantFailed`) rather than guess a
  grant set for.
- A `Job` writes as the ReadWrite user and verifies the Reader can
  read but not insert or create tables.
- Deletion blocks on explicit accesses; removing them drops their
  users and then, with `retentionPolicy: Delete`, the database.

**Assertions** (`assert.sh`):

1. Buckety and the ReadWrite/Reader accesses reach Ready, with full
   Secrets and no `ScopingNotImplemented`.
2. Both Secrets name the same database and different users.
3. The Writer access reports `GrantFailed` and has no Secret.
4. The roles Job completes.
5. `BlockedByAccesses` while accesses exist; deletion completes
   after they are removed.
