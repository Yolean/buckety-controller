# mysql / happy-path

**Scenario:** SPEC.md §End-to-end coverage #1 - Single-consumer
happy path, for the `mysql` driver.

A `Buckety` with `defaultAccess` creates a MariaDB database and a
user with ReadWrite grants on it, and mints the Secret a single
workload consumes.

**Demonstrates:**

- `spec.name: "b_${namespace}_${name}"`: database names must start
  with the backend's `namePrefix` (`b_`).
- `characterSet` / `collation` parameters, applied at creation.
- The minted Secret carries the documented mysql keys: `host`,
  `port`, `database` (resource-type key), `username`, `password`,
  `jdbcUrl`, `url`.
- A consumer `Job` connects with the Secret's values, creates and
  alters a table (schema migrations need DDL), and reads back a row.

**Assertions** (`assert.sh`):

1. `Buckety/orders` and its implicit `BucketyAccess/orders` reach
   `Ready=True`.
2. `Secret/orders-db` has all keys and the owned label.
3. `database` and `status.backendResourceName` are
   `b_<namespace>_orders`; `status.principal` is `<username>@%`.
4. No `ScopingNotImplemented`: the mysql driver scopes per role.
5. The consumer `Job` completes.
