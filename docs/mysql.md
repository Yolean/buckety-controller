# The `mysql` driver

Companion to `SPEC.md` for the MariaDB/MySQL driver (v0.1). The
SPEC has the contract; this page has the security model, the
naming rules and the choices behind them. Setup steps are in
[`examples/mysql/README.md`](../examples/mysql/README.md).

| Resource | On the server |
| --- | --- |
| `Buckety` | a database, `CREATE DATABASE IF NOT EXISTS` with optional character set and collation |
| `BucketyAccess` | a user of its own, with a generated password and the role's privileges on that one database |
| `retentionPolicy: Delete` | `DROP DATABASE` (adopted databases are retained, as for every driver) |
| `BucketyAccess` deletion | `DROP USER`, under every retention policy |

CI tests it against MariaDB 10.11 and 11.4 and MySQL 8.0 and 8.4
(`test/integration/mysql.sh`, which runs
`pkg/drivers/mysql/integration_test.go` against each in Docker),
and end to end against MariaDB 10.11.

## The controller's account

```sql
CREATE USER 'buckety'@'%' IDENTIFIED BY '...';
GRANT CREATE USER ON *.* TO 'buckety'@'%';
GRANT ALL PRIVILEGES ON `b\_%`.* TO 'buckety'@'%' WITH GRANT OPTION;
```

The account can create databases whose names start with `b_`,
create users, and grant on those databases. It cannot read or
change any other database, and it cannot read the `mysql` schema
(so it cannot see other accounts' password hashes or grants).
`\_` matters: in `GRANT ... ON db.*` an unescaped `_` is a
wildcard, and `b_%` would also cover `bad_db`.

`examples/mysql/bootstrap/` creates the account on the server's
first boot from a mounted Secret, so no password is in source.

### The CREATE USER caveat

Neither MariaDB nor MySQL can scope `CREATE USER` by name, and the
privilege also allows `ALTER USER`, `RENAME USER` and `DROP USER`
on **every** account on the server. The server does not confine
the controller's account to its own users; the driver does:

- every database and user name passes one check (`checkManaged`)
  before it reaches SQL: it must start with `namePrefix`, be
  lowercase letters, digits, `_` and `-` within the length limits,
  and not be a system schema or account (`mysql`, `sys`, `root`,
  `mariadb.sys`, ...);
- `namePrefix` is at most 10 characters ending in `_`, which keeps
  it off every system schema and account name, and the admin
  account itself must be outside it;
- `RevokeAccess` parses `status.principal` and refuses to drop
  anything outside the prefix, so a tampered status cannot drop
  `root`. Within the prefix the driver trusts status: whoever can
  write `BucketyAccess` status (by RBAC, only the controller) could
  make it drop another access's user.

A controller compromise (its credentials, or a code path that
bypasses these checks) can still alter or drop any account on the
server. **Sensitive accounts belong on an instance the controller
has no backend for.** On MySQL 8.0.16+ an account holding
`SYSTEM_USER` (root has it) cannot be altered or dropped by an
account without it, which narrows the exposure there; MariaDB has
no equivalent.

**`namePrefix` is fixed for the backend's lifetime.** Databases
and principals stamped under one prefix are refused under another:
reconciles fail and access deletion blocks. To change it, add a
second backend with the new prefix and migrate; to retire one
anyway, drop its users and databases by hand and remove the
finalizers.

## Naming

**Databases** are the resolved `spec.name`, which must start with
`namePrefix`: `spec.name: "b_${namespace}_${name}"`. The driver
does not prepend the prefix itself, so `status.backendResourceName`
and the Secret's `database` key are the real database name. A
`Buckety` without a prefixed `spec.name` is refused at admission
with that suggestion. Limit: 64 characters.

The prefix is literal in the CR. A cluster that wants CRs portable
across prefixes can put it in the backend's `defaults` and write
`spec.name: "${backend.namePrefix}${namespace}_${name}"`.

`spec.name` is tenant-controlled, as for every driver: the prefix
keeps tenants off databases outside it, not off each other's.
`spec.adoption: AdoptEmpty` stops a CR from claiming a database
with tables; `Adopt` claims it regardless. Where tenants are
mutually untrusted, require `${namespace}` in `spec.name` (and
restrict `adoption: Adopt`) with an admission policy.

**Users** are derived from the `BucketyAccess` namespace and name
(the driver gets them in `GrantRequest`):

- `b_<namespace>_<name>` when it fits and neither part has a dot,
  e.g. `b_shop_orders`;
- otherwise `b_<start of namespace-name>__<16 base32 characters of
  sha256(namespace/name)>`: access `orders` in namespace
  `e2e-mysql-mariadb-mysql-happy-path` is
  `b_e2e-mysql-ma__eug6f3psticceco5`.

Kubernetes names contain no `_`, so the first form has one
underscore after the prefix and the second two: the forms cannot
collide, and two accesses share a user only through an 80-bit hash
collision. Names are unique within one cluster, not across
clusters: run one controller per server and `namePrefix`, or give
each cluster's backend a prefix of its own. The limit is 32 characters: MariaDB allows 80, MySQL 8
only 32, and one scheme for both keeps a backend portable between
them.

`status.principal` is `user@host`, with the `userHost` the user
was created with.

## Roles

Privileges are granted on the one database, by its escaped name
(`b\_t1\_orders`), never on a wildcard pattern.

| `spec.role` | Privileges |
| --- | --- |
| `Reader` | `SELECT` |
| `ReadWrite` (default) | `SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES, CREATE TEMPORARY TABLES, LOCK TABLES` |
| `Writer` | refused: `Ready=False`, reason `GrantFailed`, no Secret; an access changed to `Writer` loses its user |

ReadWrite includes the DDL that schema migrations need (an
application that migrates its own schema on startup, as Keycloak
does). Database-level `DROP` and `CREATE` also let a ReadWrite
user drop its own database, or recreate it; nothing beyond it.
On every reconcile the driver grants what is missing and revokes
everything else the user holds on the database by its exact
name, including privileges granted out of band (`EXECUTE`,
`CREATE VIEW`, ...) and `GRANT OPTION`. Grants it cannot see are
listed under *Known limitations*. `Scoped` is true, so
`ScopingNotImplemented` never surfaces for this driver.

## Credentials and drift

The Secret holds `host`, `port`, `database` (the resource-type
key), `username`, `password`, `jdbcUrl`
(`jdbc:mariadb://host:port/database`) and `url`
(`mysql://username:password@host:port/database`).

- The first grant generates a 32-character password from
  `crypto/rand` (letters, digits and `-._~`, all four classes, so
  `simple_password_check` accepts it).
- Later grants reuse the Secret's password. A password changed in
  the Secret is applied to the server; it must be 16-128
  characters of `A-Za-z0-9._~-`, the characters that need no
  escaping anywhere the driver puts them. Anything else is refused
  without echoing it.
- Deleting the Secret rotates the password: the next reconcile
  generates one and sets it with `ALTER USER`. Each change is an
  account statement, which on Galera replicates as a cluster-wide
  blocking (TOI) operation; there is no rate limit, so a tenant
  editing its Secret in a loop can slow the cluster.

The admin account cannot see other users or their grants without
SELECT on the `mysql` schema, so the driver checks each user by
logging in as it with the Secret's password:

- login succeeds: read the user's own rows in
  `information_schema.SCHEMA_PRIVILEGES` and align the grants. When
  nothing differs, nothing is written: steady state costs one login
  per access per re-check and no statements that replicate.
- login is refused: the user is missing or its password differs.
  `CREATE USER`, or `ALTER USER` when it exists, then grant, then
  log in again (retrying for about four seconds, for Galera) to
  verify. A password the driver has just generated is not verified
  in the same pass: it must reach the Secret first, and the next
  re-check logs in with it.

So **`userHost` must admit the controller's own address**; with
the default `%` it does. Changing `userHost` moves each access to
a new account on its next reconcile, and the controller drops the
old one.

Passwords never appear in logs or errors: the driver builds no DSN
strings, and an error from a statement that carries a password
keeps only the server's error number (a parse error's message
quotes the statement) before it reaches a condition or event.

## SQL construction

- Identifiers (database names, character sets, collations) are
  validated against the rules above and backtick-quoted.
- Account names and passwords go in as `?` arguments. The account
  statements (`CREATE USER`, `GRANT`) are not parameterisable in
  the server-side prepared statement protocol, so the client
  interpolates the arguments as escaped string literals
  (go-sql-driver's `interpolateParams`); the values are validated
  first regardless.
- Privilege names read back from the server are checked against
  `^[A-Z]+( [A-Z]+)*$` before they are spliced into `REVOKE`.
- Multi-statements and `LOAD DATA LOCAL` stay disabled.

## Parameters

`characterSet` and `collation` are applied at creation and
immutable. The driver never runs `ALTER DATABASE`: it changes the
default for new tables only, and converging silently would hide
that existing tables still differ. A database whose defaults differ
from the parameters (changed out of band, or adopted) surfaces
`ParameterDrift`, and the controller stops re-checking it until the
`Buckety` changes. Fix it out of band and then touch the `Buckety`
(`kubectl annotate buckety/<name> example.com/recheck="$(date +%s)"
--overwrite`, any annotation will do), or recreate the `Buckety`.
`utf8` is refused as ambiguous (utf8mb3 today, utf8mb4 later).

## Adoption and retention

Adoption works as for every driver, within the prefix: a database
is empty when it has no tables, views, routines or events. Existing
unprefixed databases (say, a `keycloak` database created before
buckety) cannot be adopted, because the driver refuses every name
outside the prefix; migrate them into prefixed databases (dump and
restore), or keep them outside buckety.

`retentionPolicy: Delete` runs `DROP DATABASE` for databases the
controller created. Users are dropped with their `BucketyAccess`
under every policy; a retained database keeps no credentials.

## TLS

`tls: {}` in the backend config requires verified TLS on the
controller's connections (`caFile` and `serverName` optional).
Without it, every access's password crosses the network in
cleartext, inside `CREATE USER`/`ALTER USER` statements and the
login checks; leave `tls` out only where that network is trusted
(an in-cluster Service with NetworkPolicies, say).
Created users get no `REQUIRE SSL`; to require TLS from consumers,
set `require_secure_transport` on the server. On MySQL 8 without
`tls`, `caching_sha2_password` makes the client fetch the server's
RSA key unauthenticated before sending the password; configure
`tls` where the network is not trusted.

## MySQL 8 notes

- `partial_revokes` must stay `OFF` (the default): with it on, the
  `%` in the account's `b\_%` grant is literal.
- User names are capped at 32 characters, which the naming above
  already respects.
- The `jdbcUrl` key is in the MariaDB Connector/J form; MySQL
  Connector/J wants `jdbc:mysql://`.

## Design decisions

1. **Roles.** ReadWrite includes DDL; no separate Owner role.
   The CRD's `Writer` is refused rather than given a guessed
   meaning; a DML-only Writer (`SELECT, INSERT, UPDATE, DELETE`) is
   the obvious candidate if one is wanted.
2. **Naming.** Literal prefix in `spec.name`, validated, 64
   characters for databases; derived user names capped at 32 for
   MySQL 8, with a hashed form for long names.
3. **TLS.** Optional `tls` for the controller's connections; no
   `REQUIRE SSL` on created users (server-side
   `require_secure_transport` instead).
4. **Password rotation.** On demand: delete the Secret, or write a
   new password into it. No scheduled rotation; `ALTER USER` does
   not end sessions that are already open.
5. **Galera.** `CREATE USER`, `GRANT` and DDL replicate as Total
   Order Isolation, which briefly blocks the whole cluster, so the
   driver writes only on change. A login check right after a change
   may reach a node that has not applied it yet; verification
   retries for about four seconds.

## Known limitations

- `DROP USER` and `ALTER USER` do not end open sessions; revoking
  an access stops new logins, not running ones. Database-level
  grant changes, such as a role change from ReadWrite to Reader,
  reach an open session only when it next selects the database,
  usually on reconnect. Ending sessions would need
  `CONNECTION ADMIN`/`SUPER`.
- `DROP DATABASE` waits for open transactions on the database. The
  controller's sessions set `lock_wait_timeout` to 50 seconds, so a
  blocked drop fails and is retried rather than queueing behind
  long-lived consumer sessions.
- A user created in a pass whose Secret write then fails is
  dropped again. A user can still end up recorded by no
  `BucketyAccess`: a controller crash between `CREATE USER` and the
  status write, a failed status write, or a failed drop of a
  replaced principal followed by the access's deletion. Such users
  are found by the prefix. An access later created with the same
  namespace and name takes the user over with its own password.
- Grants the driver cannot see are not managed: table- and
  column-level grants, grants that reach the database through a
  wildcard pattern (`b\_%`, `b_t1%`), global privileges and roles.
  The login check reads only the user's database-level row for the
  exact database name.
- An account with the same user name and a more specific host
  (created by hand) shadows the managed one: the driver cannot
  inspect the managed account through it, so every reconcile sets
  its password and grants again, which on Galera means replicated
  account statements on every re-check. Drop the shadowing account.
