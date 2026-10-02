# mysql driver examples

MariaDB/MySQL databases as `Buckety`, users as `BucketyAccess`.
The design and the security model are in
[`docs/mysql.md`](../../docs/mysql.md); this page is the setup.

| Path | What |
| --- | --- |
| `bootstrap/` | Yolean/kubernetes-mysql-cluster `variants/scale-1`, plus the `initdb/` component that creates the controller's account on first boot |
| `buckety-controller.yaml` | the backend config |
| `controller-env-patch.yaml` | the controller Deployment env the config reads |
| `happy-path/`, `multi-consumer/`, `oob-drift/`, `adoption/`, `parameter-mutation/`, `retention-policy/` | e2e scenarios for this driver's behaviour |
| `backend-stickiness/`, `driver-version/`, `misconfigured-startup/`, `scaled-to-zero/` | e2e scenarios whose bodies every driver shares (`test/e2e/lib.sh`) |

## 1. Generate the account password once

The MariaDB pod mounts the password and the controller reads it as
env; a pod can only use Secrets from its own namespace, so the same
Secret goes into both:

```sh
umask 077
head -c 24 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n' > mysql-buckety-password
kubectl -n mysql   create secret generic mysql-buckety --from-file=password=mysql-buckety-password
kubectl -n buckety create secret generic mysql-buckety --from-file=password=mysql-buckety-password
rm mysql-buckety-password
```

Use 16-128 characters of `A-Za-z0-9._~-` and no trailing newline;
the bootstrap script refuses anything else, and the controller
refuses a line break.

## 2. Create the controller's account

kubernetes-mysql-cluster lets root connect only from localhost, so
the account is created inside the MariaDB pod.

**New instance:** deploy with the component, e.g.
`kubectl apply -k examples/mysql/bootstrap`, or add it to your own
kustomization of any kubernetes-mysql-cluster variant:

```yaml
resources:
- github.com/Yolean/kubernetes-mysql-cluster/variants/scale-1?ref=<commit>
components:
- <path to>/examples/mysql/bootstrap/initdb
```

When the image entrypoint initialises the empty data directory, it
runs `/docker-entrypoint-initdb.d/buckety-account.sh`, which reads
`/run/secrets/buckety/password` and runs:

```sql
CREATE USER IF NOT EXISTS 'buckety'@'%' IDENTIFIED BY '<password>';
GRANT CREATE USER ON *.* TO 'buckety'@'%';
GRANT ALL PRIVILEGES ON `b\_%`.* TO 'buckety'@'%' WITH GRANT OPTION;
```

`BUCKETY_ADMIN_USER` and `BUCKETY_NAME_PREFIX` override `buckety`
and `b_`. With Galera (scale-2, the 3-node base) the first node
creates the account and the others receive it with the data.

**Existing instance:** the entrypoint skips init scripts when data
exists. Apply the component (the pod restarts with the script and
the Secret mounted) and run the script once; it is idempotent:

```sh
kubectl -n mysql exec mariadb-0 -c mariadb -- /docker-entrypoint-initdb.d/buckety-account.sh
```

Or run the three statements above as root by hand.

## 3. Configure the backend

Add the backend from `buckety-controller.yaml` to the controller
config and the env from `controller-env-patch.yaml` to its
Deployment (see the top-level README, *Install*). The controller
connects lazily: a database that is down fails reconciles, not
startup.

## 4. Use it

```yaml
apiVersion: buckety.yolean.se/v1alpha1
kind: Buckety
metadata:
  name: orders
  namespace: shop
spec:
  backend: cluster-mysql
  # Must start with the backend's namePrefix: b_shop_orders
  name: "b_${namespace}_${name}"
  parameters:
    characterSet: utf8mb4
    collation: utf8mb4_unicode_ci
  retentionPolicy: Retain        # Delete drops the database with its data
  defaultAccess:
    role: ReadWrite
    credentialsSecretName: orders-db
---
# A second consumer that only reads; it gets a user of its own.
# (An explicit access replaces the implicit one from defaultAccess;
# see SPEC.md "Implicit access".)
apiVersion: buckety.yolean.se/v1alpha1
kind: BucketyAccess
metadata:
  name: orders-reporting
  namespace: shop
spec:
  bucketyRef:
    name: orders
  credentialsSecretName: orders-reporting-db
  role: Reader
```

The Secret has `host`, `port`, `database`, `username`, `password`,
`jdbcUrl` and `url`. For Keycloak, for example:

```yaml
env:
- name: KC_DB
  value: mariadb
- name: KC_DB_URL
  valueFrom: { secretKeyRef: { name: keycloak-db, key: jdbcUrl } }
- name: KC_DB_USERNAME
  valueFrom: { secretKeyRef: { name: keycloak-db, key: username } }
- name: KC_DB_PASSWORD
  valueFrom: { secretKeyRef: { name: keycloak-db, key: password } }
```

Roles: `Reader` gets `SELECT`; `ReadWrite` gets data changes plus
the DDL migrations need; `Writer` is refused. To rotate a password,
delete the access's Secret (the next reconcile generates one), or
write a new password into it (it is applied to the server). Running
consumers keep their open connections; restart them to pick up a
new password.

Databases that predate the prefix (say, a `keycloak` database
created by hand) cannot be adopted; see
[`docs/mysql.md`](../../docs/mysql.md#adoption-and-retention).
