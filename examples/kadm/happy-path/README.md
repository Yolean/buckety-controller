# kadm / happy-path

**Scenario:** SPEC.md §End-to-end coverage #1 — Single-consumer happy path.

A `Buckety` with `defaultAccess` provisions a Kafka topic and
materialises the Secret a single workload consumes. The
implicit `BucketyAccess` is reclaimable via owner-ref (see
SPEC §Implicit access).

**Demonstrates:**

- A `Buckety` against backend `kafka` (driver-typed name; the
  per-implementation overlay defines what that backend points
  at; redpanda for the CI matrix).
- `defaultAccess.role: ReadWrite` materialises an implicit
  `BucketyAccess` of the same name with label
  `buckety.yolean.se/implicit=true`.
- The minted Secret carries the documented kadm keys:
  `bootstrap` and the resource-type key `topic`.
- A consumer `Job` reads bootstrap + topic from the Secret via
  `valueFrom.secretKeyRef` (no init-container parsing) and
  produces+consumes one message.

**Assertions** (`assert.sh`):

1. `Buckety/orders` reaches `Ready=True`.
2. `BucketyAccess/orders` (implicit) exists with the implicit
   label and an owner-ref to `Buckety/orders`.
3. `Secret/orders-topic` exists with keys `bootstrap` and
   `topic`, both non-empty.
4. The topic named in `Secret/.data.topic` exists on the broker.
5. The consumer `Job` completes successfully.
