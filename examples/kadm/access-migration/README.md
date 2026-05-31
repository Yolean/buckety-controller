# kadm / access-migration

**Scenario:** SPEC.md §Implicit access (`defaultAccess`) -
migration from the single-consumer shortcut to one or more
explicit `BucketyAccess` resources.

Every platform that outgrows `defaultAccess` walks this path.
The mechanics are non-obvious: the implicit access is a real
`BucketyAccess` with an owner-ref to the `Buckety`, labelled
`buckety.yolean.se/implicit=true`, and is reclaimed by the
controller when an explicit access targeting the same Buckety
is observed (or when `defaultAccess` is removed). This
scenario asserts the reclaim ordering CI must catch.

**Demonstrates:**

- Stage 1: `Buckety/orders` with
  `defaultAccess.credentialsSecretName: orig-secret`. The
  controller materialises `BucketyAccess/orders` (implicit)
  and `Secret/orig-secret`.
- Stage 2: re-apply with `defaultAccess` removed and a new
  explicit `BucketyAccess/orders-explicit` carrying
  `credentialsSecretName: explicit-secret` (fresh name; the
  zero-gap migration path the README recommends).
- The controller deletes the implicit `BucketyAccess` and
  `Secret/orig-secret` (owner-ref GC); the explicit access
  reconciles to a fresh Secret.

The same-name case (explicit access reuses `orig-secret`) is
documented in [`README.md`](../../../README.md#use-consumer-view)
as the race-and-gap path. CI focuses on the fresh-name path
because it is the recommended one and the more easily asserted.

**Assertions** (`assert.sh`):

1. After stage 1: `Buckety/orders` Ready;
   `BucketyAccess/orders` exists, carries
   `buckety.yolean.se/implicit=true` and an owner-ref to the
   Buckety; `Secret/orig-secret` carries the kadm keys.
2. Apply stage 2. Within 60s:
   - `BucketyAccess/orders` (implicit) is gone.
   - `Secret/orig-secret` is gone.
   - `BucketyAccess/orders-explicit` reaches Ready.
   - `Secret/explicit-secret` carries the kadm keys, and
     `topic` resolves to the same broker topic as before
     (the `Buckety` itself was not recreated).
