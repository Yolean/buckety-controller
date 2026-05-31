# kadm / multi-consumer

**Scenario:** SPEC.md §End-to-end coverage #2 — Multiple
consumers, different roles.

A single `Buckety` is the source of truth for the topic; three
explicit `BucketyAccess` resources each mint their own Secret.
In v1alpha1 all three Secrets carry identical credentials
(no per-consumer scoping); the assertion is that the
multi-resource lifecycle is correct, not that scope is enforced.

**Demonstrates:**

- One `Buckety/orders` (no `defaultAccess` since explicit
  accesses cover it).
- Three `BucketyAccess` resources with distinct `role` values
  (`Reader`, `Writer`, `ReadWrite`) and distinct
  `credentialsSecretName`.
- Each access mints its own Secret in the scenario namespace.
- All three Secrets carry the same flat kadm keys; values for
  `bootstrap` and `topic` are identical across all three (the
  driver does not yet scope per-consumer).
- v1alpha1 drivers surface a `ScopingNotImplemented` condition
  on each non-ReadWrite access rather than silently treating
  it as ReadWrite.

**Assertions** (`assert.sh`):

1. `Buckety/orders` reaches `Ready=True`.
2. Three BucketyAccess resources reach `Ready=True`.
3. Three Secrets exist, each with `bootstrap` and `topic`.
4. The `topic` value is identical across all three Secrets.
5. `BucketyAccess/orders-reader` and `orders-writer` carry
   `ScopingNotImplemented=True`; `orders-rw` does not.
