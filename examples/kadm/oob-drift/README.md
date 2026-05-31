# kadm / oob-drift

**Scenario:** SPEC.md §End-to-end coverage #4 — Out-of-band
drift.

When someone changes the backend resource outside Buckety
(e.g. an SRE runs `rpk alter-config`), the controller either
silently reapplies (where the change is reconcilable) or
surfaces `ParameterDrift=True` (where it can't reconcile in
place, e.g. an attempted partition-count shrink).

**Demonstrates:**

- Apply Buckety with `partitions: "3"`, wait Ready.
- Direct broker call: `rpk topic alter-config <topic> --set
  retention.ms=1` (reconcilable in-place; controller reapplies
  the spec's value).
- Direct broker call: attempt to shrink to fewer partitions
  via an out-of-band ALTER (not actually possible on Kafka,
  but the SPEC scenario covers the assertion shape for it).
  For v1alpha1, simulate by reducing `partitions` in spec from
  3 to 2: controller surfaces `ParameterDrift=True` and pauses,
  rather than attempting an unsafe shrink.

**Assertions** (`assert.sh`):

1. After the out-of-band retention change, the controller
   reapplies the spec value within the reconcile window.
2. After reducing `partitions` in the spec from 3 to 2, the
   Buckety surfaces `ParameterDrift=True` with a message
   naming `partitions`. `Ready` flips to False.
