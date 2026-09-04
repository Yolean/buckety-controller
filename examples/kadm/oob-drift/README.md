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
- Direct broker call: `rpk topic add-partitions <topic> --num 2`
  grows the topic from 3 to 5 partitions. Kafka cannot shrink a
  partition count, so the spec's 3 can never be re-applied: the
  controller surfaces `ParameterDrift=True` and pauses rather
  than attempting an unsafe shrink.

**Assertions** (`assert.sh`):

1. After the out-of-band retention change, the controller
   reapplies the spec value within the reconcile window.
2. After the out-of-band partition growth, the Buckety
   surfaces `ParameterDrift=True` with a message naming
   `partitions`. `Ready` flips to False.
