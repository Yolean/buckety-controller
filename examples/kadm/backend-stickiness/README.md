# kadm / backend-stickiness

**Scenario:** SPEC.md §End-to-end coverage #7 — Backend
stickiness.

`status.backend`, `status.driver`, `status.driverMajor` and
`status.backendResourceName` are stamped at first reconcile and
never change. If the maintainer renames or removes a backend
after consumers exist, the resource keeps reconciling against
the original triple; if it's gone, `BackendUnavailable=True`
surfaces.

**Demonstrates:**

- Apply a Buckety against backend `kafka`. Stickiness fields
  are stamped.
- Rotate the controller config Secret so the backend `kafka`
  no longer exists (replaced by `kafka-renamed` with the same
  driver and connection). Reroll the controller Pod.
- The existing Buckety still reports `status.backend: kafka`
  and surfaces `BackendUnavailable=True` because no backend by
  that name is registered anymore.
- A *new* Buckety against `kafka-renamed` reconciles
  successfully, proving the cluster is still functional.

**Assertions** (`assert.sh`):

1. Original Buckety reaches Ready; status.backend == "kafka".
2. After the controller config rotation:
   - `status.backend` on the original is still `"kafka"`.
   - `BackendUnavailable=True` is set.
   - `Ready=False`.
3. The new Buckety against `kafka-renamed` reaches Ready.
4. Restore the controller config so subsequent scenarios are
   not affected.

The config-rotation dance lives in the assert script because
this scenario, by design, mutates the controller's deploy-time
state and must restore it.
