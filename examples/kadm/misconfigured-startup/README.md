# kadm / misconfigured-startup

**Scenario:** SPEC.md §End-to-end coverage #10 — Misconfigured
controller startup.

The Pod's restart loop is the user-visible symptom; this
scenario proves the log message on each failure path is enough
to diagnose. Failure paths covered:

- **Strict-decode failure.** An unknown key in
  `buckety-controller.yaml` (e.g. `seedBroker` singular).
- **Undefined `${VAR}`.** A field tagged `envsubst:"true"`
  references a variable with no default and no env value.
- **Unknown driver.** A backend lists `driver: badname`.
- **Duplicate backend names.** Two `backends:` entries share
  a name.
- **Missing required per-driver field.** A `kadm` backend
  omits `seedBrokers`.

Each variant is a single-file Secret the harness applies in
turn; the controller Pod CrashLoopBackOffs and the log line
on each variant is grepped for the expected error fragment.

**Assertions** (`assert.sh`):

For each variant in the broken-configs directory:

1. Apply the broken `buckety-controller-config` Secret.
2. Reroll the controller Pod.
3. Wait for the Pod to enter CrashLoopBackOff (or simply
   produce a terminating exit code).
4. Grep the most recent Pod's logs for the expected error
   fragment. Fail if the fragment is absent.
5. Restore the original good config before exiting.

The error-fragment expectations are in `expectations.txt`,
one line per variant: `<variant-filename> <regex>`.
