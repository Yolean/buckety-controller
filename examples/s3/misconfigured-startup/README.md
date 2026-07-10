# s3 / misconfigured-startup

**Scenario:** SPEC e2e coverage #10 for the s3 driver's config
surface: strict-decode typo, missing required field, undefined
`${VAR}`, and the s3-specific `implementation:` discriminator with
an unknown value. Each broken config must crash the controller
with a log line a maintainer can act on; the pod restart loop is
the user-visible symptom, the log message is the diagnostic.

Driver-neutral startup failures (duplicate backend names, unknown
driver) are covered once, in `examples/kadm/misconfigured-startup`.
