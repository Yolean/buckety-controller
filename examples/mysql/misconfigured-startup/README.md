# mysql misconfigured controller startup

SPEC scenario 10 for the mysql driver: each broken config variant
makes the controller exit non-zero with a log message that is
enough to diagnose the mistake. The assert swaps the controller's
config Secret, watches for the expected message in the crash
logs, and restores the original config afterwards.

Variants: strict-decode failure (typoed field), missing required
`host`, undefined `${VAR}` without a default, and an `adminUser`
inside `namePrefix` (the prefix boundary would not exclude the
controller's own account).
