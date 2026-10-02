# mysql / oob-drift

**Scenario:** SPEC.md §End-to-end coverage #4 - out-of-band
changes, for the `mysql` driver. Each change is made as root on the
server; the controller repairs it on a periodic re-check, or reports
it when repairing would hide something.

1. `DROP USER` of the ReadWrite user: recreated with the password
   in its Secret, so consumers need no restart.
2. `GRANT INSERT ... WITH GRANT OPTION` to the Reader: revoked,
   `GRANT OPTION` included.
3. `DROP DATABASE`: recreated, empty, and the user can use it.
4. `ALTER DATABASE ... CHARACTER SET latin1`: `ParameterDrift`
   with `Ready=False`; the driver does not convert the database.
   Restoring the character set and annotating the `Buckety` (drift
   pauses re-checks until the resource changes) clears it.
5. Deleting the access Secret rotates the password: a new one is
   generated, set on the server and written to a new Secret.
6. A password written into the Secret is applied to the server.
