# kadm / secret-conflict

**Scenario:** A BucketyAccess whose `credentialsSecretName` names a
pre-existing Secret the controller does not own MUST NOT adopt or
overwrite it. Adoption would clobber the user's data and, worse,
garbage-collect the Secret when the access is deleted.

The access surfaces `Ready=False reason=SecretConflict` (with a
Warning Event) while the conflicting Secret's data stays intact.
Deleting the conflicting Secret resolves the conflict on the next
periodic re-check and the access mints its Secret normally.
