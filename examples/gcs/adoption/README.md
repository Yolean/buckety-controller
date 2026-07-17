# adoption

SPEC scenario 11 ("Adoption"): a Buckety whose resolved name
collides with a pre-existing backend resource must not silently
claim it, and deleting an adopted Buckety must never destroy
content that predates the CR.

- `adopt-pre` collides with a pre-created resource holding
  content: the default policy (AdoptEmpty) surfaces
  `Ready=False/BackendResourceExists` and mints no Secret;
  `spec.adoption=Adopt` unblocks it with
  `status.provenance=Adopted`.
- `adopt-void` collides with a pre-created EMPTY resource and
  adopts silently under the default policy.
- `adopt-fresh` proves `provenance=Created` and that created
  resources still honour `retentionPolicy=Delete`.
- Deleting either adopted Buckety retains the backend resource
  and its content despite `retentionPolicy=Delete`
  (`RetainedOnDelete`); assert.sh cleans the retained resources
  up out-of-band at the end.
