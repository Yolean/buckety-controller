# gcs multiple consumers, different roles

SPEC scenario 2 for the gcs driver: one `Buckety` plus explicit
`BucketyAccess` resources with different roles. In v0.1 every
Secret carries the backend's static HMAC pair; the controller
surfaces `ScopingNotImplemented=True` on non-ReadWrite roles so
the gap is honest. Also asserts `Buckety` deletion blocks on
referencing accesses (`BlockedByAccesses`) instead of cascading.
