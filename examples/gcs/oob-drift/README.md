# gcs out-of-band drift

SPEC scenario 4 for the gcs driver: the bucket is deleted directly
on the backend, and the controller recreates it on its periodic
re-check without changing the sticky `backendResourceName`.
