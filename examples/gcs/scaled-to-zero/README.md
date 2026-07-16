# gcs operator scaled to zero

SPEC scenario 5 for the gcs driver: after the Secret is minted the
controller is scaled to zero, and a consumer Job still round-trips
an object using only the Secret. The operator is off the data
path.
