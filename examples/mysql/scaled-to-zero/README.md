# mysql operator scaled to zero

SPEC scenario 5 for the mysql driver: after the Secret is minted
the controller is scaled to zero, and a consumer Job still writes
and reads a row using only the Secret. The operator is off the
data path.
